"""Connection-local request lifecycle used by the polling service."""
import asyncio
from dataclasses import dataclass
from protocol import read_adu, request, response_value

@dataclass
class Ticket:
    token: int
    tid: int
    address: int
    deadline: int
    future: object

class Client:
    def __init__(self, reader, writer, connection, clock, trace):
        self.reader, self.writer = reader, writer
        self.connection, self.clock, self.trace = connection, clock, trace
        self.cursor = 0
        self.serial = 0
        self.pending = {}
        self.received = asyncio.Queue()
        self.wire_seen = asyncio.Queue()
        self.dispatch_gate = asyncio.Event()
        self.dispatch_gate.set()
        self.closed = False
        self.task = asyncio.create_task(self._receive())

    @classmethod
    async def connect(cls, host, port, connection, clock, trace):
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer, connection, clock, trace)

    def event(self, kind, **fields):
        self.trace.append(dict(at=self.clock(), side="client",
                               connection=self.connection, kind=kind, **fields))

    def pause_dispatch(self):
        self.dispatch_gate.clear()

    def resume_dispatch(self):
        self.dispatch_gate.set()

    def set_cursor(self, value):
        if not 0 <= value <= 65535:
            raise ValueError("cursor")
        self.cursor = value

    async def issue(self, address, timeout=5):
        if self.closed:
            raise ConnectionError("closed")
        for _ in range(65536):
            tid = self.cursor
            self.cursor = (self.cursor + 1) & 65535
            if tid not in self.pending:
                break
        else:
            raise RuntimeError("transaction IDs exhausted")
        ticket = Ticket(self.serial, tid, address, self.clock() + timeout,
                        asyncio.get_running_loop().create_future())
        self.serial += 1
        self.pending[tid] = ticket
        self.writer.write(request(tid, address))
        await self.writer.drain()
        self.event("sent", token=ticket.token, tid=tid, address=address,
                   deadline=ticket.deadline)
        return ticket

    def expire(self):
        for tid, ticket in list(self.pending.items()):
            if ticket.deadline <= self.clock():
                self.pending.pop(tid)
                ticket.future.set_exception(TimeoutError("read deadline"))
                self.event("timeout", token=ticket.token, tid=tid)

    def _fail(self, message):
        for ticket in self.pending.values():
            if not ticket.future.done():
                ticket.future.set_exception(ConnectionError(message))
        self.pending.clear()

    async def _receive(self):
        try:
            while True:
                tid, pdu = await read_adu(self.reader)
                self.event("wire_seen", tid=tid)
                self.wire_seen.put_nowait(tid)
                await self.dispatch_gate.wait()
                ticket = self.pending.pop(tid, None)
                if ticket is None:
                    self.event("unclaimed", tid=tid)
                else:
                    try:
                        value = response_value(pdu)
                    except ValueError as exc:
                        ticket.future.set_exception(exc)
                        self.event("read_error", token=ticket.token, tid=tid,
                                   error=str(exc))
                    else:
                        ticket.future.set_result(value)
                        self.event("answer", token=ticket.token, tid=tid, value=value)
                self.received.put_nowait(tid)
        except (asyncio.IncompleteReadError, OSError, ValueError) as exc:
            self.closed = True
            self._fail(type(exc).__name__)
            self.event("disconnected", error=type(exc).__name__)
            self.writer.close()

    async def close(self):
        self.closed = True
        self._fail("connection retired")
        self.writer.close()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
