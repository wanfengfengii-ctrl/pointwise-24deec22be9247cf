"""Controlled device. The in-memory link and TCP listener use the same ADUs."""
import asyncio
from protocol import read_adu, request_address, response

class MemoryWriter:
    def __init__(self, peer, local):
        self.peer, self.local, self.closed = peer, local, False

    def write(self, data):
        if self.closed:
            raise ConnectionError("closed stream")
        # Fragment headers and payloads; consumers cannot rely on write boundaries.
        for start in range(0, len(data), 3):
            self.peer.feed_data(data[start:start + 3])

    async def drain(self):
        await asyncio.sleep(0)

    def close(self):
        if not self.closed:
            self.closed = True
            self.peer.feed_eof()
            self.local.feed_eof()

    def is_closing(self):
        return self.closed

class Device:
    LATE_BOUND = 50

    def __init__(self, clock, trace):
        self.clock, self.trace = clock, trace
        self.values = {0: 0, 1: 0}
        self.sessions = {}
        self.requests = []
        self.arrivals = asyncio.Queue()
        self.tasks = []
        self.next_connection = 0

    def accept(self, reader, writer):
        connection = self.next_connection
        self.next_connection += 1
        self.sessions[connection] = writer
        self.tasks.append(asyncio.create_task(self._serve(connection, reader, writer)))
        return connection

    def memory_link(self):
        client_reader, device_reader = asyncio.StreamReader(), asyncio.StreamReader()
        client_writer = MemoryWriter(device_reader, client_reader)
        device_writer = MemoryWriter(client_reader, device_reader)
        connection = self.accept(device_reader, device_writer)
        return client_reader, client_writer, connection

    async def _serve(self, connection, reader, writer):
        try:
            while True:
                tid, pdu = await read_adu(reader)
                address = request_address(pdu)
                row = dict(seq=len(self.requests), connection=connection,
                           tid=tid, address=address, value=self.values[address],
                           at=self.clock(), released=False)
                self.requests.append(row)
                self.trace.append(dict(side="device", kind="processed", **row))
                self.arrivals.put_nowait(row["seq"])
        except (asyncio.IncompleteReadError, OSError, ValueError):
            writer.close()

    async def release(self, seq, error=None):
        row = self.requests[seq]
        if row["released"]:
            raise ValueError("response already released")
        row["released"] = True
        writer = self.sessions[row["connection"]]
        delivered = not writer.is_closing()
        if delivered and self.clock() - row["at"] > self.LATE_BOUND:
            raise ValueError("fixture exceeds response bound")
        self.trace.append(dict(at=self.clock(), side="device", kind="released",
                               seq=seq, connection=row["connection"],
                               tid=row["tid"], delivered=delivered, error=error))
        if delivered:
            writer.write(response(row["tid"], row["value"], error))
            await writer.drain()
        return delivered

    async def disconnect(self, connection):
        self.sessions[connection].close()
        await asyncio.sleep(0)

    async def close(self):
        for writer in self.sessions.values():
            writer.close()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
