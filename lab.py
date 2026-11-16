"""Deterministic replay through production entry points; no oracle in this module."""
import asyncio
import json
import sys
from device import Device
from sampler import Sampler
from transport import Client

async def bounded(awaitable):
    return await asyncio.wait_for(awaitable, 2)

async def replay(case, backend="memory"):
    now = [0]
    clock = lambda: now[0]
    trace = []
    device = Device(clock, trace)
    sampler = Sampler(trace, clock)
    clients, submitted, sealed = [], {}, []
    listener = None
    if backend == "tcp":
        listener = await asyncio.start_server(device.accept, "127.0.0.1", 0)

    async def connect():
        if listener is None:
            reader, writer, connection = device.memory_link()
            client = Client(reader, writer, connection, clock, trace)
        else:
            port = listener.sockets[0].getsockname()[1]
            client = await Client.connect("127.0.0.1", port, len(clients), clock, trace)
        clients.append(client)
        return client

    client = await connect()
    try:
        for action in case["actions"]:
            op = action["op"]
            if op == "values":
                device.values = dict(enumerate(action["values"]))
            elif op == "tick":
                if action["at"] < now[0]:
                    raise ValueError("time moved backwards")
                now[0] = action["at"]
            elif op == "start":
                if "cursor" in action:
                    client.set_cursor(action["cursor"])
                tickets = await sampler.start(action["round"], client)
                slots = []
                for ticket in tickets:
                    seq = await bounded(device.arrivals.get())
                    request = device.requests[seq]
                    if (request["connection"], request["tid"], request["address"]) != (
                            client.connection, ticket.tid, ticket.address):
                        raise AssertionError("replay submission barrier")
                    slots.append(dict(seq=seq, token=ticket.token,
                                      connection=client.connection,
                                      address=ticket.address, tid=ticket.tid))
                submitted[action["round"]] = slots
            elif op == "expire":
                client.expire()
            elif op == "reply":
                slot = submitted[action["round"]][action["address"]]
                source_client = clients[slot["connection"]]
                delivered = await device.release(slot["seq"], action.get("error"))
                if delivered:
                    await bounded(source_client.wire_seen.get())
                    if source_client.dispatch_gate.is_set():
                        await bounded(source_client.received.get())
            elif op == "pause":
                client.pause_dispatch()
            elif op == "resume":
                client.resume_dispatch()
                for _ in range(action.get("drain", 1)):
                    await bounded(client.received.get())
            elif op == "disconnect":
                await device.disconnect(client.connection)
                await bounded(asyncio.shield(client.task))
            elif op == "reconnect":
                await client.close()
                await device.disconnect(client.connection)
                client = await connect()
            elif op == "wait":
                sealed.append(await bounded(sampler.wait(action["round"])))
            else:
                raise ValueError("unknown action " + op)
        if any(not row["released"] and
               not device.sessions[row["connection"]].is_closing()
               for row in device.requests):
            raise ValueError("open connection has unreleased fixture responses")
        if any(not item["task"].done() for item in sampler.rounds.values()):
            raise ValueError("fixture left a round open")
        return dict(name=case["name"], submitted=submitted,
                    device=[dict(row) for row in device.requests],
                    rounds=sealed, publications=list(sampler.publications), trace=trace)
    finally:
        for current in clients:
            await current.close()
        await device.close()
        await asyncio.gather(*(item["task"] for item in sampler.rounds.values()),
                             return_exceptions=True)
        if listener is not None:
            listener.close()
            await listener.wait_closed()

def load_cases(path):
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)

def main():
    if len(sys.argv) not in (3, 4):
        raise SystemExit("usage: lab.py CASES.json NAME [memory|tcp]")
    cases = load_cases(sys.argv[1])
    case = next(item for item in cases if item["name"] == sys.argv[2])
    result = asyncio.run(replay(case, sys.argv[3] if len(sys.argv) == 4 else "memory"))
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2))

if __name__ == "__main__":
    main()
