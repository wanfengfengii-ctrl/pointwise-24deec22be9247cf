"""Two-register batch publication and last-known-value cache."""
import asyncio

class Sampler:
    def __init__(self, trace, clock):
        self.trace, self.clock = trace, clock
        self.cache = {}
        self.rounds = {}
        self.publications = []

    async def start(self, round_id, client):
        if round_id in self.rounds:
            raise ValueError("duplicate round")
        if sum(not item["task"].done() for item in self.rounds.values()) >= 2:
            raise ValueError("too many overlapping rounds")
        tickets = [await client.issue(address) for address in (0, 1)]
        task = asyncio.create_task(self._finish(round_id, client, tickets))
        self.rounds[round_id] = dict(tickets=tickets, client=client, task=task)
        return tickets

    async def _finish(self, round_id, client, tickets):
        values = dict(self.cache)
        outcomes = await asyncio.gather(*(t.future for t in tickets),
                                        return_exceptions=True)
        errors = []
        for ticket, outcome in zip(tickets, outcomes):
            if isinstance(outcome, BaseException):
                errors.append(dict(address=ticket.address,
                                   error=type(outcome).__name__))
            else:
                values[ticket.address] = dict(value=outcome,
                    connection=client.connection, token=ticket.token)
        status = "success" if len(values) == 2 else "failed"
        record = dict(round=round_id, status=status,
                      values={str(k): v["value"] for k, v in values.items()},
                      sources={str(k): dict(connection=v["connection"],
                                           token=v["token"])
                               for k, v in values.items()}, errors=errors)
        if status == "success":
            self.cache = values
            self.publications.append(record)
        self.trace.append(dict(at=self.clock(), side="sampler",
                               kind="sealed", **record))
        return record

    async def wait(self, round_id):
        return await self.rounds[round_id]["task"]
