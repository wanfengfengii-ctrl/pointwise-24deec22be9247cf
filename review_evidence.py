"""Independent expectations and replay evidence for the polling review.

`expected(case)` is a synchronous pure function.  It reads only the scenario
actions, the documented device value rule (a request observes the device value
at the tick the device processes it) and the documented publication rules.  It
does not import or call Client, Sampler or replay, and it never reads replay
results or the saved evidence.json.

Run without arguments to regenerate evidence.json.  Run with `demo` to show the
real normal and abnormal batches with the request attribution reconstructed
from the actual byte stream.
"""
import json
import sys
from pathlib import Path

CASES = "review_cases.json"
EVIDENCE = "evidence.json"


def expected(case):
    """Return the contract-correct seal records, in `wait` order."""
    now = 0
    device_values = [0, 0]
    next_seq = 0
    connection = 0
    open_connections = {0}
    rounds = {}
    waited = []

    for action in case["actions"]:
        op = action["op"]
        if op == "values":
            device_values = list(action["values"])
        elif op == "tick":
            now = action["at"]
        elif op == "start":
            slots = []
            for address in (0, 1):
                slots.append(dict(address=address, deadline=now + 5,
                                  state="pending", value=None, origin=None,
                                  seq=next_seq,
                                  observed=device_values[address]))
                next_seq += 1
            rounds[action["round"]] = dict(connection=connection, slots=slots)
        elif op == "expire":
            for round_ in rounds.values():
                for slot in round_["slots"]:
                    if (slot["state"] == "pending" and
                            slot["deadline"] <= now):
                        slot["state"] = "failed"
        elif op == "reply":
            round_ = rounds[action["round"]]
            slot = round_["slots"][action["address"]]
            # A response from a closed connection never reaches a reader, and
            # a response can only complete the attempt that was still pending.
            if (round_["connection"] in open_connections and
                    slot["state"] == "pending"):
                if action.get("error") is not None:
                    slot["state"] = "failed"
                else:
                    slot["state"] = "done"
                    slot["value"] = slot["observed"]
                    slot["origin"] = slot["seq"]
        elif op in ("disconnect", "reconnect"):
            for round_ in rounds.values():
                if round_["connection"] == connection:
                    for slot in round_["slots"]:
                        if slot["state"] == "pending":
                            slot["state"] = "failed"
            open_connections.discard(connection)
            if op == "reconnect":
                connection += 1
                open_connections.add(connection)
        elif op == "wait":
            waited.append(action["round"])
        elif op in ("pause", "resume"):
            # Dispatch timing cannot change which attempt a value belongs to.
            pass
        else:
            raise ValueError("unsupported action in expected(): " + op)

    result = []
    for round_id in waited:
        values, origins = {}, {}
        for slot in rounds[round_id]["slots"]:
            if slot["state"] == "done":
                address = str(slot["address"])
                values[address] = slot["value"]
                origins[address] = slot["origin"]
        result.append(dict(round=round_id,
                           status="success" if len(values) == 2 else "failed",
                           values=values, origins=origins))
    return result


def _actual_view(actual):
    """Reconstruct real origins from the saved replay and compare-able view."""
    from evidence_gate import observed_origins
    completions = observed_origins(actual)
    view = {}
    for row in actual["rounds"]:
        origins = {}
        for address, source in row["sources"].items():
            origins[address] = completions[source["connection"], source["token"]]
        view[row["round"]] = dict(status=row["status"], values=row["values"],
                                  origins=origins)
    return view


async def _build():
    from lab import replay, load_cases
    cases = load_cases(CASES)
    rows = []
    for case in cases:
        actual = await replay(case)
        wanted = expected(case)
        observed = _actual_view(actual)
        differences = {}
        for item in wanted:
            now = observed[item["round"]]
            differences[item["round"]] = bool(
                item["status"] != now["status"] or
                item["values"] != now["values"] or
                item["origins"] != now["origins"])
        rows.append(dict(name=case["name"], expected=wanted,
                         actual=actual, differences=differences))
    report = {
        "summary": (
            "Replays the production entry points; `expected` is independent. "
            "differences[round]=true means the implementation's status, values "
            "or real device origin diverges from the contract."),
        "key_findings": [
            "late_reuse: an expired 16-bit transaction id is reused while the "
            "old response is still arriving; the old value is delivered to the "
            "new attempt (round `new` address 0 reports 11, real origin seq 0).",
            "paused_arrival: the old frame arrived before the 50-tick bound but "
            "was only dispatched after reuse; dispatch timing does not create "
            "identity (round `new` address 0 reports 11, real origin seq 0).",
            "partial_failure_cache / total_failure_cache: Sampler seeds each "
            "round from last-known cache, so missing or failed slots become "
            "`success` with values from earlier rounds and stale origins.",
            "cold_failure: on the first round the cache is empty, so the same "
            "seeding bug is masked and the round correctly seals `failed`; the "
            "cache is the necessary condition, not the failure itself.",
            "late_no_reuse: identical gate and expiry but no id collision, so "
            "the round succeeds correctly; it isolates transaction-id reuse "
            "as the cause of the misattribution.",
            "retire_pending_round: retiring a connection fails the pending "
            "round and the next round on the new connection is correct; this "
            "is the transport behaviour the retirement proposal must preserve.",
            "retire_after_success: when a prior success populated the cache, "
            "the same retirement failure is sealed `success` with the old "
            "values; transport-side failure does not reach the publication "
            "judgement while cache seeding exists.",
            "normal_out_of_order, other_valid_read and "
            "new_connection_isolation: this run's attribution is correct when "
            "transaction ids do not collide and after a new connection; these "
            "show the baseline and the masking condition, not a fix.",
        ],
    }
    return dict(cases=rows, report=report)


def _write_evidence(payload):
    Path(EVIDENCE).write_text(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8")


def demo():
    """Show real normal and abnormal batches with their request attribution."""
    import asyncio
    from lab import replay, load_cases
    cases = {case["name"]: case for case in load_cases(CASES)}
    chosen = ["normal_out_of_order", "other_valid_read", "late_reuse",
              "late_no_reuse", "paused_arrival", "partial_failure_cache",
              "cold_failure", "total_failure_cache", "retire_after_success"]
    for name in chosen:
        case = cases[name]
        actual = asyncio.run(replay(case))
        observed = _actual_view(actual)
        print("=" * 72)
        print("case:", name)
        print("submitted (round/address -> device seq, tid, token):")
        for round_id, slots in actual["submitted"].items():
            for slot in slots:
                print("  %-22s address %d -> seq %d conn %d tid %d token %d"
                      % (round_id, slot["address"], slot["seq"],
                         slot["connection"], slot["tid"], slot["token"]))
        print("device processing order (seq, conn, tid, address, value):")
        for row in actual["device"]:
            print("  seq %d conn %d tid %d address %d value %d"
                  % (row["seq"], row["connection"], row["tid"],
                     row["address"], row["value"]))
        print("published:", json.dumps(actual["publications"], sort_keys=True))
        print("actual seals with real origins:",
              json.dumps(observed, sort_keys=True))
        print("independent expected:",
              json.dumps({item["round"]: item for item in expected(case)},
                         sort_keys=True))


def main(argv):
    if argv == ["demo"]:
        demo()
        return
    if argv:
        raise SystemExit("usage: review_evidence.py [demo]")
    import asyncio
    payload = asyncio.run(_build())
    _write_evidence(payload)
    print("wrote", EVIDENCE, "cases:", len(payload["cases"]))


if __name__ == "__main__":
    main(sys.argv[1:])
