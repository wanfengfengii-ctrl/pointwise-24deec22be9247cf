"""Replay submitted evidence; the initial runtime uses checks.py instead."""
import asyncio
from collections import defaultdict, deque
import importlib
import json
from pathlib import Path
from lab import replay, load_cases
from checks import normal_check

def observed_origins(actual):
    sent, parsing, completed = defaultdict(deque), {}, {}
    for event in actual["trace"]:
        connection = event.get("connection")
        if event["side"] == "device" and event["kind"] == "released":
            if event["delivered"]:
                sent[connection].append(event["seq"])
        elif event["side"] == "client":
            if event["kind"] == "wire_seen":
                seq = sent[connection].popleft()
                assert actual["device"][seq]["tid"] == event["tid"]
                parsing[connection] = seq
            elif event["kind"] == "answer":
                completed[connection, event["token"]] = parsing.pop(connection)
            elif event["kind"] in ("unclaimed", "read_error"):
                parsing.pop(connection)
    return completed

async def verify():
    await normal_check(load_cases("samples/polls.json"))
    evidence = importlib.import_module("review_evidence")
    cases = load_cases("review_cases.json")
    report = json.loads(Path("evidence.json").read_text(encoding="utf-8"))
    if not cases or len({case["name"] for case in cases}) != len(cases):
        raise AssertionError("empty or duplicate case names")
    rows = report["cases"]
    assert [row["name"] for row in rows] == [case["name"] for case in cases]
    found_difference = False
    for case, row in zip(cases, rows):
        actual = await replay(case)
        assert row["actual"] == actual, "actual replay changed: " + case["name"]
        expected = evidence.expected(case)
        assert row["expected"] == expected, "expected changed: " + case["name"]
        actual_rounds = {r["round"]: r for r in actual["rounds"]}
        completions = observed_origins(actual)
        assert {r["round"] for r in expected} == set(actual_rounds)
        for wanted in expected:
            assert wanted["status"] in ("success", "failed")
            assert set(wanted) == {"round", "status", "values", "origins"}
            observed = actual_rounds[wanted["round"]]
            actual_origins = {}
            for address, source in observed["sources"].items():
                actual_origins[address] = completions[
                    source["connection"], source["token"]]
            differs = (wanted["status"] != observed["status"] or
                       wanted["values"] != observed["values"] or
                       wanted["origins"] != actual_origins)
            assert type(row["differences"][wanted["round"]]) is bool
            assert row["differences"][wanted["round"]] == differs
            found_difference |= differs
        print(case["name"], json.dumps(actual["rounds"], sort_keys=True))
    assert found_difference, "report contains no observed contract discrepancy"
    assert Path("REVIEW.md").read_text(encoding="utf-8").strip()
    assert Path("docs/polling.md").read_text(encoding="utf-8").strip()
    print("REVIEW_REPLAY_OK")

if __name__ == "__main__":
    asyncio.run(verify())
