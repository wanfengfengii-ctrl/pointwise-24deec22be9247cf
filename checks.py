import asyncio
import json
import sys
from lab import replay, load_cases

def summary(result):
    return {row["round"]: (row["status"], row["values"])
            for row in result["rounds"]}

async def normal_check(cases):
    result = await replay(cases[0])
    assert summary(result) == {
        "a": ("success", {"0": 10, "1": 100}),
        "b": ("success", {"0": 20, "1": 200})}
    assert [r["tid"] for r in result["device"]] == [65535, 0, 1, 2]
    error_case = dict(name="device-error", actions=[
        dict(op="values", values=[3, 30]), dict(op="start", round="x"),
        dict(op="reply", round="x", address=0, error=2),
        dict(op="reply", round="x", address=1), dict(op="wait", round="x")])
    error = await replay(error_case)
    assert summary(error) == {"x": ("failed", {"1": 30})}
    assert not error["publications"]
    reconnect_case = dict(name="fresh-connection", actions=[
        dict(op="reconnect"), dict(op="values", values=[4, 40]),
        dict(op="start", round="r", cursor=65535),
        dict(op="reply", round="r", address=1),
        dict(op="reply", round="r", address=0), dict(op="wait", round="r")])
    fresh = await replay(reconnect_case)
    assert summary(fresh) == {"r": ("success", {"0": 4, "1": 40})}
    assert fresh["device"][0]["connection"] == 1
    print("NORMAL_POLL_OK", json.dumps(summary(result), sort_keys=True))

async def incident_check(cases):
    result = await replay(cases[1])
    print(json.dumps(result, sort_keys=True))
    actual = next(row for row in result["rounds"] if row["round"] == "new")
    assert actual["values"] == {"0": 22, "1": 220}, (
        "POLL_ORIGIN_MISMATCH expected new={0:22,1:220}, got " +
        repr(actual["values"]))

def main():
    cases = load_cases("samples/polls.json")
    if sys.argv[1:] == ["normal"]:
        asyncio.run(normal_check(cases))
    elif sys.argv[1:] == ["incident"]:
        asyncio.run(incident_check(cases))
    else:
        raise SystemExit("usage: checks.py normal|incident")

if __name__ == "__main__":
    main()
