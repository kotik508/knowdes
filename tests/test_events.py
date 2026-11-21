import asyncio
import json

from cluster import LocalCluster

from knowdes import protocol
from knowdes.debug import EventLog


def test_event_log_writes_file_and_filters_shards(tmp_path):
    log = EventLog("n1", tmp_path / "events.log")
    only_shard_1 = log.subscribe([1])
    everything = log.subscribe()
    log.emit("leader", 0, term=3)
    log.emit("commit", 1, term=3, index=7)
    log.close()
    assert everything.qsize() == 2
    assert only_shard_1.qsize() == 1
    assert only_shard_1.get_nowait()["kind"] == "commit"
    lines = [json.loads(line) for line in (tmp_path / "events.log").read_text().splitlines()]
    assert [e["kind"] for e in lines] == ["leader", "commit"]
    assert lines[1]["index"] == 7


def test_slow_subscriber_loses_oldest_events(monkeypatch):
    monkeypatch.setattr("knowdes.debug.QUEUE_SIZE", 2)
    log = EventLog("n1")
    queue = log.subscribe()
    for i in range(5):
        log.emit("commit", 0, index=i)
    assert [queue.get_nowait()["index"] for _ in range(2)] == [3, 4]


async def read_events(reader, kinds, timeout=5.0):
    """Read events until one of the kinds was seen."""
    seen = []
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not set(kinds) & {e["kind"] for e in seen}:
        line = await asyncio.wait_for(reader.readline(), end - loop.time())
        seen.append(protocol.decode(line))
    return seen


async def test_subscriber_sees_election_after_leader_dies():
    cluster = LocalCluster()
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        watcher = next(n for n in cluster.ports if n != leaders[0])
        reader, writer = await asyncio.open_connection("127.0.0.1", cluster.ports[watcher])
        writer.write(protocol.encode({"type": "Subscribe", "shards": [0]}))
        await writer.drain()
        assert protocol.decode(await reader.readline())["type"] == "Subscribed"
        cluster.kill(leaders[0])
        # The watcher either starts the election itself or votes in it.
        events = await read_events(reader, {"election", "vote"})
        assert all(e["shard"] == 0 and e["node"] == watcher for e in events)
        writer.close()
    finally:
        cluster.stop()


async def test_subscriber_sees_commit_of_a_write():
    cluster = LocalCluster()
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        leader = leaders[2]
        reader, writer = await asyncio.open_connection("127.0.0.1", cluster.ports[leader])
        writer.write(protocol.encode({"type": "Subscribe"}))
        await writer.drain()
        await reader.readline()
        assert (await cluster.put(2, "CSM13001"))["ok"]
        events = await read_events(reader, {"commit"})
        commit = next(e for e in events if e["kind"] == "commit")
        assert commit["shard"] == 2
        assert commit["replicas"] >= 2
        writer.close()
    finally:
        cluster.stop()
