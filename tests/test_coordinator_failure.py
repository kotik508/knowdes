"""Recovery when the 2PC coordinator fails (#12, design plan section 3.3)."""

import asyncio

from cluster import LocalCluster
from test_two_phase import two_subjects

from knowdes import protocol
from knowdes.client import Client
from knowdes.sharding import shard_of


async def setup(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    await cluster.wait_for_leaders()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    return cluster, nodes, Client(nodes, cluster.shards)


async def prepare(cluster, shard, tx, facts, coordinator):
    """A Prepare as the coordinator sends it, then the coordinator is gone."""
    leaders = await cluster.leaders()
    msg = {"type": "Prepare", "shard": shard, "tx": tx, "facts": facts, "coordinator": coordinator}
    reply = await cluster.request(leaders[shard], msg)
    assert reply["vote"] is True, reply


async def wait_until(check, timeout=10.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await check():
            return
        await asyncio.sleep(0.2)
    raise AssertionError("condition not reached in time")


async def test_participant_aborts_when_the_coordinator_never_decided(tmp_path):
    cluster, _, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        shard_a, shard_b = shard_of(a, 3), shard_of(b, 3)
        await prepare(
            cluster, shard_b, "lost", [{"op": "put", "s": b, "p": "p", "o": "x"}], shard_a
        )
        # The leader of the coordinator shard crashes before it decides.
        leaders = await cluster.leaders()
        cluster.kill(leaders[shard_a])
        await cluster.wait_for_leaders(avoid=leaders[shard_a], timeout=10.0)

        # b asks the new leader of the coordinator shard, which decides abort.
        async def free():
            facts = [{"op": "put", "s": b, "p": "p", "o": "y"}]
            # A new transaction each time, the first decision of one is final.
            reply = await client.transact(facts)
            return reply["decision"] == "commit"

        await wait_until(free)
        assert await client.get(b) == {"p": ["y"]}
        # The abort is final, a late coordinator with the same transaction gets it too.
        facts = [{"op": "put", "s": a, "p": "p", "o": "x"}]
        again = await client.transact(facts, tx="lost")
        assert again["decision"] == "abort"
        assert await client.get(a) == {}
    finally:
        cluster.stop()


async def test_participant_commits_when_the_decision_got_lost(tmp_path):
    cluster, nodes, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        shard_a, shard_b = shard_of(a, 3), shard_of(b, 3)
        await prepare(cluster, shard_b, "t", [{"op": "rput", "s": a, "p": "room", "o": b}], shard_a)
        # The coordinator decides commit and writes its own shard, then fails
        # before it sends Commit to b. The decision stays in the log of shard a.
        leaders = await cluster.leaders()
        tx = {
            "type": "TxWrite",
            "shard": shard_a,
            "tx": "t",
            "writes": [{"shard": shard_a, "facts": [{"op": "put", "s": a, "p": "room", "o": b}]}],
        }
        reply = await protocol.request(nodes[leaders[shard_a]], tx, 5.0)
        assert reply["decision"] == "commit"
        assert (await client.read(b))["incoming"] == {}
        cluster.kill(leaders[shard_a])

        async def committed():
            return (await client.read(b))["incoming"] == {"room": [a]}

        await wait_until(committed)
        assert await client.get(a) == {"room": [b]}
    finally:
        cluster.stop()


async def test_prepare_without_coordinator_waits(tmp_path):
    cluster, _, client = await setup(tmp_path)
    try:
        _, b = two_subjects()
        shard_b = shard_of(b, 3)
        await prepare(cluster, shard_b, "manual", [{"op": "put", "s": b, "p": "p", "o": "x"}], None)
        await asyncio.sleep(4.0)
        facts = [{"op": "put", "s": b, "p": "p", "o": "y"}]
        assert (await client.transact(facts))["decision"] == "abort"
    finally:
        cluster.stop()
