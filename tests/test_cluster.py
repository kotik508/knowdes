"""Election and writes with three node processes over TCP (issue #2)."""

import asyncio
import time

import pytest
from cluster import LocalCluster


@pytest.fixture
async def cluster():
    c = LocalCluster()
    c.start()
    yield c
    c.stop()


async def test_every_shard_elects_one_leader(cluster):
    leaders = await cluster.wait_for_leaders()
    await asyncio.sleep(0.5)
    assert await cluster.leaders() == leaders
    status = await cluster.status()
    for shard, leader in leaders.items():
        terms = {st["shards"][shard]["term"] for st in status.values()}
        assert len(terms) == 1
        assert all(st["shards"][shard]["leader"] == leader for st in status.values())


async def test_new_leader_after_leader_is_killed(cluster):
    leaders = await cluster.wait_for_leaders()
    victim = leaders[0]
    cluster.kill(victim)
    start = time.monotonic()
    new = await cluster.wait_for_leaders(avoid=victim)
    assert time.monotonic() - start < 1.5
    assert victim not in new.values()
    reply = await cluster.put(0, "CSM13001")
    assert reply["ok"]


async def test_killed_node_rejoins_as_follower(cluster):
    leaders = await cluster.wait_for_leaders()
    victim = leaders[0]
    cluster.kill(victim)
    await cluster.wait_for_leaders(avoid=victim)
    cluster.start(victim)
    await asyncio.sleep(1.0)
    status = await cluster.status()
    assert victim in status
    after = await cluster.leaders()
    assert all(v is not None for v in after.values())
