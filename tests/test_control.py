"""Pause and isolate nodes through Control messages (design plan section 5)."""

import asyncio
import subprocess
import sys

import pytest
from cluster import LocalCluster


@pytest.fixture
async def cluster():
    c = LocalCluster()
    c.start()
    yield c
    c.stop()


async def test_paused_leader_is_replaced_and_steps_down_on_resume(cluster):
    leaders = await cluster.wait_for_leaders()
    old = leaders[0]
    reply = await cluster.control(old, "pause")
    assert reply["paused"]
    new = await cluster.wait_for_leaders(avoid=old)
    assert (await cluster.put(0, "CSM13001"))["ok"]
    await cluster.control(old, "resume")
    await asyncio.sleep(0.5)
    status = (await cluster.status())[old]
    assert status["shards"][0]["role"] == "follower"
    assert status["shards"][0]["leader"] == new[0]


async def test_isolated_minority_cannot_commit(cluster):
    leaders = await cluster.wait_for_leaders()
    lonely = leaders[1]
    others = [n for n in cluster.ports if n != lonely]
    await cluster.control(lonely, "isolate")
    for n in others:
        await cluster.control(n, "isolate", [lonely])
    new = await cluster.wait_for_leaders(avoid=lonely)
    # The majority side goes on.
    msg = {"type": "Put", "shard": 1, "s": "CSM13001", "p": "p", "o": "o"}
    assert (await cluster.request(new[1], msg))["ok"]
    # The old leader on its own does not get its write committed.
    reply = await cluster.request(lonely, msg)
    assert not reply.get("ok")


async def test_healed_node_catches_up(cluster):
    leaders = await cluster.wait_for_leaders()
    follower = next(n for n in cluster.ports if n != leaders[2])
    await cluster.control(follower, "isolate")
    for i in range(5):
        assert (await cluster.put(2, f"room{i}"))["ok"]
    await cluster.control(follower, "heal")
    await asyncio.sleep(1.0)
    status = await cluster.status()
    assert (
        status[follower]["shards"][2]["commit_index"]
        == (status[leaders[2]]["shards"][2]["commit_index"])
    )


async def test_command_line_control(cluster):
    leaders = await cluster.wait_for_leaders()
    address = f"127.0.0.1:{cluster.ports[leaders[0]]}"
    cmd = [sys.executable, "-m", "knowdes.debug", "control", address, "isolate", "n9"]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE)
    out, _ = await proc.communicate()
    assert proc.returncode == 0
    assert b'"isolated": ["n9"]' in out
