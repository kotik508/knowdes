"""Leader transfer with TimeoutNow (Raft thesis section 3.10, issue #14)."""

import asyncio

import pytest
from cluster import LocalCluster
from simnet import Network

from knowdes.raft import NotLeaderError


async def test_leadership_moves_to_the_chosen_replica():
    net = Network()
    net.start()
    try:
        old = await net.wait_for_leader()
        for i in range(5):
            await old.propose({"op": "put", "s": f"s{i}", "p": "p", "o": "o"})
        target = old.peers[0]
        assert await old.transfer(target)
        new = await net.wait_for_leader(newer_than=old.term - 1)
        assert new.id == target
        assert new.term == old.term
        # The handover loses nothing that was committed.
        assert new.commit_index >= 6
        assert [e["command"] for e in new.log[2:7]] == [e["command"] for e in old.log[2:7]]
        await new.propose({"op": "put", "s": "after", "p": "p", "o": "o"})
    finally:
        await net.stop()


async def test_transfer_to_a_dead_replica_keeps_the_leader():
    net = Network()
    net.start()
    try:
        old = await net.wait_for_leader()
        target = old.peers[0]
        net.down.add(target)
        assert not await old.transfer(target)
        assert net.leaders() == [old]
        assert old.transferring is None
        await old.propose({"op": "put", "s": "still", "p": "p", "o": "o"})
    finally:
        await net.stop()


async def test_transfer_needs_the_leader_and_a_replica():
    net = Network()
    net.start()
    try:
        leader = await net.wait_for_leader()
        follower = net.groups[leader.peers[0]]
        with pytest.raises(NotLeaderError):
            await follower.transfer(leader.id)
        with pytest.raises(ValueError):
            await leader.transfer("z")
    finally:
        await net.stop()


async def test_transfer_over_tcp(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        old = leaders[1]
        target = next(n for n in cluster.ports if n != old)
        reply = await cluster.request(
            old, {"type": "Control", "action": "transfer", "shard": 1, "target": target}, 3.0
        )
        assert reply["ok"], reply
        for _ in range(40):
            if (await cluster.leaders())[1] == target:
                break
            await asyncio.sleep(0.05)
        assert (await cluster.leaders())[1] == target
        assert (await cluster.put(1, "after transfer"))["ok"]
        other = await cluster.request(
            target, {"type": "Control", "action": "transfer", "shard": 1, "target": "n9"}
        )
        assert other["type"] == "Error"
    finally:
        cluster.stop()
