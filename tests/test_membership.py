"""Adding and removing replicas while the cluster runs (#14, Raft thesis section 4.1)."""

import asyncio

from cluster import LocalCluster
from simnet import Network

from knowdes.client import Client
from knowdes.placement import add_replica, remove_replica
from knowdes.raft import RaftGroup


async def until(check, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not reached")


def test_members_follow_the_newest_config_entry():
    group = RaftGroup(0, "a", ["a", "b", "c"], send=None)
    assert group.members == ["a", "b", "c"] and group.majority == 2
    config = {"op": "config", "members": ["a", "b", "c", "d"], "addresses": {}}
    group._append_entries(1, [{"term": 1, "command": config}])
    assert group.members == ["a", "b", "c", "d"] and group.majority == 3
    # A conflicting tail from a new leader drops the entry, the old members count again.
    group._append_entries(1, [{"term": 2, "command": None}])
    assert group.members == ["a", "b", "c"]


def test_a_replica_outside_the_members_never_starts_an_election():
    group = RaftGroup(0, "d", [], send=None)
    group.deadline = 0
    assert not group.election_due()


async def test_learner_joins_and_counts_for_the_majority():
    net = Network(ids=("a", "b", "c", "d"))
    # d starts empty, outside the members, as a node started with --empty does.
    for i in "abc":
        net.groups[i] = RaftGroup(0, i, ["a", "b", "c"], net._sender(i))
    net.groups["d"] = RaftGroup(0, "d", [], net._sender("d"))
    net.start()
    try:
        leader = await net.wait_for_leader()
        for i in range(3):
            await leader.propose({"op": "put", "s": f"s{i}", "p": "p", "o": "o"})
        assert await leader.add_replica("d", {})
        assert leader.members == ["a", "b", "c", "d"]
        d = net.groups["d"]
        await until(lambda: asyncio.sleep(0, d.members == leader.members))
        await until(lambda: asyncio.sleep(0, d.last_applied == leader.last_applied))
        # Four members need three for a majority: with two down nothing commits.
        others = [g for g in leader.peers if g != "d"]
        net.down.update(others)
        try:
            await leader.propose({"op": "put", "s": "x", "p": "p", "o": "o"})
            raise AssertionError("committed without a majority")
        except TimeoutError:
            pass
    finally:
        await net.stop()


async def test_node_joins_a_running_cluster(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path, spare=("n4",))
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items() if n != "n4"}
    client = Client(dict(nodes), cluster.shards)
    try:
        await cluster.wait_for_leaders()
        for i in range(20):
            await client.put(f"course{i}", "teacher", "niklander")
        cluster.start("n4")
        n4 = f"127.0.0.1:{cluster.ports['n4']}"
        reply = await add_replica(nodes, cluster.shards, 0, "n4", n4)
        assert reply.get("ok"), reply
        assert reply["members"] == ["n1", "n2", "n3", "n4"]

        async def n4_has_shard_0():
            st = await cluster.request("n4", {"type": "Status"})
            replica = next((r for r in st["shards"] if r["shard"] == 0), None)
            return replica is not None and replica["last_applied"] > 0

        await until(n4_has_shard_0)
        # Replace n1 in shard 0. If n1 leads it, the leadership moves first.
        leaders = await cluster.leaders()
        if leaders[0] == "n1":
            msg = {"type": "Control", "action": "transfer", "shard": 0, "target": "n4"}
            assert (await cluster.request("n1", msg))["ok"]
            await until(lambda: _leads(cluster, 0, "n4"))
        reply = await remove_replica(
            {**nodes, "n4": ("127.0.0.1", cluster.ports["n4"])}, 3, 0, "n1"
        )
        assert reply.get("ok"), reply
        assert reply["members"] == ["n2", "n3", "n4"]
        # Writes and reads go on, also through a client that only knew n1 to n3.
        await client.put("after", "teacher", "su")
        assert (await client.get("course0"))["teacher"] == ["niklander"]
    finally:
        cluster.stop()


async def _leads(cluster, shard, node):
    return (await cluster.leaders()).get(shard) == node
