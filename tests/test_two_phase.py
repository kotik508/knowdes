"""Two-phase commit across shards (#12, design plan section 3.3)."""

from cluster import LocalCluster

from knowdes import protocol
from knowdes.client import Client
from knowdes.sharding import shard_of


def two_subjects(shards=3):
    """Two subjects on different shards."""
    first = "CSM13001"
    for i in range(100):
        other = f"room{i}"
        if shard_of(other, shards) != shard_of(first, shards):
            return first, other
    raise AssertionError


async def setup(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    await cluster.wait_for_leaders()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    return cluster, nodes, Client(nodes, cluster.shards)


async def test_both_shards_commit(tmp_path):
    cluster, _, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        reply = await client.transact(
            [
                {"op": "put", "s": a, "p": "room", "o": b},
                {"op": "rput", "s": a, "p": "room", "o": b},
                {"op": "put", "s": b, "p": "building", "o": "Exactum"},
            ]
        )
        assert reply["decision"] == "commit", reply
        assert reply["pending"] == []
        assert await client.get(a) == {"room": [b]}
        back = await client.read(b)
        assert back["facts"] == {"building": ["Exactum"]}
        assert back["incoming"] == {"room": [a]}
    finally:
        cluster.stop()


async def test_a_locked_node_aborts_on_every_shard(tmp_path):
    cluster, nodes, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        # Another transaction holds b on its shard and has not been decided.
        leaders = await cluster.leaders()
        shard_b = shard_of(b, cluster.shards)
        blocker = {
            "type": "Prepare",
            "shard": shard_b,
            "tx": "blocker",
            "facts": [{"op": "put", "s": b, "p": "building", "o": "Physicum"}],
        }
        vote = await protocol.request(nodes[leaders[shard_b]], blocker, 2.0)
        assert vote["vote"] is True

        facts = [
            {"op": "put", "s": a, "p": "room", "o": b},
            {"op": "put", "s": b, "p": "building", "o": "Exactum"},
        ]
        reply = await client.transact(facts, tx="t1")
        assert reply["decision"] == "abort"
        assert reply["votes"] == {str(shard_of(a, 3)): True, str(shard_b): False}
        # Nothing of t1 is visible, also not on the shard that voted yes.
        assert await client.get(a) == {}
        assert await client.get(b) == {}

        abort = {"type": "Abort", "shard": shard_b, "tx": "blocker"}
        assert (await protocol.request(nodes[leaders[shard_b]], abort, 2.0))["state"] == "aborted"
        reply = await client.transact(facts, tx="t2")
        assert reply["decision"] == "commit"
        assert await client.get(b) == {"building": ["Exactum"]}
    finally:
        cluster.stop()


async def test_repeated_transaction_keeps_its_first_decision(tmp_path):
    cluster, _, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        facts = [
            {"op": "put", "s": a, "p": "p", "o": "x"},
            {"op": "put", "s": b, "p": "p", "o": "y"},
        ]
        first = await client.transact(facts, tx="same")
        again = await client.transact(facts, tx="same")
        assert first["decision"] == again["decision"] == "commit"
        assert await client.get(a) == {"p": ["x"]}
    finally:
        cluster.stop()


async def test_follower_redirects_a_transaction(tmp_path):
    cluster, nodes, _ = await setup(tmp_path)
    try:
        leaders = await cluster.leaders()
        follower = next(n for n in nodes if n != leaders[0])
        msg = {"type": "TxWrite", "shard": 0, "tx": "t", "writes": []}
        reply = await protocol.request(nodes[follower], msg, 2.0)
        assert reply["type"] == "Redirect"
        assert reply["leader"] == leaders[0]
    finally:
        cluster.stop()
