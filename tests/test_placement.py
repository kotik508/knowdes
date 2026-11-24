"""Spreading the shard leaders over the nodes (#14)."""

import asyncio

from cluster import LocalCluster

from knowdes.placement import balance, current_leaders, plan


def status(roles, terms=None, paused=False):
    """Fake Status reply: roles is one role letter per shard, l, f or c."""
    names = {"l": "leader", "f": "follower", "c": "candidate"}
    terms = terms or [1] * len(roles)
    shards = [
        {"shard": i, "role": names[r], "term": t}
        for i, (r, t) in enumerate(zip(roles, terms, strict=True))
    ]
    return {"type": "Status", "paused": paused, "shards": shards}


def test_one_node_leading_everything_gives_two_moves():
    st = {"n1": status("lll"), "n2": status("fff"), "n3": status("fff")}
    assert plan(st, 3) == [(1, "n1", "n2"), (2, "n1", "n3")]


def test_spread_leaders_stay():
    st = {"n1": status("lff"), "n2": status("flf"), "n3": status("ffl")}
    assert plan(st, 3) == []


def test_old_leader_with_lower_term_does_not_count():
    st = {"n1": status("lff", [1, 2, 2]), "n2": status("lff", [2, 2, 2]), "n3": None}
    assert current_leaders(st, 3)[0] == "n2"
    # n3 is down, so the two nodes that answer share the shards: 0 and 2 on n1, 1 on n2.
    assert plan(st, 3) == [(0, "n2", "n1")]


def test_paused_node_gets_no_leadership():
    st = {"n1": status("fff", paused=True), "n2": status("lll"), "n3": status("fff")}
    assert plan(st, 3) == [(1, "n2", "n3")]


async def test_balance_on_a_real_cluster(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    try:
        await cluster.wait_for_leaders()
        results = await balance(nodes, cluster.shards)
        assert all(r["ok"] for r in results), results
        for _ in range(40):
            if await cluster.leaders() == {0: "n1", 1: "n2", 2: "n3"}:
                break
            await asyncio.sleep(0.05)
        assert await cluster.leaders() == {0: "n1", 1: "n2", 2: "n3"}
        assert await balance(nodes, cluster.shards) == []
    finally:
        cluster.stop()
