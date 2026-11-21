"""Router over a real cluster (issues #6 and #7)."""

import contextlib

from cluster import LocalCluster

from knowdes import protocol
from knowdes.client import Client
from knowdes.router import Router
from knowdes.sharding import shard_of

COURSES = ["CSM13001", "CSM14204", "TKT20005", "TKT21028"]


@contextlib.asynccontextmanager
async def setup(tmp_path, routers=1):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    started = []
    try:
        await cluster.wait_for_leaders()
        for _ in range(routers):
            router = Router(Client(nodes, cluster.shards))
            await router.start("127.0.0.1", 0)
            started.append(router)
        yield cluster, started
    finally:
        for router in started:
            await router.stop()
        cluster.stop()


async def ask(router, msg):
    return await protocol.request(router.address, msg, 5.0)


async def test_writes_through_any_router_reach_every_shard(tmp_path):
    async with setup(tmp_path, routers=2) as (cluster, (r1, r2)):
        assert {shard_of(c, cluster.shards) for c in COURSES} == {0, 1, 2}
        for i, course in enumerate(COURSES):
            router = (r1, r2)[i % 2]
            reply = await ask(
                router, {"type": "Put", "s": course, "p": "teacher", "o": "niklander"}
            )
            assert reply["ok"], reply
        for course in COURSES:
            reply = await ask(r2, {"type": "Get", "s": course})
            assert reply["facts"] == {"teacher": ["niklander"]}
        # The reverse edges sit on the shard of the object.
        reply = await ask(r1, {"type": "Get", "s": "niklander"})
        assert reply["incoming"] == {"teacher": sorted(COURSES)}
        await ask(r1, {"type": "Delete", "s": "CSM14204", "p": "teacher", "o": "niklander"})
        reply = await ask(r2, {"type": "Get", "s": "niklander"})
        assert "CSM14204" not in reply["incoming"]["teacher"]


async def test_neighbors_walk_both_directions(tmp_path):
    async with setup(tmp_path) as (cluster, (router,)):
        facts = [
            ("CSM13001", "teacher", "niklander"),
            ("CSM13001", "exam_room", "Exactum_A111"),
            ("TKT20005", "exam_room", "Exactum_A111"),
            ("Exactum_A111", "building", "Exactum"),
        ]
        reply = await ask(
            router,
            {"type": "BatchPut", "facts": [{"s": s, "p": p, "o": o} for s, p, o in facts]},
        )
        assert reply["ok"], reply

        out = await ask(router, {"type": "Neighbors", "s": "CSM13001", "depth": 2})
        assert {n["id"]: n["distance"] for n in out["nodes"]} == {
            "CSM13001": 0,
            "niklander": 1,
            "Exactum_A111": 1,
            "Exactum": 2,
        }
        # Which other course has its exam in the same room: one step out, one step back.
        both = await ask(
            router,
            {
                "type": "Neighbors",
                "s": "CSM13001",
                "depth": 2,
                "p": "exam_room",
                "direction": "both",
            },
        )
        assert ["TKT20005", "exam_room", "Exactum_A111"] in both["edges"]
        assert {"id": "TKT20005", "distance": 2} in both["nodes"]

        bad = await ask(router, {"type": "Neighbors", "s": "CSM13001", "depth": 9})
        assert bad["type"] == "Error"


async def test_router_survives_a_leader_crash(tmp_path):
    async with setup(tmp_path) as (cluster, (router,)):
        assert (await ask(router, {"type": "Put", "s": "CSM13001", "p": "p", "o": "1"}))["ok"]
        leader = (await cluster.leaders())[shard_of("CSM13001", cluster.shards)]
        cluster.kill(leader)
        reply = await ask(router, {"type": "Put", "s": "CSM13001", "p": "p", "o": "2"})
        assert reply.get("ok"), reply
        reply = await ask(router, {"type": "Get", "s": "CSM13001"})
        assert reply["facts"] == {"p": ["1", "2"]}
        assert reply["node"] != leader
