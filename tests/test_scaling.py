"""Growing the cluster from three nodes to six while it runs (#14).

The project description asks to "begin with 3 nodes, and then show the system
can support 4 nodes, 5 nodes, and more". Nodes n4, n5 and n6 start empty, join
every shard one after the other, and writes go on between the steps.
"""

import asyncio
import json
import subprocess
import sys

from cluster import LocalCluster

from knowdes.client import Client
from knowdes.placement import balance, current_leaders, join, status_of
from knowdes.sharding import shard_of

NEW = ("n4", "n5", "n6")


async def until(check, timeout=10.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while loop.time() < end:
        if await check():
            return
        await asyncio.sleep(0.1)
    raise AssertionError("condition not reached")


async def test_cluster_grows_to_six_nodes(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path, spare=NEW)
    cluster.start()
    first = {n: ("127.0.0.1", p) for n, p in cluster.ports.items() if n not in NEW}
    client = Client(dict(first), cluster.shards)
    nodes = dict(first)
    try:
        await cluster.wait_for_leaders()
        for i in range(30):
            await client.put(f"course{i}", "teacher", "niklander")

        for count, node in enumerate(NEW, start=4):
            cluster.start(node)
            address = f"127.0.0.1:{cluster.ports[node]}"
            replies = await join(nodes, cluster.shards, node, address)
            assert all(r.get("ok") for r in replies), replies
            nodes[node] = ("127.0.0.1", cluster.ports[node])
            assert all(len(r["members"]) == count for r in replies)
            # Writes go on with every size, the client only ever knew n1 to n3.
            await client.put(f"step{count}", "nodes", str(count))

        async def new_nodes_caught_up():
            status = await status_of(nodes)
            applied = {
                n: {r["shard"]: r["last_applied"] for r in st["shards"]}
                for n, st in status.items()
                if st is not None
            }
            return all(applied[n] == applied["n1"] for n in NEW)

        await until(new_nodes_caught_up)

        # Placement spreads the three leaders over different nodes.
        await balance(nodes, cluster.shards)

        async def spread():
            leaders = current_leaders(await status_of(nodes), cluster.shards)
            return None not in leaders.values() and len(set(leaders.values())) == cluster.shards

        await until(spread)

        # Six replicas per shard need four for a majority, so two nodes may fail.
        cluster.kill("n1")
        cluster.kill("n2")
        await client.put("after", "failed", "n1 n2")
        assert (await client.get("course0"))["teacher"] == ["niklander"]
        assert (await client.get("step6"))["nodes"] == ["6"]
        # A follower read from a node that joined last has the old facts too.
        msg = {"type": "Get", "shard": shard_of("course7", 3), "s": "course7", "read": "follower"}
        reply = await cluster.request("n6", msg)
        assert reply["node"] == "n6"
        assert reply["facts"] == {"teacher": ["niklander"]}
    finally:
        cluster.stop()


async def test_join_from_the_command_line(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path, spare=("n4",))
    cluster.start()
    try:
        await cluster.wait_for_leaders()
        cluster.start("n4")
        config = tmp_path / "cluster.toml"
        config.write_text(
            "shards = 3\n"
            + "".join(
                f'[nodes.{n}]\nhost = "127.0.0.1"\nport = {p}\n'
                for n, p in cluster.ports.items()
                if n != "n4"
            )
        )
        node = f"n4=127.0.0.1:{cluster.ports['n4']}"
        cmd = [sys.executable, "-m", "knowdes.placement", "--config", str(config), "--join", node]
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE)
        out, _ = await asyncio.wait_for(proc.communicate(), 60)
        replies = [json.loads(line) for line in out.splitlines()]
        assert [r["shard"] for r in replies] == [0, 1, 2]
        assert all(r["ok"] and "n4" in r["members"] for r in replies)
    finally:
        cluster.stop()
