"""The demo script (#16) runs through against a local cluster, a fourth node included."""

from cluster import LocalCluster

from knowdes.demo import Demo


async def test_demo_runs_through(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path, spare=("n4",))
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items() if n != "n4"}
    lines: list[str] = []
    try:
        await cluster.wait_for_leaders()
        cluster.start("n4")
        demo = Demo(nodes, cluster.shards, say=lines.append)
        await demo.run(grow=("n4", f"127.0.0.1:{cluster.ports['n4']}"))
    finally:
        cluster.stop()
    text = "\n".join(lines)
    assert "niklander teaches CSM13001, DATA11001" in text
    assert "elected" in text and "caught up" in text
    assert "n4 joined all 3 shards, each has 4 replicas" in text
    assert "7. Leaders spread again" in text


async def test_paused_leader_is_replaced(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    try:
        await cluster.wait_for_leaders()
        demo = Demo(nodes, cluster.shards, say=lambda _: None)
        old = await demo.stop_leader(shard=1)
        assert (await demo.leaders())[1] not in (None, old)
        await demo.come_back(old)
        status = await cluster.status()
        assert not status[old]["paused"]
    finally:
        cluster.stop()
