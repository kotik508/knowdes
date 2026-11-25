"""The measurement scripts on a small local cluster (#15)."""

from cluster import LocalCluster

from knowdes.bench import bench_failover, bench_reads, bench_writes, summary
from knowdes.client import Client
from knowdes.router import Router


def test_summary_of_latencies():
    result = summary("writes", [0.001, 0.002, 0.003, 0.004], 0.5, 1)
    assert result["count"] == 4 and result["errors"] == 1
    assert result["per_second"] == 8.0
    assert result["p50_ms"] == 3.0 and result["p99_ms"] == 4.0
    assert summary("empty", [], 0, 0)["mean_ms"] == 0.0


async def test_runs_against_a_cluster(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    router = Router(Client(dict(nodes), cluster.shards))
    try:
        await cluster.wait_for_leaders()
        await router.start("127.0.0.1", 0)
        writes = await bench_writes(router.address, 60, 4, label="test")
        assert writes["count"] == 60 and writes["errors"] == 0
        assert writes["label"] == "test"
        for level in ("leader", "follower"):
            reads = await bench_reads(router.address, 40, 4, level)
            assert reads["count"] == 40 and reads["run"] == f"reads {level}"
        failover = await bench_failover(nodes, cluster.shards, 0, 1)
        assert failover["count"] == 1 and failover["errors"] == 0
        # Detection takes at least one election timeout (150 ms).
        assert 100 < failover["mean_ms"] < 3000
    finally:
        await router.stop()
        cluster.stop()
