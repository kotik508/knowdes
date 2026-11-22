import asyncio
import subprocess
import sys
from pathlib import Path

import pytest
from cluster import free_port

from knowdes import config, protocol

DEPLOY = Path(__file__).parent.parent / "deploy"


def test_course_cluster_has_three_machines():
    cluster = config.load(DEPLOY / "cluster.toml")
    assert sorted(cluster.nodes) == ["n1", "n2", "n3"]
    hosts = {host for host, _ in cluster.nodes.values()}
    assert len(hosts) == 3
    assert all(host.endswith(".cs.helsinki.fi") for host in hosts)
    assert cluster.shards == 3


def test_local_cluster():
    cluster = config.load(DEPLOY / "local.toml")
    assert {port for _, port in cluster.nodes.values()} == {7101, 7102, 7103}
    assert cluster.data_dir == Path("data")


def test_too_few_nodes(tmp_path):
    path = tmp_path / "two.toml"
    path.write_text('[nodes.a]\nhost = "x"\nport = 1\n[nodes.b]\nhost = "y"\nport = 2\n')
    with pytest.raises(ValueError, match="three nodes"):
        config.load(path)


async def test_nodes_start_from_config(tmp_path):
    ports = {f"n{i}": free_port() for i in (1, 2, 3)}
    lines = [f'data_dir = "{tmp_path}"']
    for node, port in ports.items():
        lines += [f"[nodes.{node}]", 'host = "127.0.0.1"', f"port = {port}"]
    path = tmp_path / "cluster.toml"
    path.write_text("\n".join(lines) + "\n")
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", "knowdes.node", "--config", str(path), "--id", node],
            stderr=subprocess.DEVNULL,
        )
        for node in ports
    ]
    try:
        await asyncio.sleep(1.5)
        status = await protocol.request(("127.0.0.1", ports["n1"]), {"type": "Status"}, 1.0)
        assert len(status["shards"]) == 3
        assert all(s["leader"] in ports for s in status["shards"])
        assert (tmp_path / "n1.db").exists()
    finally:
        for proc in procs:
            proc.kill()
            proc.wait()
