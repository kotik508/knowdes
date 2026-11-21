"""Cluster config: which nodes exist and where they listen (design plan section 6)."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Cluster:
    nodes: dict[str, tuple[str, int]]
    shards: int
    data_dir: Path | None
    # Every machine runs a router next to its storage node on this port (#7).
    router_port: int = 7110

    def routers(self) -> list[tuple[str, int]]:
        return sorted({(host, self.router_port) for host, _ in self.nodes.values()})


def load(path: Path) -> Cluster:
    with path.open("rb") as f:
        raw = tomllib.load(f)
    nodes = {}
    for node_id, entry in raw.get("nodes", {}).items():
        nodes[node_id] = (entry["host"], int(entry["port"]))
    if len(nodes) < 3:
        raise ValueError(f"{path}: a cluster needs at least three nodes, found {len(nodes)}")
    data_dir = raw.get("data_dir")
    return Cluster(
        nodes=nodes,
        shards=int(raw.get("shards", 3)),
        data_dir=Path(data_dir).expanduser() if data_dir else None,
        router_port=int(raw.get("router_port", 7110)),
    )


def main() -> None:
    """Print one line "id host port" per node, for the scripts in deploy/."""
    import sys

    cluster = load(Path(sys.argv[1]))
    for node_id, (host, port) in cluster.nodes.items():
        print(node_id, host, port)


if __name__ == "__main__":
    main()
