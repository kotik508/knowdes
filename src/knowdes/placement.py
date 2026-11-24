"""Spread the shard leaders over the machines (#14).

Raft elects whoever times out first, so after a start or a crash one machine
often leads several shards and does all their writes. Placement moves the
leaders so that shard i is led by the i-th node in sorted order, with the
leader transfer from the Raft thesis (section 3.10). Only nodes that answer
Status take part, a node that is down keeps no leadership.

    python -m knowdes.placement --config deploy/local.toml [--dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from knowdes import config, protocol
from knowdes.protocol import Message

TIMEOUT = 2.0
# Move = (shard, current leader, new leader)
Move = tuple[int, str, str]


def current_leaders(status: dict[str, Message | None], shards: int) -> dict[int, str | None]:
    """Leader of every shard. A cut-off old leader still claims it, the newest term counts."""
    found: dict[int, str | None] = {}
    for shard in range(shards):
        claims = sorted(
            (st["shards"][shard]["term"], node)
            for node, st in status.items()
            if st is not None and st["shards"][shard]["role"] == "leader"
        )
        unique = claims and (len(claims) == 1 or claims[-1][0] > claims[-2][0])
        found[shard] = claims[-1][1] if unique else None
    return found


def plan(status: dict[str, Message | None], shards: int) -> list[Move]:
    """The transfers that put shard i on the i-th node that is up and not paused."""
    up = sorted(n for n, st in status.items() if st is not None and not st.get("paused"))
    if not up:
        return []
    moves = []
    for shard, leader in current_leaders(status, shards).items():
        target = up[shard % len(up)]
        if leader is not None and leader != target:
            moves.append((shard, leader, target))
    return moves


async def status_of(nodes: dict[str, tuple[str, int]]) -> dict[str, Message | None]:
    async def ask(address: tuple[str, int]) -> Message | None:
        try:
            return await protocol.request(address, {"type": "Status"}, TIMEOUT)
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError):
            return None

    replies = await asyncio.gather(*(ask(a) for a in nodes.values()))
    return dict(zip(nodes, replies, strict=True))


async def balance(nodes: dict[str, tuple[str, int]], shards: int) -> list[Message]:
    """Carry out the plan, one transfer per shard, all shards at once."""
    moves = plan(await status_of(nodes), shards)

    async def move(shard: int, leader: str, target: str) -> Message:
        msg = {"type": "Control", "action": "transfer", "shard": shard, "target": target}
        try:
            reply = await protocol.request(nodes[leader], msg, TIMEOUT)
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError):
            reply = {"ok": False, "error": f"{leader} does not answer"}
        return {"shard": shard, "from": leader, "to": target, "ok": bool(reply.get("ok"))}

    return list(await asyncio.gather(*(move(*m) for m in moves)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Spread the shard leaders over the nodes.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true", help="only print the moves")
    args = parser.parse_args()
    cluster = config.load(args.config)
    nodes = dict(cluster.nodes)
    if args.dry_run:
        moves = plan(asyncio.run(status_of(nodes)), cluster.shards)
        for shard, leader, target in moves:
            print(f"shard {shard}: {leader} -> {target}")
        if not moves:
            print("leaders are spread already")
        return
    for result in asyncio.run(balance(nodes, cluster.shards)):
        print(json.dumps(result))


if __name__ == "__main__":
    main()
