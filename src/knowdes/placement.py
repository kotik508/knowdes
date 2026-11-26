"""Spread the shard leaders over the machines (#14).

Raft elects whoever times out first, so after a start or a crash one machine
often leads several shards and does all their writes. Placement moves the
leaders so that shard i is led by the i-th node in sorted order, with the
leader transfer from the Raft thesis (section 3.10). Only nodes that answer
Status take part, a node that is down keeps no leadership.

    python -m knowdes.placement --config deploy/local.toml [--dry-run]

Nodes can join while the cluster runs (#14). A node started with --empty gets
the replica of a shard with --add, an old replica goes with --remove:

    python -m knowdes.placement --config deploy/local.toml --shard 0 --add n4=10.0.0.4:7101
    python -m knowdes.placement --config deploy/local.toml --shard 0 --remove n1

--join adds a new node to every shard, the way the cluster grows from three
nodes to six in tests/test_scaling.py:

    python -m knowdes.placement --config deploy/local.toml --join n4=10.0.0.4:7101
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


def _replica(status, shard):
    """The replica of one shard in a Status reply, empty if the node has none."""
    return next((r for r in status["shards"] if r["shard"] == shard), {})


def current_leaders(status: dict[str, Message | None], shards: int) -> dict[int, str | None]:
    """Leader of every shard. A cut-off old leader still claims it, the newest term counts."""
    found: dict[int, str | None] = {}
    for shard in range(shards):
        claims = sorted(
            (_replica(st, shard)["term"], node)
            for node, st in status.items()
            if st is not None and _replica(st, shard).get("role") == "leader"
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
        if leader is None:
            continue
        # Only a replica of the shard can lead it.
        members = _replica(status[leader], shard).get("members") or up
        candidates = [n for n in up if n in members]
        if not candidates:
            continue
        target = candidates[shard % len(candidates)]
        if leader != target:
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


async def change_replicas(
    nodes: dict[str, tuple[str, int]], shards: int, shard: int, msg: Message
) -> Message:
    """Send AddReplica or RemoveReplica to the leader of the shard, following one Redirect."""
    leader = current_leaders(await status_of(nodes), shards).get(shard)
    if leader is None:
        return {"ok": False, "error": f"shard {shard} has no leader"}
    msg = {**msg, "shard": shard}
    # Catching up a new replica takes longer than one message.
    reply = await protocol.request(nodes[leader], msg, 10.0)
    if reply.get("type") == "Redirect" and reply.get("leader") in nodes:
        reply = await protocol.request(nodes[reply["leader"]], msg, 10.0)
    return reply


async def add_replica(
    nodes: dict[str, tuple[str, int]], shards: int, shard: int, node: str, address: str
) -> Message:
    msg = {"type": "AddReplica", "node": node, "address": address}
    return await change_replicas(nodes, shards, shard, msg)


async def remove_replica(
    nodes: dict[str, tuple[str, int]], shards: int, shard: int, node: str
) -> Message:
    return await change_replicas(nodes, shards, shard, {"type": "RemoveReplica", "node": node})


async def join(
    nodes: dict[str, tuple[str, int]], shards: int, node: str, address: str
) -> list[Message]:
    """Add a node to every shard. One shard after the other, because a shard
    takes one membership change at a time and the new replica has to catch up."""
    nodes = {**nodes, node: protocol.parse_address(address)}
    return [await add_replica(nodes, shards, shard, node, address) for shard in range(shards)]


def main() -> None:
    parser = argparse.ArgumentParser(description="Spread the shard leaders over the nodes.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--dry-run", action="store_true", help="only print the moves")
    parser.add_argument("--shard", type=int, help="shard for --add and --remove")
    change = parser.add_mutually_exclusive_group()
    change.add_argument("--add", metavar="ID=HOST:PORT", help="add a replica of --shard")
    change.add_argument("--remove", metavar="ID", help="remove a replica of --shard")
    change.add_argument("--join", metavar="ID=HOST:PORT", help="add a node to every shard")
    args = parser.parse_args()
    cluster = config.load(args.config)
    nodes = dict(cluster.nodes)
    if args.join:
        node, _, address = args.join.partition("=")
        for reply in asyncio.run(join(nodes, cluster.shards, node, address)):
            print(json.dumps(reply))
        return
    if args.add or args.remove:
        if args.shard is None:
            parser.error("--add and --remove need --shard")
        if args.add:
            node, _, address = args.add.partition("=")
            nodes[node] = protocol.parse_address(address)
            run = add_replica(nodes, cluster.shards, args.shard, node, address)
        else:
            run = remove_replica(nodes, cluster.shards, args.shard, args.remove)
        print(json.dumps(asyncio.run(run)))
        return
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
