"""Talking to the storage nodes: what the router does (design plan section 2, #7).

Picks the shard from the subject, sends to the shard's leader and follows a
Redirect. A plain write also adds the reverse edge on the shard of the object,
so Neighbors can walk edges backwards. Both go in one two-phase commit (#12),
so a fact never exists without its reverse edge or the other way round. Only
BatchPut, which loads whole sources, still writes them one after the other;
repeating a failed batch is harmless because a set does not mind a second put.

The router process (knowdes.router) wraps this class, the source adapters use
it directly. The CLI talks to a router (knowdes.cli).

    python -m knowdes.client --config deploy/local.toml get SUBJECT
    python -m knowdes.client --config deploy/local.toml put SUBJECT PREDICATE OBJECT
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any

from knowdes import config, protocol
from knowdes.protocol import Message
from knowdes.sharding import shard_of

# Facts per BatchPut, so one log entry stays small.
BATCH_SIZE = 500
# Neighbors stops here, a walk over the whole graph is not a query.
MAX_DEPTH = 3
READ_LEVELS = ("leader", "follower")
# A write aborts when another transaction holds its subject. It tries again with
# a new transaction this often, waiting a little longer each time.
TX_RETRIES = 5
TX_BACKOFF = 0.05


class Client:
    def __init__(self, nodes: dict[str, tuple[str, int]], shards: int, timeout: float = 3.0):
        self.nodes = nodes
        self.shards = shards
        self.timeout = timeout
        self.leaders: dict[int, str] = {}

    async def call(self, shard: int, msg: Message, patience: float = 3.0) -> Message:
        """Send to the leader of the shard, trying the others while it is unknown.

        After a crash the followers still name the old leader until the election
        timeout runs out (150 to 300 ms), so the client keeps trying for a while
        instead of a fixed number of times.
        """
        msg = {**msg, "shard": shard}
        order = list(self.nodes)
        loop = asyncio.get_running_loop()
        end = loop.time() + patience
        last: Message | None = None
        attempt = 0
        while loop.time() < end:
            node = self.leaders.get(shard) or order[attempt % len(order)]
            attempt += 1
            try:
                reply = await protocol.request(self.nodes[node], msg, self.timeout)
            except (OSError, TimeoutError, ValueError):
                self.leaders.pop(shard, None)
                await asyncio.sleep(0.05)
                continue
            last = reply
            if reply["type"] == "Redirect":
                # A node added at runtime (#14): the redirect brings its address.
                if (
                    reply.get("leader")
                    and reply.get("address")
                    and reply["leader"] not in self.nodes
                ):
                    self.nodes[reply["leader"]] = (reply["address"][0], int(reply["address"][1]))
                if reply.get("leader") in self.nodes and reply["leader"] != node:
                    self.leaders[shard] = reply["leader"]
                else:  # election still running
                    self.leaders.pop(shard, None)
                    await asyncio.sleep(0.05)
                continue
            if reply.get("ok"):
                self.leaders[shard] = node
                return reply
            self.leaders.pop(shard, None)
            await asyncio.sleep(0.05)
        raise ConnectionError(f"shard {shard} did not take {msg['type']}: {last}")

    async def call_any(self, shard: int, msg: Message) -> Message:
        """A follower read: the first replica that answers, the cached leader last."""
        msg = {**msg, "shard": shard}
        leader = self.leaders.get(shard)
        order = sorted(self.nodes, key=lambda n: n == leader)
        last: Message | None = None
        for node in order:
            try:
                reply = await protocol.request(self.nodes[node], msg, self.timeout)
            except (OSError, TimeoutError, ValueError):
                continue
            if reply.get("ok"):
                return reply
            last = reply
        raise ConnectionError(f"no replica of shard {shard} answered: {last}")

    async def write(self, op: str, s: str, p: str, o: str, version: int | None = None) -> Message:
        """Put or Delete one fact and, for a plain write, its reverse edge."""
        if version is not None:
            # A source fact replaces its old value and the old reverse edge would
            # stay behind, so source facts have no reverse edges.
            msg: Message = {"type": op, "s": s, "p": p, "o": o, "version": version}
            return await self.call(shard_of(s, self.shards), msg)
        kind = op.lower()
        facts = [{"op": kind, "s": s, "p": p, "o": o}, {"op": "r" + kind, "s": s, "p": p, "o": o}]
        reply: Message = {}
        for attempt in range(TX_RETRIES):
            reply = await self.transact(facts)
            if reply["decision"] == "commit":
                return reply
            await asyncio.sleep(TX_BACKOFF * (attempt + 1) * (1 + random.random()))
        raise ConnectionError(f"{op} {s} {p} {o} aborted {TX_RETRIES} times: {reply}")

    async def transact(self, facts: list[dict[str, Any]], tx: str | None = None) -> Message:
        """Write facts on several shards with two-phase commit (#12): all or none.

        A fact is {"op", "s", "p", "o"} with op put, delete, rput or rdelete. The
        shard of a reverse edge (rput, rdelete) is the one of its object. The
        leader of the shard of the first fact coordinates.
        """
        writes: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for fact in facts:
            key = fact["o"] if fact["op"] in ("rput", "rdelete") else fact["s"]
            writes[shard_of(key, self.shards)].append(fact)
        first = facts[0]
        coordinator = shard_of(
            first["o"] if first["op"] in ("rput", "rdelete") else first["s"], self.shards
        )
        msg = {
            "type": "TxWrite",
            "tx": tx or uuid.uuid4().hex,
            "writes": [{"shard": shard, "facts": f} for shard, f in sorted(writes.items())],
        }
        return await self.call(coordinator, msg, patience=6.0)

    async def put(self, s: str, p: str, o: str, version: int | None = None) -> Message:
        return await self.write("Put", s, p, o, version)

    async def delete(self, s: str, p: str, o: str) -> Message:
        return await self.write("Delete", s, p, o)

    async def put_many(self, facts: list[dict[str, Any]]) -> None:
        """Write many facts with one log entry per shard and batch."""
        by_shard: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for fact in facts:
            by_shard[shard_of(fact["s"], self.shards)].append(fact)
        reverse: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for fact in facts:
            if fact.get("version") is None:
                reverse[shard_of(fact["o"], self.shards)].append({**fact, "reverse": True})

        async def send(shard: int, facts: list[dict[str, Any]]) -> None:
            for i in range(0, len(facts), BATCH_SIZE):
                await self.call(shard, {"type": "BatchPut", "facts": facts[i : i + BATCH_SIZE]})

        await asyncio.gather(*(send(shard, f) for shard, f in by_shard.items()))
        await asyncio.gather(*(send(shard, f) for shard, f in reverse.items()))

    async def read(self, s: str, read: str = "leader") -> Message:
        """The reply of Get: facts, incoming, which node answered and how far it was."""
        if read not in READ_LEVELS:
            raise ValueError(f"read level is one of {READ_LEVELS}")
        shard = shard_of(s, self.shards)
        msg = {"type": "Get", "s": s, "read": read}
        return await (self.call(shard, msg) if read == "leader" else self.call_any(shard, msg))

    async def get(self, s: str, read: str = "leader") -> dict[str, list[str]]:
        return (await self.read(s, read))["facts"]

    async def neighbors(
        self,
        start: str,
        depth: int = 1,
        predicate: str | None = None,
        direction: str = "out",
        read: str = "leader",
    ) -> Message:
        """Breadth-first walk from start, one Get per node and level.

        Returns the nodes found with their distance and the edges walked as
        (subject, predicate, object), whatever the direction they were walked in.
        """
        if not 1 <= depth <= MAX_DEPTH:
            raise ValueError(f"depth is 1 to {MAX_DEPTH}")
        if direction not in ("out", "in", "both"):
            raise ValueError("direction is out, in or both")
        distance = {start: 0}
        edges: set[tuple[str, str, str]] = set()
        frontier = [start]
        for level in range(1, depth + 1):
            replies = await asyncio.gather(*(self.read(n, read) for n in frontier))
            found: list[str] = []
            for node, reply in zip(frontier, replies, strict=True):
                steps = []
                if direction in ("out", "both"):
                    steps += [(node, p, x, x) for p, xs in reply["facts"].items() for x in xs]
                if direction in ("in", "both"):
                    steps += [(x, p, node, x) for p, xs in reply["incoming"].items() for x in xs]
                for s, p, o, other in steps:
                    if predicate is not None and p != predicate:
                        continue
                    edges.add((s, p, o))
                    if other not in distance:
                        distance[other] = level
                        found.append(other)
            frontier = found
            if not frontier:
                break
        return {
            "nodes": [{"id": n, "distance": d} for n, d in sorted(distance.items(), key=_by_dist)],
            "edges": [list(e) for e in sorted(edges)],
        }


def _by_dist(item: tuple[str, int]) -> tuple[int, str]:
    return item[1], item[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Read and write facts.")
    parser.add_argument("--config", type=Path, required=True)
    sub = parser.add_subparsers(dest="cmd", required=True)
    get = sub.add_parser("get")
    get.add_argument("subject")
    put = sub.add_parser("put")
    put.add_argument("subject")
    put.add_argument("predicate")
    put.add_argument("object")
    args = parser.parse_args()
    cluster = config.load(args.config)
    client = Client(dict(cluster.nodes), cluster.shards)

    async def run() -> Any:
        if args.cmd == "get":
            return await client.get(args.subject)
        return await client.put(args.subject, args.predicate, args.object)

    print(json.dumps(asyncio.run(run()), indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
