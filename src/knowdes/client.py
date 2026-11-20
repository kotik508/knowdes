"""Client side of the storage nodes, until the router exists (#7).

Picks the shard from the subject, sends to the shard's leader and follows a
Redirect, which is what the router will do for the CLI, the console and the
source adapters.

    python -m knowdes.client --config deploy/local.toml get SUBJECT
    python -m knowdes.client --config deploy/local.toml put SUBJECT PREDICATE OBJECT
"""

from __future__ import annotations

import argparse
import asyncio
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from knowdes import config, protocol
from knowdes.protocol import Message
from knowdes.sharding import shard_of

# Facts per BatchPut, so one log entry stays small.
BATCH_SIZE = 500


class Client:
    def __init__(self, nodes: dict[str, tuple[str, int]], shards: int, timeout: float = 3.0):
        self.nodes = nodes
        self.shards = shards
        self.timeout = timeout
        self.leaders: dict[int, str] = {}

    async def call(self, shard: int, msg: Message, attempts: int = 20) -> Message:
        """Send to the leader of the shard, trying the others while it is unknown."""
        msg = {**msg, "shard": shard}
        order = list(self.nodes)
        last: Message | None = None
        for attempt in range(attempts):
            node = self.leaders.get(shard) or order[attempt % len(order)]
            try:
                reply = await protocol.request(self.nodes[node], msg, self.timeout)
            except (OSError, TimeoutError, ValueError):
                self.leaders.pop(shard, None)
                order.append(order.pop(0))
                continue
            last = reply
            if reply["type"] == "Redirect":
                if reply.get("leader") in self.nodes:
                    self.leaders[shard] = reply["leader"]
                else:  # election still running
                    self.leaders.pop(shard, None)
                    await asyncio.sleep(0.1)
                continue
            if reply.get("ok"):
                self.leaders[shard] = node
                return reply
            self.leaders.pop(shard, None)
            await asyncio.sleep(0.1)
        raise ConnectionError(f"shard {shard} did not take {msg['type']}: {last}")

    async def put(self, s: str, p: str, o: str, version: int | None = None) -> Message:
        msg: Message = {"type": "Put", "s": s, "p": p, "o": o, "version": version}
        return await self.call(shard_of(s, self.shards), msg)

    async def put_many(self, facts: list[dict[str, Any]]) -> None:
        """Write many facts with one log entry per shard and batch."""
        by_shard: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for fact in facts:
            by_shard[shard_of(fact["s"], self.shards)].append(fact)

        async def send(shard: int, facts: list[dict[str, Any]]) -> None:
            for i in range(0, len(facts), BATCH_SIZE):
                await self.call(shard, {"type": "BatchPut", "facts": facts[i : i + BATCH_SIZE]})

        await asyncio.gather(*(send(shard, f) for shard, f in by_shard.items()))

    async def get(self, s: str) -> dict[str, list[str]]:
        reply = await self.call(shard_of(s, self.shards), {"type": "Get", "s": s})
        return reply["facts"]


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
