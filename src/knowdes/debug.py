"""Subscribe, Event and Control for the web console and for fault tests (design plan section 5).

Every storage node writes each important event with time, shard, term and
index to a local log file (section 3.2) and pushes it to every subscriber.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
import time
from pathlib import Path
from typing import Any

from knowdes import protocol

Message = dict[str, Any]

# A subscriber that does not read its events in time loses the oldest ones,
# so a slow console never slows down Raft.
QUEUE_SIZE = 1000


class EventLog:
    def __init__(self, node_id: str, path: Path | None = None) -> None:
        self.node_id = node_id
        self.file = path.open("a", encoding="utf-8") if path else None
        self.subscribers: list[tuple[asyncio.Queue, set[int] | None]] = []

    def emit(self, kind: str, shard: int | None = None, **details: Any) -> Message:
        event = {
            "type": "Event",
            "time": time.time(),
            "node": self.node_id,
            "shard": shard,
            "kind": kind,
            **details,
        }
        if self.file is not None:
            self.file.write(json.dumps(event) + "\n")
            self.file.flush()
        for queue, shards in self.subscribers:
            if shards is not None and shard is not None and shard not in shards:
                continue
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(event)
        return event

    def subscribe(self, shards: list[int] | None = None) -> asyncio.Queue:
        queue: asyncio.Queue = asyncio.Queue(QUEUE_SIZE)
        self.subscribers.append((queue, set(shards) if shards is not None else None))
        return queue

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self.subscribers = [(q, s) for q, s in self.subscribers if q is not queue]

    def close(self) -> None:
        if self.file is not None:
            self.file.close()


# Command line, until the web console exists (#11):
#
#     python -m knowdes.debug subscribe 127.0.0.1:7101 --shard 0
#     python -m knowdes.debug control 127.0.0.1:7101 isolate n2
#     python -m knowdes.debug control 127.0.0.1:7101 heal
#     python -m knowdes.debug transfer 127.0.0.1:7101 0 n2


async def print_events(address: tuple[str, int], shards: list[int] | None) -> None:
    reader, writer = await asyncio.open_connection(*address, limit=protocol.LINE_LIMIT)
    writer.write(protocol.encode({"type": "Subscribe", "shards": shards}))
    await writer.drain()
    while line := await reader.readline():
        event = protocol.decode(line)
        stamp = time.strftime("%H:%M:%S", time.localtime(event.get("time", time.time())))
        details = {k: v for k, v in event.items() if k not in ("type", "time", "node", "kind")}
        print(stamp, event.get("node"), event.get("kind"), json.dumps(details), flush=True)


async def send_transfer(address: tuple[str, int], shard: int, target: str) -> Message:
    msg = {"type": "Control", "action": "transfer", "shard": shard, "target": target}
    return await protocol.request(address, msg, timeout=2.0)


async def send_control(address: tuple[str, int], action: str, peers: list[str]) -> Message:
    msg = {"type": "Control", "action": action, "peers": peers or None}
    return await protocol.request(address, msg, timeout=2.0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Watch and disturb a knowdes node.")
    sub = parser.add_subparsers(dest="command", required=True)
    watch = sub.add_parser("subscribe", help="print the events of a node")
    watch.add_argument("address", help="host:port of the node")
    watch.add_argument("--shard", type=int, action="append", help="only this shard")
    ctl = sub.add_parser("control", help="pause, resume, isolate or heal a node")
    ctl.add_argument("address", help="host:port of the node")
    ctl.add_argument("action", choices=["pause", "resume", "isolate", "heal"])
    ctl.add_argument("peers", nargs="*", help="node ids, all other nodes if none")
    move = sub.add_parser("transfer", help="hand the leadership of a shard to another node")
    move.add_argument("address", help="host:port of the current leader")
    move.add_argument("shard", type=int)
    move.add_argument("target", help="node id of the new leader")
    args = parser.parse_args()
    address = protocol.parse_address(args.address)

    if args.command == "subscribe":
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(print_events(address, args.shard))
    else:
        if args.command == "transfer":
            reply = asyncio.run(send_transfer(address, args.shard, args.target))
        else:
            reply = asyncio.run(send_control(address, args.action, args.peers))
        print(json.dumps(reply))
        sys.exit(0 if reply.get("ok") else 1)


if __name__ == "__main__":
    main()
