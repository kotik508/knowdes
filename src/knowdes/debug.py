"""Subscribe, Event and Control for the web console and for fault tests (design plan section 5).

Every storage node writes each important event with time, shard, term and
index to a local log file (section 3.2) and pushes it to every subscriber.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

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
