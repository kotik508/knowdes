"""Message framing over TCP (design plan section 4).

Every message is one JSON object on one line. A request opens a connection,
sends one line and reads one line as the answer. Subscribe keeps the
connection open and the node writes one Event per line until the other side
closes it.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

Message = dict[str, Any]

# Large enough for AppendEntries with many entries.
LINE_LIMIT = 16 * 1024 * 1024


def encode(msg: Message) -> bytes:
    return (json.dumps(msg, separators=(",", ":")) + "\n").encode()


def decode(line: bytes) -> Message:
    msg = json.loads(line)
    if not isinstance(msg, dict) or "type" not in msg:
        raise ValueError("a message is a JSON object with a type")
    return msg


def parse_address(text: str) -> tuple[str, int]:
    host, _, port = text.rpartition(":")
    return host, int(port)


async def request(address: tuple[str, int], msg: Message, timeout: float = 0.1) -> Message:
    """Send one message and wait for one answer. Raises OSError or TimeoutError."""
    reader, writer = await asyncio.wait_for(
        asyncio.open_connection(*address, limit=LINE_LIMIT), timeout
    )
    try:
        writer.write(encode(msg))
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout)
        if not line:
            raise ConnectionError("connection closed without an answer")
        return decode(line)
    finally:
        writer.close()
