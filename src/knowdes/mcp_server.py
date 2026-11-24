"""MCP server, so a language model can query the graph as a tool (#13).

It is a thin layer over a router: every tool call becomes one message to the
router and its reply goes back as the result. The server keeps no state, it
speaks MCP over stdin and stdout and is started by the MCP client.

put and delete change the graph. Start the server with --read-only to give a
model only get and neighbors.

    python -m knowdes.mcp_server --router 127.0.0.1:7110
    python -m knowdes.mcp_server --config deploy/local.toml

Needs the official MCP SDK: uv sync --extra mcp.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Literal

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from knowdes import config, protocol
from knowdes.protocol import Message

# A neighbour walk over three shards can take a few round trips.
TIMEOUT = 10.0

Read = Literal["leader", "follower"]


def build(router: tuple[str, int], read_only: bool = False) -> MCPServer:
    server = MCPServer(
        "knowdes",
        instructions=(
            "A distributed knowledge graph of university data: courses, people, rooms and "
            "their relations as subject, predicate, object. Use get for the edges of one "
            "node and neighbors to walk further. Leader reads see every committed write, "
            "follower reads are faster but may miss the newest ones. put and delete add or "
            "remove one edge, the reverse edge follows by itself."
        ),
    )

    async def ask(msg: Message) -> Message:
        try:
            reply = await protocol.request(router, msg, TIMEOUT)
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError) as err:
            raise ToolError(f"router {router[0]}:{router[1]} does not answer: {err}") from err
        if not reply.get("ok"):
            raise ToolError(reply.get("error", "request failed"))
        return {k: v for k, v in reply.items() if k not in ("type", "id", "ok")}

    reading = ToolAnnotations(readOnlyHint=True, idempotentHint=True)
    writing = ToolAnnotations(readOnlyHint=False, destructiveHint=False, idempotentHint=True)
    removing = ToolAnnotations(readOnlyHint=False, destructiveHint=True, idempotentHint=True)

    @server.tool(annotations=reading)
    async def get(subject: str, read: Read = "leader") -> dict:
        """All edges of one node: facts maps each predicate to its objects, incoming
        maps each predicate to the subjects that point at this node."""
        return await ask({"type": "Get", "s": subject, "read": read})

    @server.tool(annotations=reading)
    async def neighbors(
        subject: str,
        depth: int = 1,
        predicate: str | None = None,
        direction: Literal["out", "in", "both"] = "out",
        read: Read = "leader",
    ) -> dict:
        """Nodes reachable from subject in at most depth steps (1 to 3), optionally only
        along one predicate. direction "in" follows edges backwards."""
        return await ask(
            {
                "type": "Neighbors",
                "s": subject,
                "depth": depth,
                "p": predicate,
                "direction": direction,
                "read": read,
            }
        )

    if read_only:
        return server

    @server.tool(annotations=writing)
    async def put(subject: str, predicate: str, object: str) -> dict:
        """Add the edge subject -predicate-> object. Adding it twice changes nothing.
        Returns the log index at which the write was committed."""
        return await ask({"type": "Put", "s": subject, "p": predicate, "o": object})

    @server.tool(annotations=removing)
    async def delete(subject: str, predicate: str, object: str) -> dict:
        """Remove the edge subject -predicate-> object together with its reverse edge."""
        return await ask({"type": "Delete", "s": subject, "p": predicate, "o": object})

    return server


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the knowledge graph over MCP (stdio).")
    where = parser.add_mutually_exclusive_group(required=True)
    where.add_argument("--router", help="host:port of a router")
    where.add_argument("--config", type=Path, help="use the first router of this cluster")
    parser.add_argument("--read-only", action="store_true", help="only get and neighbors")
    args = parser.parse_args()
    if args.router:
        router = protocol.parse_address(args.router)
    else:
        router = config.load(args.config).routers()[0]
    build(router, args.read_only).run("stdio")


if __name__ == "__main__":
    main()
