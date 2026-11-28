"""Router: stateless entry point for clients (design plan section 2, issue #7).

A client sends Put, Delete, BatchPut, Get or Neighbors without knowing about
shards. The router only translates: knowdes.client hashes the subject to its
shard, finds the leader, follows Redirects and runs the two-phase commit of a
write with its reverse edge. The only state is the client's guess of who leads
which shard, so any router can be used and a router can be restarted at any time.
One request per connection, answered with Reply or Error.

    python -m knowdes.router --config deploy/local.toml [--listen 127.0.0.1:7110]
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from pathlib import Path

from knowdes import config, protocol
from knowdes.client import Client
from knowdes.protocol import Message


class Router:
    def __init__(self, client: Client) -> None:
        self.client = client
        self.server: asyncio.Server | None = None

    async def start(self, host: str, port: int) -> None:
        self.server = await asyncio.start_server(self._serve, host, port, limit=protocol.LINE_LIMIT)

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    @property
    def address(self) -> tuple[str, int]:
        assert self.server is not None
        return self.server.sockets[0].getsockname()[:2]

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if line:
                msg = protocol.decode(line)
                writer.write(protocol.encode(await self.handle(msg)))
                await writer.drain()
        except (ValueError, json.JSONDecodeError) as err:
            writer.write(protocol.encode({"type": "Error", "error": str(err)}))
            await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()

    async def handle(self, m: Message) -> Message:
        """One request, never raises: a bad message or an unreachable shard becomes Error."""
        ident = m.get("id")
        try:
            result = await self.dispatch(m)
        except (KeyError, TypeError, ValueError) as err:
            return {"type": "Error", "id": ident, "error": f"bad request: {err}"}
        except ConnectionError as err:
            return {"type": "Error", "id": ident, "error": str(err)}
        return {"type": "Reply", "id": ident, "ok": True, **result}

    async def dispatch(self, m: Message) -> Message:
        c, kind = self.client, m["type"]
        # A plain Put or Delete is a two-phase commit with the reverse edge (#12).
        if kind in ("Put", "Delete"):
            reply = await c.write(kind, str(m["s"]), str(m["p"]), str(m["o"]), m.get("version"))
            return {"index": reply["index"]}
        # BatchPut is for imports: batches per shard, the reverse edges in a second
        # round and without two-phase commit, so they can lag behind the facts.
        if kind == "BatchPut":
            await c.put_many(list(m["facts"]))
            return {"count": len(m["facts"])}
        if kind == "Get":
            reply = await c.read(str(m["s"]), m.get("read", "leader"))
            # node and applied tell which replica answered and how far it was.
            keep = ("facts", "incoming", "node", "read", "applied")
            return {k: reply[k] for k in keep}
        # Neighbors walks the graph in the router, one Get per node and level.
        if kind == "Neighbors":
            return await c.neighbors(
                str(m["s"]),
                int(m.get("depth", 1)),
                m.get("p"),
                m.get("direction", "out"),
                m.get("read", "leader"),
            )
        raise ValueError(f"unknown message type {kind}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a knowdes router.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--listen", help="host:port, default 127.0.0.1 and router_port")
    args = parser.parse_args()
    cluster = config.load(args.config)
    host, port = (
        protocol.parse_address(args.listen) if args.listen else ("127.0.0.1", cluster.router_port)
    )

    async def run() -> None:
        router = Router(Client(dict(cluster.nodes), cluster.shards))
        await router.start(host, port)
        try:
            await asyncio.Event().wait()
        finally:
            await router.stop()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run())


if __name__ == "__main__":
    main()
