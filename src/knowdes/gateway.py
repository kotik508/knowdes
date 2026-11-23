"""Gateway between the web console and the cluster (#11, docs/console-plan.md).

The browser cannot speak the line protocol of the nodes, so the gateway does it
for the browser. It holds one Subscribe connection to every storage node and
merges their events into one stream, and it passes requests from the console
to a router or, for Control, to the node they name.

    GET  /events    server-sent events, the last events first and then every new one
    POST /request   one message as JSON, answered with the reply and the time it took
    GET  /          the console page from console/, once it exists

    python -m knowdes.gateway --config deploy/local.toml [--listen 127.0.0.1:7120]

Only the standard library, so it runs on the course machines as it is.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import contextlib
import json
import mimetypes
import time
from pathlib import Path

from knowdes import config, protocol
from knowdes.client import Client
from knowdes.protocol import Message
from knowdes.router import Router

# How long the gateway waits before it tries a node again that dropped out.
RETRY = 1.0
# Events a new console gets at once, so the timeline is not empty after a reload.
HISTORY = 500
# A browser that falls this far behind loses the oldest events, like a slow subscriber.
CLIENT_QUEUE = 1000
STATIC = Path(__file__).resolve().parents[2] / "console"
# Messages the gateway sends to one node instead of a router.
NODE_MESSAGES = ("Control", "Status")


class Gateway:
    def __init__(
        self, nodes: dict[str, tuple[str, int]], shards: int, static: Path = STATIC
    ) -> None:
        self.nodes = nodes
        self.router = Router(Client(nodes, shards))
        self.static = static
        self.history: collections.deque[Message] = collections.deque(maxlen=HISTORY)
        self.clients: list[asyncio.Queue] = []
        self.up: dict[str, bool] = {}
        self.server: asyncio.Server | None = None
        self._tasks: list[asyncio.Task] = []
        # Open event streams, closed on stop so the server can shut down.
        self._streams: set[asyncio.StreamWriter] = set()

    async def start(self, host: str, port: int) -> None:
        self.server = await asyncio.start_server(self._serve, host, port)
        self._tasks = [asyncio.create_task(self._follow(n)) for n in self.nodes]

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self.server is not None:
            self.server.close()
            for writer in list(self._streams):
                writer.close()
            await self.server.wait_closed()

    @property
    def address(self) -> tuple[str, int]:
        assert self.server is not None
        return self.server.sockets[0].getsockname()[:2]

    # Events from the nodes.

    def publish(self, kind: str, data: Message) -> None:
        item = {"kind": kind, "data": data}
        if kind == "event":
            self.history.append(item)
        for queue in self.clients:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(item)

    def mark(self, node: str, up: bool) -> None:
        """Report once when a node drops out or comes back, as an event of the gateway."""
        if self.up.get(node) == up:
            return
        self.up[node] = up
        event = {"type": "Event", "time": time.time(), "node": node, "shard": None}
        self.publish("event", {**event, "kind": "up" if up else "down"})

    async def _follow(self, node: str) -> None:
        """Keep a Subscribe connection to one node open, reconnect when it breaks."""
        while True:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(*self.nodes[node], limit=protocol.LINE_LIMIT), RETRY
                )
            except (OSError, TimeoutError):
                self.mark(node, False)
                await asyncio.sleep(RETRY)
                continue
            try:
                writer.write(protocol.encode({"type": "Subscribe"}))
                await writer.drain()
                while line := await reader.readline():
                    msg = protocol.decode(line)
                    if msg["type"] == "Subscribed":
                        self.mark(node, True)
                    elif msg["type"] == "Event":
                        self.publish("event", msg)
            except (OSError, ValueError, json.JSONDecodeError):
                pass
            finally:
                writer.close()
            self.mark(node, False)
            await asyncio.sleep(RETRY)

    # Requests from the console.

    async def request(self, m: Message) -> Message:
        start = time.perf_counter()
        if m.get("type") in NODE_MESSAGES:
            node = m.get("node")
            if node not in self.nodes:
                reply = {"type": "Error", "error": f"unknown node {node}"}
            else:
                try:
                    reply = await protocol.request(self.nodes[node], m, timeout=2.0)
                except (OSError, TimeoutError, ValueError, json.JSONDecodeError):
                    reply = {"type": "Error", "error": f"{node} does not answer"}
        else:
            reply = await self.router.handle(m)
        return {"reply": reply, "ms": round((time.perf_counter() - start) * 1000, 2)}

    # HTTP, only as much as the console needs.

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            lines = head.decode("latin-1").split("\r\n")
            method, path, _ = lines[0].split(" ", 2)
            headers = {}
            for line in lines[1:]:
                name, _, value = line.partition(":")
                headers[name.strip().lower()] = value.strip()
            path = path.split("?", 1)[0]
            if method == "GET" and path == "/events":
                await self._events(reader, writer)
            elif method == "POST" and path == "/request":
                body = await reader.readexactly(int(headers.get("content-length", 0)))
                try:
                    msg = json.loads(body)
                    if not isinstance(msg, dict):
                        raise ValueError("a request is a JSON object")
                except (ValueError, json.JSONDecodeError) as err:
                    await respond(writer, 400, {"error": str(err)})
                else:
                    await respond(writer, 200, await self.request(msg))
            elif method == "GET":
                await self._file(writer, path)
            else:
                await respond(writer, 405, {"error": f"{method} not allowed"})
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError, ValueError, OSError):
            pass
        finally:
            writer.close()

    async def _events(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        queue: asyncio.Queue = asyncio.Queue(CLIENT_QUEUE)
        for item in self.history:
            queue.put_nowait(item)
        self.clients.append(queue)
        self._streams.add(writer)
        # The browser sends nothing more, so EOF means it closed the page.
        closed = asyncio.create_task(reader.read())
        try:
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n"
                b"Cache-Control: no-cache\r\nConnection: close\r\n\r\n"
            )
            await writer.drain()
            while True:
                next_item = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait(
                    {next_item, closed}, return_when=asyncio.FIRST_COMPLETED
                )
                if closed in done:
                    next_item.cancel()
                    return
                item = next_item.result()
                data = json.dumps(item["data"], separators=(",", ":"))
                writer.write(f"event: {item['kind']}\ndata: {data}\n\n".encode())
                await writer.drain()
        finally:
            closed.cancel()
            self._streams.discard(writer)
            self.clients.remove(queue)

    async def _file(self, writer: asyncio.StreamWriter, path: str) -> None:
        name = "index.html" if path == "/" else path.lstrip("/")
        file = (self.static / name).resolve()
        if not file.is_relative_to(self.static.resolve()) or not file.is_file():
            await respond(writer, 404, {"error": f"{path} not found"})
            return
        kind = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
        await respond(writer, 200, file.read_bytes(), kind)


async def respond(
    writer: asyncio.StreamWriter, status: int, body: Message | bytes, kind: str = "application/json"
) -> None:
    data = body if isinstance(body, bytes) else json.dumps(body).encode()
    reason = {200: "OK", 400: "Bad Request", 404: "Not Found", 405: "Method Not Allowed"}[status]
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\nContent-Type: {kind}\r\n"
        f"Content-Length: {len(data)}\r\nConnection: close\r\n\r\n".encode()
        + data
    )
    await writer.drain()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the gateway for the web console.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--listen", default="127.0.0.1:7120", help="host:port")
    args = parser.parse_args()
    cluster = config.load(args.config)
    host, port = protocol.parse_address(args.listen)

    async def run() -> None:
        gateway = Gateway(dict(cluster.nodes), cluster.shards)
        await gateway.start(host, port)
        print(f"console on http://{host}:{port}/", flush=True)
        try:
            await asyncio.Event().wait()
        finally:
            await gateway.stop()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run())


if __name__ == "__main__":
    main()
