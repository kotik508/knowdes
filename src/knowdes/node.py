"""Storage node: one process per machine with a replica of every shard (design plan section 6).

The node listens on one TCP port for messages from the other nodes and from
clients and hands each message to the Raft group of its shard.

    python -m knowdes.node --id n1 --listen 127.0.0.1:7101 \\
        --peer n2=127.0.0.1:7102 --peer n3=127.0.0.1:7103
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json

from knowdes import protocol
from knowdes.protocol import Message
from knowdes.raft import NotLeaderError, RaftGroup

# Timeout for one message to another node. A heartbeat interval is 50 ms.
PEER_TIMEOUT = 0.1


class StorageNode:
    def __init__(
        self,
        node_id: str,
        addresses: dict[str, tuple[str, int]],
        shards: int = 3,
    ) -> None:
        self.id = node_id
        self.addresses = addresses
        members = sorted(addresses)
        self.groups = {s: RaftGroup(s, node_id, members, self.send) for s in range(shards)}
        self.server: asyncio.Server | None = None

    async def send(self, peer: str, msg: Message) -> Message | None:
        """Send to another node. A node that does not answer in time counts as gone."""
        msg = {**msg, "sender": self.id}
        try:
            return await protocol.request(self.addresses[peer], msg, PEER_TIMEOUT)
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError):
            return None

    async def start(self) -> None:
        host, port = self.addresses[self.id]
        self.server = await asyncio.start_server(self._serve, host, port, limit=protocol.LINE_LIMIT)
        for group in self.groups.values():
            group.start()

    async def stop(self) -> None:
        for group in self.groups.values():
            await group.stop()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            reply = await self.handle(protocol.decode(line))
            if reply is not None:
                writer.write(protocol.encode(reply))
                await writer.drain()
        except (ValueError, json.JSONDecodeError) as err:
            writer.write(protocol.encode({"type": "Error", "error": str(err)}))
            await writer.drain()
        except OSError:
            pass
        finally:
            writer.close()

    async def handle(self, m: Message) -> Message | None:
        kind = m["type"]
        if kind == "RequestVote":
            return self.groups[m["shard"]].on_request_vote(m)
        if kind == "AppendEntries":
            return self.groups[m["shard"]].on_append_entries(m)
        if kind in ("Put", "Delete"):
            return await self.write(m)
        if kind == "Status":
            return self.status()
        return {"type": "Error", "error": f"unknown message type {kind}"}

    async def write(self, m: Message) -> Message:
        """A client write. Until the router exists (#7) the client names the shard itself."""
        group = self.groups.get(m.get("shard"))
        if group is None:
            return {"type": "Error", "id": m.get("id"), "error": "unknown shard"}
        command = {"op": m["type"].lower(), "s": m["s"], "p": m["p"], "o": m["o"]}
        try:
            index = await group.propose(command)
        except NotLeaderError as err:
            return {"type": "Redirect", "id": m.get("id"), "leader": err.leader}
        except TimeoutError:
            return {"type": "Error", "id": m.get("id"), "error": "not committed in time"}
        return {"type": "Reply", "id": m.get("id"), "ok": True, "index": index}

    def status(self) -> Message:
        return {
            "type": "Status",
            "node": self.id,
            "shards": [g.status() for g in self.groups.values()],
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one knowdes storage node.")
    parser.add_argument("--id", required=True)
    parser.add_argument("--listen", required=True, help="host:port of this node")
    parser.add_argument("--peer", action="append", default=[], help="id=host:port")
    parser.add_argument("--shards", type=int, default=3)
    args = parser.parse_args()

    addresses = {args.id: protocol.parse_address(args.listen)}
    for peer in args.peer:
        peer_id, _, address = peer.partition("=")
        addresses[peer_id] = protocol.parse_address(address)

    async def run() -> None:
        node = StorageNode(args.id, addresses, args.shards)
        await node.start()
        try:
            await asyncio.Event().wait()
        finally:
            await node.stop()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run())


if __name__ == "__main__":
    main()
