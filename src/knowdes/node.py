"""Storage node: one process per machine with a replica of every shard (design plan section 6).

The node listens on one TCP port for messages from the other nodes and from
clients and hands each message to the Raft group of its shard.

    python -m knowdes.node --config deploy/cluster.toml --id n1

A node can also start empty and get its replicas later, while the cluster
runs (#14): the leader of a shard adds it with AddReplica, it catches up from
the log and then counts as a member.

    python -m knowdes.node --id n4 --listen 127.0.0.1:7104 --empty --peer n1=127.0.0.1:7101

or without a config file

    python -m knowdes.node --id n1 --listen 127.0.0.1:7101 \\
        --peer n2=127.0.0.1:7102 --peer n3=127.0.0.1:7103
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
from pathlib import Path

from knowdes import config, protocol
from knowdes.debug import EventLog
from knowdes.protocol import Message
from knowdes.raft import NotLeaderError, RaftGroup
from knowdes.raft.storage import MemoryStorage, SqliteStorage, open_database
from knowdes.store import ShardStore

# Timeout for one message to another node. A heartbeat interval is 50 ms.
PEER_TIMEOUT = 0.1


class StorageNode:
    def __init__(
        self,
        node_id: str,
        addresses: dict[str, tuple[str, int]],
        shards: int = 3,
        data_dir: Path | None = None,
        empty: bool = False,
    ) -> None:
        self.id = node_id
        self.addresses = addresses
        self.shards = shards
        self.members = sorted(addresses)
        # Without a data directory nothing survives a restart, which suits some tests.
        self.db = open_database(data_dir / f"{node_id}.db") if data_dir else None
        self.events = EventLog(node_id, data_dir / f"{node_id}-events.log" if data_dir else None)
        self.stores: dict[int, ShardStore] = {}
        self.groups: dict[int, RaftGroup] = {}
        self.started = False
        # An empty node waits until a leader adds it to a shard (#14). It does not
        # know the members yet, the log it receives tells it.
        for s in [] if empty else range(shards):
            self.add_group(s, self.members)
        self.server: asyncio.Server | None = None
        # Fault injection through Control messages, for the console and the tests.
        self.paused = False
        self.isolated: set[str] = set()

    def add_group(self, shard: int, members: list[str]) -> RaftGroup:
        self.stores[shard] = store = ShardStore()
        group = RaftGroup(
            shard,
            self.id,
            members,
            self.send,
            apply=lambda index, command: store.apply(command),
            storage=SqliteStorage(self.db, shard) if self.db else MemoryStorage(),
            emit=self.events.emit,
            on_config=self.learn_addresses,
        )
        self.groups[shard] = group
        if self.started:
            group.start()
        return group

    def group_for(self, m: Message) -> RaftGroup | None:
        """The replica a Raft message is for. A leader that added this node creates it."""
        shard = m.get("shard")
        if shard in self.groups:
            return self.groups[shard]
        if isinstance(shard, int) and 0 <= shard < self.shards:
            return self.add_group(shard, [])
        return None

    def learn_addresses(self, members: list[str], addresses: dict[str, list]) -> None:
        for node, address in addresses.items():
            if node != self.id:
                self.addresses[node] = (address[0], int(address[1]))

    async def send(self, peer: str, msg: Message) -> Message | None:
        """Send to another node. A node that does not answer in time counts as gone."""
        if self.paused or peer in self.isolated or peer not in self.addresses:
            return None
        msg = {**msg, "sender": self.id}
        try:
            return await protocol.request(self.addresses[peer], msg, PEER_TIMEOUT)
        except (OSError, TimeoutError, ValueError, json.JSONDecodeError):
            return None

    async def start(self) -> None:
        host, port = self.addresses[self.id]
        self.server = await asyncio.start_server(self._serve, host, port, limit=protocol.LINE_LIMIT)
        self.started = True
        for group in self.groups.values():
            group.start()

    async def stop(self) -> None:
        for group in self.groups.values():
            await group.stop()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
        if self.db is not None:
            self.db.close()
        self.events.close()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            line = await reader.readline()
            if not line:
                return
            msg = protocol.decode(line)
            if msg["type"] == "Subscribe":
                await self.stream_events(msg, reader, writer)
                return
            reply = await self.handle(msg)
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

    async def stream_events(
        self, m: Message, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Push every event to the subscriber until it closes the connection."""
        queue = self.events.subscribe(m.get("shards"))
        closed = asyncio.create_task(reader.read())
        try:
            writer.write(protocol.encode({"type": "Subscribed", "node": self.id}))
            await writer.drain()
            while True:
                next_event = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait(
                    {next_event, closed}, return_when=asyncio.FIRST_COMPLETED
                )
                if closed in done:
                    next_event.cancel()
                    return
                writer.write(protocol.encode(next_event.result()))
                await writer.drain()
        finally:
            closed.cancel()
            self.events.unsubscribe(queue)

    async def handle(self, m: Message) -> Message | None:
        kind = m["type"]
        if kind == "Control" and m.get("action") == "transfer":
            return await self.transfer(m)
        if kind in ("AddReplica", "RemoveReplica"):
            return await self.change_replicas(m)
        if kind == "Control":
            return self.control(m)
        if kind == "Status":
            return self.status()
        # Look like a crashed node or a cut link: no answer at all.
        if self.paused or m.get("sender") in self.isolated:
            return None
        if kind in ("RequestVote", "AppendEntries", "TimeoutNow"):
            group = self.group_for(m)
            if group is None:
                return {"type": "Error", "error": "unknown shard"}
            if kind == "RequestVote":
                return group.on_request_vote(m)
            if kind == "AppendEntries":
                return group.on_append_entries(m)
            return group.on_timeout_now(m)
        if kind in ("Put", "Delete", "BatchPut"):
            return await self.write(m)
        if kind == "Get":
            return await self.get(m)
        return {"type": "Error", "error": f"unknown message type {kind}"}

    async def write(self, m: Message) -> Message:
        """A client write. Until the router exists (#7) the client names the shard itself."""
        group = self.groups.get(m.get("shard"))
        if group is None:
            return {"type": "Error", "id": m.get("id"), "error": "unknown shard"}
        try:
            command = write_command(m)
        except (KeyError, TypeError, ValueError) as err:
            return {"type": "Error", "id": m.get("id"), "error": f"bad write: {err}"}
        try:
            index = await group.propose(command)
        except NotLeaderError as err:
            return self.redirect(m, err.leader)
        except TimeoutError:
            return {"type": "Error", "id": m.get("id"), "error": "not committed in time"}
        return {"type": "Reply", "id": m.get("id"), "ok": True, "index": index}

    async def get(self, m: Message) -> Message:
        """Edges of one node in both directions, at one of two read levels (#10).

        read "leader": only the leader answers, after ReadIndex has confirmed that a
        majority still follows it, so the answer contains every committed write.
        read "follower": any replica answers from what it has applied. It may miss
        the newest writes, the reply says up to which log index it is.
        """
        group = self.groups.get(m.get("shard"))
        if group is None:
            return {"type": "Error", "id": m.get("id"), "error": "unknown shard"}
        level = m.get("read", "leader")
        if level == "leader":
            try:
                await group.read_index()
            except NotLeaderError as err:
                return self.redirect(m, err.leader)
            except TimeoutError:
                return {"type": "Error", "id": m.get("id"), "error": "leadership not confirmed"}
        elif level != "follower":
            return {"type": "Error", "id": m.get("id"), "error": f"unknown read level {level}"}
        store = self.stores[m["shard"]]
        return {
            "type": "Reply",
            "id": m.get("id"),
            "ok": True,
            "node": self.id,
            "read": level,
            "applied": group.last_applied,
            "facts": store.get(m["s"]),
            "incoming": store.incoming(m["s"]),
        }

    def redirect(self, m: Message, leader: str | None) -> Message:
        """Name the leader, with its address, as a client may not know a node added later."""
        reply: Message = {"type": "Redirect", "id": m.get("id"), "leader": leader}
        if leader in self.addresses:
            reply["address"] = list(self.addresses[leader])
        return reply

    async def change_replicas(self, m: Message) -> Message:
        """Add a node to the replicas of a shard or remove one (#14), sent to the leader."""
        group = self.groups.get(m.get("shard"))
        if group is None:
            return {"type": "Error", "error": "unknown shard"}
        node = str(m.get("node"))
        try:
            if m["type"] == "AddReplica":
                if m.get("address"):
                    self.addresses[node] = protocol.parse_address(str(m["address"]))
                if node not in self.addresses:
                    return {"type": "Error", "error": f"address of {node} unknown"}
                done = await group.add_replica(node, self.addresses)
            else:
                done = await group.remove_replica(node, self.addresses)
        except NotLeaderError as err:
            return self.redirect(m, err.leader)
        except (ValueError, TimeoutError) as err:
            return {"type": "Error", "error": str(err) or "not committed in time"}
        return {"type": "ControlReply", "ok": done, "shard": group.shard, "members": group.members}

    async def transfer(self, m: Message) -> Message:
        """Hand the leadership of one shard to another node (#14), sent to the leader."""
        group = self.groups.get(m.get("shard"))
        if group is None:
            return {"type": "Error", "error": "unknown shard"}
        try:
            done = await group.transfer(str(m.get("target")))
        except NotLeaderError as err:
            return {"type": "Redirect", "leader": err.leader}
        except ValueError as err:
            return {"type": "Error", "error": str(err)}
        return {"type": "ControlReply", "ok": done, "shard": group.shard, "leader": group.leader}

    def control(self, m: Message) -> Message:
        """Pause or resume the node, cut or heal its links to other nodes."""
        action = m.get("action")
        peers = set(m.get("peers") or self.addresses) - {self.id}
        if action == "pause":
            self.paused = True
        elif action == "resume":
            self.paused = False
            for group in self.groups.values():
                group.reset_election_timer()
        elif action == "isolate":
            self.isolated |= peers
        elif action == "heal":
            self.isolated -= peers
        else:
            return {"type": "Error", "error": f"unknown control action {action}"}
        for group in self.groups.values():
            group.paused = self.paused
        self.events.emit(action, peers=sorted(peers) if action in ("isolate", "heal") else None)
        return {
            "type": "ControlReply",
            "ok": True,
            "paused": self.paused,
            "isolated": sorted(self.isolated),
        }

    def status(self) -> Message:
        return {
            "type": "Status",
            "node": self.id,
            "paused": self.paused,
            "isolated": sorted(self.isolated),
            "shards": [g.status() for g in self.groups.values()],
        }


def write_command(m: Message) -> Message:
    """The log entry for a client write.

    A fact from a source system carries the source's version (for Sisu the
    modification ordinal). The store then ignores repeated and older versions.
    """
    if m["type"] == "BatchPut":
        return {"op": "batch", "facts": [fact_command(f.get("op", "put"), f) for f in m["facts"]]}
    return fact_command(m["type"].lower(), m)


def fact_command(op: str, m: Message) -> Message:
    # A reverse edge lives on the shard of its object (#6).
    if m.get("reverse"):
        op = "r" + op
    command = {"op": op, "s": str(m["s"]), "p": str(m["p"]), "o": str(m["o"])}
    if m.get("version") is not None:
        command["version"] = int(m["version"])
    return command


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one knowdes storage node.")
    parser.add_argument("--id", required=True)
    parser.add_argument("--config", type=Path, help="cluster config, see deploy/")
    parser.add_argument("--listen", help="host:port of this node")
    parser.add_argument("--peer", action="append", default=[], help="id=host:port")
    parser.add_argument("--shards", type=int)
    parser.add_argument("--data-dir", type=Path, help="keep term, vote and log here")
    parser.add_argument("--empty", action="store_true", help="start without replicas (#14)")
    args = parser.parse_args()

    if args.config:
        cluster = config.load(args.config)
        if args.id not in cluster.nodes:
            parser.error(f"{args.id} is not in {args.config}")
        addresses = dict(cluster.nodes)
        shards = args.shards or cluster.shards
        data_dir = args.data_dir or cluster.data_dir
    else:
        if not args.listen:
            parser.error("give --config or --listen")
        addresses = {args.id: protocol.parse_address(args.listen)}
        shards = args.shards or 3
        data_dir = args.data_dir
    for peer in args.peer:
        peer_id, _, address = peer.partition("=")
        addresses[peer_id] = protocol.parse_address(address)
    host, port = addresses[args.id]
    if args.config and host not in ("127.0.0.1", "localhost"):
        # On a course machine listen on every interface, as the name may not resolve locally.
        addresses[args.id] = ("0.0.0.0", port)

    async def run() -> None:
        node = StorageNode(args.id, addresses, shards, data_dir, empty=args.empty)
        await node.start()
        try:
            await asyncio.Event().wait()
        finally:
            await node.stop()

    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(run())


if __name__ == "__main__":
    main()
