"""Three storage nodes as real processes on localhost, for tests over TCP."""

from __future__ import annotations

import asyncio
import socket
import subprocess
import sys

from knowdes import protocol


def free_port() -> int:
    return free_ports(1)[0]


def free_ports(count: int) -> list[int]:
    """Distinct free ports. All sockets stay open until the last one is bound,
    otherwise the system may hand out the same port twice."""
    sockets = [socket.socket() for _ in range(count)]
    try:
        for s in sockets:
            s.bind(("127.0.0.1", 0))
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


class LocalCluster:
    def __init__(self, ids=("n1", "n2", "n3"), shards=3, data_dir=None):
        self.ports = dict(zip(ids, free_ports(len(ids)), strict=True))
        self.shards = shards
        self.data_dir = data_dir
        self.procs: dict[str, subprocess.Popen] = {}

    def args(self, node_id):
        peers = [f"--peer={i}=127.0.0.1:{p}" for i, p in self.ports.items() if i != node_id]
        listen = f"127.0.0.1:{self.ports[node_id]}"
        args = ["--id", node_id, "--listen", listen, "--shards", str(self.shards), *peers]
        if self.data_dir is not None:
            args += ["--data-dir", str(self.data_dir)]
        return args

    def start(self, node_id=None):
        for i in [node_id] if node_id else list(self.ports):
            cmd = [sys.executable, "-m", "knowdes.node", *self.args(i)]
            self.procs[i] = subprocess.Popen(cmd, stderr=subprocess.DEVNULL)

    def kill(self, node_id):
        proc = self.procs.pop(node_id)
        proc.kill()
        proc.wait()

    def stop(self):
        for proc in self.procs.values():
            proc.kill()
            proc.wait()
        self.procs.clear()

    async def request(self, node_id, msg, timeout=2.0):
        return await protocol.request(("127.0.0.1", self.ports[node_id]), msg, timeout)

    async def status(self):
        """Status of every running node that answers, by node id."""
        result = {}
        for i in self.procs:
            try:
                result[i] = await self.request(i, {"type": "Status"}, 0.5)
            except (OSError, TimeoutError):
                pass
        return result

    async def leaders(self):
        """Leader of every shard, None while unclear.

        A cut-off old leader still thinks it leads, so the leader of the highest term counts.
        """
        status = await self.status()
        found = {}
        for shard in range(self.shards):
            claims = sorted(
                (st["shards"][shard]["term"], node)
                for node, st in status.items()
                if st["shards"][shard]["role"] == "leader"
            )
            unique = claims and (len(claims) == 1 or claims[-1][0] > claims[-2][0])
            found[shard] = claims[-1][1] if unique else None
        return found

    async def wait_for_leaders(self, timeout=5.0, avoid=None):
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            leaders = await self.leaders()
            if all(v is not None and v != avoid for v in leaders.values()):
                return leaders
            await asyncio.sleep(0.05)
        raise AssertionError(f"no leader for every shard: {await self.status()}")

    async def put(self, shard, s, p="p", o="o"):
        """Write through any node and follow one redirect, like the router will."""
        msg = {"type": "Put", "shard": shard, "s": s, "p": p, "o": o}
        reply = None
        for node in self.procs:
            try:
                reply = await self.request(node, msg)
            except (OSError, TimeoutError, ValueError):
                continue  # a paused node closes the connection without an answer
            if reply["type"] == "Redirect":
                reply = await self.request(reply["leader"], msg)
            if reply.get("ok"):
                return reply
        return reply

    async def control(self, node_id, action, peers=None):
        return await self.request(node_id, {"type": "Control", "action": action, "peers": peers})
