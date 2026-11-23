"""Simulated network for Raft tests: groups in one process, messages as function calls."""

from __future__ import annotations

import asyncio

from knowdes.raft import RaftGroup, Role


class Network:
    def __init__(self, ids=("a", "b", "c"), shard=0):
        self.down: set[str] = set()
        self.groups = {i: RaftGroup(shard, i, list(ids), self._sender(i)) for i in ids}

    def _sender(self, source):
        async def send(peer, msg):
            await asyncio.sleep(0.001)
            if source in self.down or peer in self.down:
                return None
            target = self.groups[peer]
            if msg["type"] == "RequestVote":
                return target.on_request_vote(msg)
            if msg["type"] == "AppendEntries":
                return target.on_append_entries(msg)
            if msg["type"] == "TimeoutNow":
                return target.on_timeout_now(msg)
            raise AssertionError(msg["type"])

        return send

    def start(self):
        for g in self.groups.values():
            g.start()

    async def stop(self):
        for g in self.groups.values():
            await g.stop()

    def leaders(self):
        return [g for i, g in self.groups.items() if i not in self.down and g.role is Role.LEADER]

    async def wait_for_leader(self, timeout=3.0, newer_than=0):
        loop = asyncio.get_running_loop()
        end = loop.time() + timeout
        while loop.time() < end:
            leaders = [g for g in self.leaders() if g.term > newer_than]
            if len(leaders) == 1:
                return leaders[0]
            await asyncio.sleep(0.01)
        raise AssertionError(
            f"no single leader, roles {[g.status() for g in self.groups.values()]}"
        )
