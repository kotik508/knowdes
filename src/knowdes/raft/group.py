"""One Raft group, the three replicas of one shard (design plan sections 3.4 and 3.5).

A storage node runs one RaftGroup per shard. The group never opens a socket
itself. It gets a send function from the node, so the tests can run several
groups on a simulated network.
"""

from __future__ import annotations

import asyncio
import contextlib
import enum
import random
import time
from collections.abc import Awaitable, Callable
from typing import Any

Message = dict[str, Any]
Send = Callable[[str, Message], Awaitable[Message | None]]

# Range of the random election timeout and the heartbeat interval, section 3.4.
ELECTION_TIMEOUT = (0.15, 0.30)
HEARTBEAT_INTERVAL = 0.05
# How often the timer loop looks at the election deadline.
TICK = 0.01


class Role(enum.StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftGroup:
    """State of one replica in one Raft group."""

    def __init__(
        self,
        shard: int,
        node_id: str,
        members: list[str],
        send: Send,
        rng: random.Random | None = None,
    ) -> None:
        self.shard = shard
        self.id = node_id
        self.peers = [m for m in members if m != node_id]
        self.send = send
        self.rng = rng or random.Random()

        # Persistent state in the Raft paper. Kept in memory for now.
        self.term = 0
        self.voted_for: str | None = None

        # Volatile state.
        self.role = Role.FOLLOWER
        self.leader: str | None = None
        self.deadline = 0.0
        self.reset_election_timer()
        self._tasks: list[asyncio.Task] = []

    @property
    def majority(self) -> int:
        return (len(self.peers) + 1) // 2 + 1

    def reset_election_timer(self) -> None:
        self.deadline = time.monotonic() + self.rng.uniform(*ELECTION_TIMEOUT)

    def election_due(self) -> bool:
        return self.role is not Role.LEADER and time.monotonic() >= self.deadline

    def observe_term(self, term: int) -> bool:
        """Apply the rule for all servers: a newer term turns us into a follower.

        Returns True when the term was newer.
        """
        if term <= self.term:
            return False
        self.term = term
        self.voted_for = None
        self.become_follower(None)
        return True

    def become_follower(self, leader: str | None) -> None:
        self.role = Role.FOLLOWER
        self.leader = leader
        self.reset_election_timer()

    def become_candidate(self) -> None:
        self.term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.id
        self.leader = None
        self.reset_election_timer()

    def become_leader(self) -> None:
        if self.role is not Role.CANDIDATE:
            raise RuntimeError("only a candidate can become leader")
        self.role = Role.LEADER
        self.leader = self.id
        self._spawn(self._heartbeats(self.term))

    @property
    def last_index(self) -> int:
        return 0

    @property
    def last_term(self) -> int:
        return 0

    # Timer and background tasks.

    def start(self) -> None:
        self._spawn(self._timer())

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()

    def _spawn(self, coro) -> None:
        self._tasks = [t for t in self._tasks if not t.done()]
        self._tasks.append(asyncio.create_task(coro))

    async def _timer(self) -> None:
        while True:
            await asyncio.sleep(TICK)
            if self.election_due():
                await self.run_election()

    # Election, section 3.4.

    async def run_election(self) -> None:
        self.become_candidate()
        term = self.term
        request = {
            "type": "RequestVote",
            "shard": self.shard,
            "term": term,
            "candidate": self.id,
            "last_index": self.last_index,
            "last_term": self.last_term,
        }
        replies = await asyncio.gather(*(self.send(p, request) for p in self.peers))
        votes = 1
        for reply in replies:
            if reply is None:
                continue
            if self.observe_term(reply["term"]):
                return
            votes += bool(reply.get("granted"))
        # A heartbeat from a new leader or a newer term may have arrived meanwhile.
        if self.role is Role.CANDIDATE and self.term == term and votes >= self.majority:
            self.become_leader()

    async def _heartbeats(self, term: int) -> None:
        while self.role is Role.LEADER and self.term == term:
            await asyncio.gather(*(self._replicate(p) for p in self.peers))
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _replicate(self, peer: str) -> None:
        request = {
            "type": "AppendEntries",
            "shard": self.shard,
            "term": self.term,
            "leader": self.id,
        }
        reply = await self.send(peer, request)
        if reply is not None:
            self.observe_term(reply["term"])

    # Handlers for messages from the other replicas.

    def on_request_vote(self, m: Message) -> Message:
        self.observe_term(m["term"])
        log_ok = (m["last_term"], m["last_index"]) >= (self.last_term, self.last_index)
        granted = m["term"] == self.term and self.voted_for in (None, m["candidate"]) and log_ok
        if granted:
            self.voted_for = m["candidate"]
            self.reset_election_timer()
        return {"type": "VoteReply", "shard": self.shard, "term": self.term, "granted": granted}

    def on_append_entries(self, m: Message) -> Message:
        self.observe_term(m["term"])
        if m["term"] < self.term:
            return {"type": "AppendReply", "shard": self.shard, "term": self.term, "success": False}
        # Same term: there is exactly one leader, so a candidate gives up.
        self.become_follower(m["leader"])
        return {"type": "AppendReply", "shard": self.shard, "term": self.term, "success": True}

    def status(self) -> Message:
        return {
            "shard": self.shard,
            "role": str(self.role),
            "term": self.term,
            "leader": self.leader,
        }
