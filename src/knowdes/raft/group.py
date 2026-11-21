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

from knowdes.raft.storage import MemoryStorage

Message = dict[str, Any]
Send = Callable[[str, Message], Awaitable[Message | None]]
# Called once for every committed entry, in log order: apply(index, command).
Apply = Callable[[int, Message | None], None]

# Range of the random election timeout and the heartbeat interval, section 3.4.
ELECTION_TIMEOUT = (0.15, 0.30)
HEARTBEAT_INTERVAL = 0.05
# How long a client write waits for its entry to be committed.
COMMIT_TIMEOUT = 1.0
# How often the timer loop looks at the election deadline.
TICK = 0.01


class Role(enum.StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class NotLeaderError(Exception):
    """Raised for a client write at a replica that is not the leader."""

    def __init__(self, leader: str | None) -> None:
        super().__init__(f"not the leader, leader is {leader}")
        self.leader = leader


class RaftGroup:
    """State of one replica in one Raft group."""

    def __init__(
        self,
        shard: int,
        node_id: str,
        members: list[str],
        send: Send,
        apply: Apply | None = None,
        storage=None,
        rng: random.Random | None = None,
    ) -> None:
        self.shard = shard
        self.id = node_id
        self.peers = [m for m in members if m != node_id]
        self.send = send
        self.apply = apply or (lambda index, command: None)
        self.rng = rng or random.Random()

        # Persistent state in the Raft paper, loaded from the storage.
        self.storage = storage or MemoryStorage()
        term, voted_for, entries = self.storage.load()
        self.term = term
        self.voted_for: str | None = voted_for
        # Index 0 is a dummy entry, so the first real entry has index 1 as in the paper.
        self.log: list[Message] = [{"term": 0, "command": None}, *entries]

        # Volatile state.
        self.role = Role.FOLLOWER
        self.leader: str | None = None
        self.deadline = 0.0
        self.reset_election_timer()
        self.commit_index = 0
        self.last_applied = 0
        # Leader only: next entry to send and highest entry known to be stored, per peer.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}
        self._waiters: dict[int, asyncio.Future] = {}
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
        self._save_state()
        self.become_follower(None)
        return True

    def become_follower(self, leader: str | None) -> None:
        if self.role is Role.LEADER:
            self._fail_waiters(leader)
        self.role = Role.FOLLOWER
        self.leader = leader
        self.reset_election_timer()

    def become_candidate(self) -> None:
        self.term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.id
        self._save_state()
        self.leader = None
        self.reset_election_timer()

    def become_leader(self) -> None:
        if self.role is not Role.CANDIDATE:
            raise RuntimeError("only a candidate can become leader")
        self.role = Role.LEADER
        self.leader = self.id
        self.next_index = {p: self.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        # An empty entry of the new term lets the leader commit everything before it
        # (Raft paper, section 5.4.2).
        self._append_entries(self.last_index + 1, [{"term": self.term, "command": None}])
        self._spawn(self._heartbeats(self.term))

    @property
    def last_index(self) -> int:
        return len(self.log) - 1

    @property
    def last_term(self) -> int:
        return self.log[-1]["term"]

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
        """Send the entries the peer is missing. Without missing entries it is a heartbeat."""
        term = self.term
        prev = self.next_index[peer] - 1
        request = {
            "type": "AppendEntries",
            "shard": self.shard,
            "term": term,
            "leader": self.id,
            "prev_index": prev,
            "prev_term": self.log[prev]["term"],
            "entries": self.log[prev + 1 :],
            "leader_commit": self.commit_index,
        }
        reply = await self.send(peer, request)
        if reply is None or self.observe_term(reply["term"]):
            return
        if self.role is not Role.LEADER or self.term != term:
            return
        if reply["success"]:
            match = prev + len(request["entries"])
            self.match_index[peer] = max(self.match_index[peer], match)
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit()
        else:
            # The logs differ at prev. Step back one entry and try again next time.
            self.next_index[peer] = max(1, prev)

    def _advance_commit(self) -> None:
        for index in range(self.last_index, self.commit_index, -1):
            stored = 1 + sum(m >= index for m in self.match_index.values())
            # Only entries of the current term are committed by counting (section 5.4.2).
            if stored >= self.majority and self.log[index]["term"] == self.term:
                self.commit_index = index
                self._apply_committed()
                return

    def _apply_committed(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            self.apply(self.last_applied, self.log[self.last_applied]["command"])
            waiter = self._waiters.pop(self.last_applied, None)
            if waiter is not None and not waiter.done():
                waiter.set_result(self.last_applied)

    def _fail_waiters(self, leader: str | None) -> None:
        for waiter in self._waiters.values():
            if not waiter.done():
                waiter.set_exception(NotLeaderError(leader))
        self._waiters.clear()

    # Client writes.

    async def propose(self, command: Message) -> int:
        """Append a command and wait until a majority stores it. Returns its log index."""
        if self.role is not Role.LEADER:
            raise NotLeaderError(self.leader)
        self._append_entries(self.last_index + 1, [{"term": self.term, "command": command}])
        index = self.last_index
        waiter = asyncio.get_running_loop().create_future()
        self._waiters[index] = waiter
        # Do not wait for the next heartbeat.
        await asyncio.gather(*(self._replicate(p) for p in self.peers))
        try:
            return await asyncio.wait_for(asyncio.shield(waiter), COMMIT_TIMEOUT)
        except TimeoutError:
            self._waiters.pop(index, None)
            raise

    # Handlers for messages from the other replicas.

    def on_request_vote(self, m: Message) -> Message:
        self.observe_term(m["term"])
        log_ok = (m["last_term"], m["last_index"]) >= (self.last_term, self.last_index)
        granted = m["term"] == self.term and self.voted_for in (None, m["candidate"]) and log_ok
        if granted:
            self.voted_for = m["candidate"]
            self._save_state()
            self.reset_election_timer()
        return {"type": "VoteReply", "shard": self.shard, "term": self.term, "granted": granted}

    def on_append_entries(self, m: Message) -> Message:
        self.observe_term(m["term"])
        if m["term"] < self.term:
            return {"type": "AppendReply", "shard": self.shard, "term": self.term, "success": False}
        # Same term: there is exactly one leader, so a candidate gives up.
        self.become_follower(m["leader"])
        reply = {"type": "AppendReply", "shard": self.shard, "term": self.term}
        prev = m["prev_index"]
        if prev > self.last_index or self.log[prev]["term"] != m["prev_term"]:
            return {**reply, "success": False, "last_index": self.last_index}
        for offset, entry in enumerate(m["entries"]):
            index = prev + 1 + offset
            if index <= self.last_index and self.log[index]["term"] == entry["term"]:
                continue
            # From here on the entries are new or replace a conflicting entry and
            # everything after it, which were never committed.
            self._append_entries(index, m["entries"][offset:])
            break
        last_new = prev + len(m["entries"])
        if m["leader_commit"] > self.commit_index:
            self.commit_index = min(m["leader_commit"], last_new)
            self._apply_committed()
        return {**reply, "success": True, "match_index": last_new}

    # Persistence. Raft requires both to be on disk before the node answers.

    def _save_state(self) -> None:
        self.storage.save_state(self.term, self.voted_for)

    def _append_entries(self, index: int, entries: list[Message]) -> None:
        self.storage.append(index, entries)
        del self.log[index:]
        self.log.extend(entries)

    def status(self) -> Message:
        return {
            "shard": self.shard,
            "role": str(self.role),
            "term": self.term,
            "leader": self.leader,
            "last_index": self.last_index,
            "commit_index": self.commit_index,
        }
