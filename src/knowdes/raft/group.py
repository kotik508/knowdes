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
# Reports an event for the log file and the console: emit(kind, shard, **details).
Emit = Callable[..., Any]

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
        emit: Emit | None = None,
        rng: random.Random | None = None,
    ) -> None:
        self.shard = shard
        self.id = node_id
        self.peers = [m for m in members if m != node_id]
        self.send = send
        self.apply = apply or (lambda index, command: None)
        self._emit = emit or (lambda kind, shard, **details: None)
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
        # A paused replica neither starts elections nor sends heartbeats (Control, section 5).
        self.paused = False
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
        if self.role is not Role.FOLLOWER or self.leader != leader:
            self.event("follower", leader=leader)
        self.role = Role.FOLLOWER
        self.leader = leader
        self.reset_election_timer()

    def become_candidate(self) -> None:
        self.term += 1
        self.role = Role.CANDIDATE
        self.voted_for = self.id
        self._save_state()
        self.leader = None
        self.event("election")
        self.reset_election_timer()

    def become_leader(self) -> None:
        if self.role is not Role.CANDIDATE:
            raise RuntimeError("only a candidate can become leader")
        self.role = Role.LEADER
        self.leader = self.id
        self.event("leader")
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
            if not self.paused and self.election_due():
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
            if not self.paused:
                await asyncio.gather(*(self._replicate(p) for p in self.peers))
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _replicate(self, peer: str) -> bool:
        """Send the entries the peer is missing. Without missing entries it is a heartbeat.

        Returns True when the peer answered in this term, so it still takes us as leader.
        """
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
            return False
        if self.role is not Role.LEADER or self.term != term:
            return False
        if reply["success"]:
            match = prev + len(request["entries"])
            self.match_index[peer] = max(self.match_index[peer], match)
            self.next_index[peer] = self.match_index[peer] + 1
            self._advance_commit()
            return True
        # The logs differ at prev. A follower that was down reports where its log ends
        # and where its entries of the conflicting term start, so the leader jumps there
        # at once instead of stepping back one entry per heartbeat (#9).
        back = min(prev, reply.get("last_index", prev - 1) + 1, reply.get("conflict_index", prev))
        self.next_index[peer] = max(1, back)
        # Send the missing entries now and not with the next heartbeat.
        if self.next_index[peer] < prev:
            return await self._replicate(peer)
        return True

    def _advance_commit(self) -> None:
        for index in range(self.last_index, self.commit_index, -1):
            stored = 1 + sum(m >= index for m in self.match_index.values())
            # Only entries of the current term are committed by counting (section 5.4.2).
            if stored >= self.majority and self.log[index]["term"] == self.term:
                self.commit_index = index
                self.event("commit", replicas=stored)
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

    # Reads, section 3.3 (#10).

    async def read_index(self) -> int:
        """The index a leader read has to wait for (ReadIndex, Raft thesis section 6.4).

        A leader cut off from the others still thinks it leads. Before it answers it
        checks with one round of heartbeats that a majority still follows it, and it
        only counts once it has committed an entry of its own term.
        """
        if self.role is not Role.LEADER:
            raise NotLeaderError(self.leader)
        term = self.term
        loop = asyncio.get_running_loop()
        end = loop.time() + COMMIT_TIMEOUT
        while self.log[self.commit_index]["term"] != term:
            if self.role is not Role.LEADER or self.term != term:
                raise NotLeaderError(self.leader)
            if loop.time() > end:
                raise TimeoutError
            await asyncio.sleep(TICK)
        index = self.commit_index
        acks = await asyncio.gather(*(self._replicate(p) for p in self.peers))
        if self.role is not Role.LEADER or self.term != term:
            raise NotLeaderError(self.leader)
        if 1 + sum(acks) < self.majority:
            raise TimeoutError
        while self.last_applied < index:
            await asyncio.sleep(TICK)
        return index

    # Handlers for messages from the other replicas.

    def on_request_vote(self, m: Message) -> Message:
        self.observe_term(m["term"])
        log_ok = (m["last_term"], m["last_index"]) >= (self.last_term, self.last_index)
        granted = m["term"] == self.term and self.voted_for in (None, m["candidate"]) and log_ok
        if granted:
            self.voted_for = m["candidate"]
            self._save_state()
            self.event("vote", candidate=m["candidate"])
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
        if prev > self.last_index:
            return {**reply, "success": False, "last_index": self.last_index}
        if self.log[prev]["term"] != m["prev_term"]:
            # Everything of the conflicting term is suspect, so name where it starts.
            conflict, conflict_term = prev, self.log[prev]["term"]
            while conflict > 1 and self.log[conflict - 1]["term"] == conflict_term:
                conflict -= 1
            return {
                **reply,
                "success": False,
                "last_index": self.last_index,
                "conflict_index": conflict,
            }
        for offset, entry in enumerate(m["entries"]):
            index = prev + 1 + offset
            if index <= self.last_index and self.log[index]["term"] == entry["term"]:
                continue
            # From here on the entries are new or replace a conflicting entry and
            # everything after it, which were never committed.
            self._append_entries(index, m["entries"][offset:])
            break
        last_new = prev + len(m["entries"])
        # A heartbeat may carry fewer entries than we have, never step the commit back.
        commit = min(m["leader_commit"], last_new)
        if commit > self.commit_index:
            self.commit_index = commit
            self._apply_committed()
        return {**reply, "success": True, "match_index": last_new}

    def event(self, kind: str, **details: Any) -> None:
        self._emit(kind, self.shard, term=self.term, index=self.commit_index, **details)

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
            "last_applied": self.last_applied,
        }
