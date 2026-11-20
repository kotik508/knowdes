"""One Raft group, the three replicas of one shard (design plan sections 3.4 and 3.5).

A storage node runs one RaftGroup per shard. The group never opens a socket
itself. It gets a send function from the node, so the tests can run several
groups on a simulated network.
"""

from __future__ import annotations

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

    def status(self) -> Message:
        return {
            "shard": self.shard,
            "role": str(self.role),
            "term": self.term,
            "leader": self.leader,
        }
