"""Raft consensus for the shards (design plan section 3.4)."""

from knowdes.raft.group import NotLeaderError, RaftGroup, Role

__all__ = ["NotLeaderError", "RaftGroup", "Role"]
