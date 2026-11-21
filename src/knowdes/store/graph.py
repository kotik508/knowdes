"""The state machine behind the Raft log of one shard (design plan section 3, issue #6).

Every replica applies the committed commands in log order, so all of them end
with the same facts. Nothing here is written to disk: after a restart the
node applies its log again.

A shard holds the facts whose subject hashes to it and the reverse edges whose
object hashes to it, so Neighbors can walk an edge in both directions by
asking one shard per node. Forward fact and reverse edge sit on different
shards in general. Until two-phase commit (#12) the router writes them one
after the other.
"""

from __future__ import annotations

from typing import Any

Command = dict[str, Any]
# predicate -> objects of one subject, or predicate -> subjects pointing at one object
Edges = dict[str, set[str]]


class ShardStore:
    def __init__(self) -> None:
        self.out: dict[str, Edges] = {}
        self.into: dict[str, Edges] = {}
        # Highest source version applied per (subject, predicate).
        self.versions: dict[tuple[str, str], int] = {}

    def apply(self, command: Command | None) -> None:
        if command is None:  # the no-op a new leader appends
            return
        op = command["op"]
        if op == "batch":
            for fact in command["facts"]:
                self.apply(fact)
            return
        s, p, o = command["s"], command["p"], command["o"]
        if op == "rput":
            _add(self.into, o, p, s)
            return
        if op == "rdelete":
            _discard(self.into, o, p, s)
            return
        version = command.get("version")
        if version is None:
            if op == "put":
                _add(self.out, s, p, o)
            else:
                _discard(self.out, s, p, o)
            return
        # A write from a source system. Its streams may repeat a change or deliver
        # an old one after a reconnect, so only a newer version changes anything.
        key = (s, p)
        if version <= self.versions.get(key, -1):
            return
        self.versions[key] = version
        edges = self.out.setdefault(s, {})
        if op == "put":
            # The source holds the current value, so it replaces what was there.
            edges[p] = {o}
        else:
            edges.pop(p, None)
            if not edges:
                del self.out[s]

    def get(self, subject: str) -> dict[str, list[str]]:
        """Outgoing edges: predicate -> objects."""
        return {p: sorted(o) for p, o in self.out.get(subject, {}).items()}

    def incoming(self, obj: str) -> dict[str, list[str]]:
        """Reverse edges: predicate -> subjects that point at obj."""
        return {p: sorted(s) for p, s in self.into.get(obj, {}).items()}


def _add(index: dict[str, Edges], key: str, p: str, value: str) -> None:
    index.setdefault(key, {}).setdefault(p, set()).add(value)


def _discard(index: dict[str, Edges], key: str, p: str, value: str) -> None:
    edges = index.get(key)
    if edges is None or p not in edges:
        return
    edges[p].discard(value)
    if not edges[p]:
        del edges[p]
    if not edges:
        del index[key]
