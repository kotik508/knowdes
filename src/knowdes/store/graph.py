"""The state machine behind the Raft log of one shard (design plan section 3, issue #6).

Every replica applies the committed commands in log order, so all of them end
with the same facts. Nothing here is written to disk: after a restart the
node applies its log again.

A shard holds the facts whose subject hashes to it and the reverse edges whose
object hashes to it, so Neighbors can walk an edge in both directions by
asking one shard per node. Forward fact and reverse edge sit on different
shards in general. Until two-phase commit (#12) the router writes them one
after the other.

Two-phase commit (#12) keeps its records here as well, so they are replicated
and survive a leader change like any fact. A participant shard holds a prepared
transaction with its facts and locks the nodes they touch until the decision
comes. The coordinator shard holds the decision with the list of participants,
which a new coordinator needs to finish what the old one started.
"""

from __future__ import annotations

from typing import Any

Command = dict[str, Any]
# predicate -> objects of one subject, or predicate -> subjects pointing at one object
Edges = dict[str, set[str]]

# States of a transaction on a participant, in the order they can happen.
PREPARED, REFUSED, COMMITTED, ABORTED = "prepared", "refused", "committed", "aborted"
TX_OPS = ("prepare", "commit", "abort", "decide")


class ShardStore:
    def __init__(self) -> None:
        self.out: dict[str, Edges] = {}
        self.into: dict[str, Edges] = {}
        # Highest source version applied per (subject, predicate).
        self.versions: dict[tuple[str, str], int] = {}
        # Participant side: tx id -> {"state", "facts"}.
        self.txs: dict[str, Command] = {}
        # Node -> id of the prepared transaction that holds it.
        self.locks: dict[str, str] = {}
        # Coordinator side: tx id -> {"decision", "participants"}.
        self.decisions: dict[str, Command] = {}

    def apply(self, command: Command | None) -> None:
        if command is None:  # the no-op a new leader appends
            return
        op = command["op"]
        if op == "batch":
            for fact in command["facts"]:
                self.apply(fact)
            return
        if op in TX_OPS:
            self._apply_tx(op, command)
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

    # Two-phase commit records (#12). Every replica applies them in log order, so
    # all of them reach the same state and the same answer to a prepare.

    def _apply_tx(self, op: str, command: Command) -> None:
        tx = str(command["tx"])
        if op == "decide":
            # The first decision is final, a repeated one changes nothing.
            self.decisions.setdefault(
                tx,
                {"decision": command["decision"], "participants": list(command["participants"])},
            )
            return
        record = self.txs.get(tx)
        if op == "prepare":
            if record is not None:
                return
            facts = list(command["facts"])
            keys = {_lock_key(f) for f in facts}
            busy = any(self.locks.get(k, tx) != tx for k in keys)
            self.txs[tx] = {"state": REFUSED if busy else PREPARED, "facts": facts}
            if not busy:
                for key in keys:
                    self.locks[key] = tx
            return
        if record is None:
            # A decision for a transaction this shard never prepared: remember it,
            # so a late prepare cannot lock anything any more.
            state = COMMITTED if op == "commit" else ABORTED
            self.txs[tx] = {"state": state, "facts": []}
            return
        if record["state"] != PREPARED:
            return
        if op == "commit":
            for fact in record["facts"]:
                self.apply(fact)
        record["state"] = COMMITTED if op == "commit" else ABORTED
        self.locks = {k: v for k, v in self.locks.items() if v != tx}

    def transaction(self, tx: str) -> str | None:
        """State of a transaction on this shard, None if it never got here."""
        record = self.txs.get(tx)
        return record["state"] if record else None

    def open_transactions(self) -> list[str]:
        """Prepared transactions still waiting for their decision."""
        return sorted(t for t, r in self.txs.items() if r["state"] == PREPARED)

    def decision(self, tx: str) -> Command | None:
        return self.decisions.get(tx)

    def get(self, subject: str) -> dict[str, list[str]]:
        """Outgoing edges: predicate -> objects."""
        return {p: sorted(o) for p, o in self.out.get(subject, {}).items()}

    def incoming(self, obj: str) -> dict[str, list[str]]:
        """Reverse edges: predicate -> subjects that point at obj."""
        return {p: sorted(s) for p, s in self.into.get(obj, {}).items()}


def _lock_key(fact: Command) -> str:
    """The node a fact changes on this shard: the subject, for a reverse edge the object."""
    return fact["o"] if fact["op"] in ("rput", "rdelete") else fact["s"]


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
