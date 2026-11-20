"""The state machine behind the Raft log of one shard.

Every replica applies the committed commands in log order, so all of them end
with the same facts. Nothing here is written to disk: after a restart the
node applies its log again.
"""

from __future__ import annotations

from typing import Any

Command = dict[str, Any]


class ShardStore:
    def __init__(self) -> None:
        self.facts: dict[tuple[str, str], set[str]] = {}
        # Highest source version applied per (subject, predicate).
        self.versions: dict[tuple[str, str], int] = {}

    def apply(self, command: Command | None) -> None:
        if command is None:  # the no-op a new leader appends
            return
        if command["op"] == "batch":
            for fact in command["facts"]:
                self.apply(fact)
            return
        key = (command["s"], command["p"])
        version = command.get("version")
        if version is None:
            self._apply_plain(command, key)
            return
        # A write from a source system. Its streams may repeat a change or deliver
        # an old one after a reconnect, so only a newer version changes anything.
        if version <= self.versions.get(key, -1):
            return
        self.versions[key] = version
        if command["op"] == "put":
            # The source holds the current value, so it replaces what was there.
            self.facts[key] = {command["o"]}
        else:
            self.facts.pop(key, None)

    def _apply_plain(self, command: Command, key: tuple[str, str]) -> None:
        if command["op"] == "put":
            self.facts.setdefault(key, set()).add(command["o"])
            return
        objects = self.facts.get(key)
        if objects is not None:
            objects.discard(command["o"])
            if not objects:
                del self.facts[key]

    def get(self, subject: str) -> dict[str, list[str]]:
        return {p: sorted(o) for (s, p), o in self.facts.items() if s == subject}
