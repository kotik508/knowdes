"""Persistent Raft state: term, vote and log (design plan section 3.5, decision in issue #1).

Raft needs these three to survive a crash, or a restarted node could vote twice
in one term or forget entries it already confirmed. All shards of one node
share one SQLite file. Every change is written before the node answers.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

Entry = dict[str, Any]


class MemoryStorage:
    """Keeps nothing across restarts. Used by tests and when no data directory is given."""

    def __init__(self) -> None:
        self.term = 0
        self.voted_for: str | None = None
        self.entries: list[Entry] = []

    def load(self) -> tuple[int, str | None, list[Entry]]:
        return self.term, self.voted_for, list(self.entries)

    def save_state(self, term: int, voted_for: str | None) -> None:
        self.term, self.voted_for = term, voted_for

    def append(self, index: int, entries: list[Entry]) -> None:
        del self.entries[index - 1 :]
        self.entries.extend(entries)


def open_database(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    # FULL syncs the WAL on every commit, so a confirmed entry is on disk.
    db.execute("PRAGMA synchronous=FULL")
    db.execute(
        "CREATE TABLE IF NOT EXISTS raft_state ("
        "shard INTEGER PRIMARY KEY, term INTEGER NOT NULL, voted_for TEXT)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS raft_log ("
        "shard INTEGER NOT NULL, idx INTEGER NOT NULL, term INTEGER NOT NULL, "
        "command TEXT, PRIMARY KEY (shard, idx))"
    )
    return db


class SqliteStorage:
    """Term, vote and log of one shard in a SQLite database shared by the node."""

    def __init__(self, db: sqlite3.Connection, shard: int) -> None:
        self.db = db
        self.shard = shard

    def load(self) -> tuple[int, str | None, list[Entry]]:
        row = self.db.execute(
            "SELECT term, voted_for FROM raft_state WHERE shard = ?", (self.shard,)
        ).fetchone()
        term, voted_for = row if row else (0, None)
        rows = self.db.execute(
            "SELECT term, command FROM raft_log WHERE shard = ? ORDER BY idx", (self.shard,)
        ).fetchall()
        entries = [{"term": t, "command": json.loads(c) if c else None} for t, c in rows]
        return term, voted_for, entries

    def save_state(self, term: int, voted_for: str | None) -> None:
        self.db.execute(
            "INSERT OR REPLACE INTO raft_state (shard, term, voted_for) VALUES (?, ?, ?)",
            (self.shard, term, voted_for),
        )

    def append(self, index: int, entries: list[Entry]) -> None:
        """Store entries from log index on, replacing whatever was there."""
        rows = [
            (self.shard, index + i, e["term"], json.dumps(e["command"]) if e["command"] else None)
            for i, e in enumerate(entries)
        ]
        with self.db:
            self.db.execute("BEGIN")
            self.db.execute(
                "DELETE FROM raft_log WHERE shard = ? AND idx >= ?", (self.shard, index)
            )
            self.db.executemany("INSERT INTO raft_log VALUES (?, ?, ?, ?)", rows)
