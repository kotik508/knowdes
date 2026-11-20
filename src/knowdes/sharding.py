"""Subject to shard (design plan section 3.1).

The prototype hashes straight onto its three Raft groups. The virtual shards
of the full design come with the placement service (#14).
"""

from __future__ import annotations

import zlib


def shard_of(subject: str, shards: int) -> int:
    return zlib.crc32(subject.encode()) % shards
