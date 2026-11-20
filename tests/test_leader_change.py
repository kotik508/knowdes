"""What a new leader knows about entries of the old one (design plan section 3.3).

Two-phase commit relies on this: a new coordinator only finishes a transaction
whose decision is committed and aborts every other open one.
"""

import asyncio
import sqlite3

from cluster import LocalCluster


def commands(data_dir, node, shard=0):
    db = sqlite3.connect(data_dir / f"{node}.db")
    try:
        rows = db.execute(
            "SELECT command FROM raft_log WHERE shard = ? ORDER BY idx", (shard,)
        ).fetchall()
    finally:
        db.close()
    return [c for (c,) in rows if c]


async def wait_until(check, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not await check():
        assert loop.time() < end, "condition not reached in time"
        await asyncio.sleep(0.05)


async def test_committed_entry_is_kept_by_new_leader(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        old = (await cluster.wait_for_leaders())[0]
        assert (await cluster.put(0, "prepare tx1"))["ok"]
        cluster.kill(old)
        new = (await cluster.wait_for_leaders(avoid=old))[0]
        assert any("prepare tx1" in c for c in commands(tmp_path, new))
    finally:
        cluster.stop()


async def test_entry_without_majority_is_dropped(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        old = (await cluster.wait_for_leaders())[0]
        rest = [n for n in cluster.ports if n != old]
        await cluster.control(old, "isolate", rest)
        for n in rest:
            await cluster.control(n, "isolate", [old])

        msg = {"type": "Put", "shard": 0, "s": "prepare tx2", "p": "p", "o": "o"}
        reply = await cluster.request(old, msg, timeout=3.0)
        assert reply == {"type": "Error", "id": None, "error": "not committed in time"}
        assert any("prepare tx2" in c for c in commands(tmp_path, old))

        async def new_leader():
            status = await cluster.status()
            shard = {n: status[n]["shards"][0] for n in status}
            return next(
                (
                    n
                    for n in rest
                    if shard[n]["role"] == "leader" and shard[n]["term"] > shard[old]["term"]
                ),
                None,
            )

        await wait_until(new_leader)
        # Write through the new leader directly: the cut off one would also take
        # the write into its own log and the check below would pass too early.
        msg = {"type": "Put", "shard": 0, "s": "after tx2", "p": "p", "o": "o"}
        assert (await cluster.request(await new_leader(), msg, timeout=3.0))["ok"]
        await cluster.control(old, "heal", rest)
        for n in rest:
            await cluster.control(n, "heal", [old])

        async def replaced():
            return any("after tx2" in c for c in commands(tmp_path, old))

        await wait_until(replaced)
        for node in cluster.ports:
            assert not any("prepare tx2" in c for c in commands(tmp_path, node))
    finally:
        cluster.stop()
