"""A follower that was down gets the missing entries at once (issue #9)."""

import asyncio

from cluster import LocalCluster


async def shard_state(cluster, shard):
    return {n: st["shards"][shard] for n, st in (await cluster.status()).items()}


async def test_follower_down_for_100_writes_catches_up_within_a_second(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        follower = next(n for n in cluster.ports if n != leaders[0])
        cluster.kill(follower)
        last = 0
        for i in range(100):
            reply = await cluster.put(0, f"course{i}")
            assert reply["ok"], reply
            last = reply["index"]

        # The old leader goes as well, so the third node leads and knows nothing
        # about the restarted follower. It starts with next_index at its log end.
        cluster.kill(leaders[0])
        cluster.start(follower)
        loop = asyncio.get_running_loop()
        await cluster.wait_for_leaders()
        started = loop.time()
        while (await shard_state(cluster, 0)).get(follower, {}).get("commit_index", 0) < last:
            assert loop.time() - started < 1.0, await shard_state(cluster, 0)
            await asyncio.sleep(0.02)
    finally:
        cluster.stop()
