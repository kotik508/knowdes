"""Writes survive a restart of the nodes (issue #3)."""

import asyncio

from cluster import LocalCluster


async def wait_until(check, timeout=5.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not await check():
        assert loop.time() < end, "condition not reached in time"
        await asyncio.sleep(0.05)


async def shard_state(cluster, shard):
    return {n: st["shards"][shard] for n, st in (await cluster.status()).items()}


async def test_writes_committed_on_two_nodes_survive_full_restart(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        follower = next(n for n in cluster.ports if n != leaders[0])
        cluster.kill(follower)
        indices = [(await cluster.put(0, f"course{i}"))["index"] for i in range(20)]

        # Followers learn the commit index with the next heartbeat.
        async def both_committed():
            state = await shard_state(cluster, 0)
            return all(s["commit_index"] >= indices[-1] for s in state.values())

        await wait_until(both_committed)
        before = await shard_state(cluster, 0)

        cluster.stop()
        cluster.start()
        await cluster.wait_for_leaders()

        async def all_caught_up():
            state = await shard_state(cluster, 0)
            return len(state) == 3 and all(s["commit_index"] > indices[-1] for s in state.values())

        # The node that was down for the 20 writes gets them from the new leader.
        await wait_until(all_caught_up)
        after = await shard_state(cluster, 0)
        assert min(s["term"] for s in after.values()) > max(s["term"] for s in before.values())
    finally:
        cluster.stop()


async def test_restarted_follower_keeps_its_log(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        for i in range(5):
            assert (await cluster.put(1, f"room{i}"))["ok"]
        follower = next(n for n in cluster.ports if n != leaders[1])
        before = (await shard_state(cluster, 1))[follower]["last_index"]
        cluster.kill(follower)
        cluster.start(follower)

        async def back_with_log():
            state = (await shard_state(cluster, 1)).get(follower)
            return state is not None and state["last_index"] >= before

        await wait_until(back_with_log)
    finally:
        cluster.stop()
