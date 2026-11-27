"""Two-phase commit when a participant fails (#12, design plan section 3.3)."""

from test_coordinator_failure import prepare, setup, wait_until
from test_two_phase import two_subjects

from knowdes.sharding import shard_of


async def follower_read(cluster, node, s):
    msg = {"type": "Get", "shard": shard_of(s, cluster.shards), "s": s, "read": "follower"}
    return await cluster.request(node, msg)


async def test_participant_leader_crash_before_the_vote(tmp_path):
    cluster, _, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        shard_b = shard_of(b, 3)
        leaders = await cluster.leaders()
        # The leader of the object's shard is gone when the Prepare comes. The
        # coordinator keeps looking until the shard has a new leader.
        cluster.kill(leaders[shard_b])
        reply = await client.put(a, "room", b)
        assert reply["decision"] == "commit"
        assert (await client.read(b))["incoming"] == {"room": [a]}
    finally:
        cluster.stop()


async def test_participant_restarts_and_catches_up(tmp_path):
    cluster, _, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        shard_b = shard_of(b, 3)
        leaders = await cluster.leaders()
        down = next(n for n in cluster.ports if n != leaders[shard_b])
        cluster.kill(down)
        assert (await client.put(a, "room", b))["decision"] == "commit"
        # The replica that missed prepare and commit gets both from the log.
        cluster.start(down)

        async def caught_up():
            try:
                reply = await follower_read(cluster, down, b)
            except (OSError, TimeoutError):
                return False
            return reply.get("incoming") == {"room": [a]}

        await wait_until(caught_up)
    finally:
        cluster.stop()


async def test_prepared_participant_keeps_its_lock_over_a_restart(tmp_path):
    cluster, _, client = await setup(tmp_path)
    try:
        a, b = two_subjects()
        shard_a, shard_b = shard_of(a, 3), shard_of(b, 3)
        await prepare(cluster, shard_b, "t", [{"op": "rput", "s": a, "p": "room", "o": b}], shard_a)
        # Every node of the participant crashes after the yes vote and comes back.
        cluster.stop()
        cluster.start()
        await cluster.wait_for_leaders(timeout=10.0)
        # Still prepared, so another write of b on that shard is refused at first.
        facts = [{"op": "put", "s": b, "p": "p", "o": "x"}]
        assert (await client.transact(facts))["decision"] == "abort"

        # The coordinator never decided t, so recovery aborts it and frees b.
        async def free():
            return (await client.transact(facts))["decision"] == "commit"

        await wait_until(free)
        assert (await client.read(b))["incoming"] == {}
    finally:
        cluster.stop()
