"""Leader reads and follower reads (issue #10)."""

from cluster import LocalCluster


def get(shard, s, read):
    return {"type": "Get", "shard": shard, "s": s, "read": read}


async def test_leader_read_sees_the_write_and_a_cut_off_leader_does_not_answer(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        leader = leaders[0]
        assert (await cluster.put(0, "CSM13001", "teacher", "niklander"))["ok"]
        reply = await cluster.request(leader, get(0, "CSM13001", "leader"))
        assert reply["facts"] == {"teacher": ["niklander"]}

        # Cut off from both followers the old leader still thinks it leads, but
        # it cannot confirm that, so it must not answer a leader read.
        await cluster.control(leader, "isolate")
        reply = await cluster.request(leader, get(0, "CSM13001", "leader"))
        assert reply == {"type": "Error", "id": None, "error": "leadership not confirmed"}

        # A follower read asks nobody and answers from the local copy.
        reply = await cluster.request(leader, get(0, "CSM13001", "follower"))
        assert reply["facts"] == {"teacher": ["niklander"]}
        assert reply["read"] == "follower"
    finally:
        cluster.stop()


async def test_follower_read_can_lag_behind(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    try:
        leaders = await cluster.wait_for_leaders()
        follower = next(n for n in cluster.ports if n != leaders[0])
        others = [n for n in cluster.ports if n != follower]
        await cluster.control(follower, "isolate", others)
        for node in others:
            await cluster.control(node, "isolate", [follower])
        assert (await cluster.put(0, "CSM13001", "room", "D101"))["ok"]

        stale = await cluster.request(follower, get(0, "CSM13001", "follower"))
        fresh = await cluster.request(leaders[0], get(0, "CSM13001", "leader"))
        assert stale["facts"] == {}
        assert fresh["facts"] == {"room": ["D101"]}
        assert stale["applied"] < fresh["applied"]
        redirect = await cluster.request(follower, get(0, "CSM13001", "leader"))
        assert redirect["type"] == "Redirect"
    finally:
        cluster.stop()
