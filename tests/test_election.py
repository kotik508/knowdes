import asyncio

from simnet import Network

from knowdes.raft import Role


async def test_three_replicas_elect_one_leader():
    net = Network()
    net.start()
    try:
        leader = await net.wait_for_leader()
        await asyncio.sleep(0.4)  # longer than any election timeout
        assert net.leaders() == [leader]
        for g in net.groups.values():
            assert g.leader == leader.id
            assert g.term == leader.term
    finally:
        await net.stop()


async def test_new_leader_after_leader_fails():
    net = Network()
    net.start()
    try:
        old = await net.wait_for_leader()
        net.down.add(old.id)
        new = await net.wait_for_leader(newer_than=old.term)
        assert new.id != old.id
    finally:
        await net.stop()


async def test_old_leader_steps_down_when_it_comes_back():
    net = Network()
    net.start()
    try:
        old = await net.wait_for_leader()
        net.down.add(old.id)
        new = await net.wait_for_leader(newer_than=old.term)
        net.down.clear()
        await asyncio.sleep(0.2)
        assert old.role is Role.FOLLOWER
        assert old.leader == new.id
    finally:
        await net.stop()


async def test_no_leader_without_majority():
    net = Network()
    net.down.update({"b", "c"})
    net.start()
    try:
        await asyncio.sleep(0.8)
        assert net.groups["a"].role is not Role.LEADER
        assert net.groups["a"].term >= 2
    finally:
        await net.stop()


def test_vote_only_once_per_term():
    net = Network()
    b = net.groups["b"]
    ask = {"type": "RequestVote", "shard": 0, "term": 1, "last_index": 0, "last_term": 0}
    assert b.on_request_vote({**ask, "candidate": "a"})["granted"]
    assert not b.on_request_vote({**ask, "candidate": "c"})["granted"]
    assert b.on_request_vote({**ask, "candidate": "a"})["granted"]
