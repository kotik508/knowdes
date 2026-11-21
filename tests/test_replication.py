import asyncio

import pytest
from simnet import Network

from knowdes.raft import NotLeaderError


def put(n):
    return {"op": "put", "s": f"course{n}", "p": "exam_room", "o": "Exactum_A111"}


async def wait_until(check, timeout=2.0):
    loop = asyncio.get_running_loop()
    end = loop.time() + timeout
    while not check():
        assert loop.time() < end, "condition not reached in time"
        await asyncio.sleep(0.01)


async def test_write_is_committed_on_all_replicas():
    net = Network()
    net.start()
    try:
        leader = await net.wait_for_leader()
        index = await leader.propose(put(1))
        await wait_until(lambda: all(g.commit_index >= index for g in net.groups.values()))
        for g in net.groups.values():
            assert g.log[index]["command"] == put(1)
    finally:
        await net.stop()


async def test_write_commits_with_two_of_three():
    net = Network()
    net.start()
    try:
        leader = await net.wait_for_leader()
        follower = next(i for i in net.groups if i != leader.id)
        net.down.add(follower)
        indices = [await leader.propose(put(n)) for n in range(5)]
        assert leader.commit_index == indices[-1]
        assert net.groups[follower].last_index < indices[0]
    finally:
        await net.stop()


async def test_no_commit_without_majority():
    net = Network()
    net.start()
    try:
        leader = await net.wait_for_leader()
        net.down.update(i for i in net.groups if i != leader.id)
        with pytest.raises(TimeoutError):
            await leader.propose(put(1))
    finally:
        await net.stop()


async def test_follower_redirects_to_leader():
    net = Network()
    net.start()
    try:
        leader = await net.wait_for_leader()
        await asyncio.sleep(0.1)
        follower = next(g for g in net.groups.values() if g is not leader)
        with pytest.raises(NotLeaderError) as err:
            await follower.propose(put(1))
        assert err.value.leader == leader.id
    finally:
        await net.stop()


async def test_returning_follower_catches_up():
    net = Network()
    net.start()
    try:
        leader = await net.wait_for_leader()
        follower = next(i for i in net.groups if i != leader.id)
        net.down.add(follower)
        for n in range(10):
            await leader.propose(put(n))
        net.down.clear()
        await wait_until(lambda: net.groups[follower].commit_index == leader.commit_index)
        assert net.groups[follower].log == leader.log
    finally:
        await net.stop()


async def test_new_leader_keeps_committed_writes():
    net = Network()
    net.start()
    try:
        old = await net.wait_for_leader()
        index = await old.propose(put(1))
        net.down.add(old.id)
        new = await net.wait_for_leader(newer_than=old.term)
        assert new.log[index]["command"] == put(1)
        await new.propose(put(2))
    finally:
        await net.stop()


def test_conflicting_entries_are_replaced():
    net = Network()
    b = net.groups["b"]
    b.log += [{"term": 1, "command": put(1)}, {"term": 1, "command": put(2)}]
    reply = b.on_append_entries(
        {
            "type": "AppendEntries",
            "term": 2,
            "leader": "a",
            "prev_index": 1,
            "prev_term": 1,
            "entries": [{"term": 2, "command": put(3)}],
            "leader_commit": 0,
        }
    )
    assert reply["success"]
    assert [e["command"] for e in b.log[1:]] == [put(1), put(3)]


def test_gap_is_rejected():
    net = Network()
    reply = net.groups["b"].on_append_entries(
        {
            "type": "AppendEntries",
            "term": 1,
            "leader": "a",
            "prev_index": 4,
            "prev_term": 1,
            "entries": [],
            "leader_commit": 0,
        }
    )
    assert not reply["success"]
    assert reply["last_index"] == 0
