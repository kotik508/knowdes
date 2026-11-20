import time

import pytest

from knowdes.raft import RaftGroup, Role


async def no_send(peer, msg):
    return None


def group(node_id="a"):
    return RaftGroup(0, node_id, ["a", "b", "c"], no_send)


def test_starts_as_follower():
    g = group()
    assert g.role is Role.FOLLOWER
    assert g.term == 0
    assert g.peers == ["b", "c"]
    assert g.majority == 2


def test_candidate_votes_for_itself_in_new_term():
    g = group()
    g.become_candidate()
    assert g.role is Role.CANDIDATE
    assert g.term == 1
    assert g.voted_for == "a"


async def test_newer_term_turns_leader_into_follower():
    g = group()
    g.become_candidate()
    g.become_leader()
    assert g.observe_term(5)
    assert g.role is Role.FOLLOWER
    assert g.term == 5
    assert g.voted_for is None
    await g.stop()


def test_older_term_changes_nothing():
    g = group()
    g.become_candidate()
    assert not g.observe_term(0)
    assert g.role is Role.CANDIDATE


def test_only_candidate_becomes_leader():
    with pytest.raises(RuntimeError):
        group().become_leader()


def test_election_timeout_is_random_within_range():
    deadlines = set()
    for _ in range(20):
        g = group()
        left = g.deadline - time.monotonic()
        assert 0.14 < left <= 0.30
        deadlines.add(round(g.deadline, 4))
    assert len(deadlines) > 1
