from knowdes.raft.storage import MemoryStorage, SqliteStorage, open_database


def entry(term, n):
    return {"term": term, "command": {"op": "put", "s": f"s{n}", "p": "p", "o": "o"}}


def check(store, reopen):
    assert store.load() == (0, None, [])
    store.save_state(3, "n2")
    store.append(1, [entry(1, 1), entry(1, 2), {"term": 2, "command": None}])
    store.append(3, [entry(3, 3)])
    term, voted_for, entries = reopen().load()
    assert (term, voted_for) == (3, "n2")
    assert entries == [entry(1, 1), entry(1, 2), entry(3, 3)]


def test_memory_storage():
    store = MemoryStorage()
    check(store, lambda: store)


def test_sqlite_storage_survives_reopen(tmp_path):
    path = tmp_path / "node.db"

    def make():
        return SqliteStorage(open_database(path), shard=1)

    check(make(), make)


def test_shards_do_not_mix(tmp_path):
    db = open_database(tmp_path / "node.db")
    SqliteStorage(db, 0).append(1, [entry(1, 1)])
    SqliteStorage(db, 1).save_state(4, None)
    assert SqliteStorage(db, 0).load() == (0, None, [entry(1, 1)])
    assert SqliteStorage(db, 1).load() == (4, None, [])


def test_group_restores_term_vote_and_log(tmp_path):
    from knowdes.raft import RaftGroup

    async def no_send(peer, msg):
        return None

    path = tmp_path / "node.db"
    g = RaftGroup(0, "b", ["a", "b", "c"], no_send, storage=SqliteStorage(open_database(path), 0))
    ask = {"type": "RequestVote", "term": 2, "candidate": "a", "last_index": 0, "last_term": 0}
    assert g.on_request_vote(ask)["granted"]
    g.on_append_entries(
        {
            "term": 2,
            "leader": "a",
            "prev_index": 0,
            "prev_term": 0,
            "entries": [entry(2, 1)],
            "leader_commit": 0,
        }
    )
    again = RaftGroup(
        0, "b", ["a", "b", "c"], no_send, storage=SqliteStorage(open_database(path), 0)
    )
    assert (again.term, again.voted_for) == (2, "a")
    assert again.log[1:] == [entry(2, 1)]
    # The vote of term 2 is spent, also after the restart.
    assert not again.on_request_vote({**ask, "candidate": "c"})["granted"]
