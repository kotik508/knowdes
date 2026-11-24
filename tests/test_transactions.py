"""Records of two-phase commit in the shard store (#12)."""

from knowdes.store import ShardStore

FACT = {"op": "put", "s": "CSM13001", "p": "teacher", "o": "niklander"}
REVERSE = {"op": "rput", "s": "CSM13001", "p": "teacher", "o": "niklander"}


def prepare(tx, *facts):
    return {"op": "prepare", "tx": tx, "facts": list(facts)}


def test_prepared_facts_appear_only_after_commit():
    store = ShardStore()
    store.apply(prepare("t1", FACT))
    assert store.transaction("t1") == "prepared"
    assert store.get("CSM13001") == {}
    assert store.open_transactions() == ["t1"]
    store.apply({"op": "commit", "tx": "t1"})
    assert store.get("CSM13001") == {"teacher": ["niklander"]}
    assert store.transaction("t1") == "committed"
    assert store.open_transactions() == []


def test_abort_drops_the_facts_and_frees_the_lock():
    store = ShardStore()
    store.apply(prepare("t1", FACT))
    store.apply({"op": "abort", "tx": "t1"})
    assert store.get("CSM13001") == {}
    assert store.transaction("t1") == "aborted"
    store.apply(prepare("t2", FACT))
    assert store.transaction("t2") == "prepared"


def test_second_transaction_on_a_locked_node_is_refused():
    store = ShardStore()
    store.apply(prepare("t1", FACT))
    store.apply(prepare("t2", {**FACT, "o": "su"}))
    assert store.transaction("t2") == "refused"
    # A refused transaction never writes, even if a commit for it arrives.
    store.apply({"op": "commit", "tx": "t2"})
    assert store.get("CSM13001") == {}


def test_reverse_edge_locks_the_object():
    store = ShardStore()
    store.apply(prepare("t1", REVERSE))
    store.apply(prepare("t2", {"op": "put", "s": "niklander", "p": "room", "o": "B221"}))
    assert store.transaction("t2") == "refused"
    store.apply({"op": "commit", "tx": "t1"})
    assert store.incoming("niklander") == {"teacher": ["CSM13001"]}


def test_repeated_records_change_nothing():
    store = ShardStore()
    store.apply(prepare("t1", FACT))
    store.apply(prepare("t1", {**FACT, "o": "other"}))
    store.apply({"op": "commit", "tx": "t1"})
    store.apply({"op": "commit", "tx": "t1"})
    store.apply({"op": "abort", "tx": "t1"})
    assert store.get("CSM13001") == {"teacher": ["niklander"]}
    assert store.transaction("t1") == "committed"


def test_decision_before_prepare_blocks_a_late_prepare():
    store = ShardStore()
    store.apply({"op": "abort", "tx": "t1"})
    store.apply(prepare("t1", FACT))
    assert store.transaction("t1") == "aborted"
    assert store.open_transactions() == []


def test_first_decision_of_the_coordinator_is_final():
    store = ShardStore()
    store.apply({"op": "decide", "tx": "t1", "decision": "commit", "participants": [0, 2]})
    store.apply({"op": "decide", "tx": "t1", "decision": "abort", "participants": [0]})
    assert store.decision("t1") == {"decision": "commit", "participants": [0, 2]}
    assert store.decision("t9") is None
