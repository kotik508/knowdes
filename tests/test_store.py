"""Applying committed commands to the facts of a shard."""

from knowdes.store import ShardStore


def put(s, p, o, version=None):
    command = {"op": "put", "s": s, "p": p, "o": o}
    if version is not None:
        command["version"] = version
    return command


def test_plain_puts_collect_objects_and_delete_removes_one():
    store = ShardStore()
    store.apply(put("CSM13001", "teacher", "niklander"))
    store.apply(put("CSM13001", "teacher", "su"))
    store.apply({"op": "delete", "s": "CSM13001", "p": "teacher", "o": "su"})
    assert store.get("CSM13001") == {"teacher": ["niklander"]}


def test_newer_source_version_replaces_the_value():
    store = ShardStore()
    store.apply(put("ex@2026-12-01", "room", "Physicum D101", version=3))
    store.apply(put("ex@2026-12-01", "room", "Exactum A111", version=9))
    assert store.get("ex@2026-12-01") == {"room": ["Exactum A111"]}


def test_repeated_and_older_versions_change_nothing():
    store = ShardStore()
    store.apply(put("ex@2026-12-01", "cancelled", "true", version=9))
    store.apply(put("ex@2026-12-01", "cancelled", "true", version=9))
    store.apply(put("ex@2026-12-01", "cancelled", "false", version=3))
    assert store.get("ex@2026-12-01") == {"cancelled": ["true"]}


def test_batch_and_noop_entries():
    store = ShardStore()
    store.apply(None)
    store.apply({"op": "batch", "facts": [put("a", "p", "1"), put("b", "p", "2")]})
    assert store.get("a") == {"p": ["1"]}
    assert store.get("b") == {"p": ["2"]}


def test_reverse_edges_are_kept_apart_from_facts():
    store = ShardStore()
    store.apply({"op": "rput", "s": "CSM13001", "p": "teacher", "o": "niklander"})
    store.apply({"op": "rput", "s": "CSM14204", "p": "teacher", "o": "niklander"})
    assert store.get("niklander") == {}
    assert store.incoming("niklander") == {"teacher": ["CSM13001", "CSM14204"]}
    store.apply({"op": "rdelete", "s": "CSM14204", "p": "teacher", "o": "niklander"})
    assert store.incoming("niklander") == {"teacher": ["CSM13001"]}
    store.apply({"op": "rdelete", "s": "CSM13001", "p": "teacher", "o": "niklander"})
    assert store.incoming("niklander") == {}
    assert store.into == {}


def test_deleting_the_last_object_removes_the_subject():
    store = ShardStore()
    store.apply(put("a", "p", "1"))
    store.apply({"op": "delete", "s": "a", "p": "p", "o": "1"})
    store.apply({"op": "delete", "s": "missing", "p": "p", "o": "1"})
    assert store.out == {}
