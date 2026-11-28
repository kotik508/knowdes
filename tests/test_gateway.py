"""Gateway of the web console against a real cluster (#11), one test per endpoint."""

import asyncio
import contextlib
import json

from cluster import LocalCluster

from knowdes.gateway import STATIC, Gateway

ALL = {"n1", "n2", "n3"}


@contextlib.asynccontextmanager
async def setup(tmp_path, static=None):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    gateway = Gateway(nodes, cluster.shards, static or tmp_path)
    try:
        await cluster.wait_for_leaders()
        await gateway.start("127.0.0.1", 0)
        yield cluster, gateway
    finally:
        await gateway.stop()
        cluster.stop()


async def http(gateway, method, path, body=None):
    reader, writer = await asyncio.open_connection(*gateway.address)
    data = json.dumps(body).encode() if body is not None else b""
    writer.write(
        f"{method} {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {len(data)}\r\n\r\n".encode()
        + data
    )
    await writer.drain()
    raw = await asyncio.wait_for(reader.read(), 10)
    writer.close()
    head, _, payload = raw.partition(b"\r\n\r\n")
    return int(head.split()[1]), payload


async def post(gateway, msg):
    status, payload = await http(gateway, "POST", "/request", msg)
    assert status == 200
    return json.loads(payload)


async def read_events(reader, until, timeout=5.0):
    """Collect node events from the stream until one of them matches, skip status."""
    seen = []
    kind = b""
    async with asyncio.timeout(timeout):
        while True:
            line = await reader.readline()
            if line.startswith(b"event: "):
                kind = line[7:].strip()
            elif kind == b"event" and line.startswith(b"data: "):
                seen.append(json.loads(line[6:]))
                if until(seen[-1]):
                    return seen


async def test_events_merge_every_node_and_show_a_pause(tmp_path):
    async with setup(tmp_path) as (cluster, gateway):
        reader, writer = await asyncio.open_connection(*gateway.address)
        writer.write(b"GET /events HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        head = await reader.readuntil(b"\r\n\r\n")
        assert b"text/event-stream" in head
        up = set()

        def all_up(event):
            # The nodes come up in any order, wait until every one has been reported.
            if event["kind"] == "up":
                up.add(event["node"])
            return up == ALL

        await read_events(reader, all_up)
        await cluster.control("n2", "pause")
        events = await read_events(reader, lambda e: e["kind"] == "pause")
        assert events[-1]["node"] == "n2"
        writer.close()


async def test_request_goes_through_the_router(tmp_path):
    async with setup(tmp_path) as (_, gateway):
        put = await post(gateway, {"type": "Put", "s": "CSM13001", "p": "teacher", "o": "tiina"})
        assert put["reply"]["ok"], put
        assert put["ms"] >= 0
        got = await post(gateway, {"type": "Get", "s": "CSM13001", "read": "leader"})
        assert got["reply"]["facts"] == {"teacher": ["tiina"]}
        bad = await post(gateway, {"type": "Fly"})
        assert bad["reply"]["type"] == "Error"


async def test_control_goes_to_the_named_node(tmp_path):
    async with setup(tmp_path) as (cluster, gateway):
        reply = await post(gateway, {"type": "Control", "node": "n1", "action": "pause"})
        assert reply["reply"]["paused"] is True
        status = await cluster.request("n1", {"type": "Status"})
        assert status["paused"] is True
        unknown = await post(gateway, {"type": "Control", "node": "n9", "action": "pause"})
        assert unknown["reply"]["type"] == "Error"


async def test_static_files_and_errors(tmp_path):
    static = tmp_path / "console"
    static.mkdir()
    (static / "index.html").write_text("<h1>knowdes</h1>")
    async with setup(tmp_path, static) as (_, gateway):
        status, body = await http(gateway, "GET", "/")
        assert (status, body) == (200, b"<h1>knowdes</h1>")
        assert (await http(gateway, "GET", "/../pyproject.toml"))[0] == 404
        assert (await http(gateway, "GET", "/missing.js"))[0] == 404
        assert (await http(gateway, "DELETE", "/request"))[0] == 405
        reader, writer = await asyncio.open_connection(*gateway.address)
        writer.write(b"POST /request HTTP/1.1\r\nContent-Length: 3\r\n\r\n[1]")
        await writer.drain()
        assert b" 400 " in (await reader.read())[:20]
        writer.close()


async def test_status_shows_roles_and_a_node_that_is_gone(tmp_path):
    async with setup(tmp_path) as (cluster, gateway):
        leaders = await cluster.wait_for_leaders()
        await asyncio.sleep(0.7)
        status, payload = await http(gateway, "GET", "/status")
        assert status == 200
        nodes = json.loads(payload)["nodes"]
        assert set(nodes) == {"n1", "n2", "n3"}
        for shard, leader in leaders.items():
            assert nodes[leader]["shards"][shard]["role"] == "leader"
        cluster.kill("n3")
        reader, writer = await asyncio.open_connection(*gateway.address)
        writer.write(b"GET /events HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        await reader.readuntil(b"\r\n\r\n")
        kind = b""
        async with asyncio.timeout(5):
            while True:
                line = await reader.readline()
                if line.startswith(b"event: "):
                    kind = line[7:].strip()
                elif kind == b"status" and line.startswith(b"data: "):
                    snapshot = json.loads(line[6:])
                    if snapshot["nodes"]["n3"] is None:
                        break
        assert snapshot["nodes"]["n1"]["node"] == "n1"
        writer.close()


async def test_serves_the_console_page(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    gateway = Gateway(nodes, cluster.shards)
    try:
        await gateway.start("127.0.0.1", 0)
        status, body = await http(gateway, "GET", "/")
        assert status == 200
        page = body.decode()
        # The page only talks to the two endpoints of the gateway.
        assert 'new EventSource("/events")' in page
        assert 'fetch("/request"' in page
    finally:
        await gateway.stop()
        cluster.stop()


async def test_console_page_has_timeline_filters(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    gateway = Gateway(nodes, cluster.shards)
    try:
        await gateway.start("127.0.0.1", 0)
        _, body = await http(gateway, "GET", "/")
        page = body.decode()
        for name in ("f-shard", "f-kind", "f-node"):
            assert f'id="{name}"' in page
    finally:
        await gateway.stop()
        cluster.stop()


async def test_pause_and_resume_show_at_once(tmp_path):
    async with setup(tmp_path) as (_, gateway):
        await post(gateway, {"type": "Control", "node": "n2", "action": "pause"})
        # The snapshot is fresh right after the reply, without waiting for the next poll.
        assert gateway.snapshot()["nodes"]["n2"]["paused"] is True
        reply = await post(gateway, {"type": "Control", "node": "n2", "action": "resume"})
        assert reply["reply"]["paused"] is False
        assert gateway.snapshot()["nodes"]["n2"]["paused"] is False


async def test_console_page_has_pause_buttons(tmp_path):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    gateway = Gateway(nodes, cluster.shards)
    try:
        await gateway.start("127.0.0.1", 0)
        _, body = await http(gateway, "GET", "/")
        page = body.decode()
        assert 'type: "Control", node, action' in page
        assert 'st.paused ? "resume" : "pause"' in page
    finally:
        await gateway.stop()
        cluster.stop()


async def test_events_of_a_transaction_reach_the_console(tmp_path):
    """A Put is a two-phase commit, the console groups its events by tx (#11, #12)."""
    async with setup(tmp_path, STATIC) as (_, gateway):
        reader, writer = await asyncio.open_connection(*gateway.address)
        writer.write(b"GET /events HTTP/1.1\r\nHost: x\r\n\r\n")
        await writer.drain()
        await post(gateway, {"type": "Put", "s": "CSM13001", "p": "teacher", "o": "niklander"})
        committed = set()

        def both_committed(e):
            if e.get("kind") == "tx committed":
                committed.add(e["shard"])
            return len(committed) == 2

        seen = await read_events(reader, both_committed)
        writer.close()
        _, body = await http(gateway, "GET", "/")
    by_kind = {}
    for e in seen:
        if e.get("tx"):
            by_kind.setdefault(e["kind"], []).append(e)
    assert {e["tx"] for kind in by_kind.values() for e in kind} == {by_kind["decide"][0]["tx"]}
    assert len(by_kind["prepare"]) == 2 and all(e["vote"] for e in by_kind["prepare"])
    assert by_kind["decide"][0]["decision"] == "commit"
    page = body.decode()
    assert 'id="txs"' in page
    for kind in ('"prepare"', '"decide"', '"tx committed"', '"recovered"'):
        assert kind in page
