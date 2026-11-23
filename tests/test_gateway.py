"""Gateway of the web console against a real cluster (#11), one test per endpoint."""

import asyncio
import contextlib
import json

from cluster import LocalCluster

from knowdes.gateway import Gateway

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
    """Collect events from the stream until one of them matches."""
    seen = []
    async with asyncio.timeout(timeout):
        while True:
            line = await reader.readline()
            if line.startswith(b"data: "):
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
