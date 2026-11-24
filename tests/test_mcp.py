"""MCP server over stdio against a router and a real cluster (#13)."""

import contextlib
import json
import sys

from cluster import LocalCluster, free_port
from mcp import Client, StdioServerParameters

from knowdes import protocol
from knowdes.client import Client as GraphClient
from knowdes.router import Router


def server(address, *extra):
    host, port = address
    args = ["-m", "knowdes.mcp_server", "--router", f"{host}:{port}", *extra]
    return StdioServerParameters(command=sys.executable, args=args)


@contextlib.asynccontextmanager
async def setup(tmp_path, *extra):
    cluster = LocalCluster(data_dir=tmp_path)
    cluster.start()
    nodes = {n: ("127.0.0.1", p) for n, p in cluster.ports.items()}
    router = Router(GraphClient(nodes, cluster.shards))
    try:
        await cluster.wait_for_leaders()
        await router.start("127.0.0.1", 0)
        for s, p, o in [
            ("CSM13001", "teacher", "niklander"),
            ("CSM13001", "room", "A111"),
            ("niklander", "member_of", "CS"),
        ]:
            msg = {"type": "Put", "s": s, "p": p, "o": o}
            assert (await protocol.request(router.address, msg, 5.0))["ok"]
        async with Client(server(router.address, *extra)) as mcp:
            yield mcp
    finally:
        await router.stop()
        cluster.stop()


def result(reply):
    assert not reply.is_error, reply.content
    return json.loads(reply.content[0].text)


async def test_tools_get_and_neighbors(tmp_path):
    async with setup(tmp_path) as mcp:
        names = {t.name for t in (await mcp.list_tools()).tools}
        assert {"get", "neighbors"} <= names
        got = result(await mcp.call_tool("get", {"subject": "CSM13001"}))
        assert got["facts"] == {"room": ["A111"], "teacher": ["niklander"]}
        assert got["read"] == "leader"
        back = result(await mcp.call_tool("get", {"subject": "niklander", "read": "follower"}))
        assert back["read"] == "follower"
        walk = result(await mcp.call_tool("neighbors", {"subject": "CSM13001", "depth": 2}))
        assert {n["id"]: n["distance"] for n in walk["nodes"]} == {
            "CSM13001": 0,
            "A111": 1,
            "niklander": 1,
            "CS": 2,
        }
        into = result(await mcp.call_tool("neighbors", {"subject": "A111", "direction": "in"}))
        assert ["CSM13001", "room", "A111"] in into["edges"]


async def test_errors_come_back_as_tool_errors(tmp_path):
    async with setup(tmp_path) as mcp:
        deep = await mcp.call_tool("neighbors", {"subject": "CSM13001", "depth": 9})
        assert deep.is_error
        assert "depth" in deep.content[0].text


async def test_router_that_does_not_answer():
    async with Client(server(("127.0.0.1", free_port()))) as mcp:
        reply = await mcp.call_tool("get", {"subject": "CSM13001"})
        assert reply.is_error
        assert "does not answer" in reply.content[0].text


async def test_put_and_delete_with_reverse_edges(tmp_path):
    async with setup(tmp_path) as mcp:
        names = {t.name for t in (await mcp.list_tools()).tools}
        assert {"put", "delete"} <= names
        edge = {"subject": "TKT21028", "predicate": "teacher", "object": "niklander"}
        assert result(await mcp.call_tool("put", edge))["index"] > 0
        back = result(await mcp.call_tool("get", {"subject": "niklander"}))
        assert back["incoming"]["teacher"] == ["CSM13001", "TKT21028"]
        result(await mcp.call_tool("delete", edge))
        assert result(await mcp.call_tool("get", {"subject": "TKT21028"}))["facts"] == {}
        back = result(await mcp.call_tool("get", {"subject": "niklander"}))
        assert back["incoming"]["teacher"] == ["CSM13001"]


async def test_read_only_server_has_no_write_tools(tmp_path):
    async with setup(tmp_path, "--read-only") as mcp:
        tools = {t.name: t for t in (await mcp.list_tools()).tools}
        assert set(tools) == {"get", "neighbors"}
        assert tools["get"].annotations.read_only_hint
