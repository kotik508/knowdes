"""The command line client against a router (issue #8)."""

import asyncio
import json
import subprocess
import sys

import pytest
from test_router import setup


@pytest.mark.parametrize("read", ["leader", "follower"])
async def test_cli_does_all_four_operations(tmp_path, read):
    async with setup(tmp_path) as (cluster, (router,)):
        host, port = router.address

        async def cli(*args):
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "knowdes.cli",
                "--router",
                f"{host}:{port}",
                *args,
                stdout=subprocess.PIPE,
            )
            out, _ = await proc.communicate()
            assert proc.returncode == 0, out
            return json.loads(out)

        await cli("put", "CSM13001", "teacher", "niklander")
        await cli("put", "CSM13001", "teacher", "su")
        await cli("delete", "CSM13001", "teacher", "su")

        async def settled():
            reply = await cli("get", "CSM13001", "--read", read)
            return reply["facts"] == {"teacher": ["niklander"]}

        # A follower read may be one heartbeat behind.
        for _ in range(20):
            if await settled():
                break
            await asyncio.sleep(0.05)
        else:
            raise AssertionError("get did not show the writes")
        reply = await cli("neighbors", "niklander", "--direction", "in", "--read", read)
        assert reply["edges"] == [["CSM13001", "teacher", "niklander"]]
