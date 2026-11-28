"""The demo for 7 or 8 December (#16), five to ten minutes against a running cluster.

deploy/demo.sh starts three nodes, a router, the gateway and an empty fourth
node and then runs this script. The console at http://127.0.0.1:7120/ shows
every step while the script tells what happens:

1. the cluster with one leader per shard
2. writes, one of them with a reverse edge on another shard (two-phase commit)
3. the leader of shard 0 stops, the shard elects a new one
4. writes and reads go on while it is away
5. it comes back and catches up
6. a fourth node joins every shard while the cluster runs
7. the leaders are spread over the nodes again

    python -m knowdes.demo --config deploy/local.toml [--join n4=127.0.0.1:7104] [--wait]

With --wait the script stops after every step until Enter, so whoever presents
can talk about the console in between.
"""

from __future__ import annotations

import argparse
import asyncio
import time
from collections.abc import Awaitable, Callable
from pathlib import Path

from knowdes import config, protocol
from knowdes.client import Client
from knowdes.placement import add_replica, balance, current_leaders, status_of
from knowdes.sharding import shard_of

Say = Callable[[str], None]
Pause = Callable[[], Awaitable[None]]

FACTS = [
    ("CSM13001", "teacher", "niklander"),
    ("CSM13001", "exam_room", "Exactum_A111"),
    ("CSM13001", "credits", "5"),
    ("DATA11001", "teacher", "niklander"),
]
# A node that does not answer within this time counts as down for the demo.
TIMEOUT = 10.0
# How often a node tries to join a shard before the demo gives up.
JOIN_TRIES = 4


async def until(check: Callable[[], Awaitable[bool]], timeout: float = TIMEOUT) -> float:
    """Wait until check() holds and return how long it took in seconds."""
    start = time.perf_counter()
    while time.perf_counter() - start < timeout:
        if await check():
            return time.perf_counter() - start
        await asyncio.sleep(0.05)
    raise TimeoutError("the cluster did not get there in time")


async def control(address: tuple[str, int], action: str) -> None:
    reply = await protocol.request(address, {"type": "Control", "action": action}, 2.0)
    if not reply.get("ok"):
        raise RuntimeError(f"{action} failed: {reply}")


class Demo:
    def __init__(self, nodes: dict[str, tuple[str, int]], shards: int, say: Say = print) -> None:
        self.nodes = dict(nodes)
        self.shards = shards
        self.say = say
        self.client = Client(dict(nodes), shards)

    async def leaders(self) -> dict[int, str | None]:
        return current_leaders(await status_of(self.nodes), self.shards)

    async def cluster(self) -> dict[int, str | None]:
        await until(lambda: self._all_led())
        leaders = await self.leaders()
        for shard, leader in leaders.items():
            self.say(f"shard {shard} is led by {leader}")
        return leaders

    async def _all_led(self) -> bool:
        return all(leader is not None for leader in (await self.leaders()).values())

    async def writes(self) -> None:
        for s, p, o in FACTS:
            reply = await self.client.put(s, p, o)
            shards = ", ".join(sorted(reply["votes"]))
            self.say(f"put ({s}, {p}, {o}): {reply['decision']} on shards {shards}")
        # The incoming edges of niklander are on its own shard, written in the same commit.
        found = await self.client.neighbors("niklander", 1, "teacher", "in")
        courses = [n["id"] for n in found["nodes"] if n["distance"] == 1]
        self.say(
            f"niklander teaches {', '.join(courses)} (reverse edges on shard "
            f"{shard_of('niklander', self.shards)})"
        )

    async def stop_leader(self, shard: int = 0) -> str:
        old = (await self.leaders())[shard]
        assert old is not None
        await control(self.nodes[old], "pause")
        self.say(f"paused {old}, the leader of shard {shard}")
        took = await until(lambda: self._new_leader(shard, old))
        new = (await self.leaders())[shard]
        self.say(f"shard {shard} elected {new} after {took * 1000:.0f} ms")
        return old

    async def _new_leader(self, shard: int, old: str) -> bool:
        leader = (await self.leaders())[shard]
        return leader is not None and leader != old

    async def while_away(self) -> None:
        reply = await self.client.put("CSM13001", "room_changed", "Exactum_B123")
        self.say(f"put during the failure: {reply['decision']}")
        got = await self.client.read("CSM13001", "follower")
        # A follower may be a few entries behind, the put above can be missing here.
        predicates = ", ".join(sorted(got["facts"]))
        self.say(f"follower read from {got['node']}, applied {got['applied']}: {predicates}")

    async def come_back(self, node: str) -> None:
        await control(self.nodes[node], "resume")
        self.say(f"resumed {node}")
        took = await until(lambda: self._caught_up(node))
        self.say(f"{node} caught up after {took * 1000:.0f} ms and follows again")

    async def _caught_up(self, node: str) -> bool:
        status = await status_of(self.nodes)
        mine = status.get(node)
        if mine is None or mine.get("paused"):
            return False
        applied = {r["shard"]: r["last_applied"] for r in mine["shards"]}
        for st in status.values():
            for r in (st or {}).get("shards", []):
                if r["role"] == "leader" and applied.get(r["shard"], -1) < r["last_applied"]:
                    return False
        return True

    async def grow(self, node: str, address: str) -> None:
        nodes = {**self.nodes, node: protocol.parse_address(address)}
        replies = []
        # One shard after the other, as in placement.join. Right after the resume a
        # shard can still be changing its leader, then the change is tried again.
        for shard in range(self.shards):
            for attempt in range(JOIN_TRIES):
                try:
                    reply = await add_replica(nodes, self.shards, shard, node, address)
                except (OSError, TimeoutError) as err:
                    reply = {"ok": False, "error": str(err)}
                if reply.get("ok"):
                    break
                await asyncio.sleep(0.5 * (attempt + 1))
            else:
                raise RuntimeError(f"{node} could not join shard {shard}: {reply}")
            replies.append(reply)
        self.nodes[node] = protocol.parse_address(address)
        size = len(replies[0]["members"])
        self.say(f"{node} joined all {self.shards} shards, each has {size} replicas")
        reply = await self.client.put("CSM13001", "nodes", str(len(self.nodes)))
        self.say(f"put with {len(self.nodes)} nodes: {reply['decision']}")

    async def spread(self) -> None:
        moves = await balance(self.nodes, self.shards)
        for m in moves:
            self.say(f"shard {m['shard']}: leadership {m['from']} -> {m['to']}, ok={m['ok']}")
        if not moves:
            self.say("the leaders are spread already")
        await self.cluster()

    async def run(self, grow: tuple[str, str] | None = None, pause: Pause | None = None) -> None:
        steps: list[tuple[str, Callable[[], Awaitable[object]]]] = [
            ("Cluster", self.cluster),
            ("Writes and two-phase commit", self.writes),
        ]
        away: list[str] = []

        async def stop() -> None:
            away.append(await self.stop_leader())

        steps += [
            ("Leader of shard 0 fails", stop),
            ("Writes and reads go on", self.while_away),
            ("The old leader comes back", lambda: self.come_back(away[0])),
        ]
        if grow:
            steps.append((f"{grow[0]} joins", lambda: self.grow(*grow)))
        steps.append(("Leaders spread again", self.spread))
        for number, (title, step) in enumerate(steps, start=1):
            self.say(f"\n{number}. {title}")
            await step()
            if pause is not None:
                await pause()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the knowdes demo against a cluster.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--join", metavar="ID=HOST:PORT", help="an empty node that joins")
    parser.add_argument("--wait", action="store_true", help="wait for Enter after every step")
    args = parser.parse_args()
    cluster = config.load(args.config)
    grow = None
    if args.join:
        node, _, address = args.join.partition("=")
        grow = (node, address)

    async def enter() -> None:
        await asyncio.to_thread(input, "  (Enter) ")

    demo = Demo(dict(cluster.nodes), cluster.shards)
    asyncio.run(demo.run(grow, enter if args.wait else None))


if __name__ == "__main__":
    main()
