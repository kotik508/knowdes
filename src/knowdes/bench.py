"""Measurements for the report (#15, design plan section 7).

Runs against a cluster that is already up, through one router like any client:
write and read latency, throughput with a number of clients at once, leader
reads against follower reads, and the time until a shard has a new leader
after its leader stops. Every run appends one JSON line to --out, so the
results of several cluster sizes end up in one file for the charts.

    python -m knowdes.bench --config deploy/local.toml writes --count 2000 --clients 16
    python -m knowdes.bench --config deploy/local.toml reads --read follower
    python -m knowdes.bench --config deploy/local.toml failover --shard 0 --rounds 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import time
from pathlib import Path
from typing import Any

from knowdes import config, protocol
from knowdes.placement import current_leaders, status_of
from knowdes.protocol import Message

TIMEOUT = 5.0


def summary(name: str, latencies: list[float], seconds: float, errors: int, **extra: Any) -> dict:
    """Latencies in milliseconds and throughput in requests per second."""
    ms = sorted(x * 1000 for x in latencies)

    def pct(p: float) -> float:
        return round(ms[min(len(ms) - 1, int(p * len(ms)))], 2) if ms else 0.0

    return {
        "run": name,
        "time": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "count": len(ms),
        "errors": errors,
        "seconds": round(seconds, 3),
        "per_second": round(len(ms) / seconds, 1) if seconds else 0.0,
        "mean_ms": round(statistics.fmean(ms), 2) if ms else 0.0,
        "p50_ms": pct(0.50),
        "p95_ms": pct(0.95),
        "p99_ms": pct(0.99),
        **extra,
    }


async def load(router: tuple[str, int], messages: list[Message], clients: int) -> tuple:
    """Send the messages with that many clients at once. Returns latencies, seconds, errors."""
    queue: asyncio.Queue[Message] = asyncio.Queue()
    for m in messages:
        queue.put_nowait(m)
    latencies: list[float] = []
    errors = 0

    async def worker() -> None:
        nonlocal errors
        while not queue.empty():
            msg = queue.get_nowait()
            start = time.perf_counter()
            try:
                reply = await protocol.request(router, msg, TIMEOUT)
            except (OSError, TimeoutError, ValueError):
                errors += 1
                continue
            if reply.get("ok"):
                latencies.append(time.perf_counter() - start)
            else:
                errors += 1

    start = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(clients)))
    return latencies, time.perf_counter() - start, errors


def facts(count: int, prefix: str = "bench") -> list[Message]:
    """Course-like facts, the object repeats so reverse edges pile up as in real data."""
    return [
        {"type": "Put", "s": f"{prefix}-course{i}", "p": "teacher", "o": f"{prefix}-t{i % 50}"}
        for i in range(count)
    ]


async def bench_writes(router: tuple[str, int], count: int, clients: int, **extra: Any) -> dict:
    latencies, seconds, errors = await load(router, facts(count), clients)
    return summary("writes", latencies, seconds, errors, clients=clients, **extra)


async def bench_reads(
    router: tuple[str, int], count: int, clients: int, read: str = "leader", **extra: Any
) -> dict:
    """Reads of facts written before, so every Get finds something."""
    keys = max(1, min(count, 500))
    await load(router, facts(keys, "read"), clients)
    gets = [{"type": "Get", "s": f"read-course{i % keys}", "read": read} for i in range(count)]
    latencies, seconds, errors = await load(router, gets, clients)
    return summary(f"reads {read}", latencies, seconds, errors, clients=clients, **extra)


async def bench_failover(
    nodes: dict[str, tuple[str, int]], shards: int, shard: int, rounds: int, **extra: Any
) -> dict:
    """Pause the leader of a shard and time until another node leads it, then resume.

    Measures detection (election timeout) plus the election, as a client sees it.
    """
    times: list[float] = []
    errors = 0
    for _ in range(rounds):
        leader = current_leaders(await status_of(nodes), shards).get(shard)
        if leader is None:
            await asyncio.sleep(0.5)
            errors += 1
            continue
        pause = {"type": "Control", "action": "pause"}
        await protocol.request(nodes[leader], pause, TIMEOUT)
        start = time.perf_counter()
        others = {n: a for n, a in nodes.items() if n != leader}
        new = None
        while time.perf_counter() - start < 5.0:
            new = current_leaders(await status_of(others), shards).get(shard)
            if new is not None:
                break
            await asyncio.sleep(0.005)
        took = time.perf_counter() - start
        await protocol.request(nodes[leader], {"type": "Control", "action": "resume"}, TIMEOUT)
        if new is None:
            errors += 1
        else:
            times.append(took)
        # Let the resumed node catch up before the next round.
        await asyncio.sleep(0.5)
    total = sum(times)
    return summary("failover", times, total, errors, shard=shard, rounds=rounds, **extra)


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure a running knowdes cluster.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--router", help="host:port, default the first router of the config")
    parser.add_argument("--out", type=Path, default=Path("bench-results.jsonl"))
    parser.add_argument("--label", default="", help="kept with the result, e.g. 3 nodes")
    sub = parser.add_subparsers(dest="run", required=True)
    w = sub.add_parser("writes")
    w.add_argument("--count", type=int, default=1000)
    w.add_argument("--clients", type=int, default=8)
    r = sub.add_parser("reads")
    r.add_argument("--count", type=int, default=1000)
    r.add_argument("--clients", type=int, default=8)
    r.add_argument("--read", choices=("leader", "follower"), default="leader")
    f = sub.add_parser("failover")
    f.add_argument("--shard", type=int, default=0)
    f.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()

    cluster = config.load(args.config)
    router = protocol.parse_address(args.router) if args.router else cluster.routers()[0]
    extra = {"label": args.label, "cluster_nodes": len(cluster.nodes)}
    if args.run == "writes":
        run = bench_writes(router, args.count, args.clients, **extra)
    elif args.run == "reads":
        run = bench_reads(router, args.count, args.clients, args.read, **extra)
    else:
        run = bench_failover(dict(cluster.nodes), cluster.shards, args.shard, args.rounds, **extra)
    result = asyncio.run(run)
    with args.out.open("a") as out:
        out.write(json.dumps(result) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
