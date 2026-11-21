"""Command line client, talks to a router (design plan section 2, issue #8).

    knowdes put CSM13001 teacher niklander
    knowdes delete CSM13001 teacher niklander
    knowdes get CSM13001 [--read follower]
    knowdes neighbors niklander --depth 2 --direction in [--predicate teacher]

The router comes from --router host:port, else from --config (the router on
the first machine), else 127.0.0.1:7110. Every command prints JSON.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from knowdes import config, protocol
from knowdes.client import MAX_DEPTH, READ_LEVELS
from knowdes.protocol import Message

TIMEOUT = 5.0


def request(args: argparse.Namespace) -> Message:
    if args.cmd in ("put", "delete"):
        msg: Message = {"type": args.cmd.capitalize(), "s": args.s, "p": args.p, "o": args.o}
        if args.version is not None:
            msg["version"] = args.version
        return msg
    if args.cmd == "get":
        return {"type": "Get", "s": args.s, "read": args.read}
    return {
        "type": "Neighbors",
        "s": args.s,
        "depth": args.depth,
        "p": args.predicate,
        "direction": args.direction,
        "read": args.read,
    }


def router_address(args: argparse.Namespace) -> tuple[str, int]:
    if args.router:
        return protocol.parse_address(args.router)
    if args.config:
        return config.load(args.config).routers()[0]
    return ("127.0.0.1", 7110)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="knowdes", description="Read and write the knowledge graph.")
    p.add_argument("--router", help="host:port of a router")
    p.add_argument("--config", type=Path, help="cluster config, takes the first router")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("put", "delete"):
        w = sub.add_parser(name)
        w.add_argument("s")
        w.add_argument("p")
        w.add_argument("o")
        w.add_argument("--version", type=int, help="version of the source system")
    g = sub.add_parser("get")
    g.add_argument("s")
    n = sub.add_parser("neighbors")
    n.add_argument("s")
    n.add_argument("--depth", type=int, default=1, choices=range(1, MAX_DEPTH + 1))
    n.add_argument("--predicate")
    n.add_argument("--direction", choices=("out", "in", "both"), default="out")
    for r in (g, n):
        r.add_argument("--read", choices=READ_LEVELS, default="leader")
    return p


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        reply = asyncio.run(protocol.request(router_address(args), request(args), TIMEOUT))
    except (OSError, TimeoutError) as err:
        print(f"knowdes: router not reachable: {err}", file=sys.stderr)
        return 1
    print(json.dumps(reply, indent=2, ensure_ascii=False))
    return 0 if reply.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
