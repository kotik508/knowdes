# knowdes

A distributed knowledge graph for university data. Group 18 in Distributed Systems (CSM13001), University of Helsinki, fall 2026: Tim Kicker, Adam Kotík, Krish Chopra.

knowdes stores facts about university life as triples (subject, predicate, object), for example (CSM13001, exam_room, Exactum_A111). The graph is split into shards by subject and every shard is kept on three servers that agree on its contents with Raft. A write that also adds a reverse edge on another shard uses two-phase commit. AI assistants query the graph through MCP and there is a command line client as well.

The full design is in [docs/design-plan.pdf](docs/design-plan.pdf).

## Status

The repository is set up, no code yet. Milestones are tracked as GitHub issues.

| Until | Goal |
|---|---|
| 22.11. | Raft per shard on three machines: election, replication, logging. Subscribe, Event and Control in every node |
| 29.11. | graph store, router, CLI, catch-up after restart, leader and follower reads, first stage of the console |
| 6.12. | two-phase commit for reverse edges, MCP server, measurements, demo script |
| 7. or 8.12. | demo |
| 14.12. | code, report and individual quiz |

## Layout

Every topic of the design plan has one place in the code, so the report can point to it directly.

| Path | What goes there | Design plan |
|---|---|---|
| `src/knowdes/protocol.py` | message types and JSON framing over TCP | section 4 |
| `src/knowdes/raft/` | election, log, replication, catch-up | section 3 |
| `src/knowdes/store/` | triples and reverse edges of one shard | section 3 |
| `src/knowdes/sharding.py` | subject hash to virtual shard to Raft group | section 3 |
| `src/knowdes/router.py` | stateless router that forwards to the leader | section 2 |
| `src/knowdes/twopc.py` | two-phase commit for writes on two shards | section 3 |
| `src/knowdes/placement.py` | placement service | section 2 |
| `src/knowdes/debug.py` | Subscribe, Event and Control | section 5 |
| `src/knowdes/cli.py`, `src/knowdes/mcp_server.py` | clients | section 2 |
| `console/` | gateway and web console | section 5 |
| `deploy/` | cluster config and start and stop scripts for the three machines | section 6 |
| `bench/` | measurements for the report | section 7 |
| `tests/` | unit tests with a simulated network, local runs with three processes | |
| `docs/` | design plan and later the report | |

The folders are created together with their first code.

## Development

Python 3.11 or newer, managed with [uv](https://docs.astral.sh/uv/).

```sh
uv sync            # create the environment
uv run ruff check  # lint
uv run pytest      # tests
```

How we work together is in [CONTRIBUTING.md](CONTRIBUTING.md).
