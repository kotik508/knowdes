# knowdes

A distributed knowledge graph for university data. Group 18 in Distributed Systems (CSM13001), University of Helsinki, fall 2026: Tim Kicker, Adam Kotík, Krish Chopra.

knowdes stores facts about university life as triples (subject, predicate, object), for example (CSM13001, exam_room, Exactum_A111). The graph is split into shards by subject and every shard is kept on three servers that agree on its contents with Raft. A write that also adds a reverse edge on another shard uses two-phase commit. AI assistants query the graph through MCP and there is a command line client as well.

The full design is in [docs/design-plan.pdf](docs/design-plan.pdf).

## Status

Every storage node runs one Raft group per shard with election, log replication, a log in SQLite and fast catch-up after a restart. Each node also answers Subscribe and Control. Each shard keeps its facts and the reverse edges of its objects. A router takes Put, Delete, BatchPut, Get and Neighbors without the client knowing about shards, with leader reads (ReadIndex) or follower reads, and the CLI talks to it. Of the goals until 29.11. only the console is missing, its plan is in `docs/console-plan.md` for the group to agree on. Every goal in the table below is a GitHub issue in the milestone with the same date.

| Until | Goal |
|---|---|
| 22.11. | Raft per shard on three machines: election, replication, logging. Subscribe, Event and Control in every node |
| 29.11. | graph store, router, CLI, catch-up after restart, leader and follower reads, first stage of the console |
| 6.12. | two-phase commit for reverse edges, MCP server, measurements, demo script |
| 7. or 8.12. | demo |
| 14.12. | code, report and individual quiz |

## Decisions

The design plan left three questions open (section 7) and issue #1 answers them.

| Question | Decision | Why |
|---|---|---|
| Store | SQLite through Python's `sqlite3`, one database file per storage node | Raft term, vote and log must survive a crash. SQLite gives atomic writes and needs nothing beyond the standard library |
| Machines | the course VMs `svm-11.cs.helsinki.fi`, `svm-11-2.cs.helsinki.fi` and `svm-11-3.cs.helsinki.fi` | three separate machines with their own IP as the project description asks and the teachers can reach them |
| Placement | a static shard map in the cluster config for the prototype | the placement service (#14) comes on top once Raft, store and router run. It also moves leaders with TimeoutNow |

Each machine runs one storage node with a replica of each of the three shards. Peer and client traffic uses one TCP port per node (7100).

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

## Deploy

`deploy/cluster.toml` lists the three course machines, `deploy/local.toml` three nodes on one computer.

```sh
deploy/local.sh start   # three nodes here, data and logs in data/
deploy/local.sh status
deploy/local.sh stop

deploy/start.sh         # one node and one router on every course machine over ssh
deploy/stop.sh
```

`start.sh` expects the repository in `~/knowdes` on every machine after `uv sync --python 3.12`, since the machines only have Python 3.10. `KNOWDES_DIR` points it somewhere else. From outside the university network the machines are only reachable through `melkki.cs.helsinki.fi`:

```
Host svm-11.cs.helsinki.fi svm-11-2.cs.helsinki.fi svm-11-3.cs.helsinki.fi
    User YOUR_UNI_USERNAME
    ProxyJump YOUR_UNI_USERNAME@melkki.cs.helsinki.fi
```

`deploy/local.sh start` also starts a router on port 7110, the CLI talks to it:

```sh
uv run knowdes put CSM13001 teacher niklander
uv run knowdes get CSM13001 --read follower
uv run knowdes neighbors niklander --direction in --depth 2
uv run knowdes delete CSM13001 teacher niklander
```

A plain write and its reverse edge on the shard of the object go in one two-phase commit (#12), so either both are there or neither is. If another transaction holds the subject or the object, the write aborts and the client tries again a few times. Facts with a source version get no reverse edge, because a newer version replaces the old value and its old reverse edge would stay behind.

Until the console exists a node can be watched and disturbed from the command line:

```sh
uv run python -m knowdes.debug subscribe 127.0.0.1:7101 --shard 0
uv run python -m knowdes.debug control 127.0.0.1:7101 isolate n2
uv run python -m knowdes.debug control 127.0.0.1:7101 heal
```

## Development

Python 3.11 or newer, managed with [uv](https://docs.astral.sh/uv/).

```sh
uv sync            # create the environment
uv run ruff check  # lint
uv run pytest      # tests
```

How we work together is in [CONTRIBUTING.md](CONTRIBUTING.md).
