# Measurements

Laptop measurement, not the course machines. All numbers below come from one run on 27 November 2026 on a single laptop (AMD Ryzen 7 7840U, 16 threads, Linux 7.2, Python 3.13). Every storage node, the router and the benchmark client ran on that one machine and talked over loopback, so the numbers show what the protocols cost relative to each other and not what a cluster of separate machines can do. The raw results are in [measurements/laptop-2026-11-27.jsonl](measurements/laptop-2026-11-27.jsonl), one JSON line per run (#15).

## Setup

Three clusters one after the other with 3, 4 and 5 storage nodes, 3 shards each and one router. In this version every node holds a replica of every shard, so a bigger cluster means a bigger Raft group per shard and not more capacity. For every size `deploy/bench.sh` did the same runs through the router:

- writes: 2000 puts with 1, 8 and 32 clients at once
- reads: 5000 gets with 16 clients, once at the leader (ReadIndex) and once at any follower
- failover: for each shard five times pause the leader and time until another node leads it

All processes ran with `nice -n 15`. No run had an error.

## Writes

| nodes | clients | writes/s | median ms | p95 ms |
|---|---|---|---|---|
| 3 | 1 | 101.5 | 9.99 | 12.97 |
| 3 | 8 | 170.5 | 46.53 | 56.10 |
| 3 | 32 | 143.7 | 184.22 | 393.98 |
| 4 | 1 | 88.8 | 11.85 | 14.61 |
| 4 | 8 | 128.2 | 55.07 | 133.10 |
| 4 | 32 | 161.3 | 205.75 | 243.60 |
| 5 | 1 | 80.9 | 13.08 | 16.02 |
| 5 | 8 | 124.7 | 61.06 | 98.57 |
| 5 | 32 | 130.6 | 238.11 | 397.47 |

Every put also writes its reverse edge. Since 26 November both go through one two-phase commit (#12). A single put is therefore a prepare on two shards, a decision in the log of the coordinator shard and a commit on both, each a Raft round. With one client that takes about 10 ms. More clients raise the throughput only a little, from 80 to 100 writes per second with one client to 125 to 170 with 8 or 32. Beyond that the writes wait on the locks of the two-phase commit and on the one CPU all nodes share, so the latency grows while the throughput stays flat. Every extra node is one more replica in each Raft group, which adds one to two milliseconds to a single write.

## Reads

| nodes | read | reads/s | median ms | p95 ms |
|---|---|---|---|---|
| 3 | leader | 1846.9 | 8.13 | 12.41 |
| 3 | follower | 3265.0 | 4.77 | 5.77 |
| 4 | leader | 1844.8 | 8.98 | 13.01 |
| 4 | follower | 3197.6 | 4.90 | 5.65 |
| 5 | leader | 1574.8 | 10.95 | 15.28 |
| 5 | follower | 2915.0 | 5.16 | 7.57 |

A leader read first confirms with a majority that the leader still leads (ReadIndex), a follower read answers from what the replica has applied. The follower read is about twice as fast and may miss the newest writes (#10).

## Failover

| nodes | shard | mean ms | median ms | p95 ms |
|---|---|---|---|---|
| 3 | 0 | 178.71 | 196.32 | 206.02 |
| 3 | 1 | 168.20 | 163.69 | 213.50 |
| 3 | 2 | 153.74 | 151.21 | 176.79 |
| 4 | 0 | 268.48 | 185.58 | 625.43 |
| 4 | 1 | 228.25 | 203.55 | 341.23 |
| 4 | 2 | 281.54 | 212.63 | 563.94 |
| 5 | 0 | 164.19 | 170.36 | 182.72 |
| 5 | 1 | 150.32 | 146.90 | 179.50 |
| 5 | 2 | 150.72 | 146.82 | 171.56 |

The time runs from pausing the leader until another node reports itself leader of that shard. It is mostly the election timeout, a random value between 150 and 300 ms, plus one election round. With four nodes a new leader needs three of four votes, so all three remaining nodes have to agree. If two of them become candidates at the same time, neither gets a majority and a new election follows. That shows in the slower rounds with four nodes. With five nodes three of the four remaining votes are enough.

## Repeating it

```
deploy/local.sh start      # or a config with more nodes
deploy/bench.sh deploy/local.toml "3 nodes"
```
