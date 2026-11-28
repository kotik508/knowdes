# Demo

About five minutes for the demo session on 7 or 8 December (#16). The script runs the steps while the console shows what happens in the cluster.

## Setup

On the laptop that is connected to the projector:

```sh
uv sync
deploy/demo.sh start    # n1 to n3, router, gateway and an empty n4
```

Open http://127.0.0.1:7120/ and put the browser and a terminal side by side. Then

```sh
deploy/demo.sh run      # waits for Enter after every step
```

and `deploy/demo.sh stop` at the end, which also deletes `data/demo/`. The same script works against the course machines with `python -m knowdes.demo --config deploy/cluster.toml` once they run.

## Steps

| Step | Script | Console | What to say |
|---|---|---|---|
| 1 Cluster | prints the leader of each shard | three cards, one leader per shard | every node holds a replica of every shard, each shard is its own Raft group |
| 2 Writes | four puts about two courses, then who teaches what | commit events on two shards per put, prepare and decide in the timeline | a put also writes the reverse edge on the shard of the object, both in one two-phase commit |
| 3 Leader fails | pauses the leader of shard 0 and waits for a new one | the card turns dashed, election and vote events, a new leader with a higher term | a paused node stops answering its peers, the others time out after 150 to 300 ms |
| 4 Writes go on | one put and a follower read | the put commits without the paused node | two of three replicas are a majority. A follower read may be a few entries behind |
| 5 Comeback | resumes the node and waits until it has caught up | the card shows how far behind it is until the number is gone | the node finds the newer term, becomes a follower and gets the missing entries |
| 6 Fourth node | n4 joins every shard, then one more put | replicas in the commit events can now reach 4. n4 has no card, the gateway only knows the nodes in the config | the project description asks to begin with three nodes and show four and more |
| 7 Spread | moves leaders so that n1, n2 and n3 lead one shard each | the new leader is elected at once, without waiting for a timeout | leader transfer with TimeoutNow from the Raft thesis |

If time is left, the query panel of the console can run a Neighbors query over the reverse edges, for example from `CSM13001` with depth 2.

## Fallback

The script stops with an error and the cluster keeps running. `deploy/demo.sh stop` and `start` give a fresh cluster in a few seconds. The measurements in `docs/measurements.md` are the fallback for questions about speed.
