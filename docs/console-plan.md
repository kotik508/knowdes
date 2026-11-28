# Web console, plan for the first stage (#11)

Issue #11 asked for a plan before anything was built. The first stage below is built. Of the second stage the console also shows the steps of a two-phase commit (list of transactions). Isolate and the request route are still open.

## What the demo needs

The first stage covers what we show on 7 or 8 December: the cluster view, the timeline, the query panel and pause and resume. Isolate, the request route in the cluster view and the steps of a two-phase commit come in the second stage, when #12 exists.

## Pieces

**Gateway.** A small Python process next to a router, `python -m knowdes.gateway --config deploy/local.toml`, port 7120. It does three things:

1. It holds one Subscribe connection to every storage node and merges their Event messages into one stream. A node that drops out is retried every second and shown as down.
2. It asks every node for Status twice a second, which gives role, term, commit index and applied index per replica.
3. It passes Put, Delete, Get, Neighbors and Control from the browser to a router or a node and sends back the reply with the time it took.

The browser gets the events and the status through server-sent events (`GET /events`) and sends requests with `POST /request`. Server-sent events go one way only, which is all the console needs. They work with the standard library on the Python side and with `EventSource` in the browser. So the gateway needs no new dependency.

**Page.** One HTML file with plain JavaScript and CSS, served by the gateway at `/`. No build step, so it runs on the course machines as it is. If the second stage outgrows that we can still move to Svelte (the `.gitignore` already expects `console/build/`).

## Parts of the page

- **Cluster view.** One card per machine with its storage node and its three replicas. A replica shows its role, term, commit index and how far its applied index is behind the leader's. A node without an answer to Status turns grey.
- **Timeline.** The merged events, newest on top, filtered by shard and by kind (election, vote, leader, commit, pause, resume). It keeps the last 500.
- **Query panel.** A form for Put, Delete, Get and Neighbors with the choice of leader or follower read. Next to the answer it shows which node answered, its applied index and the time.
- **Fault panel.** Pause and resume per node, sent as Control.

## What has to change elsewhere

- Node: nothing. Subscribe, Event, Status and Control are there already.
- Router: nothing for the first stage. For the request route in the second stage it would report every forwarded request as an event.

## Open for the group

1. Plain JavaScript or Svelte from the start.
2. Who builds the gateway and who builds the page. They only meet at `/events` and `/request`, so two people can work on them at the same time.
3. Whether the console runs on one course machine or on a laptop with an SSH tunnel for the demo.

## Done when

The demo script (#16) runs against it: pause the leader of a shard, the timeline shows the election, the cluster view shows the new leader, a Put through the query panel still works, resume brings the node back and it catches up.
