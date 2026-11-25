#!/bin/sh
# One round of measurements (#15) against a running cluster, results appended
# to data/bench-results.jsonl. Start the cluster first, e.g. deploy/local.sh start.
#   deploy/bench.sh [CONFIG] [LABEL]
set -eu
CONFIG=${1:-deploy/local.toml}
LABEL=${2:-$(basename "$CONFIG" .toml)}
OUT=data/bench-results.jsonl
mkdir -p data
run() { .venv/bin/python -m knowdes.bench --config "$CONFIG" --out "$OUT" --label "$LABEL" "$@"; }
for clients in 1 8 32; do
    run writes --count 2000 --clients "$clients"
done
run reads --count 5000 --clients 16 --read leader
run reads --count 5000 --clients 16 --read follower
for shard in 0 1 2; do
    run failover --shard "$shard" --rounds 5
done
echo "results in $OUT"
