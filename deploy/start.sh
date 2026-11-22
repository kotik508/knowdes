#!/bin/sh
# Start one storage node on every machine of the cluster config.
#   deploy/start.sh [deploy/cluster.toml]
# The repository must be checked out at $KNOWDES_DIR on every machine, with uv sync done.
set -eu
CONFIG=${1:-deploy/cluster.toml}
DIR=${KNOWDES_DIR:-knowdes}
uv run python -m knowdes.config "$CONFIG" | while read -r id host port; do
    # A node that is still running keeps its port and pid file.
    if ssh -n "$host" "cd $DIR && test -f data/$id.pid && kill -0 \$(cat data/$id.pid) 2>/dev/null"; then
        echo "$id already runs on $host"
        continue
    fi
    echo "starting $id on $host:$port"
    ssh -n "$host" "cd $DIR && mkdir -p data && { \
        nohup .venv/bin/python -m knowdes.node --config $CONFIG --id $id \
        >> data/$id.out 2>&1 < /dev/null & echo \$! > data/$id.pid; }"
done
