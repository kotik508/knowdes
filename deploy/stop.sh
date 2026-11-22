#!/bin/sh
# Stop the storage nodes that deploy/start.sh started. Data stays in data/.
#   deploy/stop.sh [deploy/cluster.toml]
set -eu
CONFIG=${1:-deploy/cluster.toml}
DIR=${KNOWDES_DIR:-knowdes}
uv run python -m knowdes.config "$CONFIG" | while read -r id host port; do
    echo "stopping $id on $host"
    ssh -n "$host" "cd $DIR && test -f data/$id.pid && kill \$(cat data/$id.pid) && rm data/$id.pid" \
        || echo "$id was not running"
done
