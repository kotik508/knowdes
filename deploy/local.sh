#!/bin/sh
# Three nodes on this computer, from deploy/local.toml.
#   deploy/local.sh start | stop | status
set -eu
CONFIG=deploy/local.toml
mkdir -p data
case "${1:-}" in
start)
    uv run python -m knowdes.config "$CONFIG" | while read -r id host port; do
        if [ -f "data/$id.pid" ] && kill -0 "$(cat "data/$id.pid")" 2>/dev/null; then
            echo "$id already runs"
            continue
        fi
        .venv/bin/python -m knowdes.node --config "$CONFIG" --id "$id" >> "data/$id.out" 2>&1 &
        echo $! > "data/$id.pid"
        echo "$id on $host:$port, pid $!"
    done ;;
stop)
    for pidfile in data/n*.pid; do
        [ -f "$pidfile" ] || continue
        kill "$(cat "$pidfile")" 2>/dev/null || true
        rm "$pidfile"
    done ;;
status)
    uv run python -m knowdes.config "$CONFIG" | while read -r id host port; do
        uv run python -c "import asyncio, json; from knowdes import protocol; \
print(json.dumps(asyncio.run(protocol.request(('$host', $port), {'type': 'Status'}, 1.0))))" \
            || echo "$id not reachable"
    done ;;
*)
    echo "usage: $0 start | stop | status" >&2
    exit 2 ;;
esac
