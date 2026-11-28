#!/bin/sh
# The demo (#16) on this computer: three nodes, a router, the console and an
# empty fourth node, all with their own data in data/demo/.
#   deploy/demo.sh start   start everything, the console is on http://127.0.0.1:7120/
#   deploy/demo.sh run     the steps of src/knowdes/demo.py, Enter after each one
#   deploy/demo.sh stop    stop everything and forget the demo data
set -eu
DIR=data/demo
CONFIG=$DIR/cluster.toml
N4=n4=127.0.0.1:7104
running() { [ -f "$1" ] && kill -0 "$(cat "$1")" 2>/dev/null; }
spawn() {  # spawn NAME COMMAND..., output in data/demo/NAME.out
    name=$1
    shift
    if running "$DIR/$name.pid"; then
        echo "$name already runs"
        return
    fi
    "$@" >> "$DIR/$name.out" 2>&1 &
    echo $! > "$DIR/$name.pid"
    echo "$name started, pid $!"
}
case "${1:-}" in
start)
    mkdir -p "$DIR"
    sed "s|^data_dir = .*|data_dir = \"$DIR\"|" deploy/local.toml > "$CONFIG"
    for id in n1 n2 n3; do
        spawn "$id" .venv/bin/python -m knowdes.node --config "$CONFIG" --id "$id"
    done
    spawn router .venv/bin/python -m knowdes.router --config "$CONFIG"
    spawn gateway .venv/bin/python -m knowdes.gateway --config "$CONFIG"
    spawn n4 .venv/bin/python -m knowdes.node --id n4 --listen 127.0.0.1:7104 --empty \
        --data-dir "$DIR" --peer n1=127.0.0.1:7101 --peer n2=127.0.0.1:7102 --peer n3=127.0.0.1:7103
    echo "console on http://127.0.0.1:7120/" ;;
run)
    .venv/bin/python -m knowdes.demo --config "$CONFIG" --join "$N4" --wait ;;
stop)
    for pidfile in "$DIR"/*.pid; do
        [ -f "$pidfile" ] || continue
        kill "$(cat "$pidfile")" 2>/dev/null || true
        rm "$pidfile"
    done
    rm -rf "$DIR" ;;
*)
    echo "usage: $0 start | run | stop" >&2
    exit 2 ;;
esac
