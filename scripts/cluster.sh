#!/usr/bin/env bash
# Start an N-node cluster on localhost (default 5). Node i listens for raft on 5000+i
# and for clients on 6379+i. Output goes to cluster-logs/node-<i>.out.
# Ctrl-C stops every node.
set -euo pipefail
cd "$(dirname "$0")/.."

NODES="${1:-5}"
cargo build --quiet
mkdir -p cluster-logs

pids=()
trap 'kill "${pids[@]}" 2>/dev/null; wait 2>/dev/null' EXIT INT TERM
for i in $(seq 1 "$NODES"); do
    ./target/debug/master "$i" --nodes "$NODES" > "cluster-logs/node-$i.out" 2>&1 &
    pids+=($!)
    echo "node $i: pid $! raft 127.0.0.1:$((5000 + i)) client 127.0.0.1:$((6379 + i))"
done
wait
