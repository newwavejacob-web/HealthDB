#!/usr/bin/env bash
# 5-node failover check: start the cluster, write a key through the leader, kill the
# leader, wait for a new one to be elected, then write and read through the new leader.
# Finally kill -9 every node, restart them all, and check the data came back from disk.
# Exits non-zero if any step fails.
set -euo pipefail
cd "$(dirname "$0")/.."

NODES=5
cargo build --quiet
rm -rf cluster-logs && mkdir -p cluster-logs
rm -f log-*.txt meta-*.txt snapshot-*.bin

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

for i in $(seq 1 "$NODES"); do
    ./target/debug/master "$i" --nodes "$NODES" > "cluster-logs/node-$i.out" 2>&1 &
    pids+=($!)
done

# the leader is the node with the highest "became leader for term T" among live nodes
find_leader() {
    local best_node="" best_term=-1
    for i in $(seq 1 "$NODES"); do
        [[ " $* " == *" $i "* ]] && continue   # skip nodes passed as dead
        local t
        t=$(grep -o "became leader for term [0-9]*" "cluster-logs/node-$i.out" 2>/dev/null | tail -1 | awk '{print $NF}')
        if [[ -n "$t" && "$t" -gt "$best_term" ]]; then best_term=$t; best_node=$i; fi
    done
    echo "$best_node"
}

send() { printf '%s\n' "$2" | nc -w 2 127.0.0.1 $((6379 + $1)); }

sleep 2
leader=$(find_leader)
[[ -n "$leader" ]] || { echo "FAIL: no leader elected"; exit 1; }
echo "leader: node $leader"

reply=$(send "$leader" "SET patient 42")
echo "SET patient 42 -> $reply"
[[ "$reply" == "OK" ]] || { echo "FAIL: write before failover"; exit 1; }

echo "killing node $leader"
kill "${pids[$((leader - 1))]}"
sleep 3

new_leader=$(find_leader "$leader")
[[ -n "$new_leader" && "$new_leader" != "$leader" ]] || { echo "FAIL: no new leader"; exit 1; }
echo "new leader: node $new_leader"

reply=$(send "$new_leader" "GET patient")
echo "GET patient -> $reply"
[[ "$reply" == "42" ]] || { echo "FAIL: committed value lost"; exit 1; }

reply=$(send "$new_leader" "SET patient 43")
echo "SET patient 43 -> $reply (4 of 5 nodes up)"
[[ "$reply" == "OK" ]] || { echo "FAIL: write after failover"; exit 1; }

start_node() {
    ./target/debug/master "$1" --nodes "$NODES" >> "cluster-logs/node-$1.out" 2>&1 &
    pids[$(($1 - 1))]=$!
}

echo "kill -9 on all nodes, then restarting them"
for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
wait 2>/dev/null || true
for i in $(seq 1 "$NODES"); do echo "--- restart ---" >> "cluster-logs/node-$i.out"; done
for i in $(seq 1 "$NODES"); do start_node "$i"; done
sleep 3

# only look at output written after the restart
find_leader_after_restart() {
    local best_node="" best_term=-1
    for i in $(seq 1 "$NODES"); do
        local t
        t=$(sed -n '/--- restart ---/,$p' "cluster-logs/node-$i.out" | grep -o "became leader for term [0-9]*" | tail -1 | awk '{print $NF}')
        if [[ -n "$t" && "$t" -gt "$best_term" ]]; then best_term=$t; best_node=$i; fi
    done
    echo "$best_node"
}
leader=$(find_leader_after_restart)
[[ -n "$leader" ]] || { echo "FAIL: no leader after restart"; exit 1; }
echo "leader after restart: node $leader"

reply=$(send "$leader" "GET patient")
echo "GET patient -> $reply"
[[ "$reply" == "43" ]] || { echo "FAIL: data lost across restart"; exit 1; }

echo "PASS"
