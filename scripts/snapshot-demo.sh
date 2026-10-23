#!/usr/bin/env bash
# Snapshot check on a 3-node cluster that compacts every 5 entries:
# stop a follower, write 20 keys so the leader compacts past where that follower is,
# restart the follower and check it catches up with InstallSnapshot. Then kill the
# leader so the survivors (including the restored follower) must serve every key,
# and finally kill -9 the whole cluster and check everything reloads from snapshot + WAL.
set -euo pipefail
cd "$(dirname "$0")/.."

# TLS=1 runs the same checks with --tls: mutual TLS between nodes, TLS on the client port
TLS_ARGS=()
if [[ "${TLS:-0}" == 1 ]]; then
    TLS_ARGS=(--tls certs)
fi

NODES=3
KEYS=20
cargo build --quiet
if [[ "${TLS:-0}" == 1 && ! -f certs/ca.pem ]]; then scripts/gen-certs.sh 5 certs; fi
rm -rf cluster-logs && mkdir -p cluster-logs
rm -f log-*.txt meta-*.txt snapshot-*.bin

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

start_node() {
    ./target/debug/master "$1" --nodes "$NODES" --snapshot-every 5 ${TLS_ARGS[@]+"${TLS_ARGS[@]}"} >> "cluster-logs/node-$1.out" 2>&1 &
    pids[$(($1 - 1))]=$!
}
send() {
    if [[ "${TLS:-0}" == 1 ]]; then
        printf '%s\n' "$2" | openssl s_client -quiet -verify_return_error -CAfile certs/ca.pem \
            -connect 127.0.0.1:$((6379 + $1)) 2>/dev/null
    else
        printf '%s\n' "$2" | nc -w 2 127.0.0.1 $((6379 + $1))
    fi
}

# highest-term leader among the given nodes, looking only at output after the last marker
find_leader() {
    local best_node="" best_term=-1
    for i in "$@"; do
        local t
        t=$(awk '/--- mark ---/{buf=""} {buf=buf"\n"$0} END{print buf}' "cluster-logs/node-$i.out" \
            | grep -o "became leader for term [0-9]*" | tail -1 | awk '{print $NF}')
        if [[ -n "$t" && "$t" -gt "$best_term" ]]; then best_term=$t; best_node=$i; fi
    done
    echo "$best_node"
}
check_all_keys() {
    for k in $(seq 1 "$KEYS"); do
        local reply
        reply=$(send "$1" "GET key$k")
        [[ "$reply" == "value$k" ]] || { echo "FAIL: GET key$k on node $1 -> '$reply'"; exit 1; }
    done
    echo "node $1 returned all $KEYS keys"
}

for i in $(seq 1 "$NODES"); do start_node "$i"; done
sleep 2
leader=$(find_leader 1 2 3)
[[ -n "$leader" ]] || { echo "FAIL: no leader"; exit 1; }
lagger=$(( leader % NODES + 1 ))
echo "leader: node $leader, stopping follower node $lagger"
kill "${pids[$((lagger - 1))]}"; wait "${pids[$((lagger - 1))]}" 2>/dev/null || true

for k in $(seq 1 "$KEYS"); do
    [[ "$(send "$leader" "SET key$k value$k")" == "OK" ]] || { echo "FAIL: SET key$k"; exit 1; }
done
echo "wrote $KEYS keys with node $lagger down"
grep -q "compacted log" "cluster-logs/node-$leader.out" || { echo "FAIL: leader never compacted"; exit 1; }
grep "compacted log" "cluster-logs/node-$leader.out" | tail -1

echo "restarting node $lagger"
start_node "$lagger"
sleep 2
grep -q "installed snapshot" "cluster-logs/node-$lagger.out" || { echo "FAIL: node $lagger never installed a snapshot"; exit 1; }
grep "installed snapshot" "cluster-logs/node-$lagger.out" | tail -1

echo "killing leader node $leader"
for i in $(seq 1 "$NODES"); do echo "--- mark ---" >> "cluster-logs/node-$i.out"; done
kill "${pids[$((leader - 1))]}"; wait "${pids[$((leader - 1))]}" 2>/dev/null || true
sleep 3
survivors=$(seq 1 "$NODES" | grep -vx "$leader")
new_leader=$(find_leader $survivors)
[[ -n "$new_leader" ]] || { echo "FAIL: no new leader"; exit 1; }
echo "new leader: node $new_leader"
check_all_keys "$new_leader"

echo "kill -9 on all nodes, then restarting them"
for p in "${pids[@]}"; do kill -9 "$p" 2>/dev/null || true; done
wait 2>/dev/null || true
for i in $(seq 1 "$NODES"); do echo "--- mark ---" >> "cluster-logs/node-$i.out"; start_node "$i"; done
sleep 3
for i in $(seq 1 "$NODES"); do echo "node $i: $(grep Recovered "cluster-logs/node-$i.out" | tail -1)"; done
leader=$(find_leader 1 2 3)
[[ -n "$leader" ]] || { echo "FAIL: no leader after restart"; exit 1; }
check_all_keys "$leader"

echo "PASS"
