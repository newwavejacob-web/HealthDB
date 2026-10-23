#!/usr/bin/env bash
# Generate a cluster CA and a cert per node for --tls. Usage: scripts/gen-certs.sh [nodes] [dir]
# Each node cert is valid for 127.0.0.1 and localhost, and for both server and client auth,
# since nodes use the same cert to accept rpcs and to make them (mutual TLS).
# Keep ca-key.pem private: anyone with it can mint a cert the cluster will trust.
set -euo pipefail

NODES="${1:-5}"
DIR="${2:-certs}"
mkdir -p "$DIR"
cd "$DIR"

openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out ca-key.pem 2>/dev/null
openssl req -x509 -new -key ca-key.pem -sha256 -days 365 -subj "/CN=HealthDB cluster CA" -out ca.pem

for i in $(seq 1 "$NODES"); do
    openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "node-$i-key.pem" 2>/dev/null
    openssl req -new -key "node-$i-key.pem" -subj "/CN=healthdb-node-$i" -out "node-$i.csr"
    cat > "node-$i.ext" <<EXT
basicConstraints = CA:FALSE
keyUsage = digitalSignature
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = IP:127.0.0.1, DNS:localhost
EXT
    openssl x509 -req -in "node-$i.csr" -CA ca.pem -CAkey ca-key.pem -CAcreateserial \
        -days 365 -sha256 -extfile "node-$i.ext" -out "node-$i.pem" 2>/dev/null
    rm "node-$i.csr" "node-$i.ext"
done
chmod 600 ./*-key.pem
rm -f ca.srl
echo "wrote CA and $NODES node certs to $DIR/"
