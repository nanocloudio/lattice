#!/usr/bin/env bash
# Development TLS material for lattice's TLS-fronted graphs (configs/single.yaml):
# a local CA and a server certificate it issues, in the DER forms fluxor's
# `tls` module reads.
#
#   tools/dev-certs.sh            mint into certs/dev/ (kept if present)
#   tools/dev-certs.sh --force    mint afresh
#
# Out (certs/dev/, gitignored):
#   ca.pem, ca.der              the dev CA — what a client trusts (etcdctl --cacert)
#   server.der, server.key.der  the server's certificate and key (`cert_file` /
#                               `key_file`), valid for localhost and 127.0.0.1
#
# The server certificate has the shape fluxor's chain validator demands of a
# leaf (CA:FALSE, digitalSignature, serverAuth), so the same CA also serves
# graphs that turn on client-certificate authentication (`peer_auth: 2`).
# A deployment points `cert_file` / `key_file` at its own material instead.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
D="$ROOT/certs/dev"
[ "${1:-}" = "--force" ] && rm -rf "$D"
if [ -s "$D/server.der" ] && [ -s "$D/server.key.der" ] && [ -s "$D/ca.der" ]; then
  echo "certs/dev: present (use --force to mint afresh)"
  exit 0
fi
command -v openssl >/dev/null || { echo "FAIL: openssl not found"; exit 1; }
mkdir -p "$D"
T="$(mktemp -d)"
trap 'rm -rf "$T"' EXIT

# The CA: P-256, self-signed, a CA.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$T/ca.key.pem" -out "$D/ca.pem" -days 825 -subj "/CN=lattice-dev-ca" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
openssl x509 -in "$D/ca.pem" -outform DER -out "$D/ca.der"

# The server: P-256, issued by the CA, a leaf for serverAuth.
openssl req -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout "$T/server.key.pem" -out "$T/server.csr" -subj "/CN=localhost" 2>/dev/null
cat >"$T/server.ext" <<'EXT'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
EXT
openssl x509 -req -in "$T/server.csr" -CA "$D/ca.pem" -CAkey "$T/ca.key.pem" \
  -CAcreateserial -CAserial "$T/ca.srl" -out "$T/server.pem" -days 825 \
  -extfile "$T/server.ext" 2>/dev/null
openssl x509 -in "$T/server.pem" -outform DER -out "$D/server.der"
openssl ec -in "$T/server.key.pem" -outform DER -out "$D/server.key.der" 2>/dev/null

echo "certs/dev: minted (trust certs/dev/ca.pem)"
