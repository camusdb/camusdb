#!/usr/bin/env sh
#
# Regenerate the CamusDB local-development cluster certificate.
#
# Produces, in OUT_DIR (default: this directory):
#   development-private.key      RSA 2048-bit private key
#   development-certificate.csr  certificate signing request
#   development-certificate.crt  self-signed certificate (PEM) — add this to a trust store
#   development-certificate.pfx  PKCS#12 bundle (no password) loaded by Kestrel
#
# Two modes:
#   - Default: the SANs come from san.cnf; edit that file if the node IPs/hostnames change,
#     then rerun.
#   - Parameterized (for N-node / orchestrated clusters): set NODES and the script generates
#     the SAN list itself — DNS names camus1..camusN and container IPs SUBNET.FIRST_IP
#     onward, with SPARE extra entries so nodes added later (join-existing) are already
#     covered without regenerating and rebaking the image.
#
#       NODES=5 SPARE=5 SUBNET=172.31.0 FIRST_IP=2 OUT_DIR=/tmp/certs ./generate.sh
#
# After regenerating, rebuild the images so the new .pfx is baked in:
#   docker compose -f docker/local.yml build
#
# These are development-only, self-signed credentials. Do not use them in production.

set -eu

cd "$(dirname "$0")"

OUT_DIR="${OUT_DIR:-.}"
mkdir -p "$OUT_DIR"

if [ -n "${NODES:-}" ]; then
  SPARE="${SPARE:-5}"
  SUBNET="${SUBNET:-172.31.0}"
  FIRST_IP="${FIRST_IP:-2}"
  TOTAL=$((NODES + SPARE))

  CNF="$OUT_DIR/san.generated.cnf"
  echo "==> generating $CNF for $NODES nodes (+$SPARE spare) on $SUBNET.x"

  # Same DN/extensions as san.cnf; only the alt_names block is derived.
  cat > "$CNF" <<EOF
[req]
default_bits       = 2048
prompt             = no
default_md         = sha256
distinguished_name = dn
req_extensions     = req_ext
x509_extensions    = req_ext

[dn]
C  = CO
ST = BOG
L  = BOG
O  = CamusDB Development
CN = camus-cluster

[req_ext]
subjectAltName   = @alt_names
# Nodes act as both gRPC server and client, so both EKUs are needed.
extendedKeyUsage = serverAuth, clientAuth
# Self-signed cert doubles as its own trust anchor when added to the container CA store.
basicConstraints = critical, CA:TRUE

[alt_names]
DNS.1 = localhost
IP.1  = 127.0.0.1
IP.2  = ::1
EOF

  i=1
  while [ "$i" -le "$TOTAL" ]; do
    echo "DNS.$((i + 1)) = camus$i" >> "$CNF"
    echo "IP.$((i + 2))  = $SUBNET.$((FIRST_IP + i - 1))" >> "$CNF"
    i=$((i + 1))
  done
else
  CNF=san.cnf
fi

echo "==> generating RSA private key"
openssl genrsa -out "$OUT_DIR/development-private.key" 2048

echo "==> generating CSR from $CNF"
openssl req -new -nodes \
  -config "$CNF" \
  -key "$OUT_DIR/development-private.key" \
  -out "$OUT_DIR/development-certificate.csr"

echo "==> self-signing the certificate (10-year validity, SAN + EKU from $CNF)"
openssl x509 -req \
  -in "$OUT_DIR/development-certificate.csr" \
  -signkey "$OUT_DIR/development-private.key" \
  -out "$OUT_DIR/development-certificate.crt" \
  -days 3650 \
  -extensions req_ext \
  -extfile "$CNF"

echo "==> exporting password-less PKCS#12 bundle for Kestrel"
openssl pkcs12 -export \
  -out "$OUT_DIR/development-certificate.pfx" \
  -inkey "$OUT_DIR/development-private.key" \
  -in "$OUT_DIR/development-certificate.crt" \
  -passout pass:

echo "==> verifying SANs"
openssl x509 -in "$OUT_DIR/development-certificate.crt" -noout -text \
  | grep -A1 "Subject Alternative Name"

echo "Done. Rebuild images: docker compose -f docker/local.yml build"
