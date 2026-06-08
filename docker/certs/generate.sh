#!/usr/bin/env sh
#
# Regenerate the CamusDB local-development cluster certificate.
#
# Produces, in this directory:
#   development-private.key      RSA 2048-bit private key
#   development-certificate.csr  certificate signing request
#   development-certificate.crt  self-signed certificate (PEM) — add this to a trust store
#   development-certificate.pfx  PKCS#12 bundle (no password) loaded by Kestrel
#
# The SANs come from san.cnf; edit that file if the node IPs/hostnames change, then rerun.
# After regenerating, rebuild the images so the new .pfx is baked in:
#   docker compose -f docker/local.yml build
#
# These are development-only, self-signed credentials. Do not use them in production.

set -eu

cd "$(dirname "$0")"

echo "==> generating RSA private key"
openssl genrsa -out development-private.key 2048

echo "==> generating CSR from san.cnf"
openssl req -new -nodes \
  -config san.cnf \
  -key development-private.key \
  -out development-certificate.csr

echo "==> self-signing the certificate (10-year validity, SAN + EKU from san.cnf)"
openssl x509 -req \
  -in development-certificate.csr \
  -signkey development-private.key \
  -out development-certificate.crt \
  -days 3650 \
  -extensions req_ext \
  -extfile san.cnf

echo "==> exporting password-less PKCS#12 bundle for Kestrel"
openssl pkcs12 -export \
  -out development-certificate.pfx \
  -inkey development-private.key \
  -in development-certificate.crt \
  -passout pass:

echo "==> verifying SANs"
openssl x509 -in development-certificate.crt -noout -text \
  | grep -A1 "Subject Alternative Name"

echo "Done. Rebuild images: docker compose -f docker/local.yml build"
