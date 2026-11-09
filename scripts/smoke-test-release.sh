#!/usr/bin/env bash
#
# Start a server from a release archive and run one write and one read against it. The release
# workflow runs this on a runner of the archive's own platform before it attaches the archive to the
# release, so an archive that cannot load its native libraries (RocksDB, the .NET host) never ships.
#
# --version alone is not enough: it exits before the storage engine starts, so it does not load
# librocksdb. This script starts the node, creates a database and a table, writes a row, reads it
# back, and stops the node.
#
# Usage:
#   scripts/smoke-test-release.sh artifacts/release/camusdb-0.17.2-osx-arm64.tar.gz [http-port]

set -euo pipefail

ARCHIVE="${1:?usage: scripts/smoke-test-release.sh <archive.tar.gz> [http-port]}"
HTTP_PORT="${2:-15095}"
GRPC_PORT="$((HTTP_PORT + 1))"

WORK="$(mktemp -d)"
SERVER_PID=""

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

tar -xzf "$ARCHIVE" -C "$WORK"
APP_DIR="$(find "$WORK" -mindepth 1 -maxdepth 1 -type d | head -n1)"
SERVER="$APP_DIR/CamusDB"

echo "== $("$SERVER" --version)"

# CAMUS_HOME keeps the configuration and the data inside the scratch directory.
export CAMUS_HOME="$WORK/home"
"$SERVER" init
"$SERVER" --http-port="$HTTP_PORT" --grpc-port="$GRPC_PORT" > "$WORK/server.log" 2>&1 &
SERVER_PID=$!

URL="http://localhost:$HTTP_PORT"
for _ in $(seq 1 60); do
  if curl -s -o /dev/null "$URL/"; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "error: the server exited during startup" >&2
    cat "$WORK/server.log" >&2
    exit 1
  fi
  sleep 1
done

post() {
  local path="$1" body="$2" response
  response="$(curl -s -H 'Content-Type: application/json' -d "$body" "$URL$path")"
  echo "$path -> $response"
  if [[ "$response" != *'"status":"ok"'* ]]; then
    echo "error: $path did not return status ok" >&2
    cat "$WORK/server.log" >&2
    exit 1
  fi
  LAST_RESPONSE="$response"
}

post /create-db '{"databaseName":"smoke"}'
post /execute-sql-ddl '{"databaseName":"smoke","sql":"CREATE TABLE t (id INT64 PRIMARY KEY, name STRING)"}'
post /execute-sql-non-query "{\"databaseName\":\"smoke\",\"sql\":\"INSERT INTO t (id, name) VALUES (1, 'release')\"}"
post /execute-sql-query '{"databaseName":"smoke","sql":"SELECT name FROM t WHERE id = 1"}'

if [[ "$LAST_RESPONSE" != *'"release"'* ]]; then
  echo "error: the row written was not read back" >&2
  exit 1
fi

echo "== smoke test passed"
