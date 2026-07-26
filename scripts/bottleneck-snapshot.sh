#!/usr/bin/env bash
#
# This file is part of CamusDB
#
# For the full copyright and license information, please view the LICENSE.txt
# file that was distributed with this source code.
#
# One-command bottleneck snapshot for a STANDALONE CamusDB node. Starts a Release server with
# diagnostics enabled against an explicit temp data dir, waits for readiness (no fixed sleeps),
# drives the mixed workload under a shared run id, scrapes /metrics, and generates a self-contained
# client+server evidence bundle plus bottleneck-report.md. Stops only the server it started and
# cleans up on interruption; server logs and artifacts are preserved when a run fails.
#
# Usage:
#   scripts/bottleneck-snapshot.sh <output-dir> [--workers N] [--duration 5m] [--rows N] [--target-ops N] [--mode open|closed]
#
# The output directory must NOT already exist (a result bundle is never silently overwritten).

set -euo pipefail

OUT="${1:-}"
if [[ -z "$OUT" ]]; then
  echo "usage: $0 <output-dir> [--workers N] [--duration 5m] [--rows N] [--target-ops N] [--mode open|closed]" >&2
  exit 2
fi
shift || true

if [[ -e "$OUT" ]]; then
  echo "output directory already exists: $OUT — refusing to overwrite." >&2
  exit 2
fi

# Defaults (overridable via flags).
WORKERS=64
DURATION=5m
WARMUP=30s
ROWS=100000
TARGET_OPS=800
MODE=closed
DATABASE=perf_snapshot

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workers) WORKERS="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --target-ops) TARGET_OPS="$2"; shift 2 ;;
    --mode) MODE="$2"; shift 2 ;;
    --database) DATABASE="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GRPC_ENDPOINT="http://127.0.0.1:5096"
METRICS_URL="http://127.0.0.1:5095/metrics"
RUN_ID="snapshot-$$"

mkdir -p "$OUT"
OUT="$(cd "$OUT" && pwd)"
DATADIR="$(mktemp -d "${TMPDIR:-/tmp}/camus-snapshot-data.XXXXXX")"
CONFIG_USED="$OUT/config-used.yml"
SERVER_LOG="$OUT/server.log"
METRICS_FILE="$OUT/server-metrics.txt"
WORKLOAD_OUT="$OUT/workload"
SERVER_PID=""

cleanup() {
  local code=$?
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "stopping server pid $SERVER_PID"
    kill "$SERVER_PID" 2>/dev/null || true
    for _ in $(seq 1 10); do
      kill -0 "$SERVER_PID" 2>/dev/null || break
      sleep 0.5
    done
    kill -9 "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  rm -rf "$DATADIR"
  if [[ $code -ne 0 ]]; then
    echo "run FAILED (exit $code); logs and any partial artifacts preserved under: $OUT" >&2
  fi
}
trap cleanup EXIT INT TERM

echo "==> Building Release"
dotnet build -c Release "$REPO_ROOT/CamusDB/CamusDB.csproj" >/dev/null
dotnet build -c Release "$REPO_ROOT/CamusDB.Workload/CamusDB.Workload.csproj" >/dev/null

# Compose a config with diagnostics enabled, without mutating the checked-in default.
cp "$REPO_ROOT/CamusDB/Config/config.yml" "$CONFIG_USED"
cat >> "$CONFIG_USED" <<'YML'

diagnostics:
  enabled: true
  prometheus_enabled: true
  prometheus_path: /metrics
  trace_sample_ratio: 0.01
  include_runtime_metrics: true
YML

# Launch the built binary directly rather than via `dotnet run`. `dotnet run` forks a child process
# for the actual server, so the pid captured with $! is the launcher, not the server — killing it on
# cleanup leaves the real server orphaned (still holding the HTTP/gRPC ports). Running the binary makes
# $SERVER_PID the server itself, so the cleanup trap actually stops it.
SERVER_BIN="$REPO_ROOT/CamusDB/bin/Release/net10.0/CamusDB"
if [[ ! -x "$SERVER_BIN" ]]; then
  echo "server binary not found at $SERVER_BIN (was the Release build run?)" >&2
  exit 1
fi
SERVER_CMD="CAMUS_CONFIG_PATH=$CONFIG_USED CAMUS_DIAGNOSTICS_RUN_ID=$RUN_ID $SERVER_BIN --mode standalone --data-dir $DATADIR"
echo "$SERVER_CMD" > "$OUT/server-command.txt"

echo "==> Starting standalone server (data dir: $DATADIR)"
(
  cd "$REPO_ROOT"
  # Silence Info-level host logging by default so it doesn't add allocation noise to the measurement.
  # Overridable: run with CAMUS_LOG_LEVEL=information to see full server logs.
  exec env CAMUS_CONFIG_PATH="$CONFIG_USED" CAMUS_DIAGNOSTICS_RUN_ID="$RUN_ID" \
    CAMUS_LOG_LEVEL="${CAMUS_LOG_LEVEL:-warning}" \
    "$SERVER_BIN" --mode standalone --data-dir "$DATADIR"
) > "$SERVER_LOG" 2>&1 &
SERVER_PID=$!

echo "==> Waiting for readiness ($METRICS_URL)"
for _ in $(seq 1 120); do
  if curl -sf -o /dev/null "$METRICS_URL"; then
    READY=1; break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "server exited during startup; see $SERVER_LOG" >&2
    exit 1
  fi
  sleep 1
done
[[ "${READY:-}" == "1" ]] || { echo "server did not become ready in time" >&2; exit 1; }

echo "==> Seeding dataset (outside measurement)"
dotnet run -c Release --no-build --project "$REPO_ROOT/CamusDB.Workload" -- \
  init --endpoint "$GRPC_ENDPOINT" --database "$DATABASE" --protocol grpc --rows "$ROWS"

echo "==> Running workload (mode=$MODE, workers=$WORKERS, duration=$DURATION)"
dotnet run -c Release --no-build --project "$REPO_ROOT/CamusDB.Workload" -- \
  run --endpoint "$GRPC_ENDPOINT" --database "$DATABASE" --protocol grpc \
  --mode "$MODE" --workers "$WORKERS" --target-ops "$TARGET_OPS" \
  --rows "$ROWS" --warmup "$WARMUP" --duration "$DURATION" --output "$WORKLOAD_OUT"

echo "==> Scraping server metrics"
curl -sf "$METRICS_URL" -o "$METRICS_FILE"

echo "==> Generating bottleneck report"
dotnet run -c Release --no-build --project "$REPO_ROOT/CamusDB.Workload" -- \
  report --output "$WORKLOAD_OUT" --metrics "$METRICS_FILE"

echo "==> Done. Evidence bundle: $OUT"
echo "    - workload/summary.md, intervals.csv, reconciliation.json"
echo "    - server-metrics.txt, server.log, config-used.yml"
echo "    - workload/bottleneck-report.md"
