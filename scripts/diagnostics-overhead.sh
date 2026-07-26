#!/usr/bin/env bash
#
# This file is part of CamusDB
#
# For the full copyright and license information, please view the LICENSE.txt
# file that was distributed with this source code.
#
# Measures the runtime overhead of the diagnostics feature by alternating diagnostics-DISABLED and
# diagnostics-ENABLED Release workload runs against a freshly started standalone server each time, then
# reporting the median completed ops/s of each and the enabled-vs-disabled delta.
#
# Gate (from the spec): ENABLED overhead should be below 5% median throughput regression at the default
# 1% trace sample ratio. The stricter DISABLED-overhead gate (< 2%) is measured against a
# pre-instrumentation build — that requires building from an earlier commit and is out of scope for this
# harness, which compares the same binary with diagnostics off vs on. Overhead is environment-specific;
# record the raw runs and do not treat a single comparison as definitive.
#
# Usage:
#   scripts/diagnostics-overhead.sh <output-dir> [--runs 5] [--duration 60s] [--rows 100000] [--workers 64]

set -euo pipefail

OUT="${1:-}"
if [[ -z "$OUT" ]]; then
  echo "usage: $0 <output-dir> [--runs 5] [--duration 60s] [--rows 100000] [--workers 64]" >&2
  exit 2
fi
shift || true
if [[ -e "$OUT" ]]; then
  echo "output directory already exists: $OUT — refusing to overwrite." >&2
  exit 2
fi

RUNS=5
DURATION=60s
WARMUP=15s
ROWS=100000
WORKERS=64
DATABASE=perf_overhead

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs) RUNS="$2"; shift 2 ;;
    --duration) DURATION="$2"; shift 2 ;;
    --warmup) WARMUP="$2"; shift 2 ;;
    --rows) ROWS="$2"; shift 2 ;;
    --workers) WORKERS="$2"; shift 2 ;;
    *) echo "unknown flag: $1" >&2; exit 2 ;;
  esac
done

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GRPC_ENDPOINT="http://127.0.0.1:5096"
METRICS_URL="http://127.0.0.1:5095/metrics"
PING_URL="http://127.0.0.1:5095/"

mkdir -p "$OUT"; OUT="$(cd "$OUT" && pwd)"
DISABLED_CFG="$OUT/config-disabled.yml"
ENABLED_CFG="$OUT/config-enabled.yml"
SERVER_PID=""
DATADIR=""

cleanup() {
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
  [[ -n "$DATADIR" ]] && rm -rf "$DATADIR" || true
}
trap cleanup EXIT INT TERM

echo "==> Building Release"
dotnet build -c Release "$REPO_ROOT/CamusDB/CamusDB.csproj" >/dev/null
dotnet build -c Release "$REPO_ROOT/CamusDB.Workload/CamusDB.Workload.csproj" >/dev/null

cp "$REPO_ROOT/CamusDB/Config/config.yml" "$DISABLED_CFG"
cp "$REPO_ROOT/CamusDB/Config/config.yml" "$ENABLED_CFG"
cat >> "$ENABLED_CFG" <<'YML'

diagnostics:
  enabled: true
  prometheus_enabled: true
  trace_sample_ratio: 0.01
  include_runtime_metrics: true
YML

# Runs one measured workload against a freshly started server using $1 config; echoes achieved ops/s.
run_once() {
  local cfg="$1" tag="$2" idx="$3"
  DATADIR="$(mktemp -d "${TMPDIR:-/tmp}/camus-overhead.XXXXXX")"
  local rundir="$OUT/${tag}-${idx}"
  local log="$OUT/${tag}-${idx}.server.log"

  ( cd "$REPO_ROOT"; CAMUS_CONFIG_PATH="$cfg" dotnet run -c Release --no-build --project CamusDB -- \
      --mode standalone --data-dir "$DATADIR" ) > "$log" 2>&1 &
  SERVER_PID=$!

  local ready=""
  for _ in $(seq 1 120); do
    if curl -sf -o /dev/null "$METRICS_URL" || curl -sf -o /dev/null "$PING_URL"; then ready=1; break; fi
    kill -0 "$SERVER_PID" 2>/dev/null || { echo "server exited early; see $log" >&2; return 1; }
    sleep 1
  done
  [[ "$ready" == "1" ]] || { echo "server not ready" >&2; return 1; }

  dotnet run -c Release --no-build --project "$REPO_ROOT/CamusDB.Workload" -- \
    init --endpoint "$GRPC_ENDPOINT" --database "$DATABASE" --protocol grpc --rows "$ROWS" >/dev/null
  dotnet run -c Release --no-build --project "$REPO_ROOT/CamusDB.Workload" -- \
    run --endpoint "$GRPC_ENDPOINT" --database "$DATABASE" --protocol grpc \
    --mode closed --workers "$WORKERS" --rows "$ROWS" --warmup "$WARMUP" --duration "$DURATION" \
    --output "$rundir" >/dev/null

  kill "$SERVER_PID" 2>/dev/null && wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
  rm -rf "$DATADIR"; DATADIR=""

  # Extract achieved ops/s from summary.json (PascalCase key; JSON is indented so allow whitespace).
  grep -oE '"AchievedOpsPerSec"[[:space:]]*:[[:space:]]*[0-9.]+' "$rundir/summary.json" \
    | head -1 | sed -E 's/.*:[[:space:]]*//'
}

median() { printf '%s\n' "$@" | sort -n | awk '{a[NR]=$0} END{print (NR%2)? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2}'; }

disabled=(); enabled=()
for i in $(seq 1 "$RUNS"); do
  echo "==> Run $i/$RUNS: diagnostics DISABLED"
  d_val="$(run_once "$DISABLED_CFG" disabled "$i")"
  disabled+=("$d_val")
  echo "    ops/s: $d_val"
  echo "==> Run $i/$RUNS: diagnostics ENABLED"
  e_val="$(run_once "$ENABLED_CFG" enabled "$i")"
  enabled+=("$e_val")
  echo "    ops/s: $e_val"
done

med_disabled="$(median "${disabled[@]}")"
med_enabled="$(median "${enabled[@]}")"
delta_pct="$(awk -v d="$med_disabled" -v e="$med_enabled" 'BEGIN{ if (d>0) printf "%.2f", (d-e)/d*100; else print "n/a" }')"

{
  echo "# Diagnostics overhead"
  echo
  echo "- Runs: $RUNS (alternating), duration $DURATION, workers $WORKERS, rows $ROWS"
  echo "- Disabled ops/s (raw): ${disabled[*]}"
  echo "- Enabled  ops/s (raw): ${enabled[*]}"
  echo "- Median disabled: $med_disabled"
  echo "- Median enabled:  $med_enabled"
  echo "- Enabled-vs-disabled regression: ${delta_pct}%  (gate: < 5%)"
  echo
  echo "> The disabled-overhead gate (< 2%) is measured against a PRE-instrumentation build, not covered"
  echo "> here (this compares the same binary, diagnostics off vs on). Overhead is environment-specific;"
  echo "> treat these as one sample, not a definitive number."
} | tee "$OUT/overhead.md"

echo "==> Wrote $OUT/overhead.md"
