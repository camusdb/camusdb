#!/usr/bin/env bash
# Prepares the Join Order Benchmark (JOB) tier of the plan-quality harness:
#
#   1. downloads the JOB queries and schema at a pinned commit,
#   2. generates synthetic IMDB data at the given scale (gen_job.py, seeded),
#   3. runs the SQLite oracle on the same rows (run_sqlite.py).
#
# The JOB repository has no license, so its files are downloaded at run time and never committed.
#
# usage: scripts/plan-quality/job/prepare.sh <work-dir> <scale>
# then:  JOB_DIR=<work-dir> JOB_SCALE=<scale> dotnet test CamusDB.PlanQuality.Tests --filter FullyQualifiedName~JobTierGate
set -euo pipefail

work_dir=${1:?usage: prepare.sh <work-dir> <scale>}
scale=${2:?usage: prepare.sh <work-dir> <scale>}

# gregrahn/join-order-benchmark, master as of 2025-02-16.
job_commit=a39603662e023e449cb2121997a5034df9e02ebf
script_dir=$(cd "$(dirname "$0")" && pwd)

mkdir -p "$work_dir"
work_dir=$(cd "$work_dir" && pwd)

download="$work_dir/download"
bench="$work_dir/bench"
data="$work_dir/data"

if [ ! -f "$bench/queries/1a.sql" ]; then
  rm -rf "$download" "$bench"
  mkdir -p "$download" "$bench/data" "$bench/queries"
  curl -fsSL "https://codeload.github.com/gregrahn/join-order-benchmark/tar.gz/$job_commit" | tar -xzf - -C "$download"
  src="$download/join-order-benchmark-$job_commit"
  cp "$src/schema.sql" "$src/fkindexes.sql" "$bench/data/"
  for f in "$src"/[0-9]*[a-z].sql; do cp "$f" "$bench/queries/"; done
  count=$(ls "$bench/queries" | wc -l | tr -d ' ')
  if [ "$count" != "113" ]; then
    echo "expected 113 JOB queries, found $count" >&2
    exit 1
  fi
fi

rm -rf "$data"
mkdir -p "$data"
python3 -I "$script_dir/gen_job.py" "$bench" "$data" "$scale"
python3 -I "$script_dir/run_sqlite.py" "$data/sqlite.db" "$bench/queries" "$data/sqlite_results.json"
echo "JOB tier prepared in $work_dir at scale $scale"
