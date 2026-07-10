#!/bin/bash
#
# Runs the CamusDB API host in an optimized configuration for local development on macOS.
#
# On a Mac, launching the project the usual way (`dotnet run` with no flags) produces a
# DEBUG, workstation-GC build. For insert/commit-heavy workloads that is dramatically slower
# than the published Docker image (which is built -c Release), to the point where a commit
# batching dozens of inserts can appear to "hang" or time out. This script closes that gap by:
#
#   * building/running in Release (JIT optimizations, no debug-only paths)
#   * enabling Server GC (DOTNET_gcServer=1) for better throughput under allocation-heavy commits
#
# It does NOT change durability behavior: native macOS fsync is genuinely slower than the
# Linux VM Docker Desktop runs in, so if Release is still slow the bottleneck is the on-disk
# commit/flush path, not this configuration.
#
# Overridable via environment variables:
#   CAMUS_URLS        ASP.NET Core binding URLs (default https://localhost:7141;http://localhost:5095)
#   CAMUS_ENV         ASPNETCORE_ENVIRONMENT (default Development)
#   DOTNET_gcServer   Server GC toggle (default 1; set 0 to compare against workstation GC)
#
set -euo pipefail

cd "$(dirname "$0")/.."

URLS="${CAMUS_URLS:-https://localhost:7141;http://localhost:5095}"
ENVIRONMENT="${CAMUS_ENV:-Development}"

export DOTNET_gcServer="${DOTNET_gcServer:-1}"
export DOTNET_gcConcurrent="${DOTNET_gcConcurrent:-1}"
export DOTNET_TieredPGO="${DOTNET_TieredPGO:-1}"
export ASPNETCORE_ENVIRONMENT="${ENVIRONMENT}"
export ASPNETCORE_URLS="${URLS}"

echo ">> Configuration : Release"
echo ">> Server GC     : DOTNET_gcServer=${DOTNET_gcServer}"
echo ">> Environment   : ${ASPNETCORE_ENVIRONMENT}"
echo ">> URLs          : ${ASPNETCORE_URLS}"
echo ">> Starting CamusDB API host..."

exec dotnet run -c Release --project CamusDB/CamusDB.csproj
