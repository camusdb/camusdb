#!/usr/bin/env bash
#
# Pack and publish the CamusDB server as a .NET global tool (nuget.org: CamusDB.Server).
#
# Users install the result with:
#   dotnet tool install -g CamusDB.Server
#   camusdb
#
# Usage:
#   NUGET_API_KEY=... scripts/publish-nuget.sh          # pack + push <version> from the csproj
#   VERSION=0.9.8 NUGET_API_KEY=... scripts/publish-nuget.sh
#   PUSH=0 scripts/publish-nuget.sh                     # pack locally only, do not push
#
# The version is taken from <Version> in CamusDB/CamusDB.csproj unless overridden, the same source
# docker/publish.sh uses, so the NuGet version and the Docker tag cannot drift apart.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PROJECT="$REPO_ROOT/CamusDB/CamusDB.csproj"
OUTPUT="$REPO_ROOT/artifacts/nupkg"
SOURCE="${SOURCE:-https://api.nuget.org/v3/index.json}"
PUSH="${PUSH:-1}"

if [[ -z "${VERSION:-}" ]]; then
  VERSION="$(grep -oE '<Version>[^<]+</Version>' "$PROJECT" | head -n1 | sed -E 's/<\/?Version>//g')"
fi

if [[ -z "$VERSION" ]]; then
  echo "error: could not determine version (set VERSION=...)" >&2
  exit 1
fi

PACKAGE="$OUTPUT/CamusDB.Server.$VERSION.nupkg"

echo "Package:  CamusDB.Server"
echo "Version:  $VERSION"
echo "Push:     $PUSH"
echo

rm -f "$PACKAGE"
dotnet pack "$PROJECT" -c Release -p:PackageVersion="$VERSION"

if [[ ! -f "$PACKAGE" ]]; then
  echo "error: expected $PACKAGE to exist after pack" >&2
  exit 1
fi

if [[ "$PUSH" != "1" ]]; then
  echo
  echo "PUSH=0: built $PACKAGE without pushing."
  exit 0
fi

if [[ -z "${NUGET_API_KEY:-}" ]]; then
  echo "error: NUGET_API_KEY is not set" >&2
  exit 1
fi

dotnet nuget push "$PACKAGE" --source "$SOURCE" --api-key "$NUGET_API_KEY"

echo
echo "Done: CamusDB.Server $VERSION"
echo "Install with: dotnet tool install -g CamusDB.Server --version $VERSION"
