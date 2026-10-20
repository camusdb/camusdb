#!/usr/bin/env bash
#
# Pack and publish the CamusDB server as a .NET global tool (nuget.org: CamusDB.Server), and the C#
# extension contract at the same version (nuget.org: CamusDB.Extensions.Abstractions).
#
# Users install the result with:
#   dotnet tool install -g CamusDB.Server
#   camusdb
#
# Extension authors reference the contract with:
#   <PackageReference Include="CamusDB.Extensions.Abstractions" Version="<major>.<minor>.*" />
#
# The two are published together because the contract is versioned with the server: the server's
# loader refuses an extension built against a newer contract than its own (a newer major.minor while
# the major is 0). The contract's own <Version> must equal the server's; a test in CamusDB.Tests fails
# when it does not, and the contract pack refuses a package version that differs from its assembly.
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
CONTRACT_PROJECT="$REPO_ROOT/CamusDB.Extensions.Abstractions/CamusDB.Extensions.Abstractions.csproj"
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
CONTRACT_PACKAGE="$OUTPUT/CamusDB.Extensions.Abstractions.$VERSION.nupkg"
CONTRACT_SYMBOLS="$OUTPUT/CamusDB.Extensions.Abstractions.$VERSION.snupkg"

echo "Packages: CamusDB.Server, CamusDB.Extensions.Abstractions"
echo "Version:  $VERSION"
echo "Push:     $PUSH"
echo

rm -f "$PACKAGE" "$CONTRACT_PACKAGE" "$CONTRACT_SYMBOLS"
# The contract first: its pack refuses a version that differs from its assembly version, and that
# check is cheap, so a mismatch fails before the server tool is built.
dotnet pack "$CONTRACT_PROJECT" -c Release -p:PackageVersion="$VERSION"
dotnet pack "$PROJECT" -c Release -p:PackageVersion="$VERSION"

for expected in "$PACKAGE" "$CONTRACT_PACKAGE"; do
  if [[ ! -f "$expected" ]]; then
    echo "error: expected $expected to exist after pack" >&2
    exit 1
  fi
done

if [[ "$PUSH" != "1" ]]; then
  echo
  echo "PUSH=0: built $PACKAGE and $CONTRACT_PACKAGE without pushing."
  exit 0
fi

if [[ -z "${NUGET_API_KEY:-}" ]]; then
  echo "error: NUGET_API_KEY is not set" >&2
  exit 1
fi

# The contract first: an extension author who sees the new server should find its contract published.
# The symbols package next to the contract is pushed by the same command.
dotnet nuget push "$CONTRACT_PACKAGE" --source "$SOURCE" --api-key "$NUGET_API_KEY"
dotnet nuget push "$PACKAGE" --source "$SOURCE" --api-key "$NUGET_API_KEY"

echo
echo "Done: CamusDB.Server $VERSION and CamusDB.Extensions.Abstractions $VERSION"
echo "Install with: dotnet tool install -g CamusDB.Server --version $VERSION"
