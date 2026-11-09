#!/usr/bin/env bash
#
# Build one self-contained server archive for a release: camusdb-<version>-<rid>.tar.gz, plus a
# .sha256 file beside it. The release workflow (.github/workflows/release.yml) runs this once per
# platform, and the Homebrew formula downloads the result.
#
# Self-contained means the archive carries the .NET runtime, so the machine that installs it needs
# no .NET of its own, and a Homebrew upgrade of the `dotnet` formula cannot break an installed node.
# The archive is not trimmed and not compiled ahead of time: the extension loader loads assemblies at
# run time, and trimming removes code those assemblies may need. ReadyToRun precompiles the app the
# same way the container image does, to cut JIT work on the first requests.
#
# Usage:
#   scripts/package-release.sh <rid> [output-dir]
#   VERSION=0.17.2 scripts/package-release.sh osx-arm64 artifacts/release
#
# <rid> is a .NET runtime identifier: osx-arm64, osx-x64, linux-x64 or linux-arm64. The version comes
# from <Version> in CamusDB/CamusDB.csproj unless VERSION overrides it, the same source the NuGet and
# Docker scripts use.

set -euo pipefail

RID="${1:?usage: scripts/package-release.sh <rid> [output-dir]}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PROJECT="$REPO_ROOT/CamusDB/CamusDB.csproj"
OUTPUT="${2:-$REPO_ROOT/artifacts/release}"

if [[ -z "${VERSION:-}" ]]; then
  VERSION="$(grep -oE '<Version>[^<]+</Version>' "$PROJECT" | head -n1 | sed -E 's/<\/?Version>//g')"
fi

if [[ -z "$VERSION" ]]; then
  echo "error: could not determine version (set VERSION=...)" >&2
  exit 1
fi

NAME="camusdb-$VERSION-$RID"
mkdir -p "$OUTPUT"
OUTPUT="$(cd "$OUTPUT" && pwd)"
STAGE="$OUTPUT/$NAME"
ARCHIVE="$OUTPUT/$NAME.tar.gz"

echo "Archive: $NAME.tar.gz"
echo "Output:  $OUTPUT"
echo

rm -rf "$STAGE" "$ARCHIVE" "$ARCHIVE.sha256"

dotnet publish "$PROJECT" \
  --configuration Release \
  --runtime "$RID" \
  --self-contained \
  -p:Version="$VERSION" \
  -p:PublishReadyToRun=true \
  -p:RunAnalyzers=false \
  -o "$STAGE"

cp "$REPO_ROOT/README.md" "$REPO_ROOT/LICENSE.txt" "$STAGE/"

# COPYFILE_DISABLE keeps macOS tar from adding AppleDouble (._*) entries for extended attributes.
COPYFILE_DISABLE=1 tar -czf "$ARCHIVE" -C "$OUTPUT" "$NAME"
rm -rf "$STAGE"

if command -v sha256sum >/dev/null 2>&1; then
  (cd "$OUTPUT" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
else
  (cd "$OUTPUT" && shasum -a 256 "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
fi

echo
echo "Wrote $ARCHIVE"
cat "$ARCHIVE.sha256"
