#!/usr/bin/env bash
# Produces the self-contained SQLite migration bundle used by local Memories.
set -euo pipefail

# The oldest macOS release the desktop bundle must start on.
readonly MINIMUM_MACOS_VERSION=14.0

if [[ $# -ne 1 ]]; then
  echo "usage: build-memories-db-migrate-sqlite.sh OUTPUT_DIRECTORY" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64 | Darwin-arm64) ;;
  *)
    echo "unsupported native platform: $(uname -s)/$(uname -m); supported platforms are Linux/x86_64 and Darwin/arm64" >&2
    exit 1
    ;;
esac
output_directory=$1
if [[ -e "$output_directory" ]]; then
  echo "refusing to overwrite existing output path: $output_directory" >&2
  exit 1
fi
output_parent=$(dirname "$output_directory")
output_name=$(basename "$output_directory")
if [[ ! -d "$output_parent" ]]; then
  echo "output parent directory does not exist: $output_parent" >&2
  exit 1
fi

# shellcheck source=scripts/lib/macos-minimum.sh
source "$repo_root/scripts/lib/macos-minimum.sh"

staging_directory=$(mktemp -d "$output_parent/.${output_name}.staging.XXXXXX")
trap 'rm -rf "$staging_directory"' EXIT

cd "$repo_root"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
# The linker records the deployment target as the binary's minimum macOS.
export MACOSX_DEPLOYMENT_TARGET=${MACOSX_DEPLOYMENT_TARGET:-$MINIMUM_MACOS_VERSION}
# Local Memories may use a Lindera tokenizer for an enabled ThreadVector.
# Include it so the migration bundle accepts the application's own config.
# Build the whole workspace, as the standard release build does: a `-p`
# selection unifies dependency features differently, which recompiles the
# LanceDB stack into a second, multi-GB set of artifacts on every source change.
cargo build --release --features lindera

install -m 0755 target/release/memories-db-migrate "$staging_directory/memories-db-migrate"
if [[ "$(uname -s)" == Darwin ]]; then
  require_minimum_macos "$staging_directory/memories-db-migrate" "$MINIMUM_MACOS_VERSION"
fi
# The binary carries its catalog; this copy is for the application's
# provenance records and is checked against the binary by `bundle verify`,
# which accepts exactly the SQLite catalog.
mkdir "$staging_directory/db-migrate"
cp infra/db-migrate/post-migration-tasks.json infra/db-migrate/post-migration-task-history.json \
  "$staging_directory/db-migrate/"
cp -R infra/db-migrate/sqlite "$staging_directory/db-migrate/sqlite"

# Applications detect a stale bundle by its content digest; the revision and
# dirty flag only help diagnose where a bundle came from.
source_revision=$(git -C "$repo_root" rev-parse HEAD)
manifest_args=(--source-revision "$source_revision")
if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  manifest_args+=(--source-dirty)
fi
"$staging_directory/memories-db-migrate" bundle manifest "${manifest_args[@]}"
"$staging_directory/memories-db-migrate" bundle verify
mv "$staging_directory" "$output_directory"
trap - EXIT

echo "SQLite migration bundle created: $output_directory"
