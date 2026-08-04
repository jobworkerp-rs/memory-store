#!/usr/bin/env bash
# Produces the self-contained SQLite migration bundle used by local Memories.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: build-memories-db-migrate-sqlite.sh OUTPUT_DIRECTORY" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) native_atlas_platform=linux-amd64 ;;
  Darwin-arm64) native_atlas_platform=darwin-arm64 ;;
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

staging_directory=$(mktemp -d "$output_parent/.${output_name}.staging.XXXXXX")
trap 'rm -rf "$staging_directory"' EXIT

cd "$repo_root"
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-1}
# Local Memories may use a Lindera tokenizer for an enabled ThreadVector.
# Include it so the migration bundle accepts the application's own config.
cargo build --release -p grpc-admin --bin memories-db-migrate --features lindera

install -m 0755 target/release/memories-db-migrate "$staging_directory/memories-db-migrate"
cp -R infra/atlas "$staging_directory/atlas"
"$repo_root/scripts/fetch-atlas.sh" "$native_atlas_platform" "$staging_directory/atlas/bin/atlas"
mv "$staging_directory" "$output_directory"
trap - EXIT

echo "SQLite migration bundle created: $output_directory"
