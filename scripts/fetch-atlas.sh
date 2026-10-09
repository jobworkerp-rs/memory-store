#!/usr/bin/env bash
# Downloads only the platform-specific Atlas binary pinned by the release lock.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: fetch-atlas.sh PLATFORM OUTPUT_PATH" >&2
  exit 2
fi

platform=$1
output_path=$2
repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
lock_file="$repo_root/infra/atlas/atlas-tool.lock.json"

case "$platform" in
  linux-amd64|darwin-arm64) ;;
  *)
    echo "unsupported Atlas platform: $platform" >&2
    exit 1
    ;;
esac

command -v jq >/dev/null || {
  echo "required command is unavailable: jq" >&2
  exit 1
}

atlas_url=$(jq -r --arg platform "$platform" '.platforms[$platform].url' "$lock_file")
atlas_sha256=$(jq -r --arg platform "$platform" '.platforms[$platform].sha256' "$lock_file")
atlas_version=$(jq -r '.version' "$lock_file")
if [[ -z "$atlas_url" || -z "$atlas_sha256" || -z "$atlas_version" || "$atlas_url" == "null" || "$atlas_sha256" == "null" ]]; then
  echo "invalid Atlas tool lock: $lock_file" >&2
  exit 1
fi

if command -v sha256sum >/dev/null; then
  sha256_command=(sha256sum)
elif command -v shasum >/dev/null; then
  sha256_command=(shasum -a 256)
else
  echo "required command is unavailable: sha256sum or shasum" >&2
  exit 1
fi
sha256_of() {
  "${sha256_command[@]}" "$1" | awk '{print $1}'
}

# Verified downloads are reused so that repeated bundle builds work offline;
# a cached binary is trusted only after its SHA-256 matches the lock again.
cache_root=${MEMORIES_ATLAS_CACHE_DIR:-${XDG_CACHE_HOME:-${HOME:?set HOME, XDG_CACHE_HOME or MEMORIES_ATLAS_CACHE_DIR for the Atlas cache}/.cache}/memories-db-migrate/atlas}
cached_atlas="$cache_root/$atlas_version/$platform/atlas"
if [[ ! -f "$cached_atlas" || "$(sha256_of "$cached_atlas")" != "$atlas_sha256" ]]; then
  command -v curl >/dev/null || {
    echo "required command is unavailable: curl" >&2
    exit 1
  }
  mkdir -p "$(dirname "$cached_atlas")"
  download=$(mktemp "$cached_atlas.download.XXXXXX")
  trap 'rm -f "$download"' EXIT
  curl -fsSL "$atlas_url" -o "$download"
  if [[ "$(sha256_of "$download")" != "$atlas_sha256" ]]; then
    echo "Atlas SHA-256 does not match atlas-tool.lock.json" >&2
    exit 1
  fi
  mv "$download" "$cached_atlas"
  trap - EXIT
fi

mkdir -p "$(dirname "$output_path")"
cp "$cached_atlas" "$output_path"
chmod 0755 "$output_path"
"$output_path" version | grep -F "$atlas_version" >/dev/null
