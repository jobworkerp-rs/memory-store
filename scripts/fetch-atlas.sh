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

for command in curl jq; do
  command -v "$command" >/dev/null || {
    echo "required command is unavailable: $command" >&2
    exit 1
  }
done

atlas_url=$(jq -r --arg platform "$platform" '.platforms[$platform].url' "$lock_file")
atlas_sha256=$(jq -r --arg platform "$platform" '.platforms[$platform].sha256' "$lock_file")
atlas_version=$(jq -r '.version' "$lock_file")
if [[ -z "$atlas_url" || -z "$atlas_sha256" || -z "$atlas_version" || "$atlas_url" == "null" || "$atlas_sha256" == "null" ]]; then
  echo "invalid Atlas tool lock: $lock_file" >&2
  exit 1
fi

mkdir -p "$(dirname "$output_path")"
curl -fsSL "$atlas_url" -o "$output_path"
if command -v sha256sum >/dev/null; then
  echo "$atlas_sha256  $output_path" | sha256sum -c -
elif command -v shasum >/dev/null; then
  actual_sha256=$(shasum -a 256 "$output_path" | awk '{print $1}')
  [[ "$actual_sha256" == "$atlas_sha256" ]] || {
    echo "Atlas SHA-256 does not match atlas-tool.lock.json" >&2
    exit 1
  }
else
  echo "required command is unavailable: sha256sum or shasum" >&2
  exit 1
fi
chmod 0755 "$output_path"
"$output_path" version | grep -F "$atlas_version" >/dev/null
