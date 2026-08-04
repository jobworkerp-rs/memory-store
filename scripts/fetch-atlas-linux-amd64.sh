#!/usr/bin/env bash
# Compatibility wrapper for server image and Linux CI callers.
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: fetch-atlas-linux-amd64.sh OUTPUT_PATH" >&2
  exit 2
fi

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
exec "$repo_root/scripts/fetch-atlas.sh" linux-amd64 "$1"
