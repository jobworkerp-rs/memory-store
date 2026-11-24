#!/usr/bin/env bash
# Tests of the native platforms scripts/build-memories-db-migrate-sqlite.sh
# accepts, with uname stubbed; nothing is built. An accepted platform reaches
# the next check, which refuses the existing output path given here.
set -euo pipefail

script="$(dirname "${BASH_SOURCE[0]}")/../build-memories-db-migrate-sqlite.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/bin" "$work/existing-output"

failures=0
expect() {
  local description=$1
  shift
  if "$@"; then
    echo "ok: $description"
  else
    echo "FAILED: $description" >&2
    failures=$((failures + 1))
  fi
}
# reports_on SYSTEM MACHINE TEXT: whether the script's error output on that
# platform contains TEXT.
reports_on() {
  # shellcheck disable=SC2016 # expanded by the stub
  printf '#!/bin/sh\ncase "$1" in -s) echo %s ;; -m) echo %s ;; esac\n' "$1" "$2" >"$work/bin/uname"
  chmod +x "$work/bin/uname"
  PATH="$work/bin:$PATH" bash "$script" "$work/existing-output" >/dev/null 2>"$work/stderr" || true
  grep -Fq -- "$3" "$work/stderr"
}
accepted='refusing to overwrite'

# The release platforms.
expect "Linux/x86_64 is accepted" reports_on Linux x86_64 "$accepted"
expect "Linux/aarch64 is accepted" reports_on Linux aarch64 "$accepted"
expect "Darwin/arm64 is accepted" reports_on Darwin arm64 "$accepted"
# Platforms without a release.
expect "Darwin/x86_64 is refused" reports_on Darwin x86_64 "unsupported native platform: Darwin/x86_64"
expect "Linux/riscv64 is refused" reports_on Linux riscv64 "unsupported native platform: Linux/riscv64"

if ((failures > 0)); then
  echo "$failures test(s) failed" >&2
  exit 1
fi
echo "all migration bundle platform tests passed"
