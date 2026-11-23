#!/usr/bin/env bash
# Tests of scripts/lib/macos-minimum.sh; runs on Linux with otool stubbed.
set -euo pipefail

library="$(dirname "${BASH_SOURCE[0]}")/../lib/macos-minimum.sh"
# shellcheck source=scripts/lib/macos-minimum.sh
source "$library"

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
succeeds() { "$@" 2>/dev/null; }
fails() { ! "$@" 2>/dev/null; }

expect "an older version is at most the limit" succeeds version_at_most 13.5 14.0
expect "the limit itself is at most the limit" succeeds version_at_most 14.0 14.0
expect "missing components count as zero" succeeds version_at_most 14 14.0
expect "missing components count as zero in the limit" succeeds version_at_most 14.0 14
expect "versions compare numerically" succeeds version_at_most 9.0 14.0
expect "a patch release above the limit" fails version_at_most 14.0.1 14.0
expect "a minor release above the limit" fails version_at_most 14.1 14.0
expect "a major release above the limit" fails version_at_most 26.0 14.0
expect "an empty version" fails version_at_most "" 14.0
expect "a non-numeric version" fails version_at_most 14.x 14.0

otool_output=""
# shellcheck disable=SC2329 # called by macho_minimum_os
otool() {
  printf '%s\n' "$otool_output"
}
# Make otool report the load commands of an executable built for macOS $1,
# or one without LC_BUILD_VERSION when $1 is not given.
load_commands() {
  otool_output="Load command 9
      cmd LC_SOURCE_VERSION
  cmdsize 16
  version 0.0"
  if [[ $# -gt 0 ]]; then
    otool_output+="
Load command 10
      cmd LC_BUILD_VERSION
  cmdsize 32
 platform 1
    minos $1
      sdk 15.0"
  fi
}

load_commands 14.0
expect "minos is read from LC_BUILD_VERSION" test "$(macho_minimum_os binary)" = 14.0
expect "an executable for the minimum starts" succeeds require_minimum_macos binary 14.0
load_commands 13.0
expect "an executable for an older macOS starts" succeeds require_minimum_macos binary 14.0
load_commands 26.0
expect "an executable for a newer macOS is refused" fails require_minimum_macos binary 14.0
load_commands
expect "an executable without LC_BUILD_VERSION has no minos" test -z "$(macho_minimum_os binary)"
expect "an executable without LC_BUILD_VERSION is refused" fails require_minimum_macos binary 14.0

unset -f otool
# Only the refusal message counts: a shell that failed to start would also
# exit non-zero.
# shellcheck disable=SC2016 # expanded by the inner shell
without_otool=$(PATH=/nonexistent "$BASH" -c \
  'source "$1"; require_minimum_macos binary 14.0' _ "$library" 2>&1) || true
expect "a missing otool is refused" test "$without_otool" != "${without_otool#*requires macOS unknown}"

if ((failures > 0)); then
  echo "$failures test(s) failed" >&2
  exit 1
fi
echo "all macOS minimum tests passed"
