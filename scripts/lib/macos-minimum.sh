# shellcheck shell=bash
# Minimum-macOS check of the bundled executable, shared by the bundle build
# and its tests (scripts/tests/macos-minimum-test.sh), which run on Linux.

# version_at_most VERSION LIMIT: whether the dotted VERSION is <= LIMIT.
# Missing components count as 0; anything but digits fails.
version_at_most() {
  local -a version limit
  local index part bound
  IFS=. read -r -a version <<<"$1"
  IFS=. read -r -a limit <<<"$2"
  [[ ${#version[@]} -gt 0 && ${#limit[@]} -gt 0 ]] || return 1
  for index in 0 1 2; do
    part=${version[index]:-0}
    bound=${limit[index]:-0}
    [[ $part =~ ^[0-9]+$ && $bound =~ ^[0-9]+$ ]] || return 1
    if ((10#$part < 10#$bound)); then
      return 0
    elif ((10#$part > 10#$bound)); then
      return 1
    fi
  done
}

# macho_minimum_os EXECUTABLE: the LC_BUILD_VERSION minos, empty if absent.
macho_minimum_os() {
  otool -l "$1" | awk '/LC_BUILD_VERSION/ { found = 1 } found && $1 == "minos" { print $2; exit }'
}

# require_minimum_macos EXECUTABLE LIMIT: fails unless the executable starts
# on macOS LIMIT. A minos that cannot be read fails too.
require_minimum_macos() {
  local minimum_os
  minimum_os=$(macho_minimum_os "$1") || minimum_os=
  if [[ -z "$minimum_os" ]] || ! version_at_most "$minimum_os" "$2"; then
    echo "${1##*/} requires macOS ${minimum_os:-unknown}; the bundle must start on macOS $2" >&2
    return 1
  fi
}
