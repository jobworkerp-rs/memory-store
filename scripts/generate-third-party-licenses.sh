#!/usr/bin/env bash
# Usage: generate-third-party-licenses.sh <out-file> [manifest]
#
# Writes the license listing of every third-party crate linked into the
# workspace binaries (or the workspace of [manifest]) for all release targets
# and features, using about.toml and about.hbs at the repository root. Fails
# on a dependency whose license about.toml does not accept.
# Runs cargo-about from CARGO_ABOUT (exported by the install-cargo-about action
# of sutr-app/portable-binary-release), otherwise as the `cargo about`
# subcommand.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
out=${1-}
manifest=${2:-${ROOT}/Cargo.toml}

if [[ -z "${out}" ]]; then
  printf 'ERROR: usage: %s <out-file> [manifest]\n' "$0" >&2
  exit 2
fi

if [[ -n "${CARGO_ABOUT-}" ]]; then
  cargo_about=("${CARGO_ABOUT}")
else
  cargo_about=(cargo about)
fi

# All features: the release binaries (SQLite) and the container image
# (PostgreSQL) are built with different backends from the same workspace.
"${cargo_about[@]}" generate --locked --fail --workspace --all-features \
  --config "${ROOT}/about.toml" \
  --manifest-path "${manifest}" \
  --output-file "${out}" \
  "${ROOT}/about.hbs"
