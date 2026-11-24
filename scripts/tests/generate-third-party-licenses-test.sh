#!/usr/bin/env bash
# Tests for scripts/generate-third-party-licenses.sh. Needs cargo-about:
# CARGO_ABOUT (as exported by the install-cargo-about action) or the `cargo about`
# subcommand (e.g. after `cargo install cargo-about`). Without it the test is
# skipped locally, but fails in CI so that the license gate cannot be lost.
# The real listing is checked for what the release must ship; small fixture
# workspaces check that the license gate in about.toml rejects dependencies.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "${SCRIPT_DIR}/../.." && pwd)
generate="${ROOT}/scripts/generate-third-party-licenses.sh"
work=$(mktemp -d)
trap 'rm -rf "${work}"' EXIT

if [[ -z "${CARGO_ABOUT-}" ]] && ! cargo about --version >/dev/null 2>&1; then
  if [[ -n "${CI-}" ]]; then
    echo "FAIL: cargo-about is not installed (CARGO_ABOUT is unset)" >&2
    exit 1
  fi
  echo "SKIP: cargo-about is not installed (set CARGO_ABOUT or install the cargo subcommand)"
  exit 0
fi

failures=0
fail() {
  printf 'FAIL: %s\n' "$*" >&2
  failures=$((failures + 1))
}

# expect_generated <desc> <out-file> [manifest]
expect_generated() {
  local desc=$1
  shift
  if ! bash "${generate}" "$@" >"${work}/generate.log" 2>&1; then
    cat "${work}/generate.log" >&2
    fail "${desc}"
    return 1
  fi
}

expect_in_listing() {
  grep -Fq -- "$1" "${listing}" || fail "the listing lacks $2"
}

listing="${work}/THIRD-PARTY-LICENSES.txt"
if expect_generated "generating the real listing failed" "${listing}"; then
  # Dependencies of every release target and of every published build: the
  # binaries (SQLite) and the container image (PostgreSQL).
  expect_in_listing "- security-framework " "a macOS-only dependency"
  expect_in_listing "- sqlx-sqlite " "the SQLite backend"
  expect_in_listing "- sqlx-postgres " "the PostgreSQL backend"
  # Crates of other projects that declare their license only in a LICENSE file.
  expect_in_listing "- command-utils " "the command-utils submodule"
  # This project's own crates are covered by LICENSE, not by this listing.
  own_crates=$(cargo metadata --no-deps --format-version 1 --manifest-path "${ROOT}/Cargo.toml" \
    | python3 -c 'import json, sys; print("|".join(p["name"] for p in json.load(sys.stdin)["packages"] if p["publish"] == []))')
  [[ -n "${own_crates}" ]] || fail "no crate of this project is marked publish = false"
  if grep -Eq "^- (${own_crates}) " "${listing}"; then
    fail "the listing includes the project's own crates"
  fi
  # Plain text output: no HTML escaping of license texts.
  if grep -Eq '&(lt|gt|quot|amp|#[0-9]+|#x[0-9a-fA-F]+);' "${listing}"; then
    fail "the listing contains HTML entities"
  fi
fi

# fixture <name> <dependency-manifest-lines...>: a workspace whose root crate
# depends on one local crate declared by the given [package] lines.
fixture() {
  local dir="${work}/$1"
  shift
  mkdir -p "${dir}/app/src" "${dir}/dep/src"
  printf '[workspace]\nmembers = ["app", "dep"]\nresolver = "2"\n' >"${dir}/Cargo.toml"
  printf '[package]\nname = "fixture-app"\nversion = "0.1.0"\nedition = "2021"\npublish = false\nlicense = "MIT"\n\n[dependencies]\nfixture-dep = { path = "../dep" }\n' \
    >"${dir}/app/Cargo.toml"
  printf '[package]\nname = "fixture-dep"\nversion = "0.1.0"\nedition = "2021"\n' >"${dir}/dep/Cargo.toml"
  printf '%s\n' "$@" >>"${dir}/dep/Cargo.toml"
  : >"${dir}/app/src/lib.rs"
  : >"${dir}/dep/src/lib.rs"
  (cd "${dir}" && cargo generate-lockfile --offline -q)
  echo "${dir}/Cargo.toml"
}

expect_rejected() {
  local desc=$1 manifest=$2
  if bash "${generate}" "${work}/rejected.txt" "${manifest}" >"${work}/rejected.log" 2>&1; then
    fail "${desc} was accepted"
  fi
}

expect_rejected "a GPL-3.0 dependency" "$(fixture gpl 'license = "GPL-3.0-only"')"
expect_rejected "an LGPL-3.0 dependency" "$(fixture lgpl 'license = "LGPL-3.0"')"
# MPL-2.0 is accepted for one known crate only, not for any dependency.
expect_rejected "an MPL-2.0 dependency" "$(fixture mpl 'license = "MPL-2.0"')"
expect_rejected "a dependency without any license" "$(fixture unlicensed)"

# The same gate accepts a fixture whose dependency is under an allowed license,
# so the rejections above come from the license and not from the fixture.
expect_generated "an MIT dependency was rejected" "${work}/accepted.txt" "$(fixture mit 'license = "MIT"')" || true

[[ "${failures}" -eq 0 ]] || exit 1
echo "PASS: generate-third-party-licenses"
