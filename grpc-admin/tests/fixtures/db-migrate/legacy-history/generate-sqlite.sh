#!/usr/bin/env bash
# Generates SQLite databases carrying the Atlas revision history, using the
# Atlas-based memories-db-migrate (v0.0.24) and writes them as `.dump` SQL.
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: generate-sqlite.sh TOOL ATLAS_ARTIFACT_DIR REPO_ROOT OUTPUT_DIR" >&2
  exit 2
fi
tool=$1
artifact=$2
repo=$3
out=$4
# Last revision before the Atlas coordinator was introduced.
pre_atlas=c335b67^
# Last revision whose startup schema lacked thread.first_message_at.
pre_message_times=cdbfbae^

work=$(mktemp -d /tmp/memories-legacy-sqlite.XXXXXX)
mkdir -p "$out"
export MEMORIES_ATLAS_DIR=$artifact
export THREAD_VECTOR_ENABLED=false
export MEMORY_VECTOR_ENABLED=false
export MEMORY_FTS_TOKENIZER=simple
# Run away from any repository .env so the tool cannot pick up developer settings.
cd "$work"
export MEMORY_LANCEDB_URI=$work/lancedb/memories.lancedb
migrations=$artifact/sqlite/migrations

db() { echo "$work/$1.sqlite3"; }
run() {
  local name=$1
  shift
  MEMORIES_ATLAS_DATABASE_URL="sqlite://$(db "$name")?mode=rwc" "$tool" "$@"
}
seed() {
  local name=$1
  shift
  for file in "$@"; do
    sqlite3 "$(db "$name")" < "$file"
  done
}
# legacy_sql REVISION PATH: a pre-Atlas schema file as it existed at REVISION.
legacy_sql() {
  local target="$work/legacy-${1//[^a-z0-9]/_}-$(basename "$2")"
  git -C "$repo" show "$1:infra/sql/sqlite/$2" > "$target"
  echo "$target"
}

echo "== fresh-full"
run fresh-full release apply --maintenance-window-ack

echo "== fresh-partial (first 4 migrations applied directly by Atlas)"
(
  cd "$artifact"
  MEMORIES_ATLAS_DATABASE_URL="sqlite://file:$(db fresh-partial)?mode=rwc" \
    bin/atlas --config "file://$artifact/migrate.hcl" --env sqlite migrate apply 4
)

echo "== adopted-000001"
seed adopted-000001 "$migrations/20260803000001_adoption_baseline.sql"
run adopted-000001 release baseline
run adopted-000001 release apply --maintenance-window-ack

echo "== adopted-000002"
seed adopted-000002 \
  "$migrations/20260803000001_adoption_baseline.sql" \
  "$migrations/20260803000002_thread_message_times_schema.sql"
run adopted-000002 release baseline
run adopted-000002 release apply --maintenance-window-ack

echo "== legacy-ddl-000001 (schema created by the server startup before the message-time columns)"
seed legacy-ddl-000001 \
  "$(legacy_sql "$pre_message_times" 001_schema.sql)" \
  "$(legacy_sql "$pre_message_times" 003_reflection_schema.sql)"
run legacy-ddl-000001 release baseline
run legacy-ddl-000001 release apply --maintenance-window-ack

# A startup schema upgraded by manual/008 is not generated: Atlas compares
# SQLite CHECK constraints textually and rejects its backquoted identifiers,
# so v0.0.24 itself cannot adopt such a database.

for name in fresh-full fresh-partial adopted-000001 adopted-000002 legacy-ddl-000001; do
  echo "== check $name"
  run "$name" schema status > "$work/status.out"
  head -1 "$work/status.out"
  if [[ $name != fresh-partial ]]; then
    run "$name" schema verify > "$work/verify.out"
    tail -1 "$work/verify.out"
  fi
  sqlite3 "$(db "$name")" .dump > "$out/$name.sql"
done
echo "work directory: $work"
