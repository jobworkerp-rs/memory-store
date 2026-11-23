#!/usr/bin/env bash
# Generates PostgreSQL databases carrying the Atlas revision history, using
# the Atlas-based memories-db-migrate (v0.0.24, postgres build), and writes
# them as pg_dump SQL that an ordinary SQL connection can load.
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: generate-postgres.sh TOOL ATLAS_ARTIFACT_DIR CONTAINER OUTPUT_DIR" >&2
  exit 2
fi
tool=$1
artifact=$2
container=$3
out=$4

work=$(mktemp -d /tmp/memories-legacy-postgres.XXXXXX)
mkdir -p "$out"
export MEMORIES_ATLAS_DIR=$artifact
export THREAD_VECTOR_ENABLED=false
export MEMORY_VECTOR_ENABLED=false
export MEMORY_FTS_TOKENIZER=simple
# Run away from any repository .env so the tool cannot pick up developer settings.
cd "$work"
export MEMORY_LANCEDB_URI=$work/lancedb/memories.lancedb
migrations=$artifact/postgres/migrations
base=postgres://postgres:postgres@127.0.0.1:5432

psql_in() {
  local database=$1 schema=$2
  shift 2
  docker exec -i -e PGOPTIONS="-c search_path=$schema" "$container" \
    psql -v ON_ERROR_STOP=1 -q -U postgres -d "$database" "$@"
}
create_db() {
  local database=$1 schema=$2
  psql_in test public -c "DROP DATABASE IF EXISTS $database" -c "CREATE DATABASE $database"
  if [[ $schema != public ]]; then
    psql_in "$database" public -c "CREATE SCHEMA $schema"
  fi
}
url() {
  local database=$1 schema=$2
  if [[ $schema == public ]]; then
    echo "$base/$database?sslmode=disable"
  else
    echo "$base/$database?sslmode=disable&search_path=$schema"
  fi
}
run() {
  local database=$1 schema=$2
  shift 2
  MEMORIES_ATLAS_DATABASE_URL=$(url "$database" "$schema") "$tool" "$@"
}
seed() {
  local database=$1 schema=$2
  shift 2
  for file in "$@"; do
    psql_in "$database" "$schema" < "$file"
  done
}

generate() {
  local variant=$1 schema=$2
  local prefix="${variant}_"

  echo "== $variant fresh-full"
  create_db "${prefix}fresh_full" "$schema"
  run "${prefix}fresh_full" "$schema" release apply --maintenance-window-ack

  echo "== $variant fresh-partial (first 4 migrations applied directly by Atlas)"
  create_db "${prefix}fresh_partial" "$schema"
  (
    cd "$artifact"
    MEMORIES_ATLAS_DATABASE_URL=$(url "${prefix}fresh_partial" "$schema") \
      bin/atlas --config "file://$artifact/migrate.hcl" --env postgres migrate apply 4
  )

  echo "== $variant adopted-000001"
  create_db "${prefix}adopted_000001" "$schema"
  seed "${prefix}adopted_000001" "$schema" "$migrations/20260803000001_adoption_baseline.sql"
  run "${prefix}adopted_000001" "$schema" release baseline
  run "${prefix}adopted_000001" "$schema" release apply --maintenance-window-ack

  echo "== $variant adopted-000002"
  create_db "${prefix}adopted_000002" "$schema"
  seed "${prefix}adopted_000002" "$schema" \
    "$migrations/20260803000001_adoption_baseline.sql" \
    "$migrations/20260803000002_thread_message_times_schema.sql"
  run "${prefix}adopted_000002" "$schema" release baseline
  run "${prefix}adopted_000002" "$schema" release apply --maintenance-window-ack

  for name in fresh_full fresh_partial adopted_000001 adopted_000002; do
    local database="${prefix}${name}"
    echo "== check $database"
    run "$database" "$schema" schema status > "$work/status.out"
    head -1 "$work/status.out"
    if [[ $name != fresh_partial ]]; then
      run "$database" "$schema" schema verify > "$work/verify.out"
      tail -1 "$work/verify.out"
    fi
    # INSERT statements and no psql meta-commands, so that tests can load
    # the dump through an ordinary SQL connection.
    docker exec "$container" pg_dump -U postgres --no-owner --no-privileges --inserts \
      -d "$database" | grep -v '^\\' > "$out/${variant}-${name//_/-}.sql"
  done
}

# History in Atlas's dedicated schema (no search_path in the URL).
generate dedicated public
# History next to the application tables (search_path in the URL).
generate scoped memories
echo "work directory: $work"
