#!/usr/bin/env bash
# Generates SQLite databases left by a desktop application that migrated with
# memories-db-migrate v0.0.13 at startup, and writes them as `.dump` SQL.
#
# Each database is produced by the tools that actually wrote it: pre-Atlas
# data, the v0.0.7 memory_kind migration (commit 06ce5b4, the published v0.0.7
# tag), the v0.0.13 startup sequence, then data written by the v0.0.13 server
# code. See README.md for the inputs and how to build them.
set -euo pipefail

if [[ $# -ne 6 ]]; then
  echo "usage: generate-sqlite-v0.0.13.sh V0013_TOOL V0013_FIXTURE_DATA V007_MIGRATE_MEMORY_KIND V007_STARTUP ATLAS_ARTIFACT_DIR OUTPUT_DIR" >&2
  exit 2
fi
tool=$1
fixture_data=$2
migrate_memory_kind=$3
startup_v007=$4
artifact=$5
out=$6
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(git -C "$here" rev-parse --show-toplevel)
# The published v0.0.7 tag; the startup schema of v0.0.1..v0.0.6 is the one of v0.0.6.
v007=06ce5b4
v006=v0.0.6

work=$(mktemp -d /tmp/memories-v0013-sqlite.XXXXXX)
mkdir -p "$out"
# Run away from any repository .env: every tool here calls dotenvy.
cd "$work"

db() { echo "$work/$1/memories.sqlite3"; }
sql() { sqlite3 -bail "$(db "$1")" "${@:2}"; }

# isolated NAME VAR=VALUE... COMMAND...: run with only the given environment,
# in the database's directory, so that no developer setting leaks in.
isolated() {
  local name=$1
  shift
  (cd "$work/$name" && env -i PATH="$PATH" HOME="$work/$name" "$@")
}

# The environment the application passed to the migration tool at startup.
# The thread vector store is enabled, and its table does not exist yet.
v0013_tool() {
  local name=$1
  shift
  isolated "$name" \
    MEMORIES_ATLAS_DIR="$artifact" \
    MEMORIES_ATLAS_DATABASE_URL="sqlite://$(db "$name")?mode=rwc" \
    THREAD_VECTOR_ENABLED=true \
    THREAD_LANCEDB_URI="$work/$name/lancedb/memories.lancedb" \
    THREAD_LANCEDB_TABLE=threads \
    THREAD_VECTOR_SIZE=4 \
    MEMORY_FTS_TOKENIZER=simple \
    "$tool" "$@"
}

v0013_check() {
  v0013_tool "$1" schema status | sed -n 1p
  v0013_tool "$1" schema verify | tail -1
  v0013_tool "$1" post-migrate verify
}

# The application's startup sequence of the migration tool.
v0013_startup() {
  local name=$1 status
  status=$(v0013_tool "$name" schema status | sed -n 1p)
  echo "$status"
  if [[ $status == *"status=baseline_required"* ]]; then
    v0013_tool "$name" schema baseline | tee "$work/$name/baseline.out"
    grep -q "baseline status=completed baseline_version=20260803000001" "$work/$name/baseline.out"
  fi
  v0013_tool "$name" schema apply
  v0013_tool "$name" post-migrate run --all-required --maintenance-window-ack
  v0013_tool "$name" schema validate
  v0013_check "$name"
}

# SQLITE_URL and the connection count the application passed to the server
# and to the memory_kind migration tool.
sqlite_env() {
  echo "SQLITE_URL=sqlite://$(db "$1")?mode=rwc" SQLITE_MAX_CONNECTIONS=5
}

# Server code of a release writing to the database, with vector stores and
# embedding dispatch disabled: only the RDB is captured.
server() {
  local name=$1
  shift
  # shellcheck disable=SC2046 # sqlite_env yields whitespace-free assignments
  isolated "$name" $(sqlite_env "$name") \
    MEMORY_CACHE_NUM_COUNTERS=1000 \
    MEMORY_CACHE_MAX_COST=1000 \
    MEMORY_CACHE_USE_METRICS=false \
    MEMORY_VECTOR_ENABLED=false \
    THREAD_VECTOR_ENABLED=false \
    MEMORY_AUTO_EMBEDDING_ENABLED=false \
    MEMORY_REFLECTION_DISPATCH_ENABLED=false \
    REFLECTION_INTENT_VECTOR_ENABLED=false \
    MEDIA_STORAGE_LOCAL_DIR="$work/$name/media" \
    "$@"
}

# The memory_kind migration as the application ran it, with the data
# directory as working directory.
memory_kind() {
  local name=$1
  shift
  # shellcheck disable=SC2046 # sqlite_env yields whitespace-free assignments
  isolated "$name" $(sqlite_env "$name") "$migrate_memory_kind" "$@"
}

# legacy_sql REVISION PATH: a startup schema file as it existed at REVISION.
legacy_sql() {
  git -C "$repo" show "$1:infra/sql/sqlite/$2"
}

# new_db NAME [REVISION SEED]: a database directory, optionally holding the
# startup schema of REVISION with the data of SEED. The thread vector store
# holds only memory vectors at that point, so its directory exists without a
# threads table.
new_db() {
  mkdir -p "$work/$1/lancedb/memories.lancedb"
  if [[ $# -eq 3 ]]; then
    {
      legacy_sql "$2" 001_schema.sql
      legacy_sql "$2" 003_reflection_schema.sql
      cat "$here/seed/$3"
    } | sql "$1"
  fi
}

finish() {
  local name=$1 outcome
  server "$name" "$fixture_data"
  echo "== check $name"
  v0013_check "$name"
  outcome=$(sql "$name" "SELECT json_extract(checkpoint, '\$.payload.vector_outcome') FROM memories_data_migration_task_state WHERE task_identity = 'thread-message-times-v1@1'")
  if [[ $outcome != "not_required" ]]; then
    echo "$name: thread-message-times-v1@1 vector outcome is '$outcome', expected not_required" >&2
    exit 1
  fi
  if [[ -e "$work/$name/lancedb/memories.lancedb/threads.lance" ]]; then
    echo "$name: the migration tool created the threads table" >&2
    exit 1
  fi
  sql "$name" "SELECT 'threads=' || (SELECT COUNT(*) FROM thread) || ' memories=' || (SELECT COUNT(*) FROM memory) || ' memberships=' || (SELECT COUNT(*) FROM thread_memory)"
  sql "$name" .dump > "$out/v0.0.13-$name.sql"
}

echo "== fresh"
new_db fresh
v0013_startup fresh
finish fresh

echo "== legacy-ddl-000001 (created by the v0.0.7 server)"
new_db legacy-ddl-000001 "$v007" v0.0.7-startup-data.sql
v0013_startup legacy-ddl-000001
finish legacy-ddl-000001

echo "== memory-kind-000001 (created by a v0.0.6 server, migrated by v0.0.7)"
name=memory-kind-000001
new_db "$name" "$v006" v0.0.6-startup-data.sql
# Expand. The application skipped an ADD COLUMN whose column already
# existed; a v0.0.6 database has neither column, so the script runs as is.
legacy_sql "$v007" manual/011_add_memory_kind.sql | sql "$name"
echo '{}' > "$work/$name/mapping.json"
memory_kind "$name" client-apply --mapping "$work/$name/mapping.json" --output "$work/$name/audit-1.json"
# The application pruned only when the audit completed without failures and
# every warning was an unresolved preflight finding. This fixture must take
# that path, so anything else stops the generator.
jq -e '.status == "completed" and (.failures | length) == 0 and (.warnings | length) > 0
       and all(.warnings[]; .check == "unresolved_preflight")' "$work/$name/audit-1.json" > /dev/null
memory_kind "$name" client-prune-unresolved --force --audit "$work/$name/audit-1.json" --output "$work/$name/unresolved-records.json"
jq -e '.status == "completed" and .deleted_thread_rows >= 1' "$work/$name/unresolved-records.json" > /dev/null
memory_kind "$name" client-apply --mapping "$work/$name/mapping.json" --output "$work/$name/audit-2.json"
jq -e '.status == "completed" and (.failures | length) == 0 and (.warnings | length) == 0' "$work/$name/audit-2.json" > /dev/null
legacy_sql "$v007" manual/012_contract_memory_kind.sql | sql "$name"
# The application then restarted the v0.0.7 server.
server "$name" "$startup_v007"
v0013_startup "$name"
finish "$name"

echo "work directory: $work"
