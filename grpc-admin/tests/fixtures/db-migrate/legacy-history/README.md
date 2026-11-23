# Legacy migration-history fixtures

Databases migrated by `memories-db-migrate` v0.0.24, the last release whose
migration engine was the Atlas CLI. They carry the legacy history table
`atlas_schema_revisions` and are the input of the history-handover and
drift-check tests. Do not edit them by hand: they are evidence of what the
previous engine actually wrote.

## Contents

SQLite fixtures are `sqlite3 .dump` output; PostgreSQL fixtures are
`pg_dump --no-owner --no-privileges --inserts` output without psql
meta-commands. Both load through an ordinary SQL connection.

| Fixture | How the database was produced |
| --- | --- |
| `fresh-full` | Empty database, `release apply` (all seven migrations applied) |
| `fresh-partial` | Empty database, Atlas `migrate apply 4` (first four migrations, status `pending`) |
| `adopted-000001` | Schema of migration `20260803000001` created without history, then `release baseline` and `release apply` |
| `adopted-000002` | Schema of migrations `20260803000001` and `20260803000002` created without history, then `release baseline` and `release apply` |
| `legacy-ddl-000001` (SQLite) | Startup schema of revision `cdbfbae^` (`001_schema.sql` + `003_reflection_schema.sql`, before the message-time columns), then `release baseline` and `release apply`. Its DDL text differs from a database built by the migrations |

PostgreSQL fixtures exist in two layouts:

- `dedicated-*`: no `search_path` in the URL; the history lives in the
  dedicated `atlas_schema_revisions` schema.
- `scoped-*`: `search_path=memories` in the URL; the history lives next to
  the application tables in the `memories` schema.

Every fixture passed `schema status` and (except `fresh-partial`)
`schema verify` of v0.0.24 when it was generated.

## Regenerating

Regeneration needs the Atlas-based tool, so it only works from a checkout of
v0.0.24 (or a later revision whose `memories-db-migrate` still runs Atlas).

1. Build the tool twice and keep both binaries outside the repository:
   `cargo build --release` (SQLite) and
   `cargo build -p grpc-admin --release --bin memories-db-migrate --no-default-features --features postgres`.
2. Copy `infra/atlas` to a temporary directory and fetch the pinned Atlas
   binary into its `bin/atlas` with `scripts/fetch-atlas-linux-amd64.sh`.
   Atlas is used internally for this step only and is never committed.
3. Start PostgreSQL: `docker compose -p memories-legacy-fixture -f docker-compose.yaml up -d postgres`.
4. Run the generators (they work in a temporary directory, away from any
   repository `.env`):
   - `generate-sqlite.sh <sqlite tool> <atlas artifact dir> <repo root> <output dir>`
   - `generate-postgres.sh <postgres tool> <atlas artifact dir> <postgres container> <output dir>`
5. Copy the output here, then remove the temporary directory (including the
   Atlas binary) and stop the compose project.

A startup schema upgraded by `manual/008_thread_message_times.sql` is not
included: v0.0.24 itself rejects it with `baseline_schema_mismatch`, because
Atlas compares SQLite CHECK constraints textually and that script quotes the
identifiers.
