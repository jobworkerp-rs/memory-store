# Legacy migration-history fixtures

Databases migrated by `memories-db-migrate` releases whose migration engine
was the Atlas CLI: v0.0.24, the last of them, and v0.0.13 as a desktop
application ran it (see the last section). They carry the legacy history table
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

## Databases of a desktop application on v0.0.13

`sqlite/v0.0.13-*.sql` are databases a desktop application left behind when
it migrated with `memories-db-migrate` v0.0.13 at every startup, before
starting the v0.0.13 server. That release applied the first three migrations
and the required task `thread-message-times-v1@1` only, so the next upgrade
hands over a three-revision history, applies the remaining migrations and
runs the later tasks in one go.

The application ran, in this order: `schema status`, `schema baseline` (only
when `baseline_required`), `schema apply`,
`post-migrate run --all-required --maintenance-window-ack`, then `schema
validate`, `schema status`, `schema verify` and `post-migrate verify`. Its
environment enabled the thread vector store (`THREAD_VECTOR_ENABLED=true`)
while the store had no `threads` table yet, so the task recorded the vector
outcome `not_required`. The server created that table only later, and the
vector stores are therefore not part of these fixtures: tests create them.

| Fixture | How the database was produced |
| --- | --- |
| `v0.0.13-fresh` | Empty database, the startup sequence above, then data written by the v0.0.13 server code |
| `v0.0.13-legacy-ddl-000001` | Startup schema of v0.0.7 (`06ce5b4`; the same DDL up to `cdbfbae^`) with data (`seed/v0.0.7-startup-data.sql`), the startup sequence (adopted by `schema baseline`), then data written by the v0.0.13 server code |
| `v0.0.13-memory-kind-000001` | Startup schema of v0.0.6 (identical since v0.0.1) with data (`seed/v0.0.6-startup-data.sql`), migrated to `memory_kind` with the tools of `06ce5b4`, then the startup sequence and data written by the v0.0.13 server code |

The `memory_kind` migration follows the application's own procedure:
`manual/011_add_memory_kind.sql`; `migrate-memory-kind client-apply` with the
mapping `{}`; because every audit warning is `unresolved_preflight` (one
summary thread whose source conversation was deleted),
`client-prune-unresolved --force` and a second, clean `client-apply`;
`manual/012_contract_memory_kind.sql`, which rebuilds `thread` and `memory`
with different DDL text; and a restart of the v0.0.7 server. The generator
stops if the audits do not take that path.

Every fixture passed `schema status`, `schema verify` and `post-migrate
verify` of v0.0.13 after its data was written.

### Regenerating

The inputs are commits `34399e8` (v0.0.13) and `06ce5b4` (the published
v0.0.7 tag; refer to it by commit, since a local `v0.0.7` tag may still point
to an earlier commit).

1. Check out both commits as detached worktrees outside the repository and
   initialize their submodules.
2. Copy `v0.0.13-fixture-data.rs` to `grpc-admin/examples/fixture_data.rs` of
   the v0.0.13 worktree and `06ce5b4-startup.rs` to
   `grpc-admin/examples/startup.rs` of the other one. Build with a separate
   target directory for each worktree (a shared one mixes up the build
   scripts of the two checkouts):
   - v0.0.13: `cargo build --release --locked --bin memories-db-migrate --example fixture_data`
   - `06ce5b4`: `cargo build --release --locked --bin migrate-memory-kind --example startup`
3. Copy `infra/atlas` of the v0.0.13 worktree to a temporary directory and
   fetch the pinned Atlas binary into its `bin/atlas` with that worktree's
   `scripts/fetch-atlas-linux-amd64.sh`. Atlas is used internally for this
   step only and is never committed.
4. Run `generate-sqlite-v0.0.13.sh <memories-db-migrate> <fixture_data>
   <migrate-memory-kind> <startup> <atlas artifact dir> <output dir>`. It
   needs `sqlite3` and `jq`, and works in a temporary directory away from any
   repository `.env`.
5. Copy the output here, then remove the worktrees, the temporary directories
   and the Atlas binary.

The two example programs and the seed SQL are inputs of the generator only;
cargo does not build them in this repository.
