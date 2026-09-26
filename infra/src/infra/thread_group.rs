//! ThreadGroup RDB layer (docs/thread-groups-design_ja.md section 5,
//! schema migration `20260920000001_thread_group_schema`).
//!
//! Submodules (one per logical table cluster):
//! - `rows`               — sqlx `FromRow` structs, insert parameter
//!   structs, storage value tokens, and backend-safe column lists.
//! - `group`              — `thread_group` CRUD + redirect / split
//!   transition primitives.
//! - `member`             — `thread_group_member` current-membership CRUD
//!   (partial-unique current row per canonical key).
//! - `relation`           — `thread_relation` canonical edge CRUD with
//!   the active-parent cardinality guard on the child side.
//! - `observation`        — `thread_observation` idempotent evidence
//!   storage (full identity + fingerprint unique key).
//! - `candidate`          — `thread_group_candidate_association` CRUD.
//! - `operator_decision`  — `operator_decision` append-only audit.
//! - `source_identity`    — `source_thread_identity` resolved owner-local
//!   mapping (upsert / rebind / delete).
//! - `canonical_key`      — `thread_canonical_key` thread <-> key
//!   correspondence with collision-aware retrieve-or-create.
//! - `deletion_marker`    — `thread_deletion_marker` put / exists /
//!   consume for the design 8.2 pre-write check.
//! - `lock`               — identity / canonical-key exclusive sections
//!   with per-backend isolation (PG advisory locks; SQLite relies on its
//!   single-writer transaction).
//! - `collection`         — `manual_collection` + `manual_collection_member`.
//! - `outbox`             — `thread_group_event_outbox` transactional
//!   domain-event append (immutable rows, `event_id` is the identity).
//! - `audit`              — `thread_group_audit` merge / split history.
//!
//! Repository contract (mirrors the committed schema header): no FK
//! cascades, no triggers, no conditional SQL exceptions. Business rules
//! (cycle checks, identity resolution, grouping policy, merge / split
//! ordering) live in the app layer; transaction boundaries are owned by
//! app services, so every write is exposed as a `_tx` primitive over a
//! generic `Executor`. Reads are pool-based unless the caller must
//! observe its own uncommitted writes, in which case a `_tx` read
//! variant exists. Idempotent lookup helpers ("retrieve-or-create"
//! style) never catch a UNIQUE violation *inside* a caller transaction,
//! because PostgreSQL aborts the whole transaction on the failed
//! statement; the pool-level helpers run the fallback SELECT in a fresh
//! transaction instead.

pub mod audit;
pub mod candidate;
pub mod canonical_key;
pub mod collection;
pub mod deletion_marker;
pub mod group;
pub mod lock;
pub mod member;
pub mod memory_relation;
pub mod observation;
pub mod operator_decision;
pub mod outbox;
pub mod relation;
pub mod revision;
pub mod rows;
pub mod source_identity;

// SQLite-only fixture: the ThreadGroup tables are not part of the
// shared `infra/sql/sqlite` test migrations yet, so repo tests build a
// throwaway temp-file database straight from the committed Atlas
// migration instead. PostgreSQL runs need the `infra/sql/postgres`
// mirror before these tests can compile there. Exposed to downstream
// test crates through the `test-helper` feature so app-layer services
// can drive the same schema.
#[cfg(all(any(test, feature = "test-helper"), not(feature = "postgres")))]
pub mod test_support;
