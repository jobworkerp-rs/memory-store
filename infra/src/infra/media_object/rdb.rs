//! `media_object` persistence layer.
//!
//! Concurrency / TOCTOU model. `media_object` rows are reference-counted
//! by `memory` rows; a row's bytes may also be physically deleted from
//! object storage. Without the discipline below a memory could end up
//! pointing at media whose bytes are gone, or a still-referenced object
//! could be deleted. The invariants this module must uphold:
//!
//! - `gc_state` is a 6-state machine: 0=active / 1=orphan /
//!   2=deleted-failed / 3=unresolvable / 4=promoting / 5=deleting.
//! - `incr_ref` promotes `1=orphan → 0=active` ONLY when the row is
//!   confirmed (`storage_uri IS NOT NULL`); it never resurrects
//!   `{2=deleted-failed, 5=deleting}` (those would let a memory reference
//!   a media whose bytes may be gone / is claimed for deletion).
//! - the physical row delete is conditional
//!   (`ref_count=0 AND gc_state IN (0,1,5)`) so a re-reference cannot race
//!   the delete; `affected_rows==0` is an invariant violation, surfaced to
//!   the caller (it must mark deleted-failed + alert).
//! - row locks for the decr/incr serialization use the same feature
//!   gating as `memory::rdb::FIND_BY_IDS_FOR_UPDATE_SUFFIX` (postgres
//!   `FOR UPDATE`, sqlite relies on single-writer semantics).

use crate::error::LlmMemoryError;
use crate::infra::IdGeneratorWrapper;
use crate::infra::UseIdGenerator;
use crate::sql::{p, p_jsonb};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use sqlx::Executor;

/// `gc_state` discriminants. Kept as plain consts (not an enum) so the
/// SQL `IN (..)` lists below stay textual and unambiguous.
pub const GC_ACTIVE: i32 = 0;
pub const GC_ORPHAN: i32 = 1;
pub const GC_DELETED_FAILED: i32 = 2;
pub const GC_UNRESOLVABLE: i32 = 3;
pub const GC_PROMOTING: i32 = 4;
pub const GC_DELETING: i32 = 5;

/// Row-lock suffix: PostgreSQL takes a real row lock; SQLite ignores
/// `FOR UPDATE` and relies on its single-writer model (same approach as
/// `memory::rdb`).
#[cfg(feature = "postgres")]
const FOR_UPDATE_SUFFIX: &str = " FOR UPDATE";
#[cfg(not(feature = "postgres"))]
const FOR_UPDATE_SUFFIX: &str = "";

/// One physical `media_object` row. Proto conversion (`MediaMetadata` /
/// `MediaPayload`) is done in the app layer; infra deals in rows only.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct MediaObjectRow {
    pub id: i64,
    pub kind: i32,
    pub media_type: String,
    pub byte_size: Option<i64>,
    pub sha256: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub duration_ms: Option<i64>,
    pub storage_backend: String,
    pub storage_uri: Option<String>,
    pub alt: Option<String>,
    pub ref_count: i64,
    pub gc_state: i32,
    pub metadata: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Insert payload for a fresh Upload reservation (`storage_uri=NULL`,
/// `gc_state=1=orphan`, `ref_count=0`). `id` is caller-generated so the
/// caller can return it even on `ON CONFLICT DO NOTHING`.
#[derive(Debug, Clone)]
pub struct MediaObjectReservation {
    pub id: i64,
    pub kind: i32,
    pub media_type: String,
    pub byte_size: Option<i64>,
    pub sha256: String,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub duration_ms: Option<i64>,
    pub alt: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Result of `decr_ref_tx`: enough state for the caller to decide the
/// delete branch without a second SELECT.
#[derive(Debug, Clone)]
pub struct RefDecrResult {
    pub ref_count: i64,
    pub gc_state: i32,
    pub storage_backend: String,
    pub sha256: Option<String>,
    pub storage_uri: Option<String>,
}

/// One ref_count desync row: the stored `db_ref_count` disagrees with
/// `actual_ref_count` (the real number of `memory.media_object_id`
/// references). Only confirmed rows whose counts differ are produced;
/// in-sync rows and unconfirmed reservations are filtered out in SQL.
#[derive(Debug, Clone, PartialEq)]
pub struct RefCountDesync {
    pub id: i64,
    pub db_ref_count: i64,
    pub actual_ref_count: i64,
    pub gc_state: i32,
    pub storage_backend: String,
}

// `metadata` is JSONB on postgres; the column list casts it to text
// (`metadata::text AS metadata`) so sqlx can decode it into
// `MediaObjectRow.metadata: Option<String>` (same rationale as
// `memory_columns!` / `thread_columns!`). sqlite is untyped so the raw
// column is used.
const COLUMNS: &str = crate::sql::media_object_columns!();

// storage_uri=NULL, ref_count=0, gc_state=1. ON CONFLICT(sha256) DO
// NOTHING so a concurrent reservation for the same bytes does not error;
// the caller re-SELECTs by sha256 and branches on gc_state.
const INSERT_RESERVATION_SQL: &str = concat!(
    "INSERT INTO media_object (id, kind, media_type, byte_size, sha256, width, \
     height, duration_ms, storage_backend, storage_uri, alt, ref_count, \
     gc_state, metadata, created_at, updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ",",
    p!(6),
    ",",
    p!(7),
    ",",
    p!(8),
    ", 'pending', NULL, ",
    p!(9),
    ", 0, 1, NULL, ",
    p!(10),
    ",",
    p!(11),
    ") ON CONFLICT (sha256) DO NOTHING"
);

// url backend: sha256 is NULL (multiple url rows coexist; UNIQUE
// excludes NULL). storage_uri is the external URL, set immediately.
// gc_state=1=orphan (memory link promotes it). No ON CONFLICT (no
// sha256 dedup for url).
const INSERT_URL_SQL: &str = concat!(
    "INSERT INTO media_object (id, kind, media_type, byte_size, sha256, width, \
     height, duration_ms, storage_backend, storage_uri, alt, ref_count, \
     gc_state, metadata, created_at, updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ", NULL, ",
    p!(5),
    ",",
    p!(6),
    ", NULL, 'url', ",
    p!(7),
    ",",
    p!(8),
    ", 0, 1, NULL, ",
    p!(9),
    ",",
    p!(10),
    ")"
);

// unresolvable backend (image memory Phase 4 migration, elided source):
// the original bytes were dropped (over the inline size guard) so no
// body can be put — only sha256 / byte_size survive. storage_uri=NULL,
// gc_state=3=unresolvable so the normal GC leaves it alone and a later
// same-sha256 Upload/Register can promote it (design 3/3 §15 / 2/3
// §7.1.1). ON CONFLICT(sha256) DO NOTHING for idempotent re-runs and
// sha256 de-dup, same as the reservation path.
const INSERT_UNRESOLVABLE_SQL: &str = concat!(
    "INSERT INTO media_object (id, kind, media_type, byte_size, sha256, width, \
     height, duration_ms, storage_backend, storage_uri, alt, ref_count, \
     gc_state, metadata, created_at, updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ",",
    p!(6),
    ",",
    p!(7),
    ", NULL, 'unresolvable', NULL, ",
    p!(8),
    ", 0, 3, NULL, ",
    p!(9),
    ",",
    p!(10),
    ") ON CONFLICT (sha256) DO NOTHING"
);

const SELECT_BY_SHA256_SQL: &str = concat!(
    "SELECT ",
    crate::sql::media_object_columns!(),
    " FROM media_object WHERE sha256 = ",
    p!(1)
);

const SELECT_BY_ID_SQL: &str = concat!(
    "SELECT ",
    crate::sql::media_object_columns!(),
    " FROM media_object WHERE id = ",
    p!(1)
);

// {2,3} -> 4 claim. Conditional WHERE single-flights concurrent
// promotions/recoveries (deleted-failed=2 / unresolvable=3 share this).
const UPDATE_CLAIM_PROMOTE_SQL: &str = concat!(
    "UPDATE media_object SET gc_state = 4, updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND gc_state IN (2, 3)"
);

// Promote a claimed row ({gc_state=4}). metadata uses COALESCE so the
// inline backend can write {"inline_b64":...} while s3/file pass NULL.
const UPDATE_PROMOTE_SQL: &str = concat!(
    "UPDATE media_object SET storage_uri = ",
    p!(1),
    ", storage_backend = ",
    p!(2),
    ", byte_size = ",
    p!(3),
    ", gc_state = ",
    p!(4),
    // metadata is JSONB on postgres; the bind is a JSON string, so it
    // must be cast (`$5::jsonb`) or COALESCE fails with "types text and
    // jsonb cannot be matched" (sqlite is untyped so `?` is fine).
    ", metadata = COALESCE(",
    p_jsonb!(5),
    ", metadata)",
    ", updated_at = ",
    p!(6),
    " WHERE id = ",
    p!(7),
    " AND gc_state = 4"
);

// Confirm a fresh reservation (gc_state stays caller-supplied: 0 if
// ref_count>0 else 1). WHERE gc_state=1 AND storage_uri IS NULL pins it
// to the reservation row we own.
const UPDATE_CONFIRM_RESERVATION_SQL: &str = concat!(
    "UPDATE media_object SET storage_uri = ",
    p!(1),
    ", storage_backend = ",
    p!(2),
    ", byte_size = ",
    p!(3),
    // metadata is JSONB on postgres; cast the bound JSON string so
    // COALESCE does not fail with "types text and jsonb cannot be
    // matched" (sqlite is untyped so `?` is fine).
    ", metadata = COALESCE(",
    p_jsonb!(4),
    ", metadata)",
    ", updated_at = ",
    p!(5),
    " WHERE id = ",
    p!(6),
    " AND gc_state = 1 AND storage_uri IS NULL"
);

// Register path: single conditional UPDATE for {2,3}->confirmed (no copy
// needed, the object is already externally PUT). Register cannot use the
// inline backend so metadata is untouched.
const UPDATE_REGISTER_PROMOTE_SQL: &str = concat!(
    "UPDATE media_object SET storage_uri = ",
    p!(1),
    ", storage_backend = ",
    p!(2),
    ", byte_size = ",
    p!(3),
    ", gc_state = ",
    p!(4),
    ", updated_at = ",
    p!(5),
    " WHERE id = ",
    p!(6),
    " AND gc_state IN (2, 3)"
);

// GC: promoting crash recovery, 4 -> 3 (collapse even if origin was 2).
const UPDATE_REVERT_PROMOTING_SQL: &str = concat!(
    "UPDATE media_object SET gc_state = 3, updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND gc_state = 4"
);

// ref_count=0 reached: claim the delete duty {0,1} -> 5=deleting (CAS).
const UPDATE_CLAIM_DELETING_SQL: &str = concat!(
    "UPDATE media_object SET gc_state = 5, updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND ref_count = 0 AND gc_state IN (0, 1)"
);

// storage delete failed: 5 -> 2 (keep DB row so sha256 survives for GC
// retry). Also used by GC to recover stale deleting rows (5 -> 1).
const UPDATE_MARK_DELETE_FAILED_SQL: &str = concat!(
    "UPDATE media_object SET gc_state = 2, updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND gc_state = 5"
);
const UPDATE_REVERT_DELETING_SQL: &str = concat!(
    "UPDATE media_object SET gc_state = 1, updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND gc_state = 5"
);

// Conditional physical delete (TOCTOU double-check). gc_state IN (0,1,5):
// 5 = claimed-and-owned by this path, 0/1 = direct (no claim) path.
const DELETE_IF_UNREFERENCED_SQL: &str = concat!(
    "DELETE FROM media_object WHERE id = ",
    p!(1),
    " AND ref_count = 0 AND gc_state IN (0, 1, 5)"
);

// ref_count+1 with gc_state guard (design §6.3.1 confirmed form):
//  - 1=orphan -> 0=active ONLY when storage_uri IS NOT NULL (confirmed);
//    a NULL-uri reservation must NOT be flipped active.
//  - {0,3,4} and the NULL-uri 1 are preserved.
//  - {2=deleted-failed, 5=deleting} are rejected entirely (WHERE clause):
//    re-referencing a possibly-absent / delete-claimed body is unsafe.
const REFCOUNT_INCR_SQL: &str = concat!(
    "UPDATE media_object SET ref_count = ref_count + 1, gc_state = CASE \
     WHEN gc_state = 1 AND storage_uri IS NOT NULL THEN 0 ELSE gc_state END, \
     updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND gc_state NOT IN (2, 5)"
);

// ref_count-1, RETURNING the post-decrement state so the caller can take
// the delete branch without a second SELECT. RETURNING works on
// sqlite>=3.35 (sqlx bundles 3.45+) and postgres.
//
// `AND ref_count > 0` is a hard underflow guard: a double-decr (a bug or
// a Phase 2 implementation slip) would otherwise drive ref_count negative,
// and `claim_deleting_tx` (WHERE ref_count = 0) would then never fire so
// the row could never be physically reclaimed. With the guard a decr on
// an already-zero row affects 0 rows -> `decr_ref_tx` returns `Ok(None)`,
// which the caller treats as an invariant violation and rolls back
// (design §6.1 reverse-feedback).
const REFCOUNT_DECR_SQL: &str = concat!(
    "UPDATE media_object SET ref_count = ref_count - 1, updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND ref_count > 0",
    " RETURNING ref_count, gc_state, storage_backend, sha256, storage_uri"
);

// GC scans.
const SELECT_ORPHAN_RESERVATION_SQL: &str = concat!(
    "SELECT ",
    crate::sql::media_object_columns!(),
    " FROM media_object WHERE gc_state = 1 AND storage_uri IS NULL \
     AND ref_count = 0 AND created_at < ",
    p!(1)
);
const SELECT_PROMOTING_STALE_SQL: &str = concat!(
    "SELECT ",
    crate::sql::media_object_columns!(),
    " FROM media_object WHERE gc_state = 4 AND storage_uri IS NULL \
     AND updated_at < ",
    p!(1)
);
const SELECT_DELETING_STALE_SQL: &str = concat!(
    "SELECT ",
    crate::sql::media_object_columns!(),
    " FROM media_object WHERE gc_state = 5 AND storage_uri IS NOT NULL \
     AND updated_at < ",
    p!(1)
);

// Confirmed orphan: Upload settled (storage_uri set) but the memory
// INSERT never landed (server restart between confirm and the memory
// tx). storage_uri is the on-storage key, so this goes through the
// normal claim→storage→conditional-delete path, NOT the sha256→final
// recompute path reserved for unconfirmed (storage_uri IS NULL) rows.
const SELECT_CONFIRMED_ORPHANS_SQL: &str = concat!(
    "SELECT ",
    crate::sql::media_object_columns!(),
    " FROM media_object WHERE gc_state = 1 AND storage_uri IS NOT NULL \
     AND created_at < ",
    p!(1)
);

// Storage-delete-failed rows still carrying their confirmed key. url /
// inline never reach gc_state=2 (their delete path does not touch
// external storage), so a confirmed key is always present here; the
// filter keeps the GC retry on the only rows where a storage re-delete
// is meaningful.
const SELECT_DELETED_FAILED_CONFIRMED_SQL: &str = concat!(
    "SELECT ",
    crate::sql::media_object_columns!(),
    " FROM media_object WHERE gc_state = 2 AND storage_uri IS NOT NULL"
);

// ref_count desync: stored ref_count disagrees with the real number of
// memory rows pointing at this media. Restricted to confirmed rows
// (storage_uri IS NOT NULL): an unconfirmed reservation can legitimately
// carry ref_count>0 while storage_uri IS NULL (incr-before-confirm
// race), and that is the reservation GC's concern — reconciling it here
// would double-handle the same row. The LEFT JOIN computes the real
// count once (vs a correlated subquery evaluated per projection + per
// WHERE); COALESCE keeps zero-reference rows (the reconcile-then-delete
// class). Plain literal (no `p!()`) so clippy::useless_concat stays
// quiet; COUNT(*) is portable across postgres/sqlite.
const SELECT_REF_COUNT_DESYNC_SQL: &str = "SELECT m.id, m.ref_count, \
     COALESCE(c.actual_ref_count, 0) AS actual_ref_count, \
     m.gc_state, m.storage_backend \
     FROM media_object m \
     LEFT JOIN (SELECT media_object_id, COUNT(*) AS actual_ref_count \
                FROM memory WHERE media_object_id IS NOT NULL \
                GROUP BY media_object_id) c ON c.media_object_id = m.id \
     WHERE m.storage_uri IS NOT NULL \
     AND m.ref_count <> COALESCE(c.actual_ref_count, 0)";

// ref_count desync correction. WHERE ref_count = expected makes this a
// CAS keyed on the read-time value: a primary-path incr/decr landing
// between the scan and this UPDATE moves ref_count, the CAS affects 0
// rows, and the correction is skipped (re-evaluated next sweep — the
// primary path always wins). gc_state IN (0,1) keeps the dedicated GC
// states {2,3,4,5} off-limits.
const UPDATE_RECONCILE_REF_COUNT_SQL: &str = concat!(
    "UPDATE media_object SET ref_count = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3),
    " AND ref_count = ",
    p!(4),
    " AND gc_state IN (0, 1)"
);

// Re-claim a deleted-failed row (2 -> 5=deleting) so the GC retry rides
// the same claim→storage→conditional-delete discipline as the primary
// path (a storage re-delete failure then flips 5→2 again, idempotently
// retried). The ref_count=0 guard protects a row a recovery Upload
// re-referenced while it sat in deleted-failed: it must not be deleted.
const UPDATE_RECLAIM_DELETED_FAILED_SQL: &str = concat!(
    "UPDATE media_object SET gc_state = 5, updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND ref_count = 0 AND gc_state = 2"
);

fn now_millis() -> i64 {
    command_utils::util::datetime::now_millis()
}

/// The locked media_object's `kind` / `storage_backend`, carried out of
/// the create transaction so the post-commit dispatch can decide the
/// embedding axis without a second SELECT.
#[derive(Debug, Clone)]
pub struct MediaRefBump {
    pub kind: i32,
    pub storage_backend: String,
}

#[async_trait]
pub trait MediaObjectRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    /// Insert a reservation. `Ok(false)` = `ON CONFLICT(sha256)` (an
    /// existing row holds this sha256; the caller re-SELECTs and branches).
    async fn insert_reservation_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        row: &MediaObjectReservation,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(INSERT_RESERVATION_SQL)
            .bind(row.id)
            .bind(row.kind)
            .bind(&row.media_type)
            .bind(row.byte_size)
            .bind(&row.sha256)
            .bind(row.width)
            .bind(row.height)
            .bind(row.duration_ms)
            .bind(&row.alt)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("insert_reservation_tx")?;
        Ok(res.rows_affected() > 0)
    }

    /// url-backend insert (sha256 NULL, no ON CONFLICT). The reservation
    /// path requires a sha256, so url rows go through here.
    #[allow(clippy::too_many_arguments)]
    async fn insert_url_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        kind: i32,
        media_type: &str,
        byte_size: Option<i64>,
        width: Option<i32>,
        height: Option<i32>,
        storage_uri: &str,
        alt: Option<&str>,
    ) -> Result<()> {
        let now = now_millis();
        sqlx::query::<Rdb>(INSERT_URL_SQL)
            .bind(id)
            .bind(kind)
            .bind(media_type)
            .bind(byte_size)
            .bind(width)
            .bind(height)
            .bind(storage_uri)
            .bind(alt)
            .bind(now)
            .bind(now)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("insert_url_tx")?;
        Ok(())
    }

    /// unresolvable insert (image memory Phase 4 migration, elided
    /// source). `Ok(false)` = `ON CONFLICT(sha256)` (a row already holds
    /// this sha256 — idempotent re-run or sha256 de-dup; the caller
    /// re-SELECTs by sha256 to reuse the existing media_object).
    #[allow(clippy::too_many_arguments)]
    async fn insert_unresolvable_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        kind: i32,
        media_type: &str,
        byte_size: Option<i64>,
        sha256: &str,
        width: Option<i32>,
        height: Option<i32>,
        alt: Option<&str>,
    ) -> Result<bool> {
        let now = now_millis();
        let res = sqlx::query::<Rdb>(INSERT_UNRESOLVABLE_SQL)
            .bind(id)
            .bind(kind)
            .bind(media_type)
            .bind(byte_size)
            .bind(sha256)
            .bind(width)
            .bind(height)
            .bind(alt)
            .bind(now)
            .bind(now)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("insert_unresolvable_tx")?;
        Ok(res.rows_affected() > 0)
    }

    async fn find_by_sha256_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        sha256: &str,
    ) -> Result<Option<MediaObjectRow>> {
        sqlx::query_as::<Rdb, MediaObjectRow>(SELECT_BY_SHA256_SQL)
            .bind(sha256)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("find_by_sha256_tx")
    }

    async fn find_by_id(&self, id: i64) -> Result<Option<MediaObjectRow>> {
        sqlx::query_as::<Rdb, MediaObjectRow>(SELECT_BY_ID_SQL)
            .bind(id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("find_by_id")
    }

    /// Bulk variant of `find_by_id` (chunked `IN (...)`). Returned rows
    /// are in no particular order — callers index by `row.id`. Used by
    /// `redispatch_embeddings` to resolve a whole page of memories'
    /// linked media in one query instead of N per-row `find_by_id`s.
    async fn find_by_ids(&self, ids: &[i64]) -> Result<Vec<MediaObjectRow>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(crate::sql::IN_LIST_CHUNK_SIZE) {
            let placeholders = crate::sql::build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT id, kind, media_type, byte_size, sha256, width, \
                 height, duration_ms, storage_backend, storage_uri, alt, \
                 ref_count, gc_state, metadata, created_at, updated_at \
                 FROM media_object WHERE id IN ({placeholders})"
            );
            let mut query = sqlx::query_as::<Rdb, MediaObjectRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("find_by_ids")?;
            out.extend(rows);
        }
        Ok(out)
    }

    /// Row-locked existence read (primary defense before decr/incr; on
    /// postgres this is `SELECT ... FOR UPDATE`).
    async fn find_by_id_for_update_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        id: i64,
    ) -> Result<Option<MediaObjectRow>> {
        let sql = format!(
            "SELECT {COLUMNS} FROM media_object WHERE id = {}{}",
            p!(1),
            FOR_UPDATE_SUFFIX
        );
        sqlx::query_as::<Rdb, MediaObjectRow>(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&mut **tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("find_by_id_for_update_tx")
    }

    /// `{2,3} -> 4` claim. `Ok(false)` = lost the race / state changed.
    async fn claim_promote_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_CLAIM_PROMOTE_SQL)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("claim_promote_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// Confirm a claimed (`gc_state=4`) row to its final URI. `gc_state`
    /// is `0` when `ref_count>0` else `1` (caller computes). `metadata`
    /// is `Some` only for the inline backend.
    #[allow(clippy::too_many_arguments)]
    async fn confirm_promote_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        final_uri: &str,
        backend: &str,
        byte_size: Option<i64>,
        gc_state: i32,
        metadata: Option<&str>,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_PROMOTE_SQL)
            .bind(final_uri)
            .bind(backend)
            .bind(byte_size)
            .bind(gc_state)
            .bind(metadata)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("confirm_promote_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// Confirm a fresh reservation (`gc_state=1, storage_uri IS NULL`) to
    /// its final URI. gc_state stays 1=orphan (memory link will promote
    /// it via incr_ref). `Ok(false)` = the reservation row was no longer
    /// in the expected state (concurrent GC / promotion).
    async fn confirm_reservation_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        final_uri: &str,
        backend: &str,
        byte_size: Option<i64>,
        metadata: Option<&str>,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_CONFIRM_RESERVATION_SQL)
            .bind(final_uri)
            .bind(backend)
            .bind(byte_size)
            .bind(metadata)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("confirm_reservation_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// Register path single-UPDATE promotion ({2,3} -> confirmed).
    async fn register_promote_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        final_uri: &str,
        backend: &str,
        byte_size: Option<i64>,
        gc_state: i32,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_REGISTER_PROMOTE_SQL)
            .bind(final_uri)
            .bind(backend)
            .bind(byte_size)
            .bind(gc_state)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("register_promote_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// Conditional physical delete (`ref_count=0 AND gc_state IN (0,1,5)`).
    /// `Ok(true)` = deleted (normal). `Ok(false)` = invariant violation
    /// (a claimed row vanished / was re-referenced — only possible via a
    /// bug or manual DB edit); the caller must mark deleted-failed + alert.
    async fn delete_if_unreferenced_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_IF_UNREFERENCED_SQL)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("delete_if_unreferenced_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// `ref_count+1` with the §6.3.1 gc_state guard. `Ok(false)` = no row
    /// or `gc_state ∈ {2,5}` (caller maps to `InvalidArgument`).
    async fn incr_ref_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(REFCOUNT_INCR_SQL)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("incr_ref_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// Row-lock a referenced media_object then bump its ref_count, in the
    /// caller's create transaction. The row-lock serializes a concurrent
    /// decr/delete against this incr; skipping the bump would leave the
    /// new memory's media orphaned (ref_count=0) so the deferred GC
    /// reclaims a still-referenced object. Returns the locked row's
    /// kind/backend for the caller's post-commit embedding dispatch. A
    /// missing target or a gc_state that rejects re-reference rolls the
    /// whole tx back via `InvalidArgument`. Single source of the
    /// re-reference rejection wording (shared by every create path).
    async fn lock_and_incr_ref_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        id: i64,
    ) -> Result<MediaRefBump> {
        let Some(row) = self.find_by_id_for_update_tx(tx, id).await? else {
            return Err(
                LlmMemoryError::InvalidArgument(format!("media_object not found: {id}")).into(),
            );
        };
        let bump = MediaRefBump {
            kind: row.kind,
            storage_backend: row.storage_backend,
        };
        if !self.incr_ref_tx(&mut **tx, id).await? {
            return Err(LlmMemoryError::InvalidArgument(format!(
                "media_object {id} not found, pending GC recovery \
                 (deleted-failed), or being deleted; retry after \
                 cleanup or re-upload the bytes"
            ))
            .into());
        }
        Ok(bump)
    }

    /// `ref_count-1` (guarded by `AND ref_count > 0`). `Ok(Some(..))` =
    /// decremented. `Ok(None)` = no row OR `ref_count` was already 0 (a
    /// double-decr / underflow). The caller treats `None` on a row it
    /// expected to exist as an invariant violation and rolls the tx back
    /// (design §6.1).
    async fn decr_ref_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<Option<RefDecrResult>> {
        let row = sqlx::query_as::<Rdb, (i64, i32, String, Option<String>, Option<String>)>(
            REFCOUNT_DECR_SQL,
        )
        .bind(now_millis())
        .bind(id)
        .fetch_optional(tx)
        .await
        .map_err(LlmMemoryError::DBError)
        .context("decr_ref_tx")?;
        Ok(row.map(
            |(ref_count, gc_state, storage_backend, sha256, storage_uri)| RefDecrResult {
                ref_count,
                gc_state,
                storage_backend,
                sha256,
                storage_uri,
            },
        ))
    }

    /// `{0,1} -> 5=deleting` CAS. `Ok(true)` = this caller owns the
    /// delete duty.
    async fn claim_deleting_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_CLAIM_DELETING_SQL)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("claim_deleting_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// `5 -> 2=deleted-failed` (storage delete failed; keep row so sha256
    /// survives for GC retry).
    async fn mark_delete_failed_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_MARK_DELETE_FAILED_SQL)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("mark_delete_failed_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// `5 -> 1=orphan` (GC recovering a stale deleting row).
    async fn revert_deleting_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_REVERT_DELETING_SQL)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("revert_deleting_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// `4 -> 3=unresolvable` (GC recovering a stale promoting row).
    async fn revert_promoting_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_REVERT_PROMOTING_SQL)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("revert_promoting_tx")?;
        Ok(res.rows_affected() == 1)
    }

    async fn list_orphan_reservations(&self, before: i64) -> Result<Vec<MediaObjectRow>> {
        sqlx::query_as::<Rdb, MediaObjectRow>(SELECT_ORPHAN_RESERVATION_SQL)
            .bind(before)
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("list_orphan_reservations")
    }

    async fn list_promoting_stale(&self, before: i64) -> Result<Vec<MediaObjectRow>> {
        sqlx::query_as::<Rdb, MediaObjectRow>(SELECT_PROMOTING_STALE_SQL)
            .bind(before)
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("list_promoting_stale")
    }

    async fn list_deleting_stale(&self, before: i64) -> Result<Vec<MediaObjectRow>> {
        sqlx::query_as::<Rdb, MediaObjectRow>(SELECT_DELETING_STALE_SQL)
            .bind(before)
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("list_deleting_stale")
    }

    async fn list_confirmed_orphans(&self, before: i64) -> Result<Vec<MediaObjectRow>> {
        sqlx::query_as::<Rdb, MediaObjectRow>(SELECT_CONFIRMED_ORPHANS_SQL)
            .bind(before)
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("list_confirmed_orphans")
    }

    async fn list_deleted_failed_confirmed(&self) -> Result<Vec<MediaObjectRow>> {
        sqlx::query_as::<Rdb, MediaObjectRow>(SELECT_DELETED_FAILED_CONFIRMED_SQL)
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("list_deleted_failed_confirmed")
    }

    async fn list_ref_count_desync(&self) -> Result<Vec<RefCountDesync>> {
        let rows = sqlx::query_as::<Rdb, (i64, i64, i64, i32, String)>(SELECT_REF_COUNT_DESYNC_SQL)
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("list_ref_count_desync")?;
        Ok(rows
            .into_iter()
            .map(
                |(id, db_ref_count, actual_ref_count, gc_state, storage_backend)| RefCountDesync {
                    id,
                    db_ref_count,
                    actual_ref_count,
                    gc_state,
                    storage_backend,
                },
            )
            .collect())
    }

    /// Reconcile ref_count to `actual_ref` via a CAS on `expected_ref`
    /// (the value read during the scan); a concurrent primary-path
    /// write moves ref_count and loses this update (`Ok(false)`).
    async fn reconcile_ref_count_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        expected_ref: i64,
        actual_ref: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_RECONCILE_REF_COUNT_SQL)
            .bind(actual_ref)
            .bind(now_millis())
            .bind(id)
            .bind(expected_ref)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("reconcile_ref_count_tx")?;
        Ok(res.rows_affected() == 1)
    }

    /// `2=deleted-failed -> 5=deleting` so the GC retry rejoins the
    /// normal delete discipline. Guarded by `ref_count=0` so a row a
    /// recovery Upload re-referenced is left alone.
    async fn reclaim_deleted_failed_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(UPDATE_RECLAIM_DELETED_FAILED_SQL)
            .bind(now_millis())
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("reclaim_deleted_failed_tx")?;
        Ok(res.rows_affected() == 1)
    }
}

/// Concrete repository: an `RdbPool` + an id generator.
pub struct MediaObjectRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl MediaObjectRepositoryImpl {
    /// Argument order mirrors `MemoryRatingRepositoryImpl::new`
    /// (id_generator, pool) so the DI module stays uniform.
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for MediaObjectRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for MediaObjectRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl MediaObjectRepository for MediaObjectRepositoryImpl {}

/// DI accessor trait, mirroring `UseMemoryRatingRepository`.
pub trait UseMediaObjectRepository {
    fn media_object_repository(&self) -> &MediaObjectRepositoryImpl;
}

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use super::*;
    use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};

    fn repo(pool: &'static RdbPool) -> MediaObjectRepositoryImpl {
        MediaObjectRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool)
    }

    fn reservation(id: i64, sha: &str) -> MediaObjectReservation {
        let now = now_millis();
        MediaObjectReservation {
            id,
            kind: 2, // IMAGE
            media_type: "image/png".to_string(),
            byte_size: Some(123),
            sha256: sha.to_string(),
            width: Some(10),
            height: Some(20),
            duration_ms: None,
            alt: Some("alt".to_string()),
            created_at: now,
            updated_at: now,
        }
    }

    async fn pool() -> &'static RdbPool {
        #[cfg(feature = "postgres")]
        {
            setup_test_rdb_from("sql/postgres").await
        }
        #[cfg(not(feature = "postgres"))]
        {
            setup_test_rdb_from("sql/sqlite").await
        }
    }

    /// Reservation INSERT succeeds once; a second INSERT for the same
    /// sha256 hits ON CONFLICT DO NOTHING (Ok(false)) and the row is
    /// findable by sha256.
    #[test]
    fn reservation_insert_on_conflict() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);
            let id = r.id_generator().generate_id()?;
            let sha = format!("sha_resv_{id}");
            let mut tx = p.begin().await?;
            assert!(
                r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                    .await?
            );
            tx.commit().await?;

            let mut tx = p.begin().await?;
            let other = r.id_generator().generate_id()?;
            // Same sha256 -> ON CONFLICT DO NOTHING -> Ok(false).
            assert!(
                !r.insert_reservation_tx(&mut *tx, &reservation(other, &sha))
                    .await?
            );
            tx.commit().await?;

            let found = r.find_by_sha256_tx(p, sha.as_str()).await?.unwrap();
            assert_eq!(found.id, id);
            assert_eq!(found.gc_state, GC_ORPHAN);
            assert_eq!(found.ref_count, 0);
            assert!(found.storage_uri.is_none());
            Ok(())
        })
    }

    /// incr_ref: a confirmed orphan (storage_uri set) promotes 1->0;
    /// {2,5} reject incr; an unconfirmed reservation (storage_uri NULL)
    /// stays gc_state=1 even after incr.
    #[test]
    fn incr_ref_gc_state_guard() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);
            let id = r.id_generator().generate_id()?;
            let sha = format!("sha_incr_{id}");

            // reservation, then confirm to a final uri (still orphan).
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                .await?;
            assert!(
                r.confirm_reservation_tx(&mut *tx, id, "s3://b/k", "s3", Some(123), None)
                    .await?
            );
            tx.commit().await?;

            // confirmed orphan -> incr promotes to active.
            let mut tx = p.begin().await?;
            assert!(r.incr_ref_tx(&mut *tx, id).await?);
            tx.commit().await?;
            let row = r.find_by_id(id).await?.unwrap();
            assert_eq!(row.gc_state, GC_ACTIVE);
            assert_eq!(row.ref_count, 1);

            // unconfirmed reservation: incr must NOT flip it to active.
            let id2 = r.id_generator().generate_id()?;
            let sha2 = format!("sha_incr2_{id2}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id2, &sha2))
                .await?;
            assert!(r.incr_ref_tx(&mut *tx, id2).await?);
            tx.commit().await?;
            let row2 = r.find_by_id(id2).await?.unwrap();
            assert_eq!(
                row2.gc_state, GC_ORPHAN,
                "NULL-uri reservation must stay orphan after incr"
            );
            assert_eq!(row2.ref_count, 1);

            // deleted-failed (2) rejects incr entirely.
            let id3 = r.id_generator().generate_id()?;
            let sha3 = format!("sha_incr3_{id3}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id3, &sha3))
                .await?;
            r.confirm_reservation_tx(&mut *tx, id3, "s3://b/k3", "s3", Some(1), None)
                .await?;
            // drive to deleting then deleted-failed.
            r.incr_ref_tx(&mut *tx, id3).await?;
            let d = r.decr_ref_tx(&mut *tx, id3).await?.unwrap();
            assert_eq!(d.ref_count, 0);
            assert!(r.claim_deleting_tx(&mut *tx, id3).await?);
            assert!(r.mark_delete_failed_tx(&mut *tx, id3).await?);
            tx.commit().await?;
            let mut tx = p.begin().await?;
            assert!(
                !r.incr_ref_tx(&mut *tx, id3).await?,
                "deleted-failed (gc_state=2) must reject incr_ref"
            );
            tx.commit().await?;
            Ok(())
        })
    }

    /// claim_deleting is a {0,1}->5 CAS: only one of two concurrent
    /// claims wins; the conditional delete then removes the row.
    #[test]
    fn claim_deleting_then_conditional_delete() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);
            let id = r.id_generator().generate_id()?;
            let sha = format!("sha_del_{id}");

            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                .await?;
            r.confirm_reservation_tx(&mut *tx, id, "s3://b/k", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, id).await?; // active, ref=1
            let d = r.decr_ref_tx(&mut *tx, id).await?.unwrap();
            assert_eq!(d.ref_count, 0);
            assert_eq!(d.storage_backend, "s3");
            // First claim wins.
            assert!(r.claim_deleting_tx(&mut *tx, id).await?);
            // Second claim on the now-deleting row loses (gc_state=5 not in {0,1}).
            assert!(!r.claim_deleting_tx(&mut *tx, id).await?);
            // Conditional delete succeeds (gc_state=5 is in the allowed set).
            assert!(r.delete_if_unreferenced_tx(&mut *tx, id).await?);
            tx.commit().await?;
            assert!(r.find_by_id(id).await?.is_none());
            Ok(())
        })
    }

    /// {2,3} -> 4 claim single-flights; confirm_promote then settles it.
    #[test]
    fn claim_promote_single_flight() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);
            let id = r.id_generator().generate_id()?;
            let sha = format!("sha_prom_{id}");

            // Build an unresolvable (gc_state=3) row by hand: reservation +
            // mark as unresolvable via a raw UPDATE (migration path emulation).
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                .await?;
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET gc_state = 3, storage_backend = 'unresolvable' \
                 WHERE id = {}",
                p!(1)
            )))
            .bind(id)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            let mut tx = p.begin().await?;
            assert!(r.claim_promote_tx(&mut *tx, id).await?, "first claim wins");
            // Second claim loses (gc_state now 4, not in {2,3}).
            assert!(!r.claim_promote_tx(&mut *tx, id).await?);
            // ref_count=0 so confirm with gc_state=1 (orphan).
            assert!(
                r.confirm_promote_tx(&mut *tx, id, "s3://b/k", "s3", Some(9), GC_ORPHAN, None)
                    .await?
            );
            tx.commit().await?;
            let row = r.find_by_id(id).await?.unwrap();
            assert_eq!(row.gc_state, GC_ORPHAN);
            assert_eq!(row.storage_backend, "s3");
            assert_eq!(row.storage_uri.as_deref(), Some("s3://b/k"));
            Ok(())
        })
    }

    /// decr_ref guards against underflow: a decr on ref_count=0 affects 0
    /// rows and returns Ok(None); ref_count never goes negative. Boundary:
    /// 1->0 returns Some, a further decr returns None (review finding #2).
    #[test]
    fn decr_ref_guard_no_underflow() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);
            let id = r.id_generator().generate_id()?;
            let sha = format!("sha_decr_guard_{id}");

            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                .await?;
            r.confirm_reservation_tx(&mut *tx, id, "s3://b/k", "s3", Some(1), None)
                .await?;
            // ref_count: 0 -> 1.
            assert!(r.incr_ref_tx(&mut *tx, id).await?);
            // 1 -> 0 : decr succeeds, returns Some.
            let d = r
                .decr_ref_tx(&mut *tx, id)
                .await?
                .expect("decr on ref_count=1 returns Some");
            assert_eq!(d.ref_count, 0);
            // 0 -> would underflow : guarded, returns None, no row mutated.
            let again = r.decr_ref_tx(&mut *tx, id).await?;
            assert!(
                again.is_none(),
                "decr on ref_count=0 must return None (underflow guard)"
            );
            tx.commit().await?;

            let row = r.find_by_id(id).await?.unwrap();
            assert_eq!(row.ref_count, 0, "ref_count must not go negative");
            Ok(())
        })
    }

    /// A confirmed orphan (gc_state=1, storage_uri set) older than the
    /// cutoff is returned; a younger one and a NULL-uri reservation are
    /// not. Boundary: `created_at < before` is strict (equal is excluded).
    #[test]
    fn list_confirmed_orphans_filters_by_age_and_confirmation() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);

            // old confirmed orphan: should be returned.
            let old_id = r.id_generator().generate_id()?;
            let sha_old = format!("sha_co_old_{old_id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(old_id, &sha_old))
                .await?;
            r.confirm_reservation_tx(&mut *tx, old_id, "s3://b/old", "s3", Some(1), None)
                .await?;
            tx.commit().await?;
            // force created_at far into the past.
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET created_at = 1000 WHERE id = {}",
                p!(1)
            )))
            .bind(old_id)
            .execute(p)
            .await?;

            // young confirmed orphan: created_at = now, must NOT match.
            let young_id = r.id_generator().generate_id()?;
            let sha_young = format!("sha_co_young_{young_id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(young_id, &sha_young))
                .await?;
            r.confirm_reservation_tx(&mut *tx, young_id, "s3://b/young", "s3", Some(1), None)
                .await?;
            tx.commit().await?;

            // unconfirmed reservation (storage_uri NULL): must NOT match
            // (that is the reservation-GC path, not confirmed-orphan).
            let resv_id = r.id_generator().generate_id()?;
            let sha_resv = format!("sha_co_resv_{resv_id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(resv_id, &sha_resv))
                .await?;
            tx.commit().await?;
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET created_at = 1000 WHERE id = {}",
                p!(1)
            )))
            .bind(resv_id)
            .execute(p)
            .await?;

            let before = 2000;
            let rows = r.list_confirmed_orphans(before).await?;
            let ids: Vec<i64> = rows.iter().map(|x| x.id).collect();
            assert!(ids.contains(&old_id), "old confirmed orphan must be listed");
            assert!(
                !ids.contains(&young_id),
                "young confirmed orphan must not be listed"
            );
            assert!(
                !ids.contains(&resv_id),
                "NULL-uri reservation must not be listed as confirmed orphan"
            );

            // boundary: before == created_at excludes the row (strict <).
            let boundary = r.list_confirmed_orphans(1000).await?;
            assert!(
                !boundary.iter().any(|x| x.id == old_id),
                "before == created_at must exclude the row (strict <)"
            );
            Ok(())
        })
    }

    /// deleted-failed scan returns only confirmed rows (gc_state=2 AND
    /// storage_uri IS NOT NULL); a hypothetical NULL-uri gc_state=2 row is
    /// excluded (url/inline never reach gc_state=2).
    #[test]
    fn list_deleted_failed_confirmed_excludes_null_uri() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);

            // confirmed deleted-failed: reservation -> confirm -> 1 ->
            // 0 -> claim -> mark-failed (2). Should be returned.
            let cid = r.id_generator().generate_id()?;
            let sha_c = format!("sha_df_c_{cid}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(cid, &sha_c))
                .await?;
            r.confirm_reservation_tx(&mut *tx, cid, "s3://b/df", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, cid).await?;
            r.decr_ref_tx(&mut *tx, cid).await?;
            assert!(r.claim_deleting_tx(&mut *tx, cid).await?);
            assert!(r.mark_delete_failed_tx(&mut *tx, cid).await?);
            tx.commit().await?;

            // pathological NULL-uri gc_state=2 row (raw): must be excluded.
            let nid = r.id_generator().generate_id()?;
            let sha_n = format!("sha_df_n_{nid}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(nid, &sha_n))
                .await?;
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET gc_state = 2 WHERE id = {}",
                p!(1)
            )))
            .bind(nid)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            let rows = r.list_deleted_failed_confirmed().await?;
            let ids: Vec<i64> = rows.iter().map(|x| x.id).collect();
            assert!(
                ids.contains(&cid),
                "confirmed deleted-failed must be listed"
            );
            assert!(
                !ids.contains(&nid),
                "NULL-uri gc_state=2 must be excluded (storage_uri IS NOT NULL filter)"
            );
            Ok(())
        })
    }

    /// ref_count desync scan reports confirmed rows whose stored
    /// ref_count differs from the real `memory.media_object_id` count;
    /// in-sync rows and NULL-uri reservations are excluded.
    #[test]
    fn list_ref_count_desync_reports_only_mismatched_confirmed() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);

            // desync: confirmed, ref_count=2 but no memory references it.
            let bad_id = r.id_generator().generate_id()?;
            let sha_bad = format!("sha_ds_bad_{bad_id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(bad_id, &sha_bad))
                .await?;
            r.confirm_reservation_tx(&mut *tx, bad_id, "s3://b/ds", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, bad_id).await?;
            r.incr_ref_tx(&mut *tx, bad_id).await?; // ref_count=2, 0 memory
            tx.commit().await?;

            // in-sync: confirmed, ref_count=1, exactly one memory points
            // at it (created via the real memory repository).
            let ok_id = r.id_generator().generate_id()?;
            let sha_ok = format!("sha_ds_ok_{ok_id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(ok_id, &sha_ok))
                .await?;
            r.confirm_reservation_tx(&mut *tx, ok_id, "s3://b/ok", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, ok_id).await?; // ref_count=1
            tx.commit().await?;
            {
                use crate::infra::memory::rdb::MemoryRepository;
                use protobuf::llm_memory::data::{MediaObjectId, MemoryData};
                let mrepo = crate::infra::memory::rdb::MemoryRepositoryImpl::new(
                    crate::test_helper::shared_id_generator(),
                    p,
                );
                let mut tx = p.begin().await?;
                mrepo
                    .create(
                        &mut *tx,
                        &MemoryData {
                            parent_ids: vec![],
                            user_id: None,
                            content: "x".to_string(),
                            content_type: 2,
                            params: None,
                            metadata: None,
                            created_at: 1,
                            updated_at: 1,
                            role: 0,
                            external_id: None,
                            media_object_id: Some(MediaObjectId { value: ok_id }),
                            thread_ids: Vec::new(),
                            memory_kind: 0,
                        },
                    )
                    .await?;
                tx.commit().await?;
            }

            // NULL-uri reservation with a (pathological) ref_count: must
            // be excluded (handled by the reservation GC, not desync).
            let resv_id = r.id_generator().generate_id()?;
            let sha_resv = format!("sha_ds_resv_{resv_id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(resv_id, &sha_resv))
                .await?;
            tx.commit().await?;
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET ref_count = 3 WHERE id = {}",
                p!(1)
            )))
            .bind(resv_id)
            .execute(p)
            .await?;

            let desyncs = r.list_ref_count_desync().await?;
            let bad = desyncs.iter().find(|d| d.id == bad_id);
            assert!(bad.is_some(), "ref_count=2 vs 0 memory must be reported");
            let bad = bad.unwrap();
            assert_eq!(bad.db_ref_count, 2);
            assert_eq!(bad.actual_ref_count, 0);
            assert!(
                !desyncs.iter().any(|d| d.id == ok_id),
                "in-sync row (ref_count=1, 1 memory) must not be reported"
            );
            assert!(
                !desyncs.iter().any(|d| d.id == resv_id),
                "NULL-uri reservation must not be reported as desync"
            );
            Ok(())
        })
    }

    /// reconcile is a CAS keyed on the read-time ref_count: it succeeds
    /// when expected matches, fails (0 rows) when a concurrent write
    /// moved ref_count, and refuses non-{0,1} gc_state.
    #[test]
    fn reconcile_ref_count_cas_and_gc_state_guard() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);

            // confirmed orphan, ref_count bumped to 2 (db) but actual 0.
            let id = r.id_generator().generate_id()?;
            let sha = format!("sha_rc_{id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                .await?;
            r.confirm_reservation_tx(&mut *tx, id, "s3://b/rc", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, id).await?;
            r.incr_ref_tx(&mut *tx, id).await?; // gc_state active, ref=2
            tx.commit().await?;

            // CAS failure: expected ref_count (1) does not match actual (2).
            let mut tx = p.begin().await?;
            assert!(
                !r.reconcile_ref_count_tx(&mut *tx, id, 1, 0).await?,
                "stale expected ref_count must lose the CAS"
            );
            tx.commit().await?;
            assert_eq!(r.find_by_id(id).await?.unwrap().ref_count, 2);

            // CAS success: expected (2) matches; reconcile to actual (0).
            let mut tx = p.begin().await?;
            assert!(
                r.reconcile_ref_count_tx(&mut *tx, id, 2, 0).await?,
                "matching expected ref_count must win the CAS"
            );
            tx.commit().await?;
            assert_eq!(r.find_by_id(id).await?.unwrap().ref_count, 0);

            // gc_state guard: a deleted-failed (2) row is never reconciled.
            let did = r.id_generator().generate_id()?;
            let sha_d = format!("sha_rc_d_{did}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(did, &sha_d))
                .await?;
            r.confirm_reservation_tx(&mut *tx, did, "s3://b/rcd", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, did).await?;
            r.decr_ref_tx(&mut *tx, did).await?;
            r.claim_deleting_tx(&mut *tx, did).await?;
            r.mark_delete_failed_tx(&mut *tx, did).await?; // gc_state=2
            tx.commit().await?;
            let mut tx = p.begin().await?;
            assert!(
                !r.reconcile_ref_count_tx(&mut *tx, did, 0, 0).await?,
                "gc_state=2 must be outside reconcile (gc_state IN (0,1) guard)"
            );
            tx.commit().await?;
            Ok(())
        })
    }

    /// reclaim re-claims a deleted-failed row (2 -> 5) only when
    /// ref_count=0, so a row re-referenced by a recovery Upload is not
    /// deleted out from under it.
    #[test]
    fn reclaim_deleted_failed_requires_zero_ref() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let r = repo(p);

            // gc_state=2, ref_count=0 -> reclaim true, becomes deleting(5).
            let id = r.id_generator().generate_id()?;
            let sha = format!("sha_rdf_{id}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                .await?;
            r.confirm_reservation_tx(&mut *tx, id, "s3://b/rdf", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, id).await?;
            r.decr_ref_tx(&mut *tx, id).await?;
            r.claim_deleting_tx(&mut *tx, id).await?;
            r.mark_delete_failed_tx(&mut *tx, id).await?; // gc_state=2, ref=0
            tx.commit().await?;
            let mut tx = p.begin().await?;
            assert!(
                r.reclaim_deleted_failed_tx(&mut *tx, id).await?,
                "gc_state=2 ∧ ref_count=0 must reclaim to deleting"
            );
            tx.commit().await?;
            assert_eq!(r.find_by_id(id).await?.unwrap().gc_state, GC_DELETING);

            // gc_state=2 but ref_count>0 (recovery re-referenced it):
            // reclaim must refuse (do not delete a referenced row).
            let rid = r.id_generator().generate_id()?;
            let sha_r = format!("sha_rdf_ref_{rid}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(rid, &sha_r))
                .await?;
            r.confirm_reservation_tx(&mut *tx, rid, "s3://b/rdfr", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, rid).await?;
            r.decr_ref_tx(&mut *tx, rid).await?;
            r.claim_deleting_tx(&mut *tx, rid).await?;
            r.mark_delete_failed_tx(&mut *tx, rid).await?;
            tx.commit().await?;
            // simulate a recovery that re-references it (ref_count -> 1).
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET ref_count = 1 WHERE id = {}",
                p!(1)
            )))
            .bind(rid)
            .execute(p)
            .await?;
            let mut tx = p.begin().await?;
            assert!(
                !r.reclaim_deleted_failed_tx(&mut *tx, rid).await?,
                "ref_count>0 must refuse reclaim (ref_count=0 guard)"
            );
            tx.commit().await?;

            // gc_state != 2 (active) must also refuse.
            let aid = r.id_generator().generate_id()?;
            let sha_a = format!("sha_rdf_a_{aid}");
            let mut tx = p.begin().await?;
            r.insert_reservation_tx(&mut *tx, &reservation(aid, &sha_a))
                .await?;
            r.confirm_reservation_tx(&mut *tx, aid, "s3://b/rdfa", "s3", Some(1), None)
                .await?;
            r.incr_ref_tx(&mut *tx, aid).await?; // active
            assert!(
                !r.reclaim_deleted_failed_tx(&mut *tx, aid).await?,
                "gc_state != 2 must refuse reclaim"
            );
            tx.commit().await?;
            Ok(())
        })
    }
}
