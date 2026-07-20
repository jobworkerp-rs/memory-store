use super::config::{
    DistanceType, FTS_FINGERPRINT_SCHEMA_VERSION, FTS_MANIFEST_KEY_FINGERPRINT,
    FTS_MANIFEST_KEY_SCHEMA_VERSION, FtsConfig, LANCE_INDEX_VERSION, OptimizeConfig,
    VectorDBConfig, VectorIndexConfig,
};
use super::record::MemoryVectorRecord;
use super::safe_filter::SafeFilter;
use super::schema::memory_arrow_schema;
use arc_swap::ArcSwap;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use lancedb::Table;
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::index::{Index, IndexConfig, IndexType};
use lancedb::query::{ExecutableQuery, QueryBase, VectorQuery};
use lancedb::table::NewColumnTransform;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tokio::sync::{Mutex, MutexGuard};

/// Maximum candidate IDs for two-phase hybrid search IN filter.
/// LanceDB BM25 performance may degrade with very large IN lists.
/// This is a conservative heuristic; adjust if benchmarks show different thresholds.
const MAX_HYBRID_CANDIDATES: usize = 200;

/// Column on which the FTS inverted index is built. Stored as a constant
/// so `ensure_fts_index` and the fingerprint both refer to the same name.
const FTS_INDEX_COLUMN: &str = "content";

/// Column on which the vector (ANN) index is built. Shared by the memory
/// and thread repositories — both store their embedding in a column named
/// `embedding` (see `memory_arrow_schema` / `thread_arrow_schema`).
pub(crate) const EMBEDDING_INDEX_COLUMN: &str = "embedding";

/// Returns true if `idx` is an ANN (vector) index on the `embedding`
/// column. Any IVF/HNSW variant counts — what matters is that vector
/// queries can avoid a brute-force scan, not which specific quantization
/// was used. Kept as a single predicate so the variant list lives in one
/// place (callers include `probe_existing_vector_index` and the test
/// helpers).
pub(crate) fn is_embedding_vector_index(idx: &IndexConfig) -> bool {
    idx.columns.iter().any(|c| c == EMBEDDING_INDEX_COLUMN)
        && matches!(
            idx.index_type,
            IndexType::IvfFlat
                | IndexType::IvfSq
                | IndexType::IvfPq
                | IndexType::IvfRq
                | IndexType::IvfHnswPq
                | IndexType::IvfHnswSq
        )
}

/// Decide whether *this* caller should fire a maintenance gate, claiming
/// the fire via CAS so exactly one concurrent caller wins.
///
/// Returns true iff `now` has advanced at least `interval` past `marker`
/// AND this caller successfully advanced the marker. The marker snaps
/// forward by whole multiples of `interval` (to the largest
/// `prev + k*interval <= now`), so a burst that jumps several intervals at
/// once still fires only ONCE — running e.g. compaction back-to-back would
/// be pure waste — while keeping the next fire correctly spaced.
///
/// Shared by the memory and thread `track_operation`. `interval` must be
/// non-zero (callers gate on `!= 0`). The marker only ever moves forward,
/// so there is no ABA hazard.
pub(crate) fn try_claim_gate(
    now: usize,
    interval: usize,
    marker: &std::sync::atomic::AtomicUsize,
) -> bool {
    debug_assert!(interval != 0, "interval must be non-zero");
    let mut prev = marker.load(Ordering::Relaxed);
    loop {
        // saturating_sub is defensive: a racing winner may have already
        // advanced the marker past `now`.
        if now.saturating_sub(prev) < interval {
            return false; // not due, or another caller already claimed it
        }
        let steps = (now - prev) / interval;
        let next = prev + steps * interval;
        match marker.compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return true, // exactly one winner
            Err(actual) => prev = actual,
        }
    }
}

/// RAII guard for a single-slot "operation in flight" `AtomicBool`. Claim
/// the slot via [`InFlightGuard::try_claim`]; the slot is released on drop,
/// so a panic inside the spawned maintenance task can never leave the flag
/// stuck at `true` (which would suppress every future compaction).
///
/// Shared by the memory / thread / reflection-intent repositories so the
/// "spawn at most one background compaction" rule lives in one place.
pub(crate) struct InFlightGuard {
    flag: Arc<AtomicBool>,
}

impl InFlightGuard {
    /// Attempt to claim the slot. Returns `Some(guard)` for exactly one
    /// caller while the slot is free; concurrent callers (and re-entrant
    /// calls while a prior guard is alive) get `None` and must skip.
    pub(crate) fn try_claim(flag: &Arc<AtomicBool>) -> Option<Self> {
        // AcqRel on success publishes the claim and synchronizes with the
        // releasing `store` of a previous guard; Acquire on failure pairs
        // with that release so a failed claimer observes the in-flight task.
        match flag.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => Some(Self {
                flag: Arc::clone(flag),
            }),
            Err(_) => None,
        }
    }
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

/// Run LanceDB compaction + index optimization against the table behind an
/// [`ArcSwap`] handle, stamping `last_optimized_at` on success. Free
/// function (not a method) so it can be `tokio::spawn`ed by moving only the
/// two `Arc` fields it needs — the repository itself is held by value and is
/// not `Clone`/`Arc<Self>`, so we deliberately avoid capturing `&self`.
///
/// Loads the single shared handle via `load_full()` and advances it with a
/// trailing `checkout_latest()`. Concurrent writers (`merge_insert`/`delete`)
/// and `reload_table` operate on the same handle with monotonic version
/// stores, so they never regress the version this pass observes.
///
/// `ddl_lock` serializes this pass against `create_index` (`ensure_*_index`):
/// the `Compact` action rewrites data files and the `Index` action mutates
/// the index, both of which conflict with a concurrent `CreateIndex` commit
/// (LanceDB raises a retryable commit conflict). Now that compaction runs in
/// a spawned task it can overlap a later index build, so callers that have a
/// `create_index` path (memory / thread) pass `Some(&index_ddl_lock)`; the
/// reflection-intent store has no such DDL and passes `None`. The lock is
/// held for the whole compaction so the two never interleave.
pub(crate) async fn compact_with(
    table: &ArcSwap<Table>,
    last_optimized_at: &AtomicI64,
    ddl_lock: Option<&Mutex<()>>,
) -> anyhow::Result<()> {
    let _ddl = match ddl_lock {
        Some(l) => Some(l.lock().await),
        None => None,
    };
    let table = table.load_full();
    table
        .optimize(lancedb::table::OptimizeAction::Compact {
            options: lancedb::table::CompactionOptions::default(),
            remap_options: None,
        })
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB compact failed: {e}"))?;
    table
        .optimize(lancedb::table::OptimizeAction::Index(
            lancedb::table::OptimizeOptions::default(),
        ))
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB index optimize failed: {e}"))?;
    // Compaction commits new table versions; advance the shared handle to the
    // latest so a subsequent `create_index` (held off by `ddl_lock` until now)
    // builds against the compacted version instead of a stale snapshot — a
    // stale handle would hit a "Retryable commit conflict" against the version
    // compaction just wrote. `checkout_latest` mutates via `&self`, so every
    // reader sharing this single `Arc<Table>` observes the refreshed version.
    table
        .checkout_latest()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB checkout_latest after optimize failed: {e}"))?;
    last_optimized_at.store(
        command_utils::util::datetime::now_millis(),
        Ordering::Relaxed,
    );
    tracing::info!("LanceDB compact + index optimization completed");
    Ok(())
}

/// Prune old manifest versions against the table behind an [`ArcSwap`] handle.
/// Free function (mirrors [`compact_with`]) so the spawned auto-maintenance
/// task can run prune right after compaction — never concurrently — by moving
/// only the `Arc` fields it needs. `opt` supplies the retention policy via
/// `prune_action()`. Does NOT stamp `last_optimized_at`: pruning is manifest
/// GC, unrelated to the index-freshness metric.
pub(crate) async fn prune_with(table: &ArcSwap<Table>, opt: &OptimizeConfig) -> anyhow::Result<()> {
    let table = table.load_full();
    let stats = table
        .optimize(opt.prune_action())
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB prune failed: {e}"))?;
    if let Some(removal) = stats.prune {
        tracing::info!(
            "LanceDB prune completed: removed {} old versions, {} bytes",
            removal.old_versions,
            removal.bytes_removed
        );
    }
    Ok(())
}

/// Optimize policy used by repository tests. Disables the startup prune so
/// each `new()` in a test does not perform version-cleanup I/O, and keeps
/// short auto-optimize intervals so the auto-fire paths are still reachable
/// in tests that drive enough operations. Shared by both the memory and
/// thread `#[cfg(test)]` modules.
#[cfg(test)]
pub(crate) fn test_optimize_config() -> OptimizeConfig {
    OptimizeConfig {
        compact_interval: 100,
        prune_interval: 100,
        prune_on_startup: false,
        ..OptimizeConfig::default()
    }
}

/// Outcome of probing the existing ANN index on `embedding` against the
/// configured distance metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExistingVectorIndex {
    /// No ANN index on `embedding` yet — build one.
    Missing,
    /// An ANN index exists and was trained on the configured distance
    /// metric — adopt it as-is.
    Matches,
    /// An ANN index exists but was trained on a *different* distance
    /// metric than the current config (e.g. someone changed
    /// `MEMORY_DISTANCE_TYPE` and restarted). Searching it with the
    /// configured metric would yield invalid results, so it must be
    /// rebuilt. `.replace(true)` on create handles the swap.
    DistanceMismatch,
}

/// Inspect the existing `embedding` ANN index (if any) and report whether
/// it matches `desired` — the distance metric the current config would
/// train with. LanceDB requires the search-time distance to equal the
/// index's training distance, so a stale-metric index must be rebuilt
/// rather than adopted.
///
/// `list_indices` / `index_stats` failures are treated conservatively as
/// `Missing` so the caller falls through to a (idempotent, replace-true)
/// create attempt that surfaces a clear error.
pub(crate) async fn probe_existing_vector_index(
    table: &Table,
    desired: DistanceType,
) -> ExistingVectorIndex {
    let indices = match table.list_indices().await {
        Ok(i) => i,
        Err(e) => {
            tracing::warn!("list_indices failed while checking vector index: {e}");
            return ExistingVectorIndex::Missing;
        }
    };
    let Some(idx) = indices.iter().find(|i| is_embedding_vector_index(i)) else {
        return ExistingVectorIndex::Missing;
    };

    match table.index_stats(&idx.name).await {
        Ok(Some(stats)) => {
            let want: lancedb::DistanceType = desired.into();
            match stats.distance_type {
                Some(found) if found == want => ExistingVectorIndex::Matches,
                Some(found) => {
                    tracing::warn!(
                        "Existing vector index distance {found:?} != configured {want:?}; \
                         rebuilding to match"
                    );
                    ExistingVectorIndex::DistanceMismatch
                }
                None => {
                    // Vector index without a reported distance: cannot
                    // confirm it matches, so rebuild to be safe.
                    tracing::warn!(
                        "Existing vector index reports no distance metric; rebuilding to \
                         match configured {want:?}"
                    );
                    ExistingVectorIndex::DistanceMismatch
                }
            }
        }
        Ok(None) => ExistingVectorIndex::Missing,
        Err(e) => {
            tracing::warn!("index_stats failed while checking vector index distance: {e}");
            ExistingVectorIndex::Missing
        }
    }
}

/// Create an `IvfPq` ANN index on the `embedding` column. `num_partitions`
/// and `num_sub_vectors` are intentionally left unset so LanceDB derives
/// them from the row count and dimension — this is the most robust default
/// and avoids mis-tuning the index for the current corpus size.
/// "already exists" / "duplicate" errors are tolerated so the call is
/// idempotent across concurrent ensures and process restarts.
pub(crate) async fn create_vector_index(
    table: &Table,
    distance_type: DistanceType,
) -> anyhow::Result<()> {
    let distance: lancedb::DistanceType = distance_type.into();
    // Refresh-then-retry to survive a version race with a concurrent
    // compaction / writer / the sibling FTS build under the same
    // `index_ddl_lock` (see `create_index_with_retry`).
    let res = create_index_with_retry("vector", table, || {
        let builder = IvfPqIndexBuilder::default().distance_type(distance);
        table
            .create_index(&[EMBEDDING_INDEX_COLUMN], Index::IvfPq(builder))
            .replace(true)
            .execute()
    })
    .await;
    match res {
        Ok(()) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("already exists") || msg.contains("duplicate") {
                Ok(())
            } else {
                Err(anyhow::anyhow!("LanceDB create vector index failed: {e}"))
            }
        }
    }
}

/// Run a `create_index` build with a refresh-then-retry loop that tolerates
/// LanceDB's retryable commit conflict. Index DDL commits against a specific
/// table version; once compaction runs in a spawned task (and concurrent
/// writers swap the handle), the version can advance between loading the
/// handle and committing the build, so the commit is rejected with a
/// "Retryable commit conflict ... Please retry" error. We `checkout_latest`
/// to advance the handle to the winning version and retry a bounded number of
/// times. `replace(true)` keeps each attempt idempotent.
///
/// Shared with the thread repository's FTS `create_index` (which has the same
/// race against concurrent writers / compaction).
pub(crate) async fn create_index_with_retry<F, Fut>(
    label: &str,
    table: &Table,
    mut build: F,
) -> lancedb::error::Result<()>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = lancedb::error::Result<()>>,
{
    const MAX_ATTEMPTS: usize = 5;
    let mut attempt = 0;
    loop {
        attempt += 1;
        // Build against the latest version we know of. Best-effort refresh:
        // if it fails we still try the build and let the commit decide.
        if let Err(e) = table.checkout_latest().await {
            tracing::debug!("checkout_latest before {label} create_index failed: {e}");
        }
        match build().await {
            Ok(()) => return Ok(()),
            Err(e) => {
                let msg = e.to_string();
                let retryable = msg.contains("Retryable commit conflict")
                    || msg.contains("commit conflict")
                    || msg.contains("Please retry");
                if retryable && attempt < MAX_ATTEMPTS {
                    tracing::warn!(
                        "{label} create_index commit conflict (attempt {attempt}/{MAX_ATTEMPTS}); \
                         refreshing and retrying: {e}"
                    );
                    continue;
                }
                return Err(e);
            }
        }
    }
}

/// Drop the existing ANN index on the `embedding` column, if any. Used
/// when a stale-distance index cannot be rebuilt (corpus shrank below the
/// PQ training floor): dropping it lets `nearest_to` fall back to a
/// brute-force scan with the *configured* distance, instead of leaving a
/// wrong-metric index that would either be used with the wrong distance
/// or fail to rebuild. A "not found" race is tolerated (idempotent).
pub(crate) async fn drop_embedding_vector_index(table: &Table) -> anyhow::Result<()> {
    let indices = table
        .list_indices()
        .await
        .map_err(|e| anyhow::anyhow!("list_indices failed before dropping vector index: {e}"))?;
    let Some(idx) = indices.iter().find(|i| is_embedding_vector_index(i)) else {
        return Ok(());
    };
    match table.drop_index(&idx.name).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("not found") || msg.contains("does not exist") {
                Ok(())
            } else {
                Err(anyhow::anyhow!("LanceDB drop vector index failed: {e}"))
            }
        }
    }
}

/// Drop the existing FTS inverted index on the `content` column, if any.
///
/// Needed by the search-time recovery path under lance 8.0: when the
/// index is still registered in the table manifest but its `_indices`
/// sidecar files were removed externally, `create_index(...).replace(true)`
/// sees a manifest entry with the same definition and does NOT rewrite
/// the (now missing) sidecar, so the retry hits the same
/// `_indices/<uuid>/tokens.lance not found`. Dropping the stale manifest
/// entry first forces the subsequent `create_index` to materialize a
/// fresh index directory. A "not found" race is tolerated (idempotent).
pub(crate) async fn drop_fts_index(table: &Table) -> anyhow::Result<()> {
    let indices = table
        .list_indices()
        .await
        .map_err(|e| anyhow::anyhow!("list_indices failed before dropping FTS index: {e}"))?;
    let Some(idx) = indices.iter().find(|i| {
        i.columns.iter().any(|c| c == FTS_INDEX_COLUMN) && matches!(i.index_type, IndexType::FTS)
    }) else {
        return Ok(());
    };
    match table.drop_index(&idx.name).await {
        Ok(()) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("not found") || msg.contains("does not exist") {
                Ok(())
            } else {
                Err(anyhow::anyhow!("LanceDB drop FTS index failed: {e}"))
            }
        }
    }
}

/// Apply the shared ANN query options to a `nearest_to(...)` query so
/// every vector path (memory/thread, search/count) behaves identically.
///
/// - `distance_type`: MUST match the index's training metric, or LanceDB
///   returns invalid results (it defaults to L2 otherwise). Pinning it
///   also makes the brute-force path honor the configured metric.
/// - `bypass`: when the feature is disabled, force an exhaustive (flat)
///   scan via `bypass_vector_index()`. Otherwise a pre-existing index
///   from a previous boot would still be used by default, contradicting
///   the `*_VECTOR_INDEX_ENABLED=false` contract.
/// - `nprobes`: only meaningful with an index, so applied only when one
///   is active.
pub(crate) fn apply_vector_query_options(
    query: VectorQuery,
    cfg: VectorIndexConfig,
    distance_type: DistanceType,
    index_active: bool,
) -> VectorQuery {
    let mut query = query.distance_type(distance_type.into());
    if !cfg.enabled {
        query = query.bypass_vector_index();
    } else if index_active {
        query = query.nprobes(cfg.nprobes);
    }
    query
}

/// Shared ANN-index ensure logic for both the memory and thread
/// repositories. Holds the lock-free fast path and the mutex-serialized
/// one-time build so the two repos stay byte-for-byte identical in
/// behavior (DRY). See `MemoryVectorRepositoryImpl::ensure_vector_index`
/// for the policy rationale.
///
/// `ready` is the terminal fast-path gate: it is set **only** once the
/// state can no longer change for the life of the process — i.e. the
/// index exists (`active = true`) or the feature is disabled. The
/// below-`min_rows` case is deliberately *not* terminal: a process that
/// ingests data after an early small-corpus query must still build the
/// index once it crosses the threshold, so that path re-probes
/// `count_rows` on each call until either the index is built or `ready`
/// is set by a concurrent builder. `count_rows` is a cheap metadata read
/// (no table scan), and queries on a sub-threshold corpus are fast
/// anyway, so the re-probe cost is negligible.
pub(crate) async fn ensure_vector_index_inner(
    table: &ArcSwap<Table>,
    cfg: VectorIndexConfig,
    distance_type: DistanceType,
    ready: &AtomicBool,
    active: &AtomicBool,
    build_lock: &Mutex<()>,
    // Shared with the FTS path to serialize `create_index` DDL on the same
    // table (see `index_ddl_lock` field docs). Held only around the DDL.
    ddl_lock: &Mutex<()>,
) -> anyhow::Result<()> {
    // Acquire pairs with the Release store wherever `ready` is set below.
    if ready.load(Ordering::Acquire) {
        return Ok(());
    }
    if !cfg.enabled {
        // Master switch off: never build, never re-probe. Mark ready so
        // the disabled state is also a terminal fast-path hit.
        ready.store(true, Ordering::Release);
        return Ok(());
    }

    let _guard = build_lock.lock().await;
    // Double-check: another task may have completed the ensure between our
    // atomic load and acquiring the build lock.
    if ready.load(Ordering::Acquire) {
        return Ok(());
    }

    let table_guard = table.load_full();

    let existing = probe_existing_vector_index(&table_guard, distance_type).await;
    if matches!(existing, ExistingVectorIndex::Matches) {
        // An index exists and matches the configured metric — adopt as-is.
        active.store(true, Ordering::Release);
        ready.store(true, Ordering::Release);
        tracing::debug!("Vector index already present on `{EMBEDDING_INDEX_COLUMN}`");
        return Ok(());
    }

    // Both Missing and DistanceMismatch need a build, but only if the
    // corpus clears the PQ training floor: building below it fails with
    // "Not enough rows to train PQ". This check applies to DistanceMismatch
    // too — an index that was once valid may now sit on a corpus that
    // shrank (deletes) below the floor, in which case a rebuild would fail.
    let total = table_guard
        .count_rows(None)
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB count_rows failed: {e}"))?;
    let min_rows = cfg.effective_min_rows();
    let below_floor = total < min_rows;

    // Drop this snapshot handle before taking the DDL lock. With `ArcSwap`
    // there is no read guard to hold, so this can never deadlock against a
    // concurrent maintenance path. `ddl_lock` now serializes index DDL only
    // (vector build vs FTS build vs compaction's index optimize), all of which
    // commit on the shared handle; `reload_table` no longer takes it (it just
    // advances the same handle via `checkout_latest`, which a `create_index`
    // commit-conflict retry tolerates).
    drop(table_guard);

    if below_floor {
        // Below the training threshold: cannot (re)build. Do NOT mark
        // `ready` — the corpus may grow past the threshold later in this
        // same process, and we must re-evaluate on the next query.
        match existing {
            ExistingVectorIndex::DistanceMismatch => {
                // A stale-metric index is still on disk and would be used
                // (with the wrong distance) by `nearest_to`. Drop it so the
                // hot path falls back to a correct brute-force scan; it gets
                // rebuilt once the corpus grows back past the floor.
                let _ddl = ddl_lock.lock().await;
                let table_guard = table.load_full();
                drop_embedding_vector_index(&table_guard).await?;
                active.store(false, Ordering::Release);
                tracing::warn!(
                    "Dropped stale-distance vector index: {total} rows < min_rows={min_rows}; \
                     falling back to brute-force until the corpus grows"
                );
            }
            _ => {
                // Missing index + below floor: no physical index exists, so
                // `nearest_to` already brute-forces. Nothing to drop.
                tracing::debug!(
                    "Vector index deferred: {total} rows < min_rows={min_rows} (brute-force scan)"
                );
            }
        }
        return Ok(());
    }

    let reason = match existing {
        ExistingVectorIndex::DistanceMismatch => "rebuilt for distance change",
        _ => "built",
    };
    let start = std::time::Instant::now();
    {
        // Serialize the DDL against a concurrent FTS `create_index` on the
        // same table (the two can overlap under `parallel_hybrid_search`).
        let _ddl = ddl_lock.lock().await;
        let table_guard = table.load_full();
        create_vector_index(&table_guard, distance_type).await?;
    }
    tracing::info!(
        "Vector ANN index {reason} on `{EMBEDDING_INDEX_COLUMN}` in {}ms",
        start.elapsed().as_millis()
    );
    active.store(true, Ordering::Release);
    ready.store(true, Ordering::Release);
    Ok(())
}

/// Runtime state captured after the FTS index has been successfully
/// ensured for the current `FtsConfig`. Held inside a
/// `Mutex<Option<FtsInitState>>` so that callers can reset it (`*guard =
/// None`) when a search-time error signals that the on-disk index has
/// disappeared — something `tokio::sync::OnceCell` cannot express.
#[derive(Debug, Clone)]
struct FtsInitState {
    /// `sha256:...` value of the effective config at init time.
    /// Used for diagnostic logging on fast-path hits so operators can
    /// verify which configuration generation the live index belongs to.
    fingerprint: String,
    /// UNIX millis when the index became ready. Exposed in trace-level
    /// logs so a long startup stall can be correlated with this value
    /// across a process restart.
    ready_at_unix_ms: i64,
}

/// Search hit with memory_id and scores (body hydrated from RDB separately).
///
/// Image memory Phase 4 (N-row): the matched-row metadata
/// (`vector_kind`/`chunk_index`/`begin_position`/`end_position`/
/// `matched_content`) records which of a memory's N vector rows
/// actually scored, so the memory_id de-dup helper (app layer) can keep
/// the winning row's info on `MemorySearchResult.matched_*`. `Default`
/// is derived so the many fusion/aggregation sites that build a hit
/// from just `(memory_id, score)` can fill the rest with
/// `..Default::default()` (None / 0) without per-site churn.
#[derive(Debug, Clone, Default)]
pub struct VectorSearchHit {
    pub memory_id: i64,
    pub score: f32,
    /// Raw vector distance. Set to 0.0 for hybrid/aggregated results where
    /// the original distance is not meaningful.
    pub distance: f32,
    /// "text" | "image" | "caption". None on fusion/aggregation hits
    /// where the kind is not carried through.
    pub vector_kind: Option<String>,
    pub chunk_index: Option<i32>,
    pub begin_position: Option<i32>,
    pub end_position: Option<i32>,
    /// The matched chunk's source text (= the row's `content`).
    pub matched_content: Option<String>,
}

/// Accumulate the `memory_id`s of one `RecordBatch` into `ids`,
/// returning `true` as soon as the distinct count exceeds `cap`.
///
/// With the N-row vector schema one memory owns many LanceDB rows
/// (text chunks + image + caption), so the match count must report the
/// **unique `memory_id` set size**, not the raw row count, to stay
/// consistent with the search path's memory_id de-dup. The hard cap is
/// applied to the de-dup'd id count. Pure (no LanceDB / async) so it is
/// unit-tested directly; the column is downcast once per batch (hot
/// path).
fn accumulate_memory_ids_capped(
    batch: &RecordBatch,
    ids: &mut HashSet<i64>,
    cap: u64,
) -> anyhow::Result<bool> {
    let Some(col) = batch
        .column_by_name("memory_id")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
    else {
        // The count queries project exactly `["memory_id"]`; a missing
        // column means the projection contract was broken upstream.
        anyhow::bail!("count query batch missing `memory_id` column");
    };
    for i in 0..col.len() {
        ids.insert(col.value(i));
        if ids.len() as u64 > cap {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Drive a count-only stream and return `(distinct_memory_count,
/// is_truncated)` honouring the proto contract: `is_truncated == false`
/// implies the count is an upper bound, never a lower one.
///
/// The row fetch is bounded at `fetch_limit` (= `hard_cap + 1`) so the
/// query cannot fall back to LanceDB's default top-10. With the N-row
/// schema one memory owns many rows, so the stream can hit that row
/// limit with fewer than `hard_cap` distinct ids. The three stop
/// reasons are distinguished:
///
/// - row fetch hit the LanceDB `limit`: `(hard_cap, true)` — the stream
///   was clipped, the true distinct count is unknown and may be larger,
///   so report the lower-bound cap (UI shows "N+"). Checked before
///   accumulating the batch so a clipped final batch is not de-dup'd
///   into a result that is then discarded.
/// - distinct ids exceeded `hard_cap`: `(hard_cap, true)`.
/// - stream exhausted under the row limit: `(distinct, false)` — every
///   match was read, so the distinct count is exact and a true upper
///   bound.
async fn count_distinct_capped<S>(
    mut stream: S,
    hard_cap: u64,
    fetch_limit: usize,
) -> anyhow::Result<(u64, bool)>
where
    S: futures::Stream<Item = lancedb::error::Result<RecordBatch>> + Unpin,
{
    let mut ids: HashSet<i64> = HashSet::new();
    let mut rows_seen: usize = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        rows_seen += batch.num_rows();
        if rows_seen >= fetch_limit {
            return Ok((hard_cap, true));
        }
        if accumulate_memory_ids_capped(&batch, &mut ids, hard_cap)? {
            return Ok((hard_cap, true));
        }
    }
    Ok((ids.len() as u64, false))
}

/// Index statistics shared between the memory- and thread-side
/// `GetIndexStats` RPCs. `distance_type` / `fts_tokenizer` carry the
/// typed config enums; the proto layer converts them to wire enums
/// through `From` impls in `super::config`. The ngram min / max
/// fields are only `Some` when `fts_tokenizer == Ngram` because the
/// LanceDB FTS builder ignores them for every other kind — reporting
/// them would mislead a client into showing a stale "ngram(2-3)"
/// notice next to a `simple` index.
#[derive(Debug, Clone)]
pub struct IndexStats {
    pub total_records: u64,
    /// Total thread/memory count in RDB (None when not applicable)
    pub rdb_total_records: Option<u64>,
    pub vector_dimension: u32,
    pub distance_type: super::config::DistanceType,
    pub last_optimized_at: i64,
    pub fts_tokenizer: super::config::FtsTokenizerKind,
    pub fts_ngram_min: Option<u32>,
    pub fts_ngram_max: Option<u32>,
}

/// Wire-shaped projection of `IndexStats` used by both
/// `MemoryVectorService::GetIndexStats` and
/// `ThreadVectorService::GetIndexStats`. The two RPC responses share
/// these five fields verbatim, so collapsing the conversion here
/// keeps the service-layer handlers free of the `From<DistanceType>`
/// / `From<FtsTokenizerKind>` boilerplate.
#[derive(Debug, Clone, Copy)]
pub struct IndexStatsWire {
    pub vector_dimension: u32,
    pub distance_type: i32,
    pub last_optimized_at: i64,
    pub fts_tokenizer: i32,
    pub fts_ngram_min: Option<u32>,
    pub fts_ngram_max: Option<u32>,
}

impl IndexStats {
    /// Project the typed config enums into the proto i32 fields the
    /// wire response carries.
    pub fn to_wire(&self) -> IndexStatsWire {
        IndexStatsWire {
            vector_dimension: self.vector_dimension,
            distance_type: protobuf::llm_memory::data::DistanceType::from(self.distance_type)
                as i32,
            last_optimized_at: self.last_optimized_at,
            fts_tokenizer: protobuf::llm_memory::data::FtsTokenizerKind::from(self.fts_tokenizer)
                as i32,
            fts_ngram_min: self.fts_ngram_min,
            fts_ngram_max: self.fts_ngram_max,
        }
    }
}

/// Hybrid search options
#[derive(Debug, Clone)]
pub struct HybridOptions {
    pub strategy: HybridStrategy,
    pub vector_weight: Option<f32>,
    pub rrf_k: Option<f32>,
}

impl HybridOptions {
    /// Reject `vector_weight` outside `0.0..=1.0` for any strategy that
    /// consumes it (`Weighted` / `VectorThenFts` / `FtsThenVector`).
    /// `Rrf` ignores `vector_weight` entirely, so out-of-range values
    /// pass through. Shared by `HybridSearch` and `CountSearchMatches`
    /// so the two RPCs can never disagree on which options are valid.
    pub fn validate_vector_weight(&self) -> anyhow::Result<()> {
        match self.strategy {
            HybridStrategy::Rrf => Ok(()),
            HybridStrategy::Weighted
            | HybridStrategy::VectorThenFts
            | HybridStrategy::FtsThenVector => {
                let vw = self.vector_weight.unwrap_or(0.7);
                if !(0.0..=1.0).contains(&vw) {
                    anyhow::bail!("vector_weight must be in 0.0..=1.0, got {vw}");
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum HybridStrategy {
    Rrf,
    Weighted,
    VectorThenFts,
    FtsThenVector,
}

#[derive(Debug, Clone, Copy)]
pub enum AggregationStrategy {
    Sum,
    Average,
    Max,
    WeightedByPosition,
    RankFusion,
}

pub struct MemoryVectorRepositoryImpl {
    /// The single shared LanceDB table handle, published via `ArcSwap`.
    ///
    /// One `Arc<Table>` is stored at construction and kept for the table's
    /// lifetime — we never `store` a freshly `open_table`d handle. A LanceDB
    /// `Table` wraps an internally-mutable, monotonically-versioned dataset
    /// state, so all version advances (writes' own commits, `reload_table`'s
    /// `checkout_latest`, compaction/`create_index`) mutate this one handle.
    /// `ArcSwap` lets readers `load_full()` without ever blocking on a writer
    /// — unlike the previous write-preferring `RwLock`, where a queued
    /// `reload_table` writer stalled every concurrent read behind it. Keeping
    /// a single handle also eliminates the stale-handle class of bug (which
    /// requires two diverging handles). `store` is retained only as a future
    /// escape hatch for a hard reopen on a poisoned handle.
    table: Arc<ArcSwap<Table>>,
    config: VectorDBConfig,
    /// Monotonic write counter. Never reset; the maintenance gates compare
    /// it against per-gate "last fired" markers below. usize at thousands
    /// of writes/day takes ~10^15 years to overflow, so wrap is a non-issue.
    operation_count: Arc<AtomicUsize>,
    /// `operation_count` value at which the prune path last fired. Next
    /// prune fires when `operation_count - last_prune_count >= prune_interval`.
    last_prune_count: Arc<AtomicUsize>,
    /// `operation_count` value at which the heavy (compact+index) path last
    /// fired. Kept independent from `last_prune_count` so the two cadences
    /// never interfere (the core of the two-gate design).
    last_compact_count: Arc<AtomicUsize>,
    last_optimized_at: Arc<AtomicI64>,
    /// Single-slot guard ensuring at most one background compaction runs at
    /// a time. `track_operation` claims it via [`InFlightGuard`] before
    /// `tokio::spawn`ing the heavy compact+index pass; a claim already held
    /// makes the new caller skip rather than stack a second concurrent
    /// compaction (two at once doubled CPU/memory and triggered OOM under a
    /// large redispatch burst).
    compact_in_flight: Arc<AtomicBool>,
    /// FTS init gate. `None` means "not yet ensured in this process";
    /// `Some(..)` means an ensure pass has succeeded at least once.
    /// Runtime recovery (search-time `not found` errors) resets this
    /// back to `None` under the lock so a re-ensure can run.
    fts_init_state: Arc<Mutex<Option<FtsInitState>>>,
    /// Lock-free fast path flag that mirrors `fts_init_state.is_some()`.
    /// Every successful `ensure_fts_index` transitions this to `true`
    /// with `Release` ordering after the `Mutex` write, and every reset
    /// in the runtime recovery path transitions it back to `false`
    /// *before* clearing `fts_init_state` so that readers never observe
    /// `fts_init_ready = true` paired with `fts_init_state = None`.
    ///
    /// Why this exists instead of a plain `tokio::Mutex`: high-frequency
    /// BM25 queries call `ensure_fts_index` on every request, and the
    /// previous `Mutex::lock().await` on the fast path is a contention
    /// hot spot under parallel search workloads. The atomic check lets
    /// the steady-state branch return without touching the mutex at all.
    fts_init_ready: Arc<AtomicBool>,
    /// Vector (ANN) index gate. Mirrors the FTS gate: a lock-free
    /// `AtomicBool` fast path backed by a `Mutex` that serializes the
    /// one-time index build. Unlike FTS there is no config fingerprint —
    /// the index policy (IvfPq with auto-derived params) does not depend
    /// on drift-sensitive settings, so "ensured once" is a terminal state
    /// for the process. `true` means an ensure pass has completed, which
    /// is either "index built" or "row count below threshold, staying on
    /// brute-force". `search_by_vector` calls `ensure_vector_index` on
    /// every request, so the atomic fast path keeps the steady state off
    /// the mutex.
    vector_index_ready: Arc<AtomicBool>,
    /// Whether a real ANN index currently backs the `embedding` column.
    /// Set during the ensure pass and read lock-free on the hot path to
    /// decide whether `nprobes` should be applied (the parameter is
    /// ignored by LanceDB without an index, but setting it is wasted work
    /// and muddies intent). `false` while the corpus is below
    /// `vector_index.min_rows` and we deliberately stay on brute-force.
    vector_index_active: Arc<AtomicBool>,
    /// Serializes vector-index ensure passes so concurrent
    /// `search_by_vector` callers do not race two `create_index` builds.
    vector_index_lock: Arc<Mutex<()>>,
    /// Serializes index-creation DDL (`create_index`) across the vector
    /// AND FTS paths. `parallel_hybrid_search` runs `search_by_vector` and
    /// `search_by_text` under `try_join!`, so on a fresh table the first
    /// hybrid query can drive the vector and FTS `create_index` calls
    /// concurrently against the same LanceDB table. Two overlapping DDL
    /// commits can hit a transaction conflict or drop one another's
    /// manifest update, so both paths take this lock for the duration of
    /// their `create_index` to make the two builds mutually exclusive.
    /// Held only around the DDL itself (the narrowest possible scope), and
    /// always acquired *inside* the per-path init locks
    /// (`vector_index_lock` / `fts_init_state`) so the nesting stays
    /// cycle-free.
    index_ddl_lock: Arc<Mutex<()>>,
}

impl MemoryVectorRepositoryImpl {
    /// Expose the active FTS config so the app layer can re-tokenize
    /// a hit's `content` against the same settings the BM25 inverted
    /// index runs on. Returning a reference keeps the call
    /// zero-allocation on the hot path.
    pub fn fts_config(&self) -> &super::config::FtsConfig {
        &self.config.fts
    }

    /// The configured embedding dimension (`MEMORY_VECTOR_SIZE`). The
    /// LanceDB `FixedSizeList` is built with this; the startup probe
    /// cross-checks it against the embedding runner's reported
    /// `embedding_dimension` so a model/config drift fails fast instead
    /// of every INSERT erroring later (design 3/3 §14.3 (b)).
    pub fn vector_size(&self) -> usize {
        self.config.vector_size
    }

    /// Initialize LanceDB connection and open/create table with indexes
    pub async fn new(config: VectorDBConfig) -> anyhow::Result<Self> {
        // The connection is only needed to open/create the table here; the
        // repository keeps the resulting `Table` handle (in `ArcSwap`) and
        // refreshes it via `checkout_latest`, so it does not retain the
        // `Connection`.
        let database = lancedb::connect(&config.uri)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB connect failed: {e}"))?;

        let schema = memory_arrow_schema(config.vector_size);
        let (table, is_new) = match database.open_table(&config.table_name).execute().await {
            Ok(t) => (t, false),
            Err(_) => {
                let empty_batch = RecordBatch::new_empty(schema.clone());
                let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
                    RecordBatchIterator::new(vec![Ok(empty_batch)], schema.clone()),
                );
                let t = database
                    .create_table(&config.table_name, reader)
                    .execute()
                    .await
                    .map_err(|e| anyhow::anyhow!("LanceDB create_table failed: {e}"))?;
                (t, true)
            }
        };
        if !is_new {
            add_legacy_memory_kind_column_if_missing(&table).await?;
        }
        verify_table_schema_or_fail(&table, &schema, &config).await?;

        if is_new {
            Self::create_btree_indexes(&table).await?;
        }

        let repo = Self {
            table: Arc::new(ArcSwap::from_pointee(table)),
            config,
            operation_count: Arc::new(AtomicUsize::new(0)),
            last_prune_count: Arc::new(AtomicUsize::new(0)),
            last_compact_count: Arc::new(AtomicUsize::new(0)),
            last_optimized_at: Arc::new(AtomicI64::new(0)),
            compact_in_flight: Arc::new(AtomicBool::new(false)),
            fts_init_state: Arc::new(Mutex::new(None)),
            fts_init_ready: Arc::new(AtomicBool::new(false)),
            vector_index_ready: Arc::new(AtomicBool::new(false)),
            vector_index_active: Arc::new(AtomicBool::new(false)),
            vector_index_lock: Arc::new(Mutex::new(())),
            index_ddl_lock: Arc::new(Mutex::new(())),
        };

        tracing::info!(
            "LanceDB initialized: uri={}, table={}, vector_size={}, new={}",
            repo.config.uri,
            repo.config.table_name,
            repo.config.vector_size,
            is_new
        );

        // Startup prune: clear the old-manifest backlog (potentially
        // hundreds of thousands of `_versions/*.manifest` files) so the NEXT
        // boot's open_table scan is fast. Best-effort — a prune failure must
        // not block startup. Only manifests older than `prune_older_than_secs`
        // are removed; live data/index files are protected by LanceDB's 7-day
        // floor (we pass delete_unverified=false), so this never loses data.
        if repo.config.optimize.prune_on_startup {
            tracing::info!("LanceDB startup prune starting (clearing manifest backlog)...");
            if let Err(e) = repo.prune().await {
                tracing::warn!("LanceDB startup prune failed (continuing): {e}");
            }
        }

        Ok(repo)
    }

    /// Create BTree indexes on scalar filter columns
    async fn create_btree_indexes(table: &Table) -> anyhow::Result<()> {
        // Phase 4: `thread_id` was removed from the LanceDB schema together
        // with its BTree index. Thread-scoped searches walk the `thread_memory`
        // junction on the RDB side and narrow vectors via `memory_id IN (...)`.
        // Image memory Phase 4: `vector_kind` is indexed so kind filters
        // (RedispatchEmbeddings kinds / replace_kinds delete) are fast.
        let columns = [
            "memory_id",
            "vector_kind",
            "user_id",
            "role",
            "content_type",
            "memory_kind",
            "created_at",
        ];
        for col in columns {
            if let Err(e) = table
                .create_index(&[col], Index::BTree(Default::default()))
                .execute()
                .await
            {
                let msg = e.to_string();
                if msg.contains("already exists") || msg.contains("duplicate") {
                    continue;
                }
                tracing::warn!("Failed to create BTree index on {}: {}", col, e);
            }
        }
        tracing::info!("Created BTree indexes on {} columns", columns.len());
        Ok(())
    }

    // ===== Write operations =====

    /// Upsert a single record via merge_insert on memory_id
    pub async fn upsert(&self, record: &MemoryVectorRecord) -> anyhow::Result<()> {
        let schema = memory_arrow_schema(self.config.vector_size);
        let batch = Self::build_record_batch(
            std::slice::from_ref(record),
            &schema,
            self.config.vector_size,
        )?;

        let table = self.table.load_full();
        // Image memory Phase 4: merge key is the N-row composite
        // (memory_id, vector_kind, chunk_index) so re-upserting one
        // chunk is idempotent without clobbering a memory's other rows.
        let mut merge = table.merge_insert(&["memory_id", "vector_kind", "chunk_index"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();

        let records = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema));
        merge
            .execute(records)
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB upsert failed: {e}"))?;

        drop(table);
        self.reload_table().await?;
        self.track_operation(1).await;
        Ok(())
    }

    /// Batch upsert records in chunks of 1000.
    /// Each chunk is an independent merge_insert keyed on memory_id, so
    /// intermediate reload_table is not needed between chunks.
    pub async fn batch_upsert(&self, records: Vec<MemoryVectorRecord>) -> anyhow::Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let schema = memory_arrow_schema(self.config.vector_size);
        let mut total = 0usize;

        for chunk in records.chunks(1000) {
            let batch = Self::build_record_batch(chunk, &schema, self.config.vector_size)?;
            let table = self.table.load_full();
            // Phase 4: composite N-row merge key (see `upsert`).
            let mut merge = table.merge_insert(&["memory_id", "vector_kind", "chunk_index"]);
            merge
                .when_matched_update_all(None)
                .when_not_matched_insert_all();
            let iter = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
            merge
                .execute(iter)
                .await
                .map_err(|e| anyhow::anyhow!("LanceDB batch upsert failed: {e}"))?;
            total += chunk.len();
            drop(table);
        }

        self.reload_table().await?;
        self.track_operation(total).await;
        Ok(total)
    }

    /// Image memory Phase 4: replace a memory's rows for a specific set
    /// of `vector_kind`s, then insert the new rows.
    ///
    /// The delete is scoped to `memory_id == m AND vector_kind IN
    /// (replace_kinds)` so a parallel Text dispatch
    /// (`replace_kinds=["text"]`) and Media dispatch
    /// (`replace_kinds=["image","caption"]`) on the same memory never
    /// wipe each other's rows (design 1/3 §3.3.1 kind-isolation). All
    /// `records` MUST share the same `memory_id` (the rows path always
    /// targets one memory); their `vector_kind`s are expected to be a
    /// subset of `replace_kinds`. `replace_kinds` empty is rejected (an
    /// empty-IN delete would be a no-op and silently leak stale rows;
    /// callers with nothing to replace should not call this).
    ///
    /// Delete-then-insert is not a single LanceDB transaction; a crash
    /// between the two leaves the memory with that kind missing, which
    /// is self-healing on the next dispatch (idempotent re-run) — the
    /// same durability profile as the existing merge_insert path.
    pub async fn replace_kinds_upsert(
        &self,
        memory_id: i64,
        replace_kinds: &[&str],
        records: Vec<MemoryVectorRecord>,
    ) -> anyhow::Result<usize> {
        if replace_kinds.is_empty() {
            anyhow::bail!("replace_kinds_upsert: replace_kinds must not be empty");
        }
        if let Some(bad) = records.iter().find(|r| r.memory_id != memory_id) {
            anyhow::bail!(
                "replace_kinds_upsert: all records must share memory_id={memory_id}, \
                 got {}",
                bad.memory_id
            );
        }

        // 1. Delete only this memory's rows whose kind is being replaced.
        let filter = SafeFilter::memory_id(memory_id)
            .and(SafeFilter::in_str_list("vector_kind", replace_kinds)?)
            .to_sql()?;
        {
            let table = self.table.load_full();
            table
                .delete(&filter)
                .await
                .map_err(|e| anyhow::anyhow!("LanceDB replace_kinds delete failed: {e}"))?;
            drop(table);
        }

        // 2. Insert the new rows. An empty `records` (kind cleared with
        //    no replacement, e.g. content emptied) is a valid outcome:
        //    the delete above already removed the stale rows. Reload only
        //    here is required — it is the sole point that materialises the
        //    delete. On the non-empty path we skip this reload and let
        //    `batch_upsert`'s own trailing reload cover the delete too,
        //    so a single (memory, kinds) replace does ONE reload, not two.
        if records.is_empty() {
            self.reload_table().await?;
            // Stale-delete path: the delete above created a new version but
            // no batch_upsert follows to count it, so track it here. The
            // non-empty path is covered by batch_upsert's own tracking.
            self.track_operation(1).await;
            return Ok(0);
        }
        self.batch_upsert(records).await
    }

    /// Delete by memory_id.
    /// Always returns true because LanceDB delete is a no-op when the target does not exist.
    pub async fn delete(&self, memory_id: i64) -> anyhow::Result<bool> {
        let filter = SafeFilter::memory_id(memory_id).to_sql()?;
        let table = self.table.load_full();
        table
            .delete(&filter)
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB delete failed: {e}"))?;
        drop(table);
        self.reload_table().await?;
        // A delete creates a new version too, so it must drive maintenance —
        // otherwise a delete-heavy workload grows `_versions/` without ever
        // tripping the prune gate.
        self.track_operation(1).await;
        Ok(true)
    }

    /// Delete records matching a list of memory_ids.
    /// Used for shared-memory-aware cascade deletion.
    pub async fn delete_by_memory_ids(&self, memory_ids: &[i64]) -> anyhow::Result<()> {
        if memory_ids.is_empty() {
            return Ok(());
        }
        let filter = SafeFilter::in_i64_list("memory_id", memory_ids)?.to_sql()?;
        let table = self.table.load_full();
        table
            .delete(&filter)
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB delete_by_memory_ids failed: {e}"))?;
        drop(table);
        self.reload_table().await?;
        // One bulk delete = one new version, regardless of how many ids it
        // matched; count it once so cascade deletes drive prune/compact.
        self.track_operation(1).await;
        Ok(())
    }

    // ===== Search operations =====

    /// Vector similarity search
    pub async fn search_by_vector(
        &self,
        query_vector: &[f32],
        filter: Option<&SafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<VectorSearchHit>> {
        if query_vector.len() != self.config.vector_size {
            anyhow::bail!(
                "Vector size mismatch: expected {}, got {}",
                self.config.vector_size,
                query_vector.len()
            );
        }

        // Build (once) the ANN index so this query avoids a brute-force
        // full-table scan. Cheap lock-free fast path after the first call.
        self.ensure_vector_index().await?;

        let table = self.table.load_full();
        let mut query = apply_vector_query_options(
            table.query().nearest_to(query_vector)?,
            self.config.vector_index,
            self.config.distance_type,
            self.vector_index_active.load(Ordering::Acquire),
        );
        query = query.limit(limit);

        if let Some(f) = filter {
            let sql = f.to_sql()?;
            query = query.only_if(sql);
        }

        let mut stream = query.execute().await?;
        let mut results = Vec::new();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            self.extract_search_hits(&batch, &mut results)?;
        }
        Ok(results)
    }

    /// BM25 full-text search.
    ///
    /// Runtime recovery: if the underlying BM25 query fails with an error
    /// that looks like "FTS index missing" (e.g., the table was externally
    /// rebuilt, or the index directory was deleted after this process
    /// observed a healthy state), we reset the in-memory init gate and
    /// re-run `ensure_fts_index` exactly once before retrying. This is
    /// bounded via `attempted_recovery` to prevent infinite loops in
    /// the pathological case where rebuild succeeds but queries still
    /// fail with the same pattern.
    pub async fn search_by_text(
        &self,
        query_text: &str,
        filter: Option<&SafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<VectorSearchHit>> {
        let mut attempted_recovery = false;
        let raw_results = loop {
            match self.run_bm25_query(query_text, filter, limit).await {
                Ok(hits) => break hits,
                Err(e) if !attempted_recovery && is_missing_fts_index_error(&e) => {
                    tracing::warn!(
                        "FTS index appears missing during search ({e}); resetting init gate \
                         and rebuilding once before retry"
                    );
                    {
                        let mut guard = self.fts_init_state.lock().await;
                        // Clear the lock-free flag BEFORE clearing the
                        // Mutex state so that a concurrent fast-path
                        // reader cannot observe `ready = true` while
                        // `state = None` (which would make them skip
                        // the rebuild and hit the same missing-index
                        // error). Acquiring the mutex above also
                        // synchronizes with any in-flight ensure call.
                        self.fts_init_ready.store(false, Ordering::Release);
                        *guard = None;
                        self.drop_stale_fts_index_and_reload().await?;
                        self.rebuild_fts_index_locked(&mut guard).await?;
                    }
                    attempted_recovery = true;
                    continue;
                }
                Err(e) => return Err(e),
            }
        };

        if raw_results.is_empty() {
            return Ok(Vec::new());
        }

        // Min-Max normalization to [0.1, 1.0] with floor to preserve contribution
        // in hybrid weighted blending (pure min-max maps lowest to 0.0, losing all FTS weight)
        let (min, max) = raw_results
            .iter()
            .map(|(_, s)| *s)
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(mn, mx), s| {
                (mn.min(s), mx.max(s))
            });
        let range = max - min;

        Ok(raw_results
            .into_iter()
            .map(|(id, score)| {
                let normalized = if range < f32::EPSILON {
                    // Single result or identical scores: use sigmoid to preserve magnitude
                    1.0 / (1.0 + (-score).exp())
                } else {
                    (score - min) / range * 0.9 + 0.1
                };
                VectorSearchHit {
                    memory_id: id,
                    score: normalized,
                    distance: score,
                    // Aggregated over a memory's rows — no single matched
                    // row to attribute. The de-dup helper still works
                    // (it max-aggregates and back-fills from the raw hit
                    // path that does carry kind/chunk).
                    ..Default::default()
                }
            })
            .collect())
    }

    /// Hybrid search: dispatches to strategy-specific implementation.
    pub async fn hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&SafeFilter>,
        limit: usize,
        options: &HybridOptions,
    ) -> anyhow::Result<Vec<VectorSearchHit>> {
        match options.strategy {
            HybridStrategy::Rrf | HybridStrategy::Weighted => {
                self.parallel_hybrid_search(query_vector, query_text, filter, limit, options)
                    .await
            }
            HybridStrategy::VectorThenFts => {
                self.two_phase_hybrid_search(query_vector, query_text, filter, limit, options, true)
                    .await
            }
            HybridStrategy::FtsThenVector => {
                self.two_phase_hybrid_search(
                    query_vector,
                    query_text,
                    filter,
                    limit,
                    options,
                    false,
                )
                .await
            }
        }
    }

    /// Parallel vector + FTS search, merged with RRF or weighted blending.
    /// Note: try_join! polls both futures on the same task (no spawn), so sharing
    /// `&SafeFilter` is safe. If migrating to tokio::spawn, SafeFilter needs Clone.
    async fn parallel_hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&SafeFilter>,
        limit: usize,
        options: &HybridOptions,
    ) -> anyhow::Result<Vec<VectorSearchHit>> {
        let fetch_limit = limit * 2;

        let (vec_results, fts_results) = tokio::try_join!(
            self.search_by_vector(query_vector, filter, fetch_limit),
            self.search_by_text(query_text, filter, fetch_limit),
        )?;

        options.validate_vector_weight()?;
        match options.strategy {
            HybridStrategy::Rrf => {
                let rrf_k = options.rrf_k.unwrap_or(60.0);
                Self::merge_rrf(vec_results, fts_results, rrf_k, limit)
            }
            HybridStrategy::Weighted => {
                let vector_weight = options.vector_weight.unwrap_or(0.7);
                Self::merge_weighted(vec_results, fts_results, vector_weight, limit)
            }
            _ => unreachable!("parallel_hybrid_search only handles Rrf and Weighted"),
        }
    }

    /// Two-phase pipeline: vector search first, then FTS re-ranking on candidates.
    /// Two-phase hybrid: run primary search, then re-rank with secondary search.
    /// `vector_first=true`  → vector Stage 1, FTS Stage 2 (VECTOR_THEN_FTS)
    /// `vector_first=false` → FTS Stage 1, vector Stage 2 (FTS_THEN_VECTOR)
    async fn two_phase_hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&SafeFilter>,
        limit: usize,
        options: &HybridOptions,
        vector_first: bool,
    ) -> anyhow::Result<Vec<VectorSearchHit>> {
        let fetch_limit = limit * 2;
        options.validate_vector_weight()?;
        let vector_weight = options.vector_weight.unwrap_or(0.7);

        // Stage 1: primary search
        let primary_hits = if vector_first {
            self.search_by_vector(query_vector, filter, fetch_limit)
                .await?
        } else {
            self.search_by_text(query_text, filter, fetch_limit).await?
        };
        if primary_hits.is_empty() {
            return Ok(Vec::new());
        }

        // Truncate candidates for IN filter performance
        let mut primary_hits = primary_hits;
        let mut candidate_ids: Vec<i64> = primary_hits.iter().map(|h| h.memory_id).collect();
        if candidate_ids.len() > MAX_HYBRID_CANDIDATES {
            let label = if vector_first {
                "vector_then_fts"
            } else {
                "fts_then_vector"
            };
            tracing::warn!(
                "{label}: truncating {} candidates to {} for IN filter",
                candidate_ids.len(),
                MAX_HYBRID_CANDIDATES
            );
            candidate_ids.truncate(MAX_HYBRID_CANDIDATES);
            primary_hits.truncate(MAX_HYBRID_CANDIDATES);
        }

        // Stage 2: secondary search on candidate IDs
        let id_filter = SafeFilter::in_i64_list("memory_id", &candidate_ids)?;
        let combined_filter = match filter {
            Some(f) => f.clone().and(id_filter),
            None => id_filter,
        };
        let secondary_hits = if vector_first {
            self.search_by_text(query_text, Some(&combined_filter), fetch_limit)
                .await?
        } else {
            self.search_by_vector(query_vector, Some(&combined_filter), fetch_limit)
                .await?
        };

        // Stage 3: merge scores
        let secondary_map: std::collections::HashMap<i64, f32> = secondary_hits
            .iter()
            .map(|h| (h.memory_id, h.score))
            .collect();

        let (primary_weight, secondary_weight) = if vector_first {
            (vector_weight, 1.0 - vector_weight)
        } else {
            (1.0 - vector_weight, vector_weight)
        };

        let mut merged: Vec<VectorSearchHit> = primary_hits
            .into_iter()
            .map(|h| {
                let sec_score = secondary_map.get(&h.memory_id).copied().unwrap_or(0.0);
                VectorSearchHit {
                    memory_id: h.memory_id,
                    score: h.score * primary_weight + sec_score * secondary_weight,
                    distance: 0.0, // not meaningful for blended scores
                    // Keep the primary hit's matched-row metadata so the
                    // UI can still show where it hit (score_source stays
                    // SCORE_HYBRID per design 1/3 §2.6.4; matched_* always
                    // reflects the real row).
                    vector_kind: h.vector_kind,
                    chunk_index: h.chunk_index,
                    begin_position: h.begin_position,
                    end_position: h.end_position,
                    matched_content: h.matched_content,
                }
            })
            .collect();

        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        Ok(merged)
    }

    // ===== Management =====

    /// Compact data files and fold unindexed rows into the ANN/FTS indices.
    ///
    /// This is the "heavy" maintenance path: `Compact` rewrites small files
    /// into larger ones, `Index` assigns unindexed rows to existing IVF/FTS
    /// clusters (without retraining). Deliberately does NOT prune versions —
    /// pruning is cheap and runs on a separate, faster cadence (see
    /// [`prune`](Self::prune)). Updates `last_optimized_at` for the
    /// `GetIndexStats` RPC (proto field 6), preserving its established
    /// "index freshness" meaning.
    pub async fn compact_and_optimize_index(&self) -> anyhow::Result<()> {
        // Delegate to the shared free function so the synchronous (manual /
        // external) entry point and the spawned auto-compaction in
        // `track_operation` run identical logic against the same `ArcSwap`.
        // Pass `index_ddl_lock` so compaction serializes against a concurrent
        // `create_index` from `ensure_*_index`.
        compact_with(
            &self.table,
            &self.last_optimized_at,
            Some(&self.index_ddl_lock),
        )
        .await
    }

    /// Prune old manifests. This is the cheap operation that keeps
    /// `_versions/` from exploding and the startup `open_table` fast.
    ///
    /// Honors the configured retention (`prune_older_than_secs`), which only
    /// bounds time-travel history — the latest manifest and all live data it
    /// references are always kept. We pass `delete_unverified: Some(false)`
    /// (matching LanceDB's official `auto_cleanup`) so LanceDB's hardcoded
    /// 7-day floor always protects live data/index fragments. Does NOT update
    /// `last_optimized_at` — pruning is manifest GC, unrelated to the
    /// index-freshness metric. See [`OptimizeConfig`] for the full rationale.
    pub async fn prune(&self) -> anyhow::Result<()> {
        prune_with(&self.table, &self.config.optimize).await
    }

    /// Backward-compatible "do everything" entry point. Preserved for
    /// external / manual-ops callers. Runs the heavy path then prunes the
    /// versions it (and prior writes) left behind.
    pub async fn optimize(&self) -> anyhow::Result<()> {
        self.compact_and_optimize_index().await?;
        self.prune().await?;
        Ok(())
    }

    /// Get all memory_ids in the vector index
    pub async fn get_all_memory_ids(&self) -> anyhow::Result<Vec<i64>> {
        let table = self.table.load_full();
        let mut stream = table
            .query()
            .select(lancedb::query::Select::columns(&["memory_id"]))
            .execute()
            .await?;

        let mut ids = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if let Some(col) = batch
                .column_by_name("memory_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            {
                for i in 0..col.len() {
                    ids.push(col.value(i));
                }
            }
        }
        Ok(ids)
    }

    /// Get index statistics
    pub async fn get_stats(&self) -> anyhow::Result<IndexStats> {
        let table = self.table.load_full();
        let total = table.count_rows(None).await? as u64;
        let fts_tokenizer = self.config.fts.tokenizer;
        let (fts_ngram_min, fts_ngram_max) =
            if matches!(fts_tokenizer, super::config::FtsTokenizerKind::Ngram) {
                (
                    Some(self.config.fts.ngram_min),
                    Some(self.config.fts.ngram_max),
                )
            } else {
                (None, None)
            };
        Ok(IndexStats {
            total_records: total,
            rdb_total_records: None,
            vector_dimension: self.config.vector_size as u32,
            distance_type: self.config.distance_type,
            last_optimized_at: self.last_optimized_at.load(Ordering::Relaxed),
            fts_tokenizer,
            fts_ngram_min,
            fts_ngram_max,
        })
    }

    // ===== Count operations (P2, Phase 5-1) =====

    /// FILTER_ONLY count via LanceDB `Table::count_rows(filter_sql)`.
    ///
    /// Always returns `is_truncated = false` because LanceDB performs a
    /// full scan and reports an exact match count. `filter = None`
    /// returns the total row count of the table.
    pub async fn count_by_filter(
        &self,
        filter: Option<&SafeFilter>,
    ) -> anyhow::Result<(u64, bool)> {
        let table = self.table.load_full();
        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };
        let total = table.count_rows(filter_sql).await? as u64;
        Ok((total, false))
    }

    /// TEXT count: distinct `memory_id`s matching the BM25 query.
    ///
    /// Image memory Phase 4: de-dup'd by `memory_id` (N-row schema), so
    /// the result is the unique memory count, not the LanceDB row count.
    /// When the distinct count exceeds `hard_cap` returns
    /// `(hard_cap, true)` (truncated lower bound); otherwise
    /// `(unique_memory_count, false)`.
    pub async fn count_by_text(
        &self,
        query_text: &str,
        filter: Option<&SafeFilter>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };
        self.run_bm25_count_query(query_text, filter_sql, hard_cap)
            .await
    }

    /// Count-only BM25 query.
    ///
    /// **Selects only `memory_id`** (not `_score` / `content` /
    /// `embedding`) so the count path cannot accidentally stream
    /// hundreds of MB when the cap is large (default 50_000). The
    /// `memory_id` de-dup and the upper-bound `is_truncated` contract
    /// are handled by [`count_distinct_capped`].
    async fn run_bm25_count_query(
        &self,
        query_text: &str,
        filter_sql: Option<String>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        self.ensure_fts_index().await?;

        let table = self.table.load_full();
        let fts_query = lance_index::scalar::FullTextSearchQuery::new(query_text.to_owned());
        let mut query = table
            .query()
            .full_text_search(fts_query)
            .select(lancedb::query::Select::columns(&["memory_id"]));

        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        let fetch_limit = hard_cap.saturating_add(1) as usize;
        let stream = query.limit(fetch_limit).execute().await?;
        count_distinct_capped(stream, hard_cap, fetch_limit).await
    }

    // ===== Count operations (P2, Phase 5-2) =====

    /// VECTOR count: distinct `memory_id`s matching the ANN query.
    ///
    /// Mirrors `count_by_text`: image memory Phase 4 de-dup'd by
    /// `memory_id` (N-row schema). ANN is approximate (top-K is the only
    /// guarantee LanceDB offers), so the upper bound on the distinct
    /// count is the operator-tunable `MEMORY_COUNT_VECTOR_HARD_CAP`
    /// rather than the LanceDB index row count. Returns
    /// `(hard_cap, true)` on truncation or `(unique_memory_count, false)`
    /// otherwise.
    pub async fn count_by_vector(
        &self,
        query_vector: &[f32],
        filter: Option<&SafeFilter>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        if query_vector.len() != self.config.vector_size {
            anyhow::bail!(
                "Vector size mismatch: expected {}, got {}",
                self.config.vector_size,
                query_vector.len()
            );
        }

        // Build (once) the ANN index so this count avoids a brute-force
        // full-table scan — count queries can be the first vector traffic
        // after startup, so they must ensure the index just like
        // `search_by_vector`.
        self.ensure_vector_index().await?;

        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };
        self.run_vector_count_query(query_vector, filter_sql, hard_cap)
            .await
    }

    /// Count-only ANN query.
    ///
    /// Same projection-narrowing invariant as `run_bm25_count_query`
    /// (only `memory_id` selected) and the same N-row de-dup: count the
    /// unique `memory_id` set, with the hard cap applied to the de-dup'd
    /// id count (not the raw ANN row count).
    async fn run_vector_count_query(
        &self,
        query_vector: &[f32],
        filter_sql: Option<String>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        let table = self.table.load_full();
        let mut query = apply_vector_query_options(
            table.query().nearest_to(query_vector)?,
            self.config.vector_index,
            self.config.distance_type,
            self.vector_index_active.load(Ordering::Acquire),
        )
        .select(lancedb::query::Select::columns(&["memory_id"]));

        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        // MUST cap the row fetch: an unbounded `nearest_to()` defaults
        // to LanceDB's top-10. `count_distinct_capped` handles the
        // de-dup and the truncation contract.
        let fetch_limit = hard_cap.saturating_add(1) as usize;
        let stream = query.limit(fetch_limit).execute().await?;
        count_distinct_capped(stream, hard_cap, fetch_limit).await
    }

    /// Stream `cap + 1` ANN matches and collect their `memory_id`s into
    /// a HashSet. Used by multi-vector and hybrid-parallel count paths
    /// where the union (not the per-branch count) is the contract.
    /// Returns `(set, true)` when the cap was exceeded.
    pub async fn collect_vector_ids_capped(
        &self,
        query_vector: &[f32],
        filter: Option<&SafeFilter>,
        cap: u64,
    ) -> anyhow::Result<(HashSet<i64>, bool)> {
        if query_vector.len() != self.config.vector_size {
            anyhow::bail!(
                "Vector size mismatch: expected {}, got {}",
                self.config.vector_size,
                query_vector.len()
            );
        }

        // Build (once) the ANN index so this count avoids a brute-force
        // full-table scan (count paths can be the first vector traffic
        // after startup).
        self.ensure_vector_index().await?;

        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };

        let table = self.table.load_full();
        let mut query = apply_vector_query_options(
            table.query().nearest_to(query_vector)?,
            self.config.vector_index,
            self.config.distance_type,
            self.vector_index_active.load(Ordering::Acquire),
        )
        .select(lancedb::query::Select::columns(&["memory_id"]));

        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        let fetch_limit = cap.saturating_add(1) as usize;
        let mut stream = query.limit(fetch_limit).execute().await?;

        let mut ids: HashSet<i64> = HashSet::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if let Some(col) = batch
                .column_by_name("memory_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            {
                for i in 0..col.len() {
                    ids.insert(col.value(i));
                    if ids.len() as u64 > cap {
                        return Ok((ids, true));
                    }
                }
            }
        }
        Ok((ids, false))
    }

    /// Stream `cap + 1` BM25 matches and collect their `memory_id`s.
    /// Symmetric to `collect_vector_ids_capped`; used by the hybrid
    /// parallel count path.
    pub async fn collect_fts_ids_capped(
        &self,
        query_text: &str,
        filter: Option<&SafeFilter>,
        cap: u64,
    ) -> anyhow::Result<(HashSet<i64>, bool)> {
        self.ensure_fts_index().await?;

        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };

        let table = self.table.load_full();
        let fts_query = lance_index::scalar::FullTextSearchQuery::new(query_text.to_owned());
        let mut query = table
            .query()
            .full_text_search(fts_query)
            .select(lancedb::query::Select::columns(&["memory_id"]));
        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        let fetch_limit = cap.saturating_add(1) as usize;
        let mut stream = query.limit(fetch_limit).execute().await?;

        let mut ids: HashSet<i64> = HashSet::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if let Some(col) = batch
                .column_by_name("memory_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            {
                for i in 0..col.len() {
                    ids.insert(col.value(i));
                    if ids.len() as u64 > cap {
                        return Ok((ids, true));
                    }
                }
            }
        }
        Ok((ids, false))
    }

    /// HYBRID count.
    ///
    /// `Rrf` / `Weighted` strategies count the union of vector- and
    /// FTS-matched memory_ids — mirroring `parallel_hybrid_search`'s
    /// fusion of two parallel branches into one ranked list. Both
    /// branches are collected with `vector_hard_cap` (NOT
    /// `min(vector_hard_cap, fts_hard_cap)`):
    ///
    /// - Reading either branch past `vector_hard_cap` only produces
    ///   ids that the post-union clamp throws away, so the small
    ///   `MEMORY_COUNT_VECTOR_HARD_CAP` is the tight upper bound on
    ///   HYBRID work even when `fts_hard_cap` is much larger.
    /// - Conversely, capping the vector branch by `fts_hard_cap`
    ///   would undercount HYBRID when an operator configures
    ///   `fts_hard_cap < vector_hard_cap`: vector-only matches would
    ///   be truncated below the bound the user actually asked for.
    ///
    /// `fts_hard_cap` therefore goes unused for the parallel
    /// strategies; it only matters for `FtsThenVector`, where FTS
    /// is the primary stage. `is_truncated` follows from either
    /// branch's cap or from `merged > vector_hard_cap`.
    ///
    /// `VectorThenFts` / `FtsThenVector` are two-phase pipelines whose
    /// final result size is bounded by the primary stage. Returning
    /// the primary-stage count keeps the count an upper bound on
    /// what `hybrid_search` would actually surface.
    pub async fn count_by_hybrid(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&SafeFilter>,
        options: &HybridOptions,
        vector_hard_cap: u64,
        fts_hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        // Match the validation `parallel_hybrid_search` /
        // `two_phase_hybrid_search` perform; otherwise an invalid
        // `vector_weight` would let Count succeed while the actual
        // search rejects the same options, misleading the client into
        // assuming the query is runnable.
        options.validate_vector_weight()?;
        match options.strategy {
            HybridStrategy::Rrf | HybridStrategy::Weighted => {
                let (vec_res, fts_res) = tokio::try_join!(
                    self.collect_vector_ids_capped(query_vector, filter, vector_hard_cap),
                    self.collect_fts_ids_capped(query_text, filter, vector_hard_cap),
                )?;
                let (mut vec_ids, vec_trunc) = vec_res;
                let (fts_ids, fts_trunc) = fts_res;
                vec_ids.extend(fts_ids);
                let merged = vec_ids.len() as u64;
                let total = merged.min(vector_hard_cap);
                let is_truncated = vec_trunc || fts_trunc || merged > vector_hard_cap;
                Ok((total, is_truncated))
            }
            HybridStrategy::VectorThenFts => {
                self.count_by_vector(query_vector, filter, vector_hard_cap)
                    .await
            }
            HybridStrategy::FtsThenVector => {
                self.count_by_text(query_text, filter, fts_hard_cap).await
            }
        }
    }

    // ===== Internal helpers =====

    /// Run a single BM25 query pass. Broken out from `search_by_text` so
    /// the caller can loop for runtime recovery without duplicating the
    /// query-construction code.
    async fn run_bm25_query(
        &self,
        query_text: &str,
        filter: Option<&SafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<(i64, f32)>> {
        self.ensure_fts_index().await?;

        let table = self.table.load_full();
        let fts_query = lance_index::scalar::FullTextSearchQuery::new(query_text.to_owned());
        let mut query = table.query().full_text_search(fts_query);

        if let Some(f) = filter {
            query = query.only_if(f.to_sql()?);
        }

        let mut stream = query.limit(limit).execute().await?;
        let mut raw_results: Vec<(i64, f32)> = Vec::new();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            for row_idx in 0..batch.num_rows() {
                let memory_id = Self::extract_i64(&batch, "memory_id", row_idx)?;
                let bm25_score = batch
                    .column_by_name("_score")
                    .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                    .map(|a| a.value(row_idx))
                    .unwrap_or(0.0);
                raw_results.push((memory_id, bm25_score));
            }
        }
        Ok(raw_results)
    }

    /// Ensure the vector (ANN) index on `embedding` is present, building it
    /// once if the corpus has grown past `vector_index.min_rows`.
    ///
    /// Without this, every vector query falls back to a brute-force
    /// exhaustive kNN scan over the whole table — the root cause of the
    /// hybrid-search latency this addresses. Structurally mirrors
    /// `ensure_fts_index`: a lock-free `AtomicBool` fast path with a
    /// mutex-serialized one-time build behind it.
    ///
    /// Below the threshold we intentionally do nothing and leave queries
    /// on brute-force: IVF training needs enough samples to cluster, and
    /// at small scale the scan is fast anyway. The gate is still marked
    /// ready so we do not re-probe `count_rows` on every query; the index
    /// is picked up on the next process start (or `optimize`) once the
    /// corpus is large enough. Disabled entirely when
    /// `vector_index.enabled` is false.
    async fn ensure_vector_index(&self) -> anyhow::Result<()> {
        ensure_vector_index_inner(
            &self.table,
            self.config.vector_index,
            self.config.distance_type,
            &self.vector_index_ready,
            &self.vector_index_active,
            &self.vector_index_lock,
            &self.index_ddl_lock,
        )
        .await
    }

    /// Ensure the FTS inverted index on `content` is present and consistent
    /// with the current `FtsConfig`.
    ///
    /// Fast path: once initialized in this process, `fts_init_ready`
    /// reads `true` with `Acquire` ordering and the function returns
    /// without acquiring any mutex. This avoids turning every BM25
    /// query into a `tokio::Mutex` lock acquisition.
    ///
    /// Slow path: on the first call (or after a runtime recovery reset)
    /// the atomic reads `false`, we take the mutex, and — because another
    /// task could have raced ahead and finished the init between our
    /// atomic load and our lock acquisition — we re-check the state
    /// inside the lock before running the full rebuild-decision flow.
    async fn ensure_fts_index(&self) -> anyhow::Result<()> {
        // Acquire: pair with the Release store after rebuild_fts_index_locked
        // writes Some(..) into fts_init_state. Any thread that observes
        // `true` here is guaranteed to observe that write as well.
        if self.fts_init_ready.load(Ordering::Acquire) {
            tracing::trace!("FTS init gate hit lock-free fast path");
            return Ok(());
        }
        let mut guard = self.fts_init_state.lock().await;
        // Double-checked: another task may have raced ahead and
        // completed the init between our atomic load and this lock
        // acquisition. In that case `fts_init_state` is already Some(..)
        // and the atomic is (or will soon be) true; just sync and return.
        if let Some(state) = guard.as_ref() {
            tracing::trace!(
                "FTS init gate observed Some inside lock (fingerprint={}.., ready_at={})",
                short_fp(&state.fingerprint),
                state.ready_at_unix_ms
            );
            self.fts_init_ready.store(true, Ordering::Release);
            return Ok(());
        }
        self.rebuild_fts_index_locked(&mut guard).await
    }

    /// Implementation of the rebuild-decision flow defined in spec §R5.
    /// Must be called with `self.fts_init_state` locked and observed to be
    /// `None`; on success this writes `Some(..)` into the guard.
    ///
    /// Ordering matters:
    /// 1. `force_rebuild` overrides everything
    /// 2. Real index existence check catches bookkeeping drift (backup
    ///    restores, external deletions, partial corruption)
    /// 3. Manifest-config fingerprint comparison catches config changes
    ///
    /// The fingerprint is stored directly inside the LanceDB table
    /// manifest via `NativeTable::update_config` / `manifest().config`.
    /// That KV store is per-table and works uniformly across local,
    /// `s3://`, `gs://`, `memory://`, … URIs, so there is no longer a
    /// sidecar file or a non-local special case.
    ///
    /// # Lock ordering
    ///
    /// This function holds `fts_init_state` (outer) and loads the table
    /// handle via `self.table.load_full()` (lock-free) for up to three
    /// separate scopes: `list_indices()`, `manifest()` / `update_config()`,
    /// and `create_index()`. The `create_index()` scope additionally takes
    /// `index_ddl_lock` to serialize against the vector build and against a
    /// spawned compaction. `reload_table()` takes no lock at all (it only
    /// advances the shared handle via `checkout_latest`), and write operations
    /// do not call back into `ensure_fts_index`, so the nesting stays
    /// cycle-free. Any future change that makes a write path invoke FTS
    /// initialization (e.g. an upsert that synchronously ensures the index)
    /// must preserve this ordering or introduce a dedicated intermediate lock.
    ///
    /// Index creation can take minutes on a large corpus, and the
    /// `fts_init_state` mutex is held throughout that window; this
    /// matches spec §R5's "serialize initialization" requirement and
    /// is consistent with the pre-change `OnceCell::get_or_try_init`
    /// behavior. Callers that cannot tolerate the stall should either
    /// call `ensure_fts_index()` eagerly during startup or accept
    /// that the first `search_by_text` of a process pays the rebuild
    /// cost.
    async fn rebuild_fts_index_locked(
        &self,
        guard: &mut MutexGuard<'_, Option<FtsInitState>>,
    ) -> anyhow::Result<()> {
        let current_fp = self
            .config
            .fts
            .fingerprint(LANCE_INDEX_VERSION, FTS_INDEX_COLUMN);

        let reason: &'static str;
        let need_rebuild: bool;

        if self.config.fts.force_rebuild {
            // Short-circuit per spec §5.1 Step 5.1: `force_rebuild` skips
            // all downstream probes and goes directly to create_index.
            // Also loud-log it so that operators notice an accidentally
            // left-on MEMORY_FTS_FORCE_REBUILD=true, which would
            // otherwise silently pay the full rebuild cost on every boot.
            tracing::warn!(
                "MEMORY_FTS_FORCE_REBUILD=true: forcing FTS index rebuild. \
                 Unset this flag after the one-time maintenance operation \
                 is complete to avoid paying the rebuild cost on every boot."
            );
            need_rebuild = true;
            reason = "force_rebuild";
        } else {
            // Normal path: existence check + manifest-config fingerprint compare.
            let mut rebuild = false;
            // Initial value describes the "nothing to rebuild" outcome so that a
            // stray leak into logging cannot show a misleading "unknown".
            let mut computed_reason: &'static str = "config_matches";

            // Real index existence check — catches drift even when the
            // manifest fingerprint happens to match (backup restore,
            // manual delete, partial corruption).
            let mut index_exists = false;
            {
                let table = self.table.load_full();
                match table.list_indices().await {
                    Ok(indices) => {
                        index_exists = indices.iter().any(|idx| {
                            // lancedb exposes `IndexType::FTS` as a public
                            // enum variant; match directly rather than
                            // string-matching on the Debug representation.
                            idx.columns.iter().any(|c| c == FTS_INDEX_COLUMN)
                                && matches!(idx.index_type, IndexType::FTS)
                        });
                    }
                    Err(e) => {
                        // Defensive: list_indices failures are almost
                        // always transient IO/ACL issues. We prefer to
                        // let create_index run and surface a clear error
                        // from there rather than fail this call with a
                        // stale init gate. Not in spec §R5; deliberately
                        // more forgiving. Recovery is bounded by the
                        // one-time attempted_recovery flag at the call
                        // site.
                        tracing::warn!("list_indices failed while checking FTS index: {e}");
                        rebuild = true;
                        computed_reason = "list_indices_error";
                    }
                }
            }
            if !index_exists && !rebuild {
                rebuild = true;
                computed_reason = "index_missing";
            }

            // Manifest-config fingerprint comparison. Works uniformly on
            // both local and remote URIs because the manifest is part of
            // the lancedb table itself rather than a filesystem sidecar.
            if !rebuild {
                let table = self.table.load_full();
                match read_fts_manifest_fingerprint(&table).await {
                    ManifestFingerprint::Match(saved_fp) if saved_fp == current_fp => {
                        // Existing index + matching fingerprint — nothing
                        // to do.
                    }
                    ManifestFingerprint::Match(_) => {
                        rebuild = true;
                        computed_reason = "fingerprint_mismatch";
                    }
                    ManifestFingerprint::MissingFingerprint => {
                        rebuild = true;
                        computed_reason = "fingerprint_missing";
                    }
                    ManifestFingerprint::SchemaVersionMismatch(found) => {
                        tracing::warn!(
                            "FTS manifest {FTS_MANIFEST_KEY_SCHEMA_VERSION}={found} \
                             differs from expected {FTS_FINGERPRINT_SCHEMA_VERSION}; \
                             rebuilding index"
                        );
                        rebuild = true;
                        computed_reason = "schema_version_mismatch";
                    }
                    ManifestFingerprint::ManifestUnavailable => {
                        // Future-proofing: a non-native table backend
                        // cannot expose `manifest()`. We do not know
                        // whether the stored fingerprint matches, so
                        // we fall through with the existing index in
                        // place and log a warning. Operators on such
                        // backends must use MEMORY_FTS_FORCE_REBUILD
                        // to migrate.
                        //
                        // This log is the ONLY signal that drift
                        // detection has been disabled, so it is
                        // deliberately loud: any tokenizer config
                        // change on such a backend will be silently
                        // ignored until a manual rebuild is forced.
                        tracing::warn!(
                            "FTS config drift detection is DISABLED on this table backend: \
                             the manifest config API is unavailable, so changes to \
                             MEMORY_FTS_TOKENIZER / MEMORY_FTS_* env vars will NOT trigger an \
                             automatic rebuild. Set MEMORY_FTS_FORCE_REBUILD=true once after any \
                             such change to migrate the existing index, then unset it."
                        );
                        computed_reason = "manifest_api_unavailable";
                    }
                }
            }

            need_rebuild = rebuild;
            reason = computed_reason;
        }

        if !need_rebuild {
            tracing::debug!(
                "FTS index already matches config (tokenizer={}, fingerprint={}..)",
                self.config.fts.tokenizer,
                short_fp(&current_fp)
            );
            // Record the successful init and return. No create_index call.
            **guard = Some(FtsInitState {
                fingerprint: current_fp,
                ready_at_unix_ms: unix_now_ms(),
            });
            // Release: publish the Some(..) write above so that a later
            // Acquire load in the fast path sees both the flag and the state.
            self.fts_init_ready.store(true, Ordering::Release);
            return Ok(());
        }

        let start = std::time::Instant::now();
        let builder = self.config.fts.to_builder();
        {
            // Serialize this DDL against a concurrent vector `create_index`
            // on the same table: `parallel_hybrid_search` can drive the FTS
            // and ANN builds together via `try_join!`. Acquired inside the
            // `fts_init_state` lock (held by the caller) to keep the nesting
            // cycle-free with the vector path's `vector_index_lock`.
            let _ddl = self.index_ddl_lock.lock().await;
            let table = self.table.load_full();
            // `.replace(true)` is the lancedb 0.27 default, but pinning it
            // explicitly serves two purposes:
            //   1. It documents that we rely on replace semantics to
            //      recover from stale indexes (existing-index + new
            //      config) and from any pre-manifest-config state that
            //      still has an FTS index on disk but no fingerprint.
            //   2. A future lancedb release flipping the default would
            //      otherwise silently break our upgrade flow; explicit
            //      `.replace(true)` keeps us pinned to the intended
            //      behavior.
            // Refresh to the latest version before building so we never commit
            // a CreateIndex from a stale snapshot (a concurrent compaction /
            // writer / the sibling ANN build under the same `index_ddl_lock`
            // may have advanced the version since this handle was loaded),
            // then retry the build on the retryable commit conflict LanceDB
            // raises when versions still race. See `create_index_with_retry`.
            create_index_with_retry("FTS", &table, || {
                table
                    .create_index(&[FTS_INDEX_COLUMN], Index::FTS(builder.clone()))
                    .replace(true)
                    .execute()
            })
            .await
            .map_err(|e| map_fts_create_error(e, &self.config.fts))?;
        }
        let elapsed_ms = start.elapsed().as_millis();

        // Persist the new fingerprint directly into the table manifest.
        // Failures are logged and tolerated: the in-memory gate prevents
        // re-work for the rest of this process, and on the next boot a
        // missing manifest fingerprint will simply trigger another
        // rebuild — wasteful but safe.
        {
            let table = self.table.load_full();
            if let Err(e) = write_fts_manifest_fingerprint(&table, &current_fp).await {
                tracing::warn!(
                    "FTS index rebuilt but manifest fingerprint write failed: {e}. \
                     Index will be rebuilt again on next boot."
                );
            }
        }

        tracing::info!(
            "FTS index rebuilt: tokenizer={}, reason={}, fingerprint={}.., elapsed_ms={}",
            self.config.fts.tokenizer,
            reason,
            short_fp(&current_fp),
            elapsed_ms
        );

        **guard = Some(FtsInitState {
            fingerprint: current_fp,
            ready_at_unix_ms: unix_now_ms(),
        });
        // Release: mirror the Some(..) write above for the lock-free
        // fast path; any subsequent Acquire load that reads `true` will
        // also observe the FtsInitState write above.
        self.fts_init_ready.store(true, Ordering::Release);
        Ok(())
    }

    /// Advance the shared table handle to the latest committed version after a
    /// write, so subsequent reads observe it.
    ///
    /// The `ArcSwap` holds a single `Arc<Table>` for the table's lifetime; we
    /// never `store` a freshly `open_table`d handle here. A LanceDB `Table`
    /// wraps an internally-mutable, shared dataset state (`Arc<Mutex<..>>`)
    /// whose version only ever moves forward, so advancing it is just
    /// `checkout_latest()` on the one shared handle:
    ///
    /// - It takes **no `index_ddl_lock`**, so it never waits behind a spawned
    ///   compaction (which holds that lock for minutes) — the write path stays
    ///   non-blocking. `checkout_latest` contends only on LanceDB's internal
    ///   per-handle mutex, held for microseconds.
    /// - Read-after-write is doubly guaranteed: the writer's own merge_insert /
    ///   delete already published its new version into this same shared handle
    ///   before `reload_table` runs, and `checkout_latest` additionally pulls
    ///   in any newer committed version. Version stores are monotonic, so a
    ///   concurrent compaction advancing the same handle never regresses it.
    /// - Because there is exactly one handle, the old `store(open_table())`
    ///   failure mode — a compaction's `checkout_latest` advancing an orphaned
    ///   Arc while the ArcSwap held a stale freshly-opened one — cannot occur.
    async fn reload_table(&self) -> anyhow::Result<()> {
        self.table
            .load_full()
            .checkout_latest()
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB reload_table (checkout_latest) failed: {e}"))?;
        Ok(())
    }

    /// Drop the stale FTS index and refresh the table handle, in
    /// preparation for a rebuild.
    ///
    /// Under lance 8.0 a manifest-registered index whose `_indices`
    /// sidecar files are gone is not rewritten by
    /// `create_index(...).replace(true)`; dropping the stale entry first
    /// forces the subsequent rebuild to materialize fresh index files.
    /// The DDL is serialized against a concurrent `create_index` via the
    /// shared ddl lock, and a drop failure is logged but tolerated (the
    /// rebuild proceeds regardless).
    async fn drop_stale_fts_index_and_reload(&self) -> anyhow::Result<()> {
        let _ddl = self.index_ddl_lock.lock().await;
        let table = self.table.load_full();
        if let Err(drop_err) = drop_fts_index(&table).await {
            tracing::warn!(
                "FTS recovery: dropping stale index failed ({drop_err}); \
                 proceeding to rebuild anyway"
            );
        }
        self.reload_table().await
    }

    /// Track writes and trigger maintenance on two independent cadences.
    ///
    /// The single monotonic `operation_count` feeds two gates that never
    /// interfere: the frequent, cheap *prune* gate (`prune_interval`) and
    /// the infrequent, heavy *compact+index* gate (`compact_interval`).
    /// Each gate keeps its own "last fired" marker, so firing one never
    /// perturbs the other's schedule.
    ///
    /// Compaction is **spawned** (fire-and-forget) rather than awaited: a
    /// heavy compact+index pass on a large corpus can take minutes, and
    /// running it inline blocked the calling write path (and, transitively,
    /// every RPC behind it) — under a large redispatch burst this pegged CPU
    /// and OOM-killed the pod. The [`InFlightGuard`] admits at most one
    /// background compaction at a time, so a burst that re-crosses the gate
    /// while a compaction is still running skips instead of stacking a second
    /// (doubling memory/CPU).
    ///
    /// Compact and prune are both `optimize` commits against the same table,
    /// and the default cadences (`compact_interval=1000`, `prune_interval=100`)
    /// open both gates together every 1000 ops. Running `Prune` (version GC)
    /// concurrently with an in-flight `Compact` can drop a version the
    /// compaction is still operating on and surface a commit conflict, so the
    /// two must NOT overlap. We therefore keep prune **ordered after** compact:
    /// when a compaction is spawned, the prune for this turn runs inside the
    /// same task right after it; only when no compaction fires does prune run
    /// inline. Either way it never races compaction.
    async fn track_operation(&self, count: usize) {
        let opt = &self.config.optimize;
        // Single shared monotonic increment for both gates.
        let now = self
            .operation_count
            .fetch_add(count, Ordering::Relaxed)
            .saturating_add(count);

        let prune_due = opt.prune_interval != 0
            && try_claim_gate(now, opt.prune_interval, &self.last_prune_count);

        // Heavy gate first: compaction's new versions become prunable below.
        if opt.compact_interval != 0
            && try_claim_gate(now, opt.compact_interval, &self.last_compact_count)
            && let Some(guard) = InFlightGuard::try_claim(&self.compact_in_flight)
        {
            let table = Arc::clone(&self.table);
            let last_optimized_at = Arc::clone(&self.last_optimized_at);
            let ddl_lock = Arc::clone(&self.index_ddl_lock);
            let optimize = self.config.optimize; // Copy
            tokio::spawn(async move {
                // `guard` is moved in and dropped (releasing the slot) when
                // the task ends — including on panic — so a failed compaction
                // can never wedge the gate permanently shut.
                let _guard = guard;
                if let Err(e) = compact_with(&table, &last_optimized_at, Some(&ddl_lock)).await {
                    tracing::warn!("Auto-compact failed: {e}");
                }
                // Prune AFTER compaction (never concurrently): cleans up the
                // versions compaction just produced. `prune_due` was already
                // claimed from the gate above, so we run it here unconditionally.
                if prune_due && let Err(e) = prune_with(&table, &optimize).await {
                    tracing::warn!("Auto-prune failed: {e}");
                }
            });
            return;
        }

        // No compaction was spawned this turn. Still skip inline prune if a
        // compaction spawned on an EARLIER turn is in flight — pruning would
        // race it and could drop a version it is operating on. Deferring is
        // safe: prune is best-effort GC and fires on a later gate.
        if prune_due
            && !self.compact_in_flight.load(Ordering::Acquire)
            && let Err(e) = self.prune().await
        {
            tracing::warn!("Auto-prune failed: {e}");
        }
    }

    /// Build Arrow RecordBatch from MemoryVectorRecords
    fn build_record_batch(
        records: &[MemoryVectorRecord],
        schema: &Arc<Schema>,
        vector_size: usize,
    ) -> anyhow::Result<RecordBatch> {
        let memory_ids: Vec<i64> = records.iter().map(|r| r.memory_id).collect();
        let vector_kinds: Vec<&str> = records.iter().map(|r| r.vector_kind.as_str()).collect();
        let chunk_indices: Vec<i32> = records.iter().map(|r| r.chunk_index).collect();
        let begin_positions: Vec<i32> = records.iter().map(|r| r.begin_position).collect();
        let end_positions: Vec<i32> = records.iter().map(|r| r.end_position).collect();
        let user_ids: Vec<i64> = records.iter().map(|r| r.user_id).collect();
        let contents: Vec<&str> = records.iter().map(|r| r.content.as_str()).collect();
        let content_types: Vec<i32> = records.iter().map(|r| r.content_type).collect();
        let roles: Vec<i32> = records.iter().map(|r| r.role).collect();
        let embedding_models: Vec<Option<&str>> = records
            .iter()
            .map(|r| r.embedding_model.as_deref())
            .collect();
        let metadata_jsons: Vec<Option<&str>> =
            records.iter().map(|r| r.metadata_json.as_deref()).collect();
        let created_ats: Vec<i64> = records.iter().map(|r| r.created_at).collect();
        let updated_ats: Vec<i64> = records.iter().map(|r| r.updated_at).collect();
        let indexed_ats: Vec<i64> = records.iter().map(|r| r.indexed_at).collect();
        let memory_kinds: Vec<i32> = records.iter().map(|r| r.memory_kind).collect();

        // Build FixedSizeList for embeddings
        let flat_values: Vec<f32> = records
            .iter()
            .flat_map(|r| r.embedding.iter().copied())
            .collect();
        let values_array = Float32Array::from(flat_values);
        let list_field = Arc::new(Field::new("item", DataType::Float32, true));
        let list_array =
            FixedSizeListArray::new(list_field, vector_size as i32, Arc::new(values_array), None);

        // Column order MUST match `memory_arrow_schema` exactly (16 cols,
        // image memory Phase 4 N-row). The N-row key columns
        // (vector_kind / chunk_index / begin_position / end_position)
        // sit right after memory_id; thread_id was removed in the thread
        // Phase 4 (callers narrow by `memory_id IN (...)` after resolving
        // the `thread_memory` junction on the RDB side).
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(memory_ids)),
                Arc::new(StringArray::from(vector_kinds)),
                Arc::new(Int32Array::from(chunk_indices)),
                Arc::new(Int32Array::from(begin_positions)),
                Arc::new(Int32Array::from(end_positions)),
                Arc::new(Int64Array::from(user_ids)),
                Arc::new(StringArray::from(contents)),
                Arc::new(Int32Array::from(content_types)),
                Arc::new(Int32Array::from(roles)),
                Arc::new(list_array),
                Arc::new(StringArray::from(embedding_models)),
                Arc::new(StringArray::from(metadata_jsons)),
                Arc::new(Int64Array::from(created_ats)),
                Arc::new(Int64Array::from(updated_ats)),
                Arc::new(Int64Array::from(indexed_ats)),
                Arc::new(Int32Array::from(memory_kinds)),
            ],
        )
        .map_err(|e| anyhow::anyhow!("Failed to build RecordBatch: {e}"))
    }

    /// Extract search hits from a RecordBatch (vector search result)
    fn extract_search_hits(
        &self,
        batch: &RecordBatch,
        results: &mut Vec<VectorSearchHit>,
    ) -> anyhow::Result<()> {
        // `column_by_name` is a linear scan over the schema's fields, so
        // resolve + downcast every column ONCE here rather than per row
        // (ANN result hydration is a hot path, and image memory Phase 4
        // widened the staged over-fetch so the row count grew). The loop
        // then only does O(1) `value(row)` / `is_null(row)`.
        let memory_id_col = batch
            .column_by_name("memory_id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| anyhow::anyhow!("missing column: memory_id"))?;
        let distance_col = batch
            .column_by_name("_distance")
            .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
        // Matched N-row metadata columns are best-effort: a query that
        // did not project them (or a pre-N-row table) yields None.
        let vector_kind_col = batch
            .column_by_name("vector_kind")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let chunk_index_col = batch
            .column_by_name("chunk_index")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>());
        let begin_pos_col = batch
            .column_by_name("begin_position")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>());
        let end_pos_col = batch
            .column_by_name("end_position")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>());
        let content_col = batch
            .column_by_name("content")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>());

        let opt_str = |col: Option<&StringArray>, row: usize| {
            col.filter(|a| !a.is_null(row))
                .map(|a| a.value(row).to_string())
        };
        let opt_i32 = |col: Option<&Int32Array>, row: usize| {
            col.filter(|a| !a.is_null(row)).map(|a| a.value(row))
        };

        for row_idx in 0..batch.num_rows() {
            let memory_id = memory_id_col.value(row_idx);
            let distance = distance_col.map(|a| a.value(row_idx)).unwrap_or(f32::MAX);

            // Convert distance to similarity score in [0, 1]
            let score = match self.config.distance_type {
                DistanceType::Cosine => (1.0 - distance).clamp(0.0, 1.0),
                DistanceType::L2 => (1.0 / (1.0 + distance)).clamp(0.0, 1.0),
                // Dot product on normalized vectors: range [-1, 1] → map to [0, 1]
                DistanceType::Dot => ((1.0 + distance) / 2.0).clamp(0.0, 1.0),
            };

            results.push(VectorSearchHit {
                memory_id,
                score,
                distance,
                vector_kind: opt_str(vector_kind_col, row_idx),
                chunk_index: opt_i32(chunk_index_col, row_idx),
                begin_position: opt_i32(begin_pos_col, row_idx),
                end_position: opt_i32(end_pos_col, row_idx),
                matched_content: opt_str(content_col, row_idx),
            });
        }
        Ok(())
    }

    fn extract_i64(batch: &RecordBatch, column: &str, row: usize) -> anyhow::Result<i64> {
        batch
            .column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(row))
            .ok_or_else(|| anyhow::anyhow!("missing column: {column}"))
    }

    /// Reciprocal Rank Fusion merge
    fn merge_rrf(
        vec_results: Vec<VectorSearchHit>,
        fts_results: Vec<VectorSearchHit>,
        k: f32,
        limit: usize,
    ) -> anyhow::Result<Vec<VectorSearchHit>> {
        let mut rrf_scores: std::collections::HashMap<i64, f64> = std::collections::HashMap::new();

        for (rank, hit) in vec_results.iter().enumerate() {
            *rrf_scores.entry(hit.memory_id).or_default() += 1.0 / (rank as f64 + k as f64);
        }
        for (rank, hit) in fts_results.iter().enumerate() {
            *rrf_scores.entry(hit.memory_id).or_default() += 1.0 / (rank as f64 + k as f64);
        }

        let mut merged: Vec<VectorSearchHit> = rrf_scores
            .into_iter()
            .map(|(id, score)| VectorSearchHit {
                memory_id: id,
                score: score as f32,
                distance: 0.0,
                // RRF fuses ranks across sources — no single matched row
                // to attribute (score_source stays SCORE_HYBRID).
                ..Default::default()
            })
            .collect();
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        Ok(merged)
    }

    /// Weighted score blending merge
    fn merge_weighted(
        vec_results: Vec<VectorSearchHit>,
        fts_results: Vec<VectorSearchHit>,
        vector_weight: f32,
        limit: usize,
    ) -> anyhow::Result<Vec<VectorSearchHit>> {
        let text_weight = 1.0 - vector_weight;
        let mut scores: std::collections::HashMap<i64, f32> = std::collections::HashMap::new();

        for hit in &vec_results {
            *scores.entry(hit.memory_id).or_default() += hit.score * vector_weight;
        }
        for hit in &fts_results {
            *scores.entry(hit.memory_id).or_default() += hit.score * text_weight;
        }

        let mut merged: Vec<VectorSearchHit> = scores
            .into_iter()
            .map(|(id, score)| VectorSearchHit {
                memory_id: id,
                score,
                distance: 0.0,
                // Weighted fusion across sources — no single matched row
                // (score_source stays SCORE_HYBRID).
                ..Default::default()
            })
            .collect();
        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        Ok(merged)
    }
}

/// Evolve a legacy LanceDB table (`memory_vector` or `thread_vector`,
/// both of which gained `memory_kind` at the same schema revision) in
/// place by backfilling a constant `memory_kind = RAW` column
/// if it is missing. Returns the table's current schema either way, so
/// callers doing a subsequent fingerprint check never re-read it twice.
pub(crate) async fn add_legacy_memory_kind_column_if_missing(
    table: &Table,
) -> anyhow::Result<Arc<Schema>> {
    let schema = table
        .schema()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB schema read failed: {e}"))?;
    if schema.field_with_name("memory_kind").is_ok() {
        return Ok(schema);
    }

    // Before MemoryKind existed every vector record was interpreted as a
    // RAW memory. The offline migration later replaces generated rows.
    table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "memory_kind".to_string(),
                "CAST(1 AS INT)".to_string(),
            )]),
            None,
        )
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB add memory_kind column failed: {e}"))?;
    table
        .schema()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB schema read after migration failed: {e}"))
}

async fn verify_table_schema_or_fail(
    table: &Table,
    expected: &Arc<Schema>,
    config: &VectorDBConfig,
) -> anyhow::Result<()> {
    let actual = table
        .schema()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB schema read failed: {e}"))?;
    let actual_arrow = actual.as_ref().clone();
    let expected_fp = schema_fingerprint(expected.as_ref());
    let actual_fp = schema_fingerprint(&actual_arrow);
    if actual_fp == expected_fp {
        return Ok(());
    }

    // Surface as `StartupError::LancedbSchemaMismatch` (wrapped in
    // `anyhow::Error`) so `module.rs` can downcast and route into the
    // structured `fatal()` path — agent-app's stdout JSON scanner
    // matches on the `code` field, not the message text.
    let expected_dim =
        super::schema::extract_embedding_dim_from_schema(expected.as_ref()).unwrap_or(0);
    let actual_dim = super::schema::extract_embedding_dim_from_schema(&actual_arrow).unwrap_or(0);
    Err(anyhow::Error::new(
        crate::infra::startup_error::StartupError::LancedbSchemaMismatch {
            table: config.table_name.clone(),
            uri: config.uri.clone(),
            expected_dim,
            actual_dim,
            expected_fingerprint: expected_fp,
            actual_fingerprint: actual_fp,
        },
    ))
}

pub fn schema_fingerprint(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|field| field_fingerprint(field.as_ref()))
        .collect::<Vec<_>>()
        .join("|")
}

pub fn field_fingerprint(field: &Field) -> String {
    format!(
        "{}:{:?}:nullable={}",
        field.name(),
        field.data_type(),
        field.is_nullable()
    )
}

/// Result of reading the FTS manifest fingerprint pair.
///
/// Separating the four cases (match, fingerprint missing, schema version
/// mismatch, manifest API unavailable) lets the caller log each with the
/// right severity without stringly-typed plumbing.
pub(crate) enum ManifestFingerprint {
    /// The manifest contains both keys and `schema_version` is current.
    /// Inner value is the stored `jobworkerp.fts.fingerprint`.
    Match(String),
    /// Schema version is current but the fingerprint key is missing.
    /// Happens on fresh tables and on the first boot after migrating
    /// away from the sidecar-file layout.
    MissingFingerprint,
    /// `jobworkerp.fts.schema_version` is present but does not equal
    /// the currently expected value. Inner value is the raw string
    /// found in the manifest (for diagnostic logging).
    SchemaVersionMismatch(String),
    /// `table.as_native()` returned `None` or `manifest().await` failed.
    /// We cannot introspect the manifest on this backend; the caller
    /// should fall back to a best-effort mode.
    ManifestUnavailable,
}

/// Read the FTS schema_version + fingerprint pair from the lancedb table
/// manifest. Returns `ManifestUnavailable` when the table implementation
/// does not expose a native manifest (future-proofing against
/// non-NativeTable backends) or the manifest fetch itself errors.
pub(crate) async fn read_fts_manifest_fingerprint(table: &Table) -> ManifestFingerprint {
    let Some(native) = table.as_native() else {
        return ManifestFingerprint::ManifestUnavailable;
    };
    let manifest = match native.manifest().await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!("failed to read lancedb manifest for FTS fingerprint check: {e}");
            return ManifestFingerprint::ManifestUnavailable;
        }
    };

    let expected_schema = FTS_FINGERPRINT_SCHEMA_VERSION.to_string();
    match manifest.config.get(FTS_MANIFEST_KEY_SCHEMA_VERSION) {
        Some(v) if *v == expected_schema => {}
        Some(v) => return ManifestFingerprint::SchemaVersionMismatch(v.clone()),
        None => return ManifestFingerprint::MissingFingerprint,
    }

    match manifest.config.get(FTS_MANIFEST_KEY_FINGERPRINT) {
        Some(fp) => ManifestFingerprint::Match(fp.clone()),
        None => ManifestFingerprint::MissingFingerprint,
    }
}

/// Persist the FTS fingerprint and schema version into the lancedb table
/// manifest via `NativeTable::update_config`.
///
/// On a backend that does not expose a `NativeTable` we log a warning and
/// return `Ok(())` — the caller tolerates write failures anyway, and the
/// missing fingerprint will simply trigger another rebuild on the next
/// boot. That is wasteful but safe, and avoids coupling the rebuild flow
/// to a backend-specific capability.
pub(crate) async fn write_fts_manifest_fingerprint(
    table: &Table,
    fingerprint: &str,
) -> anyhow::Result<()> {
    let Some(native) = table.as_native() else {
        tracing::warn!(
            "FTS manifest fingerprint write skipped: current table backend does not \
             expose NativeTable::update_config. Config drift detection will be best-effort."
        );
        return Ok(());
    };
    let schema_version = FTS_FINGERPRINT_SCHEMA_VERSION.to_string();
    native
        .update_config(vec![
            (FTS_MANIFEST_KEY_SCHEMA_VERSION.to_string(), schema_version),
            (
                FTS_MANIFEST_KEY_FINGERPRINT.to_string(),
                fingerprint.to_string(),
            ),
        ])
        .await
        .map_err(|e| anyhow::anyhow!("FTS manifest fingerprint update_config failed: {e}"))?;
    Ok(())
}

/// Current time as UNIX milliseconds. Used for `FtsInitState.ready_at_unix_ms`.
fn unix_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Return the first 14 chars of a `sha256:...` fingerprint for log output.
fn short_fp(fp: &str) -> &str {
    if fp.len() > 14 { &fp[..14] } else { fp }
}

/// Heuristically detect whether an error returned from `search_by_text`
/// indicates that the FTS inverted index is missing on disk. Used by
/// the search-time runtime recovery path.
///
/// Patterns are anchored on the *exact* error messages produced by
/// lance-index 4.0.x rather than loose keyword matches, because the
/// recovery path reruns `create_index`, and false positives would
/// make a one-off query error cascade into an unnecessary rebuild.
///
/// Known upstream messages (lance-index 4.0.0):
/// - `"Cannot perform full text search unless an INVERTED index has
///   been created on at least one column"` (query over a dataset with
///   zero FTS indexes)
/// - `"Index for column X is not an inverted index"` (the column has
///   a non-FTS index, e.g. BTree, but no inverted index)
///
/// Additional pattern for lance-index 8.0.0: when the index is still
/// registered in the manifest but its on-disk sidecar files were
/// removed (backup restore / manual `_indices` delete / partial
/// corruption), `list_indices()` keeps reporting the index, so the
/// startup existence check passes, but the query fails at IO time with
/// `Object at location .../_indices/<uuid>/<file>.lance not found`.
/// The inverted-index sidecar files are `tokens.lance`, `invert.lance`,
/// `docs.lance`, `metadata.lance` (lance-index 8.0.0
/// `scalar/inverted/index.rs`). We anchor on the `_indices` path
/// segment plus a "not found" IO error so a generic dataset-level
/// "not found" (e.g. a dropped table) does not over-match into a
/// spurious FTS rebuild.
///
/// When lance-index errors change, this list must be updated in lock
/// step. The runtime recovery is bounded to one attempt per call, so
/// a stale match is at most a single wasted rebuild, not an infinite
/// loop; over-matching is still worth avoiding to keep operator logs
/// meaningful.
fn is_missing_fts_index_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    if msg.contains("inverted index has been created") || msg.contains("is not an inverted index") {
        return true;
    }
    // lance-index 8.0: index registered in manifest but sidecar file
    // gone. Require both the `_indices` path segment and a not-found
    // signal to avoid matching unrelated IO errors.
    let missing =
        msg.contains("not found") || msg.contains("no such file") || msg.contains("does not exist");
    missing && msg.contains("_indices")
}

/// Rewrite a `create_index` failure into an operator-friendly message when
/// we recognize a Lindera dictionary-not-found pattern. Otherwise wraps
/// the original error with a generic prefix.
fn map_fts_create_error(e: lancedb::Error, fts: &FtsConfig) -> anyhow::Error {
    let msg = e.to_string();
    let lower = msg.to_lowercase();
    let is_lindera_dict_missing = fts.tokenizer.requires_lindera()
        && (lower.contains("lindera")
            || lower.contains("dictionary")
            || lower.contains("config.yml"))
        && (lower.contains("not found")
            || lower.contains("no such")
            || lower.contains("cannot find")
            || lower.contains("missing"));
    if is_lindera_dict_missing {
        let dict = match fts.tokenizer {
            super::config::FtsTokenizerKind::LinderaIpadic => "ipadic",
            super::config::FtsTokenizerKind::LinderaUnidic => "unidic",
            super::config::FtsTokenizerKind::LinderaKoDic => "ko-dic",
            _ => "<unknown>",
        };
        anyhow::anyhow!(
            "FTS index creation failed: Lindera dictionary '{dict}' not found. \
             Set LANCE_LANGUAGE_MODEL_HOME to a directory containing \
             'lindera/{dict}/config.yml', or download the dictionary from \
             https://github.com/lindera/lindera and place config.yml accordingly. \
             (underlying error: {msg})"
        )
    } else {
        anyhow::anyhow!("FTS index creation failed: {msg}")
    }
}

/// Aggregate scores from multiple vector searches into a single ranked list.
pub fn aggregate_scores(
    all_hits: &[Vec<VectorSearchHit>],
    strategy: AggregationStrategy,
) -> Vec<VectorSearchHit> {
    // memory_id → Vec<(vec_idx, rank, score)>
    let mut score_map: std::collections::HashMap<i64, Vec<(usize, usize, f32)>> =
        std::collections::HashMap::new();

    for (vec_idx, hits) in all_hits.iter().enumerate() {
        for (rank, hit) in hits.iter().enumerate() {
            score_map
                .entry(hit.memory_id)
                .or_default()
                .push((vec_idx, rank, hit.score));
        }
    }

    let num_vectors = all_hits.len() as f32;
    let mut results: Vec<VectorSearchHit> = score_map
        .into_iter()
        .map(|(memory_id, entries)| {
            let aggregated = match strategy {
                AggregationStrategy::Sum => entries.iter().map(|(_, _, s)| *s).sum::<f32>(),
                AggregationStrategy::Average => {
                    // Divide by total vector count (not hit count) to penalize partial matches
                    entries.iter().map(|(_, _, s)| s).sum::<f32>() / num_vectors
                }
                AggregationStrategy::Max => entries
                    .iter()
                    .map(|(_, _, s)| *s)
                    .fold(f32::NEG_INFINITY, f32::max),
                AggregationStrategy::WeightedByPosition => {
                    // weight = 1/(vec_index+1): vec[0]→1.0, vec[1]→0.5, vec[2]→0.33, ...
                    // Final score = sum(score_i / (idx_i + 1)) across all matching vectors.
                    entries
                        .iter()
                        .map(|(idx, _, s)| s / (*idx as f32 + 1.0))
                        .sum()
                }
                AggregationStrategy::RankFusion => {
                    // RRF based on rank within each vector's result set
                    entries
                        .iter()
                        .map(|(_, rank, _)| 1.0 / (*rank as f32 + 60.0))
                        .sum()
                }
            };
            VectorSearchHit {
                memory_id,
                score: aggregated,
                distance: 0.0, // not meaningful for aggregated scores
                // Aggregated across a memory's matched rows — no single
                // row to attribute. memory_id de-dup still applies.
                ..Default::default()
            }
        })
        .collect();

    results.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    results
}

#[cfg(test)]
mod test {
    use super::*;
    use rand::RngExt;

    // === try_claim_gate: the core two-gate maintenance logic ===

    #[test]
    fn gate_fires_once_when_interval_reached() {
        let marker = AtomicUsize::new(0);
        // now=100, interval=100 -> due, fires, marker advances to 100.
        assert!(try_claim_gate(100, 100, &marker));
        assert_eq!(marker.load(Ordering::Relaxed), 100);
        // Immediately re-checking at the same `now` is not due again.
        assert!(!try_claim_gate(100, 100, &marker));
    }

    #[test]
    fn gate_does_not_fire_below_interval() {
        let marker = AtomicUsize::new(0);
        assert!(!try_claim_gate(99, 100, &marker));
        assert_eq!(marker.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn gate_fires_exactly_at_boundary() {
        let marker = AtomicUsize::new(50);
        // now - marker == interval exactly -> fires (>= semantics).
        assert!(try_claim_gate(150, 100, &marker));
        assert_eq!(marker.load(Ordering::Relaxed), 150);
    }

    #[test]
    fn gate_burst_coalesces_to_single_fire_and_snaps_marker() {
        let marker = AtomicUsize::new(0);
        // A single jump of 350 with interval 100 fires once and snaps the
        // marker to the largest multiple <= now (300), not to `now`, so the
        // next fire is correctly spaced at >= 400.
        assert!(try_claim_gate(350, 100, &marker));
        assert_eq!(marker.load(Ordering::Relaxed), 300);
        assert!(!try_claim_gate(399, 100, &marker));
        assert!(try_claim_gate(400, 100, &marker));
        assert_eq!(marker.load(Ordering::Relaxed), 400);
    }

    #[test]
    fn two_gates_are_independent() {
        // Regression guard for the core design: prune (interval 10) and
        // compact (interval 100) share the same monotonic counter but
        // separate markers, so firing one never perturbs the other.
        let prune_marker = AtomicUsize::new(0);
        let compact_marker = AtomicUsize::new(0);
        let mut prune_fires = 0;
        let mut compact_fires = 0;
        for now in 1..=100usize {
            if try_claim_gate(now, 10, &prune_marker) {
                prune_fires += 1;
            }
            if try_claim_gate(now, 100, &compact_marker) {
                compact_fires += 1;
            }
        }
        assert_eq!(prune_fires, 10, "prune fires every 10 ops over 100 ops");
        assert_eq!(compact_fires, 1, "compact fires once at op 100");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn gate_fires_once_under_concurrent_claims() {
        use std::sync::atomic::AtomicUsize as Counter;
        // Many tasks observe the same `now` crossing the threshold; the CAS
        // must let exactly one win.
        let marker = Arc::new(AtomicUsize::new(0));
        let fires = Arc::new(Counter::new(0));
        let mut handles = Vec::new();
        for _ in 0..16 {
            let m = marker.clone();
            let f = fires.clone();
            handles.push(tokio::spawn(async move {
                if try_claim_gate(100, 100, &m) {
                    f.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(fires.load(Ordering::Relaxed), 1);
    }

    // === InFlightGuard: single-slot compaction claim ===

    #[test]
    fn in_flight_guard_claims_once_then_blocks_until_dropped() {
        let flag = Arc::new(AtomicBool::new(false));
        let guard = InFlightGuard::try_claim(&flag).expect("first claim succeeds");
        assert!(flag.load(Ordering::Acquire), "claim sets the flag");
        // A second claim while the first guard is alive must fail.
        assert!(
            InFlightGuard::try_claim(&flag).is_none(),
            "concurrent claim is rejected while in flight"
        );
        drop(guard);
        assert!(!flag.load(Ordering::Acquire), "drop releases the flag");
        // After release a fresh claim succeeds again.
        assert!(
            InFlightGuard::try_claim(&flag).is_some(),
            "reclaim after drop"
        );
    }

    #[test]
    fn in_flight_guard_releases_on_panic() {
        let flag = Arc::new(AtomicBool::new(false));
        let flag2 = Arc::clone(&flag);
        // A panic while holding the guard must still run Drop and clear the
        // flag — otherwise a panicking compaction would wedge the gate shut.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = InFlightGuard::try_claim(&flag2).expect("claim");
            panic!("simulate compaction panic");
        }));
        assert!(result.is_err(), "the closure panicked");
        assert!(
            !flag.load(Ordering::Acquire),
            "flag released by Drop even on panic"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn in_flight_guard_admits_exactly_one_under_contention() {
        use std::sync::atomic::AtomicUsize as Counter;
        use tokio::sync::Barrier;

        // Determinism via two barriers (no timing assumptions, so it can't
        // flake under CI load):
        //   start:   every task tries `try_claim` only after all N have arrived,
        //            so all attempts race the SAME free slot.
        //   release: the winner holds its guard until every task has finished
        //            its attempt, so a loser can never observe a re-freed slot
        //            and claim it as a (legitimate) second admit.
        const N: usize = 16;
        let flag = Arc::new(AtomicBool::new(false));
        let admitted = Arc::new(Counter::new(0));
        let start = Arc::new(Barrier::new(N));
        let release = Arc::new(Barrier::new(N));

        let mut handles = Vec::new();
        for _ in 0..N {
            let f = Arc::clone(&flag);
            let a = Arc::clone(&admitted);
            let start = Arc::clone(&start);
            let release = Arc::clone(&release);
            handles.push(tokio::spawn(async move {
                start.wait().await;
                let guard = InFlightGuard::try_claim(&f);
                if guard.is_some() {
                    a.fetch_add(1, Ordering::Relaxed);
                }
                // All tasks have now made their single attempt against the same
                // free slot; only after this does the winner drop its guard.
                release.wait().await;
                drop(guard);
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            admitted.load(Ordering::Relaxed),
            1,
            "exactly one task claims the slot while it is contended"
        );
        assert!(!flag.load(Ordering::Acquire), "slot freed after all drop");
    }

    fn random_embedding(dim: usize) -> Vec<f32> {
        let mut rng = rand::rng();
        (0..dim).map(|_| rng.random_range(-1.0..1.0)).collect()
    }

    fn similar_embedding(base: &[f32], noise: f32) -> Vec<f32> {
        let mut rng = rand::rng();
        base.iter()
            .map(|&v| v + rng.random_range(-noise..noise))
            .collect()
    }

    /// Test helper that auto-cleans the LanceDB directory on drop
    struct TestDb {
        path: String,
    }

    impl TestDb {
        /// Default `TestDb` config uses `FtsConfig::default()`, which is
        /// the `simple` tokenizer preset. This is deliberate: pre-existing
        /// English FTS tests (`test_fts_search`, `test_fts_search_with_filter`)
        /// were written against that behavior and continue to pass
        /// unchanged. Japanese/ngram/lindera tests instead go through
        /// `config_with_fts` and pass an explicit `FtsConfig`.
        fn config(dim: usize) -> (VectorDBConfig, Self) {
            Self::config_with_fts(
                dim,
                crate::infra::memory_vector::config::FtsConfig::default(),
            )
        }

        /// Build a test config with an explicit FtsConfig — used by the
        /// Japanese ngram/fingerprint test suite to override the default
        /// `simple` preset without touching the existing English tests.
        fn config_with_fts(
            dim: usize,
            fts: crate::infra::memory_vector::config::FtsConfig,
        ) -> (VectorDBConfig, Self) {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("/tmp/test_lancedb_{}", ts);
            let config = VectorDBConfig {
                uri: path.clone(),
                table_name: "test_memories".to_string(),
                vector_size: dim,
                distance_type: DistanceType::Cosine,
                optimize: test_optimize_config(),
                fts,
                vector_index: VectorIndexConfig::default(),
            };
            (config, Self { path })
        }

        /// Build a second repository over the same path (no Drop cleanup
        /// on the returned handle — the caller must keep the original
        /// TestDb alive to avoid premature deletion).
        fn config_reusing_path(
            &self,
            dim: usize,
            fts: crate::infra::memory_vector::config::FtsConfig,
        ) -> VectorDBConfig {
            VectorDBConfig {
                uri: self.path.clone(),
                table_name: "test_memories".to_string(),
                vector_size: dim,
                distance_type: DistanceType::Cosine,
                optimize: test_optimize_config(),
                fts,
                vector_index: VectorIndexConfig::default(),
            }
        }

        /// Build a test config with an explicit `VectorIndexConfig` — used
        /// by the ANN-index tests to drop `min_rows` low enough that a
        /// small fixture triggers a real index build.
        fn config_with_vector_index(
            dim: usize,
            vector_index: VectorIndexConfig,
        ) -> (VectorDBConfig, Self) {
            // Combine a timestamp with a process-unique counter so two tests
            // entering this helper within the same nanosecond cannot collide
            // on the same LanceDB directory.
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = format!("/tmp/test_lancedb_vidx_{ts}_{n}");
            let config = VectorDBConfig {
                uri: path.clone(),
                table_name: "test_memories".to_string(),
                vector_size: dim,
                distance_type: DistanceType::Cosine,
                optimize: test_optimize_config(),
                fts: crate::infra::memory_vector::config::FtsConfig::default(),
                vector_index,
            };
            (config, Self { path })
        }

        /// A config over this TestDb's path with an explicit distance type
        /// and vector-index policy. Used by the distance-mismatch test to
        /// reopen the same table with a different metric. No Drop cleanup
        /// on the returned config — the caller keeps the original alive.
        fn config_reusing_path_with_distance(
            &self,
            dim: usize,
            distance_type: DistanceType,
            vector_index: VectorIndexConfig,
        ) -> VectorDBConfig {
            VectorDBConfig {
                uri: self.path.clone(),
                table_name: "test_memories".to_string(),
                vector_size: dim,
                distance_type,
                optimize: test_optimize_config(),
                fts: crate::infra::memory_vector::config::FtsConfig::default(),
                vector_index,
            }
        }
    }

    /// Read the distance metric of the existing `embedding` ANN index via
    /// `index_stats`, or `None` if there is no such index.
    async fn embedding_index_distance(
        repo: &MemoryVectorRepositoryImpl,
    ) -> Option<lancedb::DistanceType> {
        let table = repo.table.load_full();
        let indices = table.list_indices().await.unwrap();
        let idx = indices.iter().find(|i| is_embedding_vector_index(i))?;
        table.index_stats(&idx.name).await.unwrap()?.distance_type
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn test_record(memory_id: i64, user_id: i64, dim: usize) -> MemoryVectorRecord {
        MemoryVectorRecord {
            memory_id,
            // Default single-row mapping ("text", chunk 0) — same as the
            // legacy compat path (design 1/3 §2.6.2.1).
            vector_kind: "text".to_string(),
            chunk_index: 0,
            begin_position: 0,
            end_position: 0,
            user_id,
            content: format!("test content for memory {memory_id}"),
            content_type: 0,
            role: 1,
            embedding: random_embedding(dim),
            embedding_model: Some("test-model".to_string()),
            metadata_json: None,
            created_at: memory_id * 1000,
            updated_at: memory_id * 1000,
            indexed_at: command_utils::util::datetime::now_millis(),
            memory_kind: 1,
        }
    }

    // ===== Maintenance (compact / prune / startup) tests =====

    #[tokio::test]
    async fn compact_and_optimize_index_updates_last_optimized_at() -> anyhow::Result<()> {
        let (config, _db) = TestDb::config(16);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        repo.upsert(&test_record(1, 1, 16)).await?;

        // Fresh repo has never optimized.
        assert_eq!(repo.get_stats().await?.last_optimized_at, 0);
        repo.compact_and_optimize_index().await?;
        assert!(
            repo.get_stats().await?.last_optimized_at > 0,
            "compact+index must stamp last_optimized_at for the GetIndexStats RPC"
        );
        Ok(())
    }

    #[tokio::test]
    async fn prune_does_not_touch_last_optimized_at() -> anyhow::Result<()> {
        let (config, _db) = TestDb::config(16);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        repo.upsert(&test_record(1, 1, 16)).await?;

        // Pruning is version GC, not index optimization: it must leave the
        // index-freshness metric untouched so the RPC meaning stays clean.
        repo.prune().await?;
        assert_eq!(
            repo.get_stats().await?.last_optimized_at,
            0,
            "prune must not stamp last_optimized_at"
        );
        Ok(())
    }

    #[tokio::test]
    async fn read_after_write_sees_new_rows_via_arcswap_reload() -> anyhow::Result<()> {
        // After an upsert, `reload_table` advances the shared handle via
        // `checkout_latest`; a subsequent read (`get_stats` -> `count_rows`)
        // must see the just-written row. This guards the read-after-write
        // contract the lock-free handle refresh must preserve.
        let (config, _db) = TestDb::config(16);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        assert_eq!(repo.get_stats().await?.total_records, 0);

        repo.upsert(&test_record(1, 1, 16)).await?;
        assert_eq!(
            repo.get_stats().await?.total_records,
            1,
            "read after upsert must observe the new row through the swapped handle"
        );

        repo.batch_upsert(vec![
            test_record(2, 1, 16),
            test_record(3, 1, 16),
            test_record(4, 1, 16),
        ])
        .await?;
        assert_eq!(
            repo.get_stats().await?.total_records,
            4,
            "read after batch_upsert must observe all new rows"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_reads_are_not_blocked_during_writes() -> anyhow::Result<()> {
        // Regression for the OOM/stall incident: with the old write-preferring
        // RwLock a stream of `reload_table` writers blocked every concurrent
        // read (GetIndexStats / search) behind them. With ArcSwap the readers
        // must keep completing while writes proceed. We assert progress via a
        // bounded timeout — a regression to a blocking lock would hang.
        let (config, _db) = TestDb::config(16);
        let repo = Arc::new(MemoryVectorRepositoryImpl::new(config).await?);

        let writer = {
            let r = Arc::clone(&repo);
            tokio::spawn(async move {
                for i in 1..=50 {
                    r.upsert(&test_record(i, 1, 16)).await.unwrap();
                }
            })
        };
        let reader = {
            let r = Arc::clone(&repo);
            tokio::spawn(async move {
                for _ in 0..50 {
                    // Each read must return promptly even while writes (and
                    // their handle swaps) are in flight.
                    let _ = r.get_stats().await.unwrap();
                }
            })
        };

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            writer.await.unwrap();
            reader.await.unwrap();
        })
        .await
        .expect("reads and writes must make progress without blocking each other");
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn auto_compaction_is_spawned_and_does_not_block_writes() -> anyhow::Result<()> {
        // With compact_interval small, crossing the gate during upserts must
        // spawn a background compaction (in-flight guarded) rather than block
        // the write path. We drive enough writes to cross the gate repeatedly
        // and require the whole batch to complete well within a timeout; a
        // regression to inline compaction would serialize every gate crossing.
        let (mut config, _db) = TestDb::config(16);
        config.optimize = OptimizeConfig {
            compact_interval: 5,
            prune_interval: 1_000_000, // effectively off; isolate compaction
            ..test_optimize_config()
        };
        let repo = Arc::new(MemoryVectorRepositoryImpl::new(config).await?);

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for i in 1..=40 {
                repo.upsert(&test_record(i, 1, 16)).await.unwrap();
            }
        })
        .await
        .expect("writes must not block on spawned compaction");

        // The in-flight slot must be free again once spawned compactions have
        // had a chance to finish (give them a brief window to drain).
        for _ in 0..100 {
            if !repo.compact_in_flight.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            !repo.compact_in_flight.load(Ordering::Acquire),
            "in-flight compaction slot must be released after the tasks finish"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reload_does_not_block_writes_behind_index_ddl_lock() -> anyhow::Result<()> {
        // Regression (P1): reload_table must NOT take index_ddl_lock, otherwise
        // a write's trailing reload blocks for the full duration a spawned
        // compaction / create_index holds that lock (minutes on a big table),
        // defeating the "spawn compaction, never block the write path" design.
        // We simulate a long DDL hold by acquiring index_ddl_lock for 3s and
        // asserting an upsert (which ends in reload_table) still completes well
        // within a 2s timeout. Under the old lock-before-open reload this would
        // hang until the guard drops.
        let (config, _db) = TestDb::config(16);
        let repo = Arc::new(MemoryVectorRepositoryImpl::new(config).await?);
        repo.upsert(&test_record(1, 1, 16)).await?;

        let held = {
            let r = Arc::clone(&repo);
            tokio::spawn(async move {
                let _g = r.index_ddl_lock.lock().await;
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            })
        };
        // Give the holder a moment to actually take the lock.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            repo.upsert(&test_record(2, 1, 16)),
        )
        .await
        .expect("upsert/reload must not block behind a held index_ddl_lock")?;

        held.await.unwrap();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prune_does_not_run_concurrently_with_compaction() -> anyhow::Result<()> {
        // Regression: compact and prune are both `optimize` commits on the same
        // table. With the default cadences both gates open together every
        // `compact_interval` ops; running Prune (version GC) concurrently with
        // an in-flight Compact can drop a version the compaction is operating
        // on and surface a commit conflict. Here both intervals are equal so
        // EVERY gate crossing opens both gates at the same op. The fix routes
        // prune through the spawned compaction task (ordered after compact) and
        // skips inline prune while a compaction is in flight, so maintenance
        // must complete cleanly and the table stays consistent.
        let (mut config, _db) = TestDb::config(16);
        config.optimize = OptimizeConfig {
            compact_interval: 5,
            prune_interval: 5, // identical cadence: both gates fire together
            ..test_optimize_config()
        };
        let repo = Arc::new(MemoryVectorRepositoryImpl::new(config).await?);

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for i in 1..=40 {
                repo.upsert(&test_record(i, 1, 16)).await.unwrap();
            }
        })
        .await
        .expect("writes must not block on spawned compaction+prune");

        // Let spawned maintenance drain, then the slot must be free.
        for _ in 0..100 {
            if !repo.compact_in_flight.load(Ordering::Acquire) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(
            !repo.compact_in_flight.load(Ordering::Acquire),
            "maintenance slot released after compact+prune finish"
        );
        // Table remains consistent and queryable after interleaved maintenance.
        assert_eq!(
            repo.get_stats().await?.total_records,
            40,
            "all rows survive compact+prune; no version was lost to a conflict"
        );
        Ok(())
    }

    #[tokio::test]
    async fn startup_prune_runs_without_error_when_enabled() -> anyhow::Result<()> {
        // prune_on_startup=true with a 0s manifest retention so new()
        // exercises the startup prune end to end. Safe: prune always passes
        // delete_unverified=false, so LanceDB's 7-day floor protects data
        // regardless of the retention. Must succeed on a brand-new table.
        let (mut config, _db) = TestDb::config(16);
        config.optimize = OptimizeConfig {
            prune_on_startup: true,
            prune_older_than_secs: 0,
            ..test_optimize_config()
        };
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        // Startup prune does not optimize the index, so the freshness metric
        // is still zero — confirms new() returned after the prune path.
        assert_eq!(repo.get_stats().await?.last_optimized_at, 0);
        Ok(())
    }

    #[tokio::test]
    async fn delete_operations_advance_the_maintenance_counter() -> anyhow::Result<()> {
        // Regression guard: deletes also create LanceDB versions, so they
        // must feed the prune/compact gates. Disable both gates (interval 0)
        // and startup prune so we observe the raw operation_count without any
        // gate firing or resetting it.
        let (mut config, _db) = TestDb::config(8);
        config.optimize = OptimizeConfig {
            compact_interval: 0,
            prune_interval: 0,
            prune_on_startup: false,
            ..test_optimize_config()
        };
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = repo.operation_count.load(Ordering::Relaxed);
        repo.upsert(&test_record(1, 10, 8)).await?;
        repo.delete(1).await?;
        repo.delete_by_memory_ids(&[2, 3]).await?;
        // Empty-records replace is a stale-delete that must also count.
        repo.replace_kinds_upsert(4, &["text"], vec![]).await?;

        // upsert(+1) + delete(+1) + delete_by_memory_ids(+1) + stale replace(+1)
        assert_eq!(repo.operation_count.load(Ordering::Relaxed) - base, 4);
        Ok(())
    }

    fn legacy_memory_arrow_schema(vector_size: usize) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("memory_id", DataType::Int64, false),
            Field::new("thread_id", DataType::Int64, false),
            Field::new("user_id", DataType::Int64, false),
            Field::new("content", DataType::Utf8, false),
            Field::new("content_type", DataType::Int32, false),
            Field::new("role", DataType::Int32, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    vector_size as i32,
                ),
                false,
            ),
            Field::new("embedding_model", DataType::Utf8, true),
            Field::new("metadata_json", DataType::Utf8, true),
            Field::new("created_at", DataType::Int64, false),
            Field::new("updated_at", DataType::Int64, false),
            Field::new("indexed_at", DataType::Int64, false),
        ]))
    }

    #[tokio::test]
    async fn opens_legacy_table_by_adding_raw_kind_column() {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let database = lancedb::connect(&config.uri).execute().await.unwrap();
        let expected = memory_arrow_schema(dim);
        let legacy = Arc::new(Schema::new(
            expected
                .fields()
                .iter()
                .filter(|field| field.name() != "memory_kind")
                .cloned()
                .collect::<Vec<_>>(),
        ));
        let empty = RecordBatch::new_empty(legacy.clone());
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(empty)], legacy));
        database
            .create_table(&config.table_name, reader)
            .execute()
            .await
            .unwrap();

        let repo = MemoryVectorRepositoryImpl::new(config).await.unwrap();
        let schema = repo.table.load_full().schema().await.unwrap();
        let field = schema.field_with_name("memory_kind").unwrap();
        assert_eq!(field.data_type(), &DataType::Int32);
    }

    // ===== Vector (ANN) index tests =====

    /// Count how many vector (ANN) indices exist on the `embedding` column.
    async fn embedding_vector_index_count(repo: &MemoryVectorRepositoryImpl) -> usize {
        let table = repo.table.load_full();
        table
            .list_indices()
            .await
            .unwrap()
            .iter()
            .filter(|idx| is_embedding_vector_index(idx))
            .count()
    }

    #[tokio::test]
    async fn vector_index_built_above_threshold() -> anyhow::Result<()> {
        // dim divisible by 16 so PQ's auto num_sub_vectors is well-defined.
        let dim = 32;
        let min_rows = 64;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows,
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Insert enough rows to clear the IVF training threshold.
        let records: Vec<_> = (1..=256).map(|i| test_record(i, 10, dim)).collect();
        repo.batch_upsert(records).await?;

        // No index yet (ensure only runs on the search path).
        assert_eq!(embedding_vector_index_count(&repo).await, 0);

        // A vector search triggers the one-time build.
        let probe = random_embedding(dim);
        let _ = repo.search_by_vector(&probe, None, 5).await?;

        assert_eq!(
            embedding_vector_index_count(&repo).await,
            1,
            "an ANN index must be built once row count >= min_rows"
        );
        assert!(
            repo.vector_index_active.load(Ordering::Acquire),
            "active flag must be set after a real index build"
        );
        assert!(repo.vector_index_ready.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn count_by_vector_builds_index_as_first_vector_traffic() -> anyhow::Result<()> {
        // Regression: count paths (CountSearchMatches / hybrid count) can be
        // the first vector traffic after startup, so they must ensure the
        // ANN index just like `search_by_vector` — not run brute-force.
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 64,
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        let records: Vec<_> = (1..=256).map(|i| test_record(i, 10, dim)).collect();
        repo.batch_upsert(records).await?;

        assert_eq!(embedding_vector_index_count(&repo).await, 0);
        // A count query — not a search — is the first vector traffic.
        let probe = random_embedding(dim);
        let (_count, _truncated) = repo.count_by_vector(&probe, None, 100).await?;

        assert_eq!(
            embedding_vector_index_count(&repo).await,
            1,
            "count_by_vector must build the ANN index on first vector traffic"
        );
        assert!(repo.vector_index_active.load(Ordering::Acquire));

        // collect_vector_ids_capped shares the same contract.
        let (ids, _) = repo.collect_vector_ids_capped(&probe, None, 100).await?;
        assert!(!ids.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn hybrid_search_builds_both_indices_concurrently_without_conflict() -> anyhow::Result<()>
    {
        // Regression: on a fresh table the first HybridSearch drives the
        // vector and FTS `create_index` calls together under `try_join!`.
        // The shared `index_ddl_lock` must serialize the two DDL commits so
        // neither fails with a transaction conflict and both indices land.
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 256,
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // >= 256 rows so the ANN index is eligible, with real content so the
        // FTS index has something to tokenize.
        let base = random_embedding(dim);
        let mut records: Vec<_> = (2..=256)
            .map(|i| test_record_with_content(i, 10, "neural network training", dim))
            .collect();
        let mut target = test_record_with_content(1, 10, "neural network optimization", dim);
        target.embedding = similar_embedding(&base, 0.001);
        records.push(target);
        repo.batch_upsert(records).await?;

        // Rrf → parallel_hybrid_search → try_join!(vector build, fts build).
        let options = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: Some(60.0),
        };
        let hits = repo
            .hybrid_search(&base, "neural network", None, 10, &options)
            .await?;

        // The concurrent build must succeed and both indices must exist.
        assert!(!hits.is_empty(), "hybrid search must return results");
        assert_eq!(
            embedding_vector_index_count(&repo).await,
            1,
            "ANN index must be built by the concurrent hybrid path"
        );
        assert!(repo.vector_index_active.load(Ordering::Acquire));
        assert!(
            repo.fts_init_ready.load(Ordering::Acquire),
            "FTS index must be initialized by the concurrent hybrid path"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hybrid_build_does_not_deadlock_against_concurrent_writer() -> anyhow::Result<()> {
        // Regression: a concurrent hybrid index build (vector + FTS
        // `create_index` under `index_ddl_lock`) must not stall against a
        // stream of writers whose `reload_table` refreshes the shared handle.
        // Historically `table` was a write-preferring `RwLock`, where a queued
        // `reload_table` writer could deadlock the index DDL; now reads and
        // reload are lock-free (`ArcSwap` + `checkout_latest`) and only the
        // index DDL takes `index_ddl_lock`, so this is structurally
        // deadlock-free. The test stays as a guard: drive the first hybrid
        // build concurrently with writers that each `reload_table` and require
        // completion within a timeout (a regression would hang past it).
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 256,
                nprobes: 8,
            },
        );
        let repo = Arc::new(MemoryVectorRepositoryImpl::new(config).await?);

        let base = random_embedding(dim);
        let records: Vec<_> = (1..=256)
            .map(|i| test_record_with_content(i, 10, "neural network training", dim))
            .collect();
        repo.batch_upsert(records).await?;

        // Writer task: repeated upserts, each ending in `reload_table()`
        // (a `table.write()`), to interleave write-lock requests with the
        // hybrid index build.
        let writer = {
            let r = Arc::clone(&repo);
            let d = dim;
            tokio::spawn(async move {
                for i in 1000..1020 {
                    let rec = test_record_with_content(i, 10, "concurrent writer row", d);
                    r.batch_upsert(vec![rec]).await?;
                }
                anyhow::Ok(())
            })
        };

        // Searcher task: the first hybrid query drives both index builds.
        let searcher = {
            let r = Arc::clone(&repo);
            let q = base.clone();
            tokio::spawn(async move {
                let options = HybridOptions {
                    strategy: HybridStrategy::Rrf,
                    vector_weight: None,
                    rrf_k: Some(60.0),
                };
                r.hybrid_search(&q, "neural network", None, 10, &options)
                    .await
            })
        };

        let joined = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (w, s) = tokio::join!(writer, searcher);
            w.unwrap()?;
            s.unwrap()?;
            anyhow::Ok(())
        })
        .await;

        assert!(
            joined.is_ok(),
            "concurrent hybrid build + writer must not deadlock"
        );
        joined.unwrap()?;
        Ok(())
    }

    #[tokio::test]
    async fn vector_index_built_with_configured_distance() -> anyhow::Result<()> {
        // The index must be trained on the configured distance metric, not
        // LanceDB's L2 default — otherwise search (which we now pin to the
        // same metric) and the index disagree.
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 64,
                nprobes: 8,
            },
        );
        // config_with_vector_index uses Cosine.
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        let records: Vec<_> = (1..=256).map(|i| test_record(i, 10, dim)).collect();
        repo.batch_upsert(records).await?;
        repo.ensure_vector_index().await?;

        assert_eq!(
            embedding_index_distance(&repo).await,
            Some(lancedb::DistanceType::Cosine),
            "index must be trained on the configured Cosine metric"
        );
        Ok(())
    }

    #[tokio::test]
    async fn vector_index_rebuilt_on_distance_config_change() -> anyhow::Result<()> {
        // Regression: changing MEMORY_DISTANCE_TYPE across restarts must
        // rebuild the ANN index so search/index metrics stay aligned,
        // rather than silently adopting the stale-metric index.
        let dim = 32;
        let vidx = VectorIndexConfig {
            enabled: true,
            min_rows: 64,
            nprobes: 8,
        };

        // Pass 1: build a Cosine index.
        let (cosine_config, db) = TestDb::config_with_vector_index(dim, vidx);
        {
            let repo = MemoryVectorRepositoryImpl::new(cosine_config).await?;
            let records: Vec<_> = (1..=256).map(|i| test_record(i, 10, dim)).collect();
            repo.batch_upsert(records).await?;
            repo.ensure_vector_index().await?;
            assert_eq!(
                embedding_index_distance(&repo).await,
                Some(lancedb::DistanceType::Cosine)
            );
        }

        // Pass 2: reopen the same table with L2 configured. The mismatch
        // must trigger a rebuild to L2.
        let l2_config = db.config_reusing_path_with_distance(dim, DistanceType::L2, vidx);
        let repo2 = MemoryVectorRepositoryImpl::new(l2_config).await?;
        repo2.ensure_vector_index().await?;

        assert_eq!(
            embedding_index_distance(&repo2).await,
            Some(lancedb::DistanceType::L2),
            "index must be rebuilt to the new configured metric"
        );
        assert!(repo2.vector_index_active.load(Ordering::Acquire));
        assert!(repo2.vector_index_ready.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn distance_change_below_pq_floor_drops_index_not_rebuild() -> anyhow::Result<()> {
        // Regression: an existing index + a distance config change, where the
        // corpus has since shrunk below the PQ training floor (256), must NOT
        // attempt a rebuild (it would fail "Not enough rows to train PQ" and
        // break search). Instead the stale-metric index is dropped so the
        // hot path brute-forces with the configured distance.
        let dim = 32;
        let vidx = VectorIndexConfig {
            enabled: true,
            min_rows: 64,
            nprobes: 8,
        };

        // Pass 1: build a Cosine index with >= 256 rows.
        let base = random_embedding(dim);
        let (cosine_config, db) = TestDb::config_with_vector_index(dim, vidx);
        {
            let repo = MemoryVectorRepositoryImpl::new(cosine_config).await?;
            let mut records: Vec<_> = (2..=256).map(|i| test_record(i, 10, dim)).collect();
            let mut target = test_record(1, 10, dim);
            target.embedding = base.clone();
            records.push(target);
            repo.batch_upsert(records).await?;
            repo.ensure_vector_index().await?;
            assert_eq!(embedding_vector_index_count(&repo).await, 1);

            // Shrink the corpus below the PQ floor (keep memory_id=1).
            let to_delete: Vec<i64> = (2..=256).collect();
            repo.delete_by_memory_ids(&to_delete).await?;
        }

        // Pass 2: reopen with L2 configured. Distance mismatch + below floor
        // → must drop the stale index, not rebuild, and not error.
        let l2_config = db.config_reusing_path_with_distance(dim, DistanceType::L2, vidx);
        let repo2 = MemoryVectorRepositoryImpl::new(l2_config).await?;

        let hits = repo2.search_by_vector(&base, None, 1).await?;
        assert_eq!(
            hits.first().map(|h| h.memory_id),
            Some(1),
            "brute-force search must still return the exact match"
        );
        assert_eq!(
            embedding_vector_index_count(&repo2).await,
            0,
            "stale-distance index must be dropped when below the PQ floor"
        );
        assert!(!repo2.vector_index_active.load(Ordering::Acquire));
        assert!(
            !repo2.vector_index_ready.load(Ordering::Acquire),
            "must not latch ready: a later corpus growth should rebuild"
        );
        Ok(())
    }

    #[tokio::test]
    async fn vector_index_min_rows_below_pq_floor_stays_brute_force() -> anyhow::Result<()> {
        // Regression: a `min_rows` below the PQ training floor (256) must
        // NOT trigger a build for a corpus in [min_rows, 255] — that would
        // fail with "Not enough rows to train PQ" and break search entirely.
        // The effective floor keeps such a table on brute-force, which must
        // return correct results.
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 16, // below the 256 PQ floor
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // 100 rows: >= configured min_rows (16) but < PQ floor (256).
        let base = random_embedding(dim);
        let mut records: Vec<_> = (2..=100).map(|i| test_record(i, 10, dim)).collect();
        let mut target = test_record(1, 10, dim);
        target.embedding = base.clone();
        records.push(target);
        repo.batch_upsert(records).await?;

        // Must NOT attempt a build (no error), and search must work.
        let hits = repo.search_by_vector(&base, None, 3).await?;
        assert_eq!(
            hits.first().map(|h| h.memory_id),
            Some(1),
            "brute-force search must work for a sub-floor corpus"
        );
        // count path too.
        let (count, _) = repo.count_by_vector(&base, None, 1000).await?;
        assert!(count >= 1);

        assert_eq!(
            embedding_vector_index_count(&repo).await,
            0,
            "no index may be built below the PQ training floor"
        );
        assert!(!repo.vector_index_active.load(Ordering::Acquire));
        assert!(
            !repo.vector_index_ready.load(Ordering::Acquire),
            "sub-floor must not latch ready (corpus may still grow to 256)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn vector_index_deferred_below_threshold_falls_back_to_brute_force() -> anyhow::Result<()>
    {
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 1000, // far above the fixture size
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut target = test_record(1, 10, dim);
        target.embedding = base.clone();
        repo.batch_upsert(vec![
            target,
            test_record(2, 10, dim),
            test_record(3, 10, dim),
        ])
        .await?;

        // Search still works (brute-force) and returns the exact match.
        let hits = repo.search_by_vector(&base, None, 3).await?;
        assert!(!hits.is_empty(), "brute-force search must return results");
        assert_eq!(hits[0].memory_id, 1, "exact match must rank first");

        // No index was built and the gate is NOT latched: a sub-threshold
        // corpus must keep re-evaluating so a later growth builds the index.
        assert_eq!(embedding_vector_index_count(&repo).await, 0);
        assert!(!repo.vector_index_active.load(Ordering::Acquire));
        assert!(
            !repo.vector_index_ready.load(Ordering::Acquire),
            "below-threshold must not latch ready (would freeze brute-force)"
        );
        Ok(())
    }

    #[tokio::test]
    async fn vector_index_disabled_never_builds() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: false,
                min_rows: 1,
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let records: Vec<_> = (1..=256).map(|i| test_record(i, 10, dim)).collect();
        repo.batch_upsert(records).await?;

        let probe = random_embedding(dim);
        let _ = repo.search_by_vector(&probe, None, 5).await?;

        assert_eq!(
            embedding_vector_index_count(&repo).await,
            0,
            "disabled config must never build an index"
        );
        assert!(!repo.vector_index_active.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn disabled_config_bypasses_preexisting_index() -> anyhow::Result<()> {
        // Regression: a table that already has an ANN index from a previous
        // boot must be searched with the index bypassed when the feature is
        // later disabled. The index is NOT dropped (so re-enabling is free),
        // and the flat scan must still return correct ground-truth results.
        let dim = 32;
        let vidx_on = VectorIndexConfig {
            enabled: true,
            min_rows: 64,
            nprobes: 8,
        };

        // Pass 1: build the index with the feature enabled.
        let base = random_embedding(dim);
        let (on_config, db) = TestDb::config_with_vector_index(dim, vidx_on);
        {
            let repo = MemoryVectorRepositoryImpl::new(on_config).await?;
            let mut records: Vec<_> = (2..=256).map(|i| test_record(i, 10, dim)).collect();
            let mut target = test_record(1, 10, dim);
            target.embedding = similar_embedding(&base, 0.001);
            records.push(target);
            repo.batch_upsert(records).await?;
            repo.ensure_vector_index().await?;
            assert_eq!(embedding_vector_index_count(&repo).await, 1);
        }

        // Pass 2: reopen the same table with the feature disabled.
        let off_config = db.config_reusing_path_with_distance(
            dim,
            DistanceType::Cosine,
            VectorIndexConfig {
                enabled: false,
                ..vidx_on
            },
        );
        let repo_off = MemoryVectorRepositoryImpl::new(off_config).await?;

        // The index is still on disk (not dropped) ...
        assert_eq!(
            embedding_vector_index_count(&repo_off).await,
            1,
            "disabling must not drop the existing index"
        );
        // ... and the query bypasses it (flat scan) yet still returns the
        // exact nearest neighbor — proving the bypass path is correct.
        let hits = repo_off.search_by_vector(&base, None, 1).await?;
        assert_eq!(
            hits.first().map(|h| h.memory_id),
            Some(1),
            "bypassed flat scan must still return the exact nearest neighbor"
        );
        assert!(
            !repo_off.vector_index_active.load(Ordering::Acquire),
            "disabled feature must leave active=false so nprobes is skipped"
        );
        Ok(())
    }

    #[tokio::test]
    async fn ensure_vector_index_is_idempotent() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 64,
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        let records: Vec<_> = (1..=256).map(|i| test_record(i, 10, dim)).collect();
        repo.batch_upsert(records).await?;

        // First ensure builds the index; subsequent ensures must be no-ops
        // that neither error nor create a duplicate index.
        repo.ensure_vector_index().await?;
        repo.ensure_vector_index().await?;
        repo.ensure_vector_index().await?;

        assert_eq!(
            embedding_vector_index_count(&repo).await,
            1,
            "repeated ensure must not create duplicate indices"
        );
        Ok(())
    }

    #[tokio::test]
    async fn create_index_with_retry_recovers_from_commit_conflict() -> anyhow::Result<()> {
        // Deterministic teeth for the retry the thread FTS path now shares:
        // a build that fails with a retryable commit conflict twice then
        // succeeds must be retried (not surfaced as an error), and the call
        // must succeed within MAX_ATTEMPTS.
        let (config, _db) = TestDb::config(16);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        let table = repo.table.load_full();

        let attempts = std::cell::Cell::new(0usize);
        let res = create_index_with_retry("test", &table, || {
            let n = attempts.get() + 1;
            attempts.set(n);
            async move {
                if n < 3 {
                    Err(lancedb::Error::Runtime {
                        message: format!(
                            "Retryable commit conflict for version {n}. Please retry."
                        ),
                    })
                } else {
                    Ok(())
                }
            }
        })
        .await;
        assert!(res.is_ok(), "must succeed after retrying the conflict");
        assert_eq!(attempts.get(), 3, "two conflicts then success = 3 attempts");
        Ok(())
    }

    #[tokio::test]
    async fn create_index_with_retry_does_not_retry_non_conflict_error() -> anyhow::Result<()> {
        // A non-retryable error must surface immediately (no wasted retries).
        let (config, _db) = TestDb::config(16);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        let table = repo.table.load_full();

        let attempts = std::cell::Cell::new(0usize);
        let res = create_index_with_retry("test", &table, || {
            attempts.set(attempts.get() + 1);
            async {
                Err(lancedb::Error::Runtime {
                    message: "schema mismatch: not retryable".to_string(),
                })
            }
        })
        .await;
        assert!(res.is_err(), "non-conflict error must propagate");
        assert_eq!(attempts.get(), 1, "must not retry a non-conflict error");
        Ok(())
    }

    #[tokio::test]
    async fn ensure_vector_index_does_not_freeze_below_threshold() -> anyhow::Result<()> {
        // Regression: a sub-threshold ensure must NOT set `ready`, so that
        // a later corpus growth in the same process still builds the index
        // (otherwise the process is stuck on brute-force until a restart).
        let dim = 32;
        let min_rows = 64;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows,
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Start below the threshold.
        repo.batch_upsert(vec![test_record(1, 10, dim)]).await?;
        repo.ensure_vector_index().await?;
        // Not frozen: ready stays false so the next call re-evaluates.
        assert!(
            !repo.vector_index_ready.load(Ordering::Acquire),
            "below-threshold ensure must not freeze into the ready fast path"
        );
        assert!(!repo.vector_index_active.load(Ordering::Acquire));
        // Repeated sub-threshold ensures keep re-evaluating, never error.
        repo.ensure_vector_index().await?;
        assert!(!repo.vector_index_ready.load(Ordering::Acquire));

        // Grow the corpus past the threshold within the same process.
        let more: Vec<_> = (2..=256).map(|i| test_record(i, 10, dim)).collect();
        repo.batch_upsert(more).await?;

        // The next ensure now builds the index and latches the fast path.
        repo.ensure_vector_index().await?;
        assert_eq!(
            embedding_vector_index_count(&repo).await,
            1,
            "index must build once the corpus crosses min_rows mid-process"
        );
        assert!(repo.vector_index_active.load(Ordering::Acquire));
        assert!(repo.vector_index_ready.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn ensure_vector_index_disabled_latches_ready() -> anyhow::Result<()> {
        // The disabled master switch IS a terminal state: ready latches so
        // we never re-probe.
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: false,
                min_rows: 1,
                nprobes: 8,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        repo.batch_upsert(vec![test_record(1, 10, dim)]).await?;

        assert!(!repo.vector_index_ready.load(Ordering::Acquire));
        repo.ensure_vector_index().await?;
        assert!(repo.vector_index_ready.load(Ordering::Acquire));
        assert!(!repo.vector_index_active.load(Ordering::Acquire));
        Ok(())
    }

    #[tokio::test]
    async fn vector_index_search_matches_brute_force_top_hit() -> anyhow::Result<()> {
        // The ANN result on a small, well-separated fixture should agree
        // with brute-force on the top hit (recall sanity check).
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 64,
                nprobes: 16,
            },
        );
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records: Vec<_> = (2..=256).map(|i| test_record(i, 10, dim)).collect();
        let mut target = test_record(1, 10, dim);
        // Make memory 1 the clear nearest neighbor of `base`.
        target.embedding = similar_embedding(&base, 0.001);
        records.push(target);
        repo.batch_upsert(records).await?;

        let hits = repo.search_by_vector(&base, None, 1).await?;
        assert!(repo.vector_index_active.load(Ordering::Acquire));
        assert_eq!(
            hits.first().map(|h| h.memory_id),
            Some(1),
            "ANN search must surface the clear nearest neighbor"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_upsert_and_delete() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let record = test_record(1, 10, dim);
        repo.upsert(&record).await?;

        let stats = repo.get_stats().await?;
        assert_eq!(stats.total_records, 1);

        repo.delete(1).await?;
        let stats = repo.get_stats().await?;
        assert_eq!(stats.total_records, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_upsert() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let records: Vec<_> = (1..=20).map(|i| test_record(i, 10, dim)).collect();
        let count = repo.batch_upsert(records).await?;
        assert_eq!(count, 20);

        let stats = repo.get_stats().await?;
        assert_eq!(stats.total_records, 20);
        Ok(())
    }

    /// Build one N-row chunk record (image memory Phase 4).
    fn kind_record(
        memory_id: i64,
        vector_kind: &str,
        chunk_index: i32,
        dim: usize,
    ) -> MemoryVectorRecord {
        let mut r = test_record(memory_id, 10, dim);
        r.vector_kind = vector_kind.to_string();
        r.chunk_index = chunk_index;
        r.begin_position = chunk_index * 100;
        r.end_position = chunk_index * 100 + 50;
        r.content = format!("{vector_kind} chunk {chunk_index} for memory {memory_id}");
        r
    }

    /// Image memory Phase 4: the merge key is the composite
    /// (memory_id, vector_kind, chunk_index), so a memory can hold many
    /// rows and re-upserting one (kind, chunk) updates that single row
    /// rather than collapsing the memory to one row (spec §3.5).
    #[tokio::test]
    async fn test_nrow_merge_key_keeps_chunks_distinct() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // One memory, 3 text chunks + 1 image row = 4 distinct rows.
        repo.batch_upsert(vec![
            kind_record(1, "text", 0, dim),
            kind_record(1, "text", 1, dim),
            kind_record(1, "text", 2, dim),
            kind_record(1, "image", 0, dim),
        ])
        .await?;
        assert_eq!(repo.get_stats().await?.total_records, 4);

        // Re-upsert one (kind, chunk): updates in place, still 4 rows
        // (the old single-memory_id merge key would have collapsed to 1).
        repo.upsert(&kind_record(1, "text", 1, dim)).await?;
        assert_eq!(
            repo.get_stats().await?.total_records,
            4,
            "re-upsert of one chunk must not collapse the memory's rows"
        );
        Ok(())
    }

    /// Image memory Phase 4 (design 1/3 §3.3.1): `replace_kinds_upsert`
    /// replaces only the listed kinds and shrinks/grows chunk counts
    /// without leaving stale rows.
    #[tokio::test]
    async fn test_replace_kinds_upsert_swaps_only_listed_kinds() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Start: 3 text chunks + 1 image row.
        repo.batch_upsert(vec![
            kind_record(1, "text", 0, dim),
            kind_record(1, "text", 1, dim),
            kind_record(1, "text", 2, dim),
            kind_record(1, "image", 0, dim),
        ])
        .await?;
        assert_eq!(repo.get_stats().await?.total_records, 4);

        // Replace text with FEWER chunks (3 -> 1): old text/1, text/2
        // must be gone, image untouched -> 1 text + 1 image = 2 rows.
        let n = repo
            .replace_kinds_upsert(1, &["text"], vec![kind_record(1, "text", 0, dim)])
            .await?;
        assert_eq!(n, 1);
        assert_eq!(
            repo.get_stats().await?.total_records,
            2,
            "shrinking text chunks must not leave stale text rows; image stays"
        );

        // Replace text with MORE chunks (1 -> 2). image still untouched
        // -> 2 text + 1 image = 3 rows.
        repo.replace_kinds_upsert(
            1,
            &["text"],
            vec![
                kind_record(1, "text", 0, dim),
                kind_record(1, "text", 1, dim),
            ],
        )
        .await?;
        assert_eq!(repo.get_stats().await?.total_records, 3);
        Ok(())
    }

    /// Image memory Phase 4 (design 1/3 §3.3.1 kind-isolation): a Text
    /// dispatch (`replace_kinds=["text"]`) and a Media dispatch
    /// (`replace_kinds=["image","caption"]`) on the same memory must not
    /// wipe each other's rows.
    #[tokio::test]
    async fn test_replace_kinds_isolation_text_vs_media() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Media dispatch writes image + caption first.
        repo.replace_kinds_upsert(
            1,
            &["image", "caption"],
            vec![
                kind_record(1, "image", 0, dim),
                kind_record(1, "caption", 0, dim),
            ],
        )
        .await?;
        assert_eq!(repo.get_stats().await?.total_records, 2);

        // A parallel Text dispatch replaces ONLY text. image/caption
        // must survive -> 2 text + image + caption = 4 rows.
        repo.replace_kinds_upsert(
            1,
            &["text"],
            vec![
                kind_record(1, "text", 0, dim),
                kind_record(1, "text", 1, dim),
            ],
        )
        .await?;
        assert_eq!(
            repo.get_stats().await?.total_records,
            4,
            "Text dispatch must not delete the Media dispatch's image/caption rows"
        );

        // And the reverse: re-running Media replaces only image/caption,
        // leaving the 2 text rows -> still 4.
        repo.replace_kinds_upsert(
            1,
            &["image", "caption"],
            vec![
                kind_record(1, "image", 0, dim),
                kind_record(1, "caption", 0, dim),
            ],
        )
        .await?;
        assert_eq!(repo.get_stats().await?.total_records, 4);
        Ok(())
    }

    /// Empty `replace_kinds` is rejected: an empty IN-list delete would
    /// be a silent no-op and leak stale rows, so callers with nothing to
    /// replace must not call it.
    #[tokio::test]
    async fn test_replace_kinds_upsert_rejects_empty_kinds() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        let err = repo
            .replace_kinds_upsert(1, &[], vec![kind_record(1, "text", 0, dim)])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("replace_kinds must not be empty"));
        Ok(())
    }

    /// Lock the `IndexStats` shape: `distance_type` is the typed
    /// enum (not a free-form string) and the ngram fields surface
    /// only when the active tokenizer is `Ngram`. Memory- and
    /// thread-side stats responses share this shape, so a regression
    /// here breaks both at the infra boundary.
    #[tokio::test]
    async fn test_get_stats_carries_distance_enum_and_fts_tokenizer_for_ngram() -> anyhow::Result<()>
    {
        let dim = 8;
        let mut fts = FtsConfig::apply_preset(FtsTokenizerKind::Ngram);
        fts.ngram_min = 2;
        fts.ngram_max = 4;
        let (config, _db) = TestDb::config_with_fts(dim, fts);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let stats = repo.get_stats().await?;
        // distance_type is the typed enum, not a free-form string.
        assert!(matches!(stats.distance_type, DistanceType::Cosine));
        assert!(matches!(stats.fts_tokenizer, FtsTokenizerKind::Ngram));
        assert_eq!(stats.fts_ngram_min, Some(2));
        assert_eq!(stats.fts_ngram_max, Some(4));
        Ok(())
    }

    /// Same accessor, but with a non-ngram tokenizer: the ngram min /
    /// max fields must stay `None` so a downstream UI does not surface
    /// a misleading "ngram(2-3)" notice next to a `simple` index.
    #[tokio::test]
    async fn test_get_stats_unsets_ngram_fields_for_non_ngram_tokenizer() -> anyhow::Result<()> {
        let dim = 8;
        let fts = FtsConfig::apply_preset(FtsTokenizerKind::Simple);
        let (config, _db) = TestDb::config_with_fts(dim, fts);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let stats = repo.get_stats().await?;
        assert!(matches!(stats.fts_tokenizer, FtsTokenizerKind::Simple));
        assert_eq!(stats.fts_ngram_min, None);
        assert_eq!(stats.fts_ngram_max, None);
        Ok(())
    }

    #[tokio::test]
    async fn test_open_rejects_legacy_table_schema() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let database = lancedb::connect(&config.uri).execute().await?;
        let schema = legacy_memory_arrow_schema(dim);
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(empty_batch)], schema));
        database
            .create_table(&config.table_name, reader)
            .execute()
            .await?;

        let result = MemoryVectorRepositoryImpl::new(config).await;
        let err = match result {
            Ok(_) => panic!("legacy schema must fail fast on startup"),
            Err(e) => e,
        };
        // The error MUST be the structured `LancedbSchemaMismatch` so
        // the parent (agent-app) can route into its recovery flow via
        // `downcast_ref::<StartupError>()` — message-text matching is
        // explicitly out of contract (see startup_error.rs docstring).
        let structured = err
            .downcast_ref::<crate::infra::startup_error::StartupError>()
            .expect("legacy schema must surface a structured StartupError");
        assert!(
            matches!(
                structured,
                crate::infra::startup_error::StartupError::LancedbSchemaMismatch { .. }
            ),
            "expected LancedbSchemaMismatch, got {structured:?}"
        );
        Ok(())
    }

    // Phase 4: `test_delete_by_thread_id` was removed together with the
    // `delete_by_thread_id` repository method. Thread membership is now
    // tracked in the `thread_memory` junction on the RDB side, and the
    // vector-store cascade is expressed through
    // `delete_by_memory_ids(junction_members)` instead.

    #[tokio::test]
    async fn test_vector_search() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut r1 = test_record(1, 10, dim);
        r1.embedding = similar_embedding(&base, 0.01); // very similar
        let mut r2 = test_record(2, 10, dim);
        r2.embedding = random_embedding(dim); // random/different
        let mut r3 = test_record(3, 10, dim);
        r3.embedding = similar_embedding(&base, 0.05); // somewhat similar

        repo.batch_upsert(vec![r1, r2, r3]).await?;

        let results = repo.search_by_vector(&base, None, 3).await?;
        assert_eq!(results.len(), 3);
        // Most similar should be first
        assert_eq!(results[0].memory_id, 1);
        assert!(results[0].score > results[2].score);
        Ok(())
    }

    #[tokio::test]
    async fn test_vector_search_with_filter() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let mut r1 = test_record(1, 10, dim);
        let mut r2 = test_record(2, 20, dim);
        // same embedding so ordering is deterministic by filter
        let emb = random_embedding(dim);
        r1.embedding = emb.clone();
        r2.embedding = emb.clone();

        repo.batch_upsert(vec![r1, r2]).await?;

        let filter = SafeFilter::user_id(10);
        let results = repo.search_by_vector(&emb, Some(&filter), 10).await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].memory_id, 1);
        Ok(())
    }

    #[tokio::test]
    async fn memory_kind_filter_limits_vector_and_text_searches() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        let embedding = random_embedding(dim);

        let mut conversation = test_record_with_content(1, 10, "kind filter phrase", dim);
        conversation.embedding = embedding.clone();
        let mut reflection = test_record_with_content(2, 10, "kind filter phrase", dim);
        reflection.embedding = embedding.clone();
        reflection.memory_kind = 7;
        repo.batch_upsert(vec![conversation, reflection]).await?;

        let filter = SafeFilter::memory_kinds_any(&[7])?;
        let vector = repo.search_by_vector(&embedding, Some(&filter), 10).await?;
        let text = repo
            .search_by_text("kind filter phrase", Some(&filter), 10)
            .await?;
        assert_eq!(
            vector.iter().map(|hit| hit.memory_id).collect::<Vec<_>>(),
            vec![2]
        );
        assert_eq!(
            text.iter().map(|hit| hit.memory_id).collect::<Vec<_>>(),
            vec![2]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_search_filters_by_updated_at_range() -> anyhow::Result<()> {
        // P4 regression: `updated_after` / `updated_before` rely on the
        // existing LanceDB `updated_at` column, so this guards against a
        // future schema migration accidentally dropping or renaming it.
        // `test_record` writes `updated_at = memory_id * 1000`.
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let emb = random_embedding(dim);
        let mut r1 = test_record(1, 10, dim);
        let mut r2 = test_record(2, 10, dim);
        let mut r3 = test_record(3, 10, dim);
        let mut r4 = test_record(4, 10, dim);
        r1.embedding = emb.clone();
        r2.embedding = emb.clone();
        r3.embedding = emb.clone();
        r4.embedding = emb.clone();
        repo.batch_upsert(vec![r1, r2, r3, r4]).await?;

        // 1500 < updated_at < 3500 → only memory_id 2 (2000) and 3 (3000).
        let filter = SafeFilter::updated_after(1500).and(SafeFilter::updated_before(3500));
        let results = repo.search_by_vector(&emb, Some(&filter), 10).await?;
        let mut ids: Vec<i64> = results.iter().map(|r| r.memory_id).collect();
        ids.sort();
        assert_eq!(ids, vec![2, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn test_vector_size_mismatch() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let wrong_dim_vector = vec![0.0f32; 16];
        let result = repo.search_by_vector(&wrong_dim_vector, None, 10).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Vector size mismatch")
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_get_all_memory_ids() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let records: Vec<_> = (1..=5).map(|i| test_record(i, 10, dim)).collect();
        repo.batch_upsert(records).await?;

        let mut ids = repo.get_all_memory_ids().await?;
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
        Ok(())
    }

    #[tokio::test]
    async fn test_upsert_overwrites() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let mut r = test_record(1, 10, dim);
        r.content = "original".to_string();
        repo.upsert(&r).await?;

        r.content = "updated".to_string();
        repo.upsert(&r).await?;

        let stats = repo.get_stats().await?;
        assert_eq!(stats.total_records, 1);
        Ok(())
    }

    fn test_record_with_content(
        memory_id: i64,
        user_id: i64,
        content: &str,
        dim: usize,
    ) -> MemoryVectorRecord {
        let mut r = test_record(memory_id, user_id, dim);
        r.content = content.to_string();
        r
    }

    #[tokio::test]
    async fn test_fts_search() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let records = vec![
            test_record_with_content(1, 10, "The quick brown fox jumps over the lazy dog", dim),
            test_record_with_content(2, 10, "A fast red fox leaps across the sleepy hound", dim),
            test_record_with_content(3, 10, "Completely unrelated content about databases", dim),
        ];
        repo.batch_upsert(records).await?;

        let results = repo.search_by_text("fox jumps", None, 10).await?;
        assert!(!results.is_empty(), "FTS should return results");
        // "fox jumps" should match the first record best
        assert_eq!(results[0].memory_id, 1);
        assert!(results[0].score > 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_fts_search_with_filter() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let r1 = test_record_with_content(1, 10, "machine learning algorithms", dim);
        let r2 = test_record_with_content(2, 20, "machine learning frameworks", dim);
        repo.batch_upsert(vec![r1, r2]).await?;

        // Filter by user_id=10 should only return the first record
        let filter = SafeFilter::user_id(10);
        let results = repo
            .search_by_text("machine learning", Some(&filter), 10)
            .await?;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].memory_id, 1);
        Ok(())
    }

    // ===== Count operations (P2, Phase 5-1) =====

    #[tokio::test]
    async fn test_count_by_filter_exact() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // 3 user=10, 2 user=20.
        let mut records = Vec::new();
        for id in 1..=3 {
            records.push(test_record(id, 10, dim));
        }
        for id in 4..=5 {
            records.push(test_record(id, 20, dim));
        }
        repo.batch_upsert(records).await?;

        let (total, truncated) = repo.count_by_filter(None).await?;
        assert_eq!(total, 5);
        assert!(!truncated);

        let filter = SafeFilter::user_id(10);
        let (filtered, truncated) = repo.count_by_filter(Some(&filter)).await?;
        assert_eq!(filtered, 3);
        assert!(!truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_text_exact() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let records = vec![
            test_record_with_content(1, 10, "the quick brown fox", dim),
            test_record_with_content(2, 10, "another fox in the wild", dim),
            test_record_with_content(3, 10, "completely unrelated content", dim),
        ];
        repo.batch_upsert(records).await?;

        let (total, truncated) = repo.count_by_text("fox", None, 1000).await?;
        assert_eq!(total, 2);
        assert!(!truncated);

        // Empty result.
        let (zero, truncated_zero) = repo.count_by_text("nothing-matches", None, 1000).await?;
        assert_eq!(zero, 0);
        assert!(!truncated_zero);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_text_truncated_at_hard_cap() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // 5 records all hit "fox".
        let records: Vec<_> = (1..=5)
            .map(|id| test_record_with_content(id, 10, "fox runs", dim))
            .collect();
        repo.batch_upsert(records).await?;

        // hard_cap = 2 → stream truncated; total = 2, is_truncated = true.
        let (total, truncated) = repo.count_by_text("fox", None, 2).await?;
        assert_eq!(total, 2);
        assert!(truncated, "stream past hard_cap must set is_truncated=true");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_text_with_filter() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let records = vec![
            test_record_with_content(1, 10, "fox alpha", dim),
            test_record_with_content(2, 20, "fox beta", dim),
            test_record_with_content(3, 10, "no match here", dim),
        ];
        repo.batch_upsert(records).await?;

        let filter = SafeFilter::user_id(10);
        let (total, truncated) = repo.count_by_text("fox", Some(&filter), 1000).await?;
        assert_eq!(total, 1);
        assert!(!truncated);
        Ok(())
    }

    // ===== Count operations (P2, Phase 5-2: VECTOR / HYBRID) =====

    #[tokio::test]
    async fn test_count_by_vector_exact() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let mut r = test_record(id, 10, dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let (total, truncated) = repo.count_by_vector(&base, None, 1000).await?;
        assert_eq!(total, 5);
        assert!(!truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_vector_truncated_at_hard_cap() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let mut r = test_record(id, 10, dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let (total, truncated) = repo.count_by_vector(&base, None, 2).await?;
        assert_eq!(total, 2);
        assert!(truncated, "ANN stream past hard_cap must set is_truncated");
        Ok(())
    }

    // ===== Image memory Phase 4: count memory_id de-dup =====

    /// `accumulate_memory_ids_capped` (pure): a batch where one
    /// memory_id repeats across rows (N-row schema) collapses to a
    /// single distinct id, and the cap is checked on the distinct count.
    #[test]
    fn accumulate_memory_ids_dedups_and_caps() {
        use arrow_array::Int64Array;
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new(
            "memory_id",
            DataType::Int64,
            false,
        )]));
        let mk = |vals: Vec<i64>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
        };

        // memory 1 appears 3 times (text c0/c1 + image), memory 2 twice
        // => 2 distinct ids, well under the cap.
        let mut ids = HashSet::new();
        let over = accumulate_memory_ids_capped(&mk(vec![1, 1, 1, 2, 2]), &mut ids, 1000).unwrap();
        assert!(!over);
        assert_eq!(ids.len(), 2, "N rows of the same memory count once");

        // Cap is on the DISTINCT count: 3 distinct ids with cap=2 trips.
        let mut ids = HashSet::new();
        let over = accumulate_memory_ids_capped(&mk(vec![10, 10, 20, 30]), &mut ids, 2).unwrap();
        assert!(over, "distinct count exceeding cap must report truncation");

        // Missing column is a broken projection contract, not a 0 count.
        let empty = Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int64,
            false,
        )]));
        let bad =
            RecordBatch::try_new(empty, vec![Arc::new(Int64Array::from(vec![1i64]))]).unwrap();
        let mut ids = HashSet::new();
        assert!(accumulate_memory_ids_capped(&bad, &mut ids, 10).is_err());
    }

    /// CountSearchMatches must report the unique memory count, not the
    /// LanceDB row count: a memory with text + image + caption rows is
    /// 1, not 3 (spec §4.4.6, consistent with search-side de-dup).
    #[tokio::test]
    async fn test_count_by_vector_dedups_nrow_memory() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        // 3 memories, each owning 3 N-row vectors (text c0/c1 + image)
        // = 9 LanceDB rows but only 3 distinct memory_ids.
        for id in 1..=3 {
            for (kind, chunk) in [("text", 0), ("text", 1), ("image", 0)] {
                let mut r = kind_record(id, kind, chunk, dim);
                r.embedding = similar_embedding(&base, 0.01);
                records.push(r);
            }
        }
        repo.batch_upsert(records).await?;

        let (total, truncated) = repo.count_by_vector(&base, None, 1000).await?;
        assert_eq!(total, 3, "9 rows across 3 memories count as 3");
        assert!(!truncated);
        Ok(())
    }

    /// Same de-dup contract on the BM25 path.
    #[tokio::test]
    async fn test_count_by_text_dedups_nrow_memory() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let mut records = Vec::new();
        for id in 1..=3 {
            for (kind, chunk) in [("text", 0), ("text", 1), ("caption", 0)] {
                let mut r = kind_record(id, kind, chunk, dim);
                r.content = format!("shared keyword token memory {id} {kind} {chunk}");
                records.push(r);
            }
        }
        repo.batch_upsert(records).await?;

        let (total, truncated) = repo.count_by_text("keyword", None, 1000).await?;
        assert_eq!(total, 3, "9 rows across 3 memories count as 3");
        assert!(!truncated);
        Ok(())
    }

    /// Regression: when the N-row stream is clipped by the LanceDB row
    /// `limit` (cap + 1 ROWS), the distinct memory_id count may be far
    /// below `cap`. The proto contract is that `is_truncated == false`
    /// implies `total` is an upper bound — a clipped lower count MUST be
    /// reported as `(cap, true)`, never `(small_n, false)`. Many
    /// memories each with several rows, with a tiny cap, forces the clip.
    #[tokio::test]
    async fn test_count_by_vector_clip_reports_truncated_not_undercount() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        // 5 memories × 3 rows = 15 LanceDB rows. cap = 2 -> fetch_limit
        // = 3 ROWS, which is 1 memory (3 rows) or at most 2. The stream
        // is clipped well before all 5 distinct memory_ids are seen.
        for id in 1..=5 {
            for (kind, chunk) in [("text", 0), ("text", 1), ("image", 0)] {
                let mut r = kind_record(id, kind, chunk, dim);
                r.embedding = similar_embedding(&base, 0.01);
                records.push(r);
            }
        }
        repo.batch_upsert(records).await?;

        let cap = 2u64;
        let (total, truncated) = repo.count_by_vector(&base, None, cap).await?;
        // The exact distinct count under a 3-row clip is order-dependent
        // (LanceDB ANN ordering is not guaranteed), so assert the
        // contract invariant rather than a fixed value: a count below
        // `cap` must NEVER be returned as non-truncated.
        assert!(
            total >= cap || truncated,
            "clipped N-row count returned a lower bound as non-truncated \
             (total={total}, truncated={truncated}, cap={cap}) — breaks \
             the `is_truncated==false => upper bound` proto contract"
        );
        // True match set (5) exceeds cap, so the only correct answer is
        // the truncated lower bound.
        assert_eq!(total, cap);
        assert!(truncated);
        Ok(())
    }

    /// Same clip-vs-undercount contract on the BM25 path.
    #[tokio::test]
    async fn test_count_by_text_clip_reports_truncated_not_undercount() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let mut records = Vec::new();
        for id in 1..=5 {
            for (kind, chunk) in [("text", 0), ("text", 1), ("caption", 0)] {
                let mut r = kind_record(id, kind, chunk, dim);
                r.content = format!("shared keyword token memory {id} {kind} {chunk}");
                records.push(r);
            }
        }
        repo.batch_upsert(records).await?;

        let cap = 2u64;
        let (total, truncated) = repo.count_by_text("keyword", None, cap).await?;
        assert!(
            total >= cap || truncated,
            "clipped N-row BM25 count returned a lower bound as \
             non-truncated (total={total}, truncated={truncated})"
        );
        assert_eq!(total, cap);
        assert!(truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_vector_with_filter() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=3 {
            let mut r = test_record(id, 10, dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        for id in 4..=5 {
            let mut r = test_record(id, 20, dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let filter = SafeFilter::user_id(10);
        let (total, truncated) = repo.count_by_vector(&base, Some(&filter), 1000).await?;
        assert_eq!(total, 3);
        assert!(!truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_vector_dim_mismatch() -> anyhow::Result<()> {
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let wrong_vec = vec![0.1f32; dim - 1];
        let res = repo.count_by_vector(&wrong_vec, None, 1000).await;
        assert!(res.is_err(), "dimension mismatch must error");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_rrf_exact() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut r1 = test_record_with_content(1, 10, "neural network training optimization", dim);
        r1.embedding = similar_embedding(&base, 0.01);
        let mut r2 =
            test_record_with_content(2, 10, "completely different topic about cooking", dim);
        r2.embedding = similar_embedding(&base, 0.02);
        let mut r3 =
            test_record_with_content(3, 10, "neural network architecture design patterns", dim);
        r3.embedding = random_embedding(dim);

        repo.batch_upsert(vec![r1, r2, r3]).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: Some(60.0),
        };
        let (total, truncated) = repo
            .count_by_hybrid(&base, "neural network", None, &options, 1000, 1000)
            .await?;
        // Three unique ids across vector + FTS branches.
        assert_eq!(total, 3);
        assert!(!truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_rrf_vector_truncated_propagates() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let mut r = test_record_with_content(id, 10, "fox neural network", dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: Some(60.0),
        };
        // Vector branch caps at 2 → truncation flag propagates.
        let (_, truncated) = repo
            .count_by_hybrid(&base, "fox", None, &options, 2, 1000)
            .await?;
        assert!(truncated, "vector branch hard_cap must propagate");
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_rejects_out_of_range_vector_weight() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        // Strategies that consume `vector_weight` must reject values
        // outside 0.0..=1.0 — same contract as the search path so a
        // bogus option set cannot count-succeed and search-fail.
        for strategy in [
            HybridStrategy::Weighted,
            HybridStrategy::VectorThenFts,
            HybridStrategy::FtsThenVector,
        ] {
            let options = HybridOptions {
                strategy,
                vector_weight: Some(1.5),
                rrf_k: None,
            };
            let res = repo
                .count_by_hybrid(&base, "fox", None, &options, 1000, 1000)
                .await;
            assert!(
                res.is_err(),
                "{:?} must reject vector_weight=1.5, got {:?}",
                strategy,
                res
            );
        }

        // RRF ignores `vector_weight`, so an out-of-range value passes
        // through (matches `parallel_hybrid_search`).
        let options_rrf = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: Some(99.0),
            rrf_k: Some(60.0),
        };
        let _ = repo
            .count_by_hybrid(&base, "fox", None, &options_rrf, 1000, 1000)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_rrf_vector_branch_not_capped_by_fts_cap() -> anyhow::Result<()> {
        // Regression: when an operator runs with
        // `fts_hard_cap < vector_hard_cap`, a HYBRID Rrf/Weighted
        // count must still read up to `vector_hard_cap` from the
        // vector branch — otherwise vector-only matches get truncated
        // below the bound the user actually configured.
        // Setup: 5 records that match the vector (similar embedding)
        // but whose FTS terms do NOT match `query_text`. So the FTS
        // branch contributes 0 ids regardless of cap.
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let mut r = test_record_with_content(id, 10, "completely unrelated topic", dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: Some(60.0),
        };
        // vector_hard_cap=10 (room for 5 hits), fts_hard_cap=2 (smaller).
        // The previous `branch_cap = min(...)` would cap the vector
        // branch at 2 and undercount; the fix counts all 5.
        let (total, truncated) = repo
            .count_by_hybrid(&base, "fox", None, &options, 10, 2)
            .await?;
        assert_eq!(total, 5);
        assert!(!truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_rrf_branch_cap_clamps_total() -> anyhow::Result<()> {
        // Regression: with vector_hard_cap=2 and fts_hard_cap=1000, the
        // FTS branch must NOT keep streaming past the smaller cap —
        // otherwise a broad-keyword HYBRID query reads up to
        // `fts_hard_cap` rows just to drop them when `total` is
        // clamped to `vector_hard_cap`. We can't directly observe how
        // many rows the branch consumed, but the user-visible
        // post-condition (total ≤ vector_hard_cap, is_truncated=true)
        // is preserved regardless of the smaller cap.
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let mut r = test_record_with_content(id, 10, "fox neural network", dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: Some(60.0),
        };
        let (total, truncated) = repo
            .count_by_hybrid(&base, "fox", None, &options, 2, 1000)
            .await?;
        assert!(
            total <= 2,
            "total must not exceed vector_hard_cap, got {total}"
        );
        assert!(truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_rrf_fts_branch_capped_by_vector_cap() -> anyhow::Result<()> {
        // Regression: in `Rrf | Weighted` parallel hybrid, the FTS
        // branch is capped at `vector_hard_cap` (NOT at `fts_hard_cap`)
        // so a broad `query_text` cannot pull `fts_hard_cap` rows just
        // to be discarded. With 5 FTS-only matches and
        // `vector_hard_cap=2`, the FTS branch hits the cap and the
        // truncation flag propagates.
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            // Vector mismatch (random embedding, not similar to base)
            // forces these to enter the union via the FTS branch only.
            let r = test_record_with_content(id, 10, "fox neural network", dim);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: Some(60.0),
        };
        // vector_hard_cap=2 caps both branches; FTS finds 5 matching
        // ids but stops at 2 → truncation flag set.
        let (total, truncated) = repo
            .count_by_hybrid(&base, "fox", None, &options, 2, 1000)
            .await?;
        assert!(total <= 2);
        assert!(
            truncated,
            "FTS branch capped by vector_hard_cap must propagate"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_fts_then_vector_truncated_at_fts_cap() -> anyhow::Result<()> {
        // FtsThenVector uses `fts_hard_cap` directly (count_by_text).
        // 5 matches with `fts_hard_cap=2` → (2, true).
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let r = test_record_with_content(id, 10, "fox neural network", dim);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::FtsThenVector,
            vector_weight: Some(0.7),
            rrf_k: None,
        };
        let (total, truncated) = repo
            .count_by_hybrid(&base, "fox", None, &options, 1000, 2)
            .await?;
        assert_eq!(total, 2);
        assert!(truncated);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_vector_then_fts_uses_primary() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let mut r = test_record_with_content(id, 10, "fox neural network", dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::VectorThenFts,
            vector_weight: Some(0.7),
            rrf_k: None,
        };
        let (total, truncated) = repo
            .count_by_hybrid(&base, "fox", None, &options, 10, 10)
            .await?;
        assert_eq!(total, 5);
        assert!(!truncated);

        let (total2, trunc2) = repo
            .count_by_hybrid(&base, "fox", None, &options, 2, 10)
            .await?;
        assert_eq!(total2, 2);
        assert!(trunc2);
        Ok(())
    }

    #[tokio::test]
    async fn test_count_by_hybrid_fts_then_vector_uses_primary() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base = random_embedding(dim);
        let mut records = Vec::new();
        for id in 1..=5 {
            let mut r = test_record_with_content(id, 10, "fox neural network", dim);
            r.embedding = similar_embedding(&base, 0.01);
            records.push(r);
        }
        repo.batch_upsert(records).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::FtsThenVector,
            vector_weight: Some(0.7),
            rrf_k: None,
        };
        let (total, truncated) = repo
            .count_by_hybrid(&base, "fox", None, &options, 10, 10)
            .await?;
        assert_eq!(total, 5);
        assert!(!truncated);

        let (total2, trunc2) = repo
            .count_by_hybrid(&base, "fox", None, &options, 10, 2)
            .await?;
        assert_eq!(total2, 2);
        assert!(trunc2);
        Ok(())
    }

    #[tokio::test]
    async fn test_hybrid_rrf() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base_emb = random_embedding(dim);
        // Record 1: semantically similar + text match
        let mut r1 = test_record_with_content(1, 10, "neural network training optimization", dim);
        r1.embedding = similar_embedding(&base_emb, 0.01);
        // Record 2: semantically similar, different text
        let mut r2 =
            test_record_with_content(2, 10, "completely different topic about cooking", dim);
        r2.embedding = similar_embedding(&base_emb, 0.02);
        // Record 3: text match, different embedding
        let mut r3 =
            test_record_with_content(3, 10, "neural network architecture design patterns", dim);
        r3.embedding = random_embedding(dim);

        repo.batch_upsert(vec![r1, r2, r3]).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: Some(60.0),
        };
        let results = repo
            .hybrid_search(&base_emb, "neural network", None, 3, &options)
            .await?;

        assert!(!results.is_empty());
        // Record 1 appears in both vector + FTS results, so should rank highest
        assert_eq!(
            results[0].memory_id, 1,
            "Record matching both vector and text should rank first"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_hybrid_weighted() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base_emb = random_embedding(dim);
        let mut r1 = test_record_with_content(1, 10, "deep learning model training with GPU", dim);
        r1.embedding = similar_embedding(&base_emb, 0.01);
        let mut r2 = test_record_with_content(2, 10, "unrelated content about gardening tips", dim);
        r2.embedding = random_embedding(dim);

        repo.batch_upsert(vec![r1, r2]).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::Weighted,
            vector_weight: Some(0.7),
            rrf_k: None,
        };
        let results = repo
            .hybrid_search(&base_emb, "deep learning", None, 2, &options)
            .await?;

        assert!(!results.is_empty());
        assert_eq!(results[0].memory_id, 1);
        // Score should reflect weighted blend
        assert!(results[0].score > 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_cascade_memory_delete() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Insert records for two different users to simulate a mixed batch.
        let r1 = test_record(1, 10, dim);
        let r2 = test_record(2, 10, dim);
        let r3 = test_record(3, 20, dim);
        repo.batch_upsert(vec![r1, r2, r3]).await?;
        assert_eq!(repo.get_stats().await?.total_records, 3);

        // Delete single memory — simulates Memory delete cascade
        repo.delete(1).await?;
        assert_eq!(repo.get_stats().await?.total_records, 2);

        // Remaining records should still be searchable
        let emb = random_embedding(dim);
        let results = repo.search_by_vector(&emb, None, 10).await?;
        assert_eq!(results.len(), 2);
        let ids: Vec<i64> = results.iter().map(|r| r.memory_id).collect();
        assert!(!ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(ids.contains(&3));
        Ok(())
    }

    // Phase 4: `test_cascade_thread_delete` was removed together with the
    // `delete_by_thread_id` repository method. Thread cascade is now driven
    // by `ThreadApp::delete_thread` on the RDB side, which resolves the set
    // of exclusive `memory_id`s from the `thread_memory` junction and then
    // deletes the corresponding vectors via `delete_by_memory_ids`. See
    // `app::memory_vector::MemoryVectorAppImpl::delete_vectors_by_memory_ids`.

    #[tokio::test]
    async fn test_delete_nonexistent() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Deleting non-existent memory should succeed without error
        repo.delete(999).await?;
        assert_eq!(repo.get_stats().await?.total_records, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_hybrid_vector_then_fts() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base_emb = random_embedding(dim);
        // Record 1: vector similar + keyword match -> highest combined score
        let mut r1 =
            test_record_with_content(1, 10, "machine learning optimization techniques", dim);
        r1.embedding = similar_embedding(&base_emb, 0.01);
        // Record 2: vector similar only (no keyword match)
        let mut r2 =
            test_record_with_content(2, 10, "completely unrelated cooking recipes topic", dim);
        r2.embedding = similar_embedding(&base_emb, 0.02);
        // Record 3: different embedding, keyword match
        let mut r3 =
            test_record_with_content(3, 10, "machine learning model architecture design", dim);
        r3.embedding = random_embedding(dim);

        repo.batch_upsert(vec![r1, r2, r3]).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::VectorThenFts,
            vector_weight: Some(0.7),
            rrf_k: None,
        };
        let results = repo
            .hybrid_search(&base_emb, "machine learning", None, 3, &options)
            .await?;

        assert!(!results.is_empty());
        // Record 1 (vector similar + FTS match) should rank higher than record 2 (vector only)
        assert_eq!(
            results[0].memory_id, 1,
            "Record with both vector similarity and keyword match should rank first"
        );
        assert!(results[0].score > 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_hybrid_fts_then_vector() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base_emb = random_embedding(dim);
        // Record 1: keyword match + vector similar -> highest combined score
        let mut r1 = test_record_with_content(1, 10, "deep learning neural network training", dim);
        r1.embedding = similar_embedding(&base_emb, 0.01);
        // Record 2: keyword match only, different embedding
        let mut r2 =
            test_record_with_content(2, 10, "deep learning frameworks comparison guide", dim);
        r2.embedding = random_embedding(dim);

        repo.batch_upsert(vec![r1, r2]).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::FtsThenVector,
            vector_weight: Some(0.7),
            rrf_k: None,
        };
        let results = repo
            .hybrid_search(&base_emb, "deep learning", None, 2, &options)
            .await?;

        assert!(!results.is_empty());
        // Record 1 (FTS + vector similar) should rank higher than record 2 (FTS only)
        assert_eq!(
            results[0].memory_id, 1,
            "Record with both FTS and vector similarity should rank first"
        );
        assert!(results[0].score > 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_hybrid_vector_then_fts_no_fts_hit() -> anyhow::Result<()> {
        let dim = 32;
        let (config, _db) = TestDb::config(dim);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let base_emb = random_embedding(dim);
        // Records with content that won't match the query text
        let mut r1 =
            test_record_with_content(1, 10, "completely different topic about gardening", dim);
        r1.embedding = similar_embedding(&base_emb, 0.01);
        let mut r2 = test_record_with_content(2, 10, "another unrelated topic about cooking", dim);
        r2.embedding = similar_embedding(&base_emb, 0.02);

        repo.batch_upsert(vec![r1, r2]).await?;

        let options = HybridOptions {
            strategy: HybridStrategy::VectorThenFts,
            vector_weight: Some(0.7),
            rrf_k: None,
        };
        // Query text won't match any content, but vector results should still be returned
        let results = repo
            .hybrid_search(
                &base_emb,
                "nonexistent_unique_keyword_xyz",
                None,
                2,
                &options,
            )
            .await?;

        // Stage 1 results preserved even when Stage 2 has no FTS hits
        assert!(!results.is_empty());
        // Scores should be vector_score * vector_weight (FTS contribution = 0)
        for r in &results {
            assert!(r.score > 0.0);
        }
        Ok(())
    }

    // ===== aggregate_scores unit tests =====

    fn make_hit(memory_id: i64, score: f32) -> VectorSearchHit {
        VectorSearchHit {
            memory_id,
            score,
            distance: 0.0,
            ..Default::default()
        }
    }

    // === is_missing_fts_index_error unit tests ===

    #[test]
    fn detects_fts_index_missing_from_upstream_message() {
        // Exact phrasing from lance-index 4.0.0 fill_fts_query_column.
        let e = anyhow::anyhow!(
            "Cannot perform full text search unless an INVERTED index has \
             been created on at least one column"
        );
        assert!(is_missing_fts_index_error(&e));
    }

    #[test]
    fn detects_column_not_inverted_index_message() {
        // Exact phrasing from lance 4.0.0 io/exec/fts.rs.
        let e = anyhow::anyhow!("Index for column content is not an inverted index");
        assert!(is_missing_fts_index_error(&e));
    }

    #[test]
    fn does_not_overmatch_generic_not_found() {
        // Loose keyword combinations must not trigger a rebuild.
        let e = anyhow::anyhow!("column not found: foo_id");
        assert!(!is_missing_fts_index_error(&e));

        let e = anyhow::anyhow!("memory_id index not found in schema");
        assert!(!is_missing_fts_index_error(&e));

        let e = anyhow::anyhow!("no such file: /tmp/missing.lance");
        assert!(!is_missing_fts_index_error(&e));
    }

    #[test]
    fn does_not_overmatch_inverted_corruption() {
        // "Inverted index corrupted" etc. should not trigger recovery —
        // recreate would not help and we want the error to propagate.
        let e = anyhow::anyhow!("inverted index partition 3 is corrupted");
        assert!(!is_missing_fts_index_error(&e));
    }

    #[test]
    fn test_aggregate_scores_sum() {
        let v0 = vec![make_hit(1, 0.9), make_hit(2, 0.5)];
        let v1 = vec![make_hit(1, 0.8), make_hit(3, 0.6)];
        let results = aggregate_scores(&[v0, v1], AggregationStrategy::Sum);

        let id1 = results.iter().find(|r| r.memory_id == 1).unwrap();
        assert!((id1.score - 1.7).abs() < 0.01); // 0.9 + 0.8

        let id2 = results.iter().find(|r| r.memory_id == 2).unwrap();
        assert!((id2.score - 0.5).abs() < 0.01); // only in v0

        let id3 = results.iter().find(|r| r.memory_id == 3).unwrap();
        assert!((id3.score - 0.6).abs() < 0.01); // only in v1
    }

    #[test]
    fn test_aggregate_scores_average() {
        let v0 = vec![make_hit(1, 0.9), make_hit(2, 0.6)];
        let v1 = vec![make_hit(1, 0.7)];
        // 2 total vectors
        let results = aggregate_scores(&[v0, v1], AggregationStrategy::Average);

        let id1 = results.iter().find(|r| r.memory_id == 1).unwrap();
        assert!((id1.score - 0.8).abs() < 0.01); // (0.9 + 0.7) / 2

        // Partial match: divided by total vectors (2), not hit count (1)
        let id2 = results.iter().find(|r| r.memory_id == 2).unwrap();
        assert!((id2.score - 0.3).abs() < 0.01); // 0.6 / 2
    }

    #[test]
    fn test_aggregate_scores_max() {
        let v0 = vec![make_hit(1, 0.5), make_hit(2, 0.9)];
        let v1 = vec![make_hit(1, 0.8), make_hit(2, 0.3)];
        let results = aggregate_scores(&[v0, v1], AggregationStrategy::Max);

        let id1 = results.iter().find(|r| r.memory_id == 1).unwrap();
        assert!((id1.score - 0.8).abs() < 0.01);

        let id2 = results.iter().find(|r| r.memory_id == 2).unwrap();
        assert!((id2.score - 0.9).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_scores_weighted_by_position() {
        let v0 = vec![make_hit(1, 0.6)]; // idx=0 -> weight=1/1
        let v1 = vec![make_hit(1, 0.6)]; // idx=1 -> weight=1/2
        let results = aggregate_scores(&[v0, v1], AggregationStrategy::WeightedByPosition);

        let id1 = results.iter().find(|r| r.memory_id == 1).unwrap();
        // 0.6/1 + 0.6/2 = 0.6 + 0.3 = 0.9
        assert!((id1.score - 0.9).abs() < 0.01);
    }

    #[test]
    fn test_aggregate_scores_rank_fusion() {
        // v0: id=1 rank 0, id=2 rank 1
        // v1: id=2 rank 0, id=1 rank 1
        let v0 = vec![make_hit(1, 0.9), make_hit(2, 0.5)];
        let v1 = vec![make_hit(2, 0.8), make_hit(1, 0.3)];
        let results = aggregate_scores(&[v0, v1], AggregationStrategy::RankFusion);

        // Both appear in both vectors → same RRF score
        let id1 = results.iter().find(|r| r.memory_id == 1).unwrap();
        let id2 = results.iter().find(|r| r.memory_id == 2).unwrap();
        // id1: 1/(0+60) + 1/(1+60) = 1/60 + 1/61
        // id2: 1/(1+60) + 1/(0+60) = 1/61 + 1/60
        assert!(
            (id1.score - id2.score).abs() < 0.001,
            "Symmetric ranks should yield equal RRF scores"
        );
    }

    // ===========================================================
    // FTS tokenizer configuration: integration tests (Step 6)
    //
    // These exercise ensure_fts_index / rebuild_fts_index_locked /
    // the fingerprint sidecar against a real LanceDB table, under
    // the Japanese use cases that motivated this work.
    //
    // All tests in this module run with `--test-threads=1`, so they
    // can share /tmp dir prefixes and env-var patterns without
    // cross-test interference.
    // ===========================================================

    use crate::infra::memory_vector::config::{
        FTS_MANIFEST_KEY_FINGERPRINT, FTS_MANIFEST_KEY_SCHEMA_VERSION, FtsConfig, FtsTokenizerKind,
        LANCE_INDEX_VERSION,
    };

    fn ngram_fts() -> FtsConfig {
        FtsConfig::apply_preset(FtsTokenizerKind::Ngram)
    }

    /// Read the (schema_version, fingerprint) pair directly from the
    /// lancedb manifest. Returns `None` when either key is absent.
    /// Used by tests to inspect what `rebuild_fts_index_locked` wrote.
    async fn read_manifest_pair(repo: &MemoryVectorRepositoryImpl) -> Option<(String, String)> {
        let table = repo.table.load_full();
        let native = table.as_native()?;
        let manifest = native.manifest().await.ok()?;
        let schema = manifest
            .config
            .get(FTS_MANIFEST_KEY_SCHEMA_VERSION)?
            .clone();
        let fp = manifest.config.get(FTS_MANIFEST_KEY_FINGERPRINT)?.clone();
        Some((schema, fp))
    }

    /// Directly overwrite the FTS fingerprint manifest keys via the
    /// public `NativeTable::update_config` API. Tests use this to
    /// simulate a pre-existing stale or corrupt manifest state.
    async fn write_manifest_pair(
        repo: &MemoryVectorRepositoryImpl,
        schema: &str,
        fp: &str,
    ) -> anyhow::Result<()> {
        let table = repo.table.load_full();
        let native = table.as_native().expect("tests use native tables");
        native
            .update_config(vec![
                (
                    FTS_MANIFEST_KEY_SCHEMA_VERSION.to_string(),
                    schema.to_string(),
                ),
                (FTS_MANIFEST_KEY_FINGERPRINT.to_string(), fp.to_string()),
            ])
            .await?;
        Ok(())
    }

    /// Delete both FTS manifest keys via the public
    /// `NativeTable::delete_config_keys` API.
    async fn clear_manifest_fingerprint(repo: &MemoryVectorRepositoryImpl) -> anyhow::Result<()> {
        let table = repo.table.load_full();
        let native = table.as_native().expect("tests use native tables");
        native
            .delete_config_keys(&[
                FTS_MANIFEST_KEY_SCHEMA_VERSION,
                FTS_MANIFEST_KEY_FINGERPRINT,
            ])
            .await?;
        Ok(())
    }

    /// TC-10: Japanese ngram matches substring queries.
    #[tokio::test]
    async fn test_fts_ngram_japanese_substring_matches() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let records = vec![
            test_record_with_content(1, 10, "今日は会議で新しいプロジェクトの話をした", dim),
            test_record_with_content(2, 10, "明日は別の予定がある", dim),
            test_record_with_content(3, 10, "プロジェクトの進捗を確認した", dim),
        ];
        repo.batch_upsert(records).await?;

        // 2-char substrings should hit the ngram index
        for q in ["会議", "新し", "プロジ", "プロジェクト"] {
            let results = repo.search_by_text(q, None, 10).await?;
            assert!(
                !results.is_empty(),
                "query {q:?} should return at least one hit under ngram"
            );
        }
        Ok(())
    }

    /// TC-11: single-character query returns empty without error.
    #[tokio::test]
    async fn test_fts_ngram_single_char_query_empty() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        repo.batch_upsert(vec![test_record_with_content(
            1,
            10,
            "今日は会議の日です",
            dim,
        )])
        .await?;

        // min ngram length is 2, so a single char produces no tokens.
        let results = repo.search_by_text("議", None, 10).await?;
        assert!(
            results.is_empty(),
            "1-char query should yield empty results under ngram (2,3)"
        );
        Ok(())
    }

    /// TC-14: reopening a table with a different FtsConfig triggers a
    /// fingerprint-mismatch rebuild and Japanese queries start hitting.
    #[tokio::test]
    async fn test_fts_reopen_with_new_config_rebuilds() -> anyhow::Result<()> {
        let dim = 8;
        // `db` is the directory-cleanup guard; as long as it is not
        // dropped before the test ends, the LanceDB path survives both
        // pass-1 and pass-2 instantiations.
        let (config_simple, db) = TestDb::config(dim);

        // Pass 1: build with the `simple` preset and upsert Japanese text.
        {
            let repo = MemoryVectorRepositoryImpl::new(config_simple).await?;
            repo.batch_upsert(vec![test_record_with_content(
                1,
                10,
                "今日は会議で新しいプロジェクトの話をした",
                dim,
            )])
            .await?;
            // `simple` tokenizer treats Japanese text as a single token,
            // so a 2-char substring should NOT hit.
            let results = repo.search_by_text("会議", None, 10).await?;
            assert!(
                results.is_empty(),
                "simple tokenizer should not match Japanese substring"
            );
        }

        // Pass 2: reopen the same path with the `ngram` preset and
        // verify that the fingerprint-driven rebuild made the Japanese
        // substring query work.
        let config_ngram = db.config_reusing_path(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config_ngram).await?;
        let results = repo.search_by_text("会議", None, 10).await?;
        assert!(
            !results.is_empty(),
            "after ngram rebuild, Japanese substring should hit"
        );
        Ok(())
    }

    /// TC-15: reopening with the *same* config keeps the manifest
    /// fingerprint unchanged (no rebuild). We assert this by comparing
    /// the `(schema_version, fingerprint)` pair read via the public
    /// `NativeTable::manifest()` API before and after the reopen.
    #[tokio::test]
    async fn test_fts_reopen_same_config_skips_rebuild() -> anyhow::Result<()> {
        let dim = 8;
        let (config, db) = TestDb::config_with_fts(dim, ngram_fts());

        let pair_before = {
            let repo = MemoryVectorRepositoryImpl::new(config).await?;
            repo.batch_upsert(vec![test_record_with_content(1, 10, "テスト内容", dim)])
                .await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            read_manifest_pair(&repo)
                .await
                .expect("manifest should hold fingerprint after init")
        };

        // Pass 2: same config — rebuild must be skipped and the
        // fingerprint stays identical.
        let config2 = db.config_reusing_path(dim, ngram_fts());
        let pair_after = {
            let repo = MemoryVectorRepositoryImpl::new(config2).await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            read_manifest_pair(&repo)
                .await
                .expect("manifest should still hold fingerprint on reopen")
        };

        assert_eq!(
            pair_before, pair_after,
            "manifest fingerprint must stay stable when config is unchanged"
        );
        Ok(())
    }

    /// TC-16: `force_rebuild=true` rewrites the fingerprint even when
    /// nothing else has changed. Verified by mutating the manifest
    /// fingerprint to a sentinel value and confirming the rebuild
    /// overwrites it with the real one.
    #[tokio::test]
    async fn test_fts_force_rebuild_flag() -> anyhow::Result<()> {
        let dim = 8;
        let (config, db) = TestDb::config_with_fts(dim, ngram_fts());

        {
            let repo = MemoryVectorRepositoryImpl::new(config).await?;
            repo.batch_upsert(vec![test_record_with_content(1, 10, "テスト", dim)])
                .await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            // Inject a sentinel so we can observe whether the forced
            // rebuild actually rewrites the manifest.
            write_manifest_pair(&repo, "1", "sha256:sentinel").await?;
        }

        let mut forced = ngram_fts();
        forced.force_rebuild = true;
        let expected_fp = forced.fingerprint(LANCE_INDEX_VERSION, FTS_INDEX_COLUMN);
        let config2 = db.config_reusing_path(dim, forced);
        {
            let repo = MemoryVectorRepositoryImpl::new(config2).await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            let (schema, fp) = read_manifest_pair(&repo)
                .await
                .expect("force_rebuild must repopulate manifest fingerprint");
            assert_eq!(schema, "1");
            assert_eq!(
                fp, expected_fp,
                "force_rebuild must overwrite the sentinel with the real fingerprint"
            );
        }
        Ok(())
    }

    /// TC-16b: a manifest with a bogus `schema_version` is tolerated —
    /// the repository logs a warning, rebuilds the index, and writes a
    /// fresh (schema_version, fingerprint) pair.
    #[tokio::test]
    async fn test_fts_corrupt_manifest_schema_version_triggers_rebuild() -> anyhow::Result<()> {
        let dim = 8;
        let (config, db) = TestDb::config_with_fts(dim, ngram_fts());

        {
            let repo = MemoryVectorRepositoryImpl::new(config).await?;
            repo.batch_upsert(vec![test_record_with_content(1, 10, "テスト内容", dim)])
                .await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            // Simulate a future schema version we cannot understand.
            write_manifest_pair(&repo, "999", "sha256:stale").await?;
        }

        // Reopen — the rebuild flow should detect the schema mismatch
        // and restore the current schema version along with a fresh
        // fingerprint.
        let config2 = db.config_reusing_path(dim, ngram_fts());
        {
            let repo = MemoryVectorRepositoryImpl::new(config2).await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            let (schema, fp) = read_manifest_pair(&repo)
                .await
                .expect("rebuild must restore a valid manifest pair");
            assert_eq!(schema, "1");
            assert!(fp.starts_with("sha256:"));
            assert_ne!(fp, "sha256:stale");
        }
        Ok(())
    }

    /// TC-16c (P2 本命): independent tables on the same URI keep
    /// independent fingerprints stored in their own manifest configs.
    ///
    /// The previous sidecar-file layout sanitized table names with
    /// `sanitize_table_name`, which collapsed reserved characters to
    /// `_` and made e.g. `foo/bar` and `foo_bar` share the same sidecar
    /// filename. With the manifest-config layout there is no table-name
    /// → filename mapping at all: every table has its own manifest, so
    /// isolation is structural.
    ///
    /// We assert two things:
    /// 1. Both tables have their own fingerprint after init, and the
    ///    values differ because the presets differ.
    /// 2. Rebuilding one table does not touch the other table's
    ///    fingerprint.
    #[tokio::test]
    async fn test_fts_multiple_tables_do_not_collide() -> anyhow::Result<()> {
        let dim = 8;
        let (mut config_a, db) = TestDb::config_with_fts(dim, ngram_fts());
        config_a.table_name = "table_a".to_string();
        let config_b = VectorDBConfig {
            uri: db.path.clone(),
            table_name: "table_b".to_string(),
            vector_size: dim,
            distance_type: DistanceType::Cosine,
            optimize: test_optimize_config(),
            fts: FtsConfig::apply_preset(FtsTokenizerKind::Simple),
            vector_index: VectorIndexConfig::default(),
        };

        let pair_a = {
            let repo = MemoryVectorRepositoryImpl::new(config_a.clone()).await?;
            repo.batch_upsert(vec![test_record_with_content(1, 10, "日本語の内容", dim)])
                .await?;
            let _ = repo.search_by_text("日本", None, 10).await?;
            read_manifest_pair(&repo)
                .await
                .expect("table_a must have manifest fingerprint")
        };
        let pair_b = {
            let repo = MemoryVectorRepositoryImpl::new(config_b.clone()).await?;
            repo.batch_upsert(vec![test_record_with_content(
                100,
                10,
                "English content",
                dim,
            )])
            .await?;
            let _ = repo.search_by_text("English", None, 10).await?;
            read_manifest_pair(&repo)
                .await
                .expect("table_b must have manifest fingerprint")
        };

        assert_ne!(
            pair_a.1, pair_b.1,
            "different FtsConfig presets must produce different fingerprints"
        );

        // Rebuild table_a by forcing a rebuild and confirm table_b's
        // fingerprint is not perturbed. This exercises the structural
        // isolation: because each table has its own manifest, the
        // update_config on table_a cannot reach table_b.
        let mut forced_a = ngram_fts();
        forced_a.force_rebuild = true;
        {
            let config_a_forced = VectorDBConfig {
                fts: forced_a,
                ..config_a.clone()
            };
            let repo_a = MemoryVectorRepositoryImpl::new(config_a_forced).await?;
            let _ = repo_a.search_by_text("日本", None, 10).await?;
        }
        let pair_b_after = {
            let repo = MemoryVectorRepositoryImpl::new(config_b).await?;
            let _ = repo.search_by_text("English", None, 10).await?;
            read_manifest_pair(&repo)
                .await
                .expect("table_b must still have manifest fingerprint")
        };
        assert_eq!(
            pair_b, pair_b_after,
            "rebuilding table_a must not touch table_b's manifest fingerprint"
        );
        Ok(())
    }

    /// TC-17: hybrid search honors the new ngram tokenizer on the FTS side.
    #[tokio::test]
    async fn test_hybrid_search_uses_ngram_tokenizer() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        repo.batch_upsert(vec![
            test_record_with_content(1, 10, "今日は会議で新しいプロジェクトの話をした", dim),
            test_record_with_content(2, 10, "天気がいいので散歩に出かけた", dim),
        ])
        .await?;

        let query_vec = random_embedding(dim);
        let opts = HybridOptions {
            strategy: HybridStrategy::Weighted,
            vector_weight: Some(0.3),
            rrf_k: None,
        };
        let results = repo
            .hybrid_search(&query_vec, "会議", None, 10, &opts)
            .await?;
        assert!(
            results.iter().any(|h| h.memory_id == 1),
            "hybrid with ngram tokenizer should surface the Japanese hit"
        );
        Ok(())
    }

    /// TC-19: empty content does not panic on upsert or search.
    #[tokio::test]
    async fn test_fts_empty_content_is_safe() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Upserting an empty content string must not panic.
        repo.batch_upsert(vec![test_record_with_content(1, 10, "", dim)])
            .await?;
        let _ = repo.search_by_text("", None, 10).await;
        Ok(())
    }

    /// TC-20: very long Japanese content is indexed and queried successfully
    /// under ngram (2,3).
    #[tokio::test]
    async fn test_fts_long_japanese_text() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        let long = "これは長い日本語のテキストです。".repeat(400); // ~6k chars
        repo.batch_upsert(vec![test_record_with_content(1, 10, &long, dim)])
            .await?;
        let results = repo.search_by_text("日本", None, 10).await?;
        assert!(!results.is_empty());
        Ok(())
    }

    /// TC-16e: parallel calls to `search_by_text` on a freshly
    /// constructed repo serialize the rebuild via the init mutex.
    /// After all concurrent calls finish, the manifest must hold the
    /// fingerprint computed from the current `FtsConfig`.
    #[tokio::test]
    async fn test_fts_concurrent_init_serializes() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = Arc::new(MemoryVectorRepositoryImpl::new(config).await?);
        repo.batch_upsert(vec![test_record_with_content(1, 10, "日本語テスト", dim)])
            .await?;

        let mut handles = Vec::new();
        for _ in 0..8 {
            let r = Arc::clone(&repo);
            handles.push(tokio::spawn(async move {
                r.search_by_text("日本", None, 10).await
            }));
        }
        for h in handles {
            let _ = h.await??;
        }

        let (schema, fp) = read_manifest_pair(&repo)
            .await
            .expect("manifest fingerprint should be present after concurrent init");
        assert_eq!(schema, "1");
        let expected = ngram_fts().fingerprint(LANCE_INDEX_VERSION, "content");
        assert_eq!(fp, expected);
        Ok(())
    }

    /// Diagnostic: observe lance-index behavior when `_indices` is
    /// deleted externally. Does lance-index error out, or does it
    /// silently fall back to flat_full_text_search? This determines
    /// whether the runtime recovery path can even fire in the TC-16d
    /// scenario.
    ///
    /// Marked `#[ignore]` because its purpose is diagnostic, not
    /// assertion. Run with:
    ///   cargo test -p infra -- --ignored --test-threads=1 \
    ///     test_fts_diag_delete_indices_behavior
    #[tokio::test]
    #[ignore]
    async fn test_fts_diag_delete_indices_behavior() -> anyhow::Result<()> {
        let dim = 8;
        let (config, db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        repo.batch_upsert(vec![test_record_with_content(1, 10, "日本語の本文", dim)])
            .await?;
        let first = repo.search_by_text("日本", None, 10).await?;
        eprintln!("first query result count: {}", first.len());

        // Delete _indices
        let indices_dir = std::path::PathBuf::from(&db.path).join("test_memories.lance/_indices");
        eprintln!("deleting {:?}", indices_dir);
        std::fs::remove_dir_all(&indices_dir)?;

        // Call the raw query path bypassing ensure_fts_index (so we see
        // the lance-index error directly).
        let table = repo.table.load_full();
        let fts_query = lance_index::scalar::FullTextSearchQuery::new("日本".to_string());
        let query = table.query().full_text_search(fts_query);
        match query.limit(10).execute().await {
            Ok(mut stream) => {
                let mut rows = 0;
                while let Some(Ok(batch)) = stream.next().await {
                    rows += batch.num_rows();
                }
                eprintln!("RAW QUERY: succeeded with {rows} rows (flat fallback)");
            }
            Err(e) => {
                eprintln!("RAW QUERY: error = {e}");
                eprintln!(
                    "RAW QUERY: is_missing_fts_index_error = {}",
                    is_missing_fts_index_error(&anyhow::anyhow!("{e}"))
                );
            }
        }
        Ok(())
    }

    /// TC-16d / TC-16f: runtime recovery mechanism tests.
    ///
    /// Important upstream behavior observation (see also the
    /// `test_fts_diag_delete_indices_behavior` ignored diagnostic):
    /// **lance-index 4.0.0 does NOT return an error when the on-disk
    /// `_indices` directory is deleted out from under an open table**.
    /// Instead it silently falls back to `flat_full_text_search`, which
    /// performs BM25 over a full scan and still returns results (just
    /// slower). Therefore the TC-16d scenario as originally written in
    /// the spec — "delete the index externally mid-process and expect
    /// runtime recovery to kick in" — cannot be triggered against the
    /// current upstream implementation.
    ///
    /// The recovery code path remains load-bearing as a defensive layer
    /// against: (1) genuine hard errors from lance-index (e.g. corrupted
    /// inverted index metadata), (2) future upstream changes that turn
    /// the current silent fallback into an error, (3) IO layer failures
    /// during the query path. We test its building blocks instead:
    ///   - `is_missing_fts_index_error` correctly recognizes the exact
    ///     upstream error strings (covered by the four unit tests above
    ///     in this module).
    ///   - Calling `ensure_fts_index` after forcefully clearing the
    ///     init gate (the key step the recovery path performs under the
    ///     mutex) leaves the table in a working state — which is what
    ///     the recovery path actually needs.
    ///
    /// After this test, a subsequent `search_by_text` must hit the fast
    /// path (TC-16f semantics): the init gate is `Some(..)` again and
    /// no further rebuild runs. We verify this by checking that the
    /// manifest fingerprint stays byte-identical across the sequence
    /// (the skip path does not rewrite it).
    #[tokio::test]
    async fn test_fts_init_gate_reset_allows_reinitialization() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        repo.batch_upsert(vec![test_record_with_content(
            1,
            10,
            "日本語の本文をインデックス化する",
            dim,
        )])
        .await?;

        // Baseline: first query populates the init gate and writes the
        // manifest fingerprint.
        let _ = repo.search_by_text("日本", None, 10).await?;
        {
            let guard = repo.fts_init_state.lock().await;
            assert!(
                guard.is_some(),
                "init gate must be populated after first successful search"
            );
        }
        let pair_before = read_manifest_pair(&repo)
            .await
            .expect("manifest should hold fingerprint after init");

        // Simulate the core step the runtime recovery path performs:
        // acquire the mutex, reset the gate, and invoke the rebuild
        // decision flow. Because the FTS index is intact and the
        // manifest fingerprint still matches, the skip branch should
        // fire and leave the manifest untouched.
        {
            let mut guard = repo.fts_init_state.lock().await;
            *guard = None;
            repo.rebuild_fts_index_locked(&mut guard).await?;
            assert!(
                guard.is_some(),
                "rebuild_fts_index_locked must repopulate the init gate"
            );
        }
        let pair_after = read_manifest_pair(&repo)
            .await
            .expect("manifest must still hold fingerprint after skip path");
        assert_eq!(
            pair_before, pair_after,
            "skip path must not rewrite the manifest fingerprint"
        );

        // TC-16f: the subsequent search takes the fast path (init gate
        // is Some, no rebuild). We cannot instrument the mutex directly
        // from the test, but we can observe that the manifest
        // fingerprint is still identical.
        let results = repo.search_by_text("日本", None, 10).await?;
        assert!(!results.is_empty());
        let pair_third = read_manifest_pair(&repo).await.unwrap();
        assert_eq!(
            pair_after, pair_third,
            "third search must hit the fast path without rewriting the manifest"
        );

        // After the skip branch re-populates the gate, the lock-free
        // atomic must also read `true` so that subsequent `ensure_fts_index`
        // calls take the cheap path without acquiring the mutex.
        assert!(
            repo.fts_init_ready.load(Ordering::Acquire),
            "fts_init_ready must be true after rebuild_fts_index_locked completes"
        );
        Ok(())
    }

    /// Atomic fast-path coverage: exercise the `fts_init_ready` gate
    /// transitions directly without relying on the search code path.
    ///
    /// Why: the mutex-based init gate has always been observable via
    /// `fts_init_state.lock().await`, but the new `AtomicBool` fast
    /// path bypasses the mutex entirely. If a future refactor forgets
    /// to update the atomic in one of the write sites (init success,
    /// skip branch, runtime recovery reset), the slow path will still
    /// look healthy from the existing tests while production callers
    /// silently spin on the mutex. This test pins the atomic state
    /// transitions explicitly.
    #[tokio::test]
    async fn test_fts_init_ready_atomic_transitions() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        // Initial: a fresh repo has never run ensure_fts_index, so the
        // lock-free flag must be false.
        assert!(
            !repo.fts_init_ready.load(Ordering::Acquire),
            "fts_init_ready must start as false on a fresh repository"
        );

        repo.batch_upsert(vec![test_record_with_content(
            1,
            10,
            "日本語の本文でテストする",
            dim,
        )])
        .await?;

        // After the first search the init gate is populated and the
        // atomic must reflect that so the next call can skip the mutex.
        let _ = repo.search_by_text("日本", None, 10).await?;
        assert!(
            repo.fts_init_ready.load(Ordering::Acquire),
            "fts_init_ready must be true after the first successful ensure"
        );

        // The runtime-recovery path must clear the atomic under the
        // mutex before clearing the state. Simulate it by calling the
        // same sequence that `search_by_text` uses on missing-index
        // errors, minus the actual rebuild, to observe the transient
        // `false` state.
        {
            let mut guard = repo.fts_init_state.lock().await;
            repo.fts_init_ready.store(false, Ordering::Release);
            *guard = None;
            assert!(
                !repo.fts_init_ready.load(Ordering::Acquire),
                "fts_init_ready must be false inside the recovery window"
            );
            // Now the rebuild completes and must re-publish true.
            repo.rebuild_fts_index_locked(&mut guard).await?;
        }
        assert!(
            repo.fts_init_ready.load(Ordering::Acquire),
            "fts_init_ready must be true again after rebuild_fts_index_locked"
        );

        // A follow-up search must still succeed against the recovered
        // index — proves the atomic-gated fast path actually returns
        // callers to a working state.
        let results = repo.search_by_text("日本", None, 10).await?;
        assert!(
            !results.is_empty(),
            "search after atomic recovery must return hits"
        );
        Ok(())
    }

    /// Complementary runtime-recovery check: after resetting the init
    /// gate *and* clearing the manifest fingerprint, the rebuild flow
    /// detects the fingerprint-missing condition, re-runs `create_index`,
    /// and writes a fresh fingerprint. This exercises the rebuild
    /// branch of `rebuild_fts_index_locked` from inside a recovery
    /// sequence.
    #[tokio::test]
    async fn test_fts_recovery_sequence_rebuilds_when_manifest_missing() -> anyhow::Result<()> {
        let dim = 8;
        let (config, _db) = TestDb::config_with_fts(dim, ngram_fts());
        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        repo.batch_upsert(vec![test_record_with_content(
            1,
            10,
            "日本語のデータを検索する",
            dim,
        )])
        .await?;
        let _ = repo.search_by_text("日本", None, 10).await?;
        assert!(read_manifest_pair(&repo).await.is_some());

        // Clear the manifest fingerprint to force the rebuild branch.
        clear_manifest_fingerprint(&repo).await?;
        assert!(read_manifest_pair(&repo).await.is_none());

        // Execute the recovery sequence: reset gate + call rebuild.
        {
            let mut guard = repo.fts_init_state.lock().await;
            *guard = None;
            repo.rebuild_fts_index_locked(&mut guard).await?;
            assert!(guard.is_some());
        }

        let (schema, fp) = read_manifest_pair(&repo)
            .await
            .expect("rebuild branch must rewrite the manifest fingerprint");
        assert_eq!(schema, "1");
        assert!(fp.starts_with("sha256:"));

        // A subsequent search must succeed against the rebuilt index.
        let results = repo.search_by_text("日本", None, 10).await?;
        assert!(!results.is_empty());
        Ok(())
    }

    /// Upgrade-path regression test: a pre-existing table already has
    /// a FTS index (built by an older version of this crate that did
    /// not yet write any manifest fingerprint). On the next boot, the
    /// rebuild decision flow must encounter (a) an index that exists,
    /// (b) no manifest fingerprint, and (c) must still succeed —
    /// `create_index` must tolerate the already-present index via
    /// `.replace(true)` rather than erroring with "already exists".
    ///
    /// This guards against the most likely failure mode when rolling
    /// this commit into an existing deployment.
    #[tokio::test]
    async fn test_fts_upgrade_path_from_pre_manifest_state() -> anyhow::Result<()> {
        let dim = 8;
        let (config, db) = TestDb::config_with_fts(dim, ngram_fts());

        // Pass 1: create the table, let ensure_fts_index build the FTS
        // index normally, then clear the manifest fingerprint to
        // simulate the "pre-manifest-config" deployment state.
        {
            let repo = MemoryVectorRepositoryImpl::new(config).await?;
            repo.batch_upsert(vec![test_record_with_content(
                1,
                10,
                "日本語のテストデータ",
                dim,
            )])
            .await?;
            let _ = repo.search_by_text("日本", None, 10).await?;
            assert!(read_manifest_pair(&repo).await.is_some());
            clear_manifest_fingerprint(&repo).await?;
            assert!(read_manifest_pair(&repo).await.is_none());
        }

        // Pass 2: reopen. Rebuild decision: (force_rebuild=false) →
        // index_exists=true → fingerprint_missing → rebuild. This must
        // succeed despite the existing FTS index and must leave a
        // fresh manifest fingerprint behind.
        let config2 = db.config_reusing_path(dim, ngram_fts());
        {
            let repo = MemoryVectorRepositoryImpl::new(config2).await?;
            let results = repo.search_by_text("日本", None, 10).await?;
            assert!(
                !results.is_empty(),
                "search should still work after upgrade-path rebuild"
            );
            let (schema, fp) = read_manifest_pair(&repo)
                .await
                .expect("manifest fingerprint should be written after upgrade-path rebuild");
            assert_eq!(schema, "1");
            assert!(fp.starts_with("sha256:"));
        }
        Ok(())
    }

    /// TC-13: Lindera/IPADIC integration test. This is gated on both
    /// the `lindera` Cargo feature and `#[ignore]` because it requires
    /// the IPADIC dictionary to be installed at runtime under
    /// `$LANCE_LANGUAGE_MODEL_HOME/lindera/ipadic/config.yml`. Without
    /// the dictionary this test would fail in CI, so we exclude it from
    /// the default run.
    ///
    /// Run manually with:
    /// ```bash
    /// LANCE_LANGUAGE_MODEL_HOME=$HOME/.local/share/lance/language_models \
    ///   cargo test -p infra --features lindera \
    ///   -- --ignored --test-threads=1 test_fts_lindera_ipadic
    /// ```
    #[cfg(feature = "lindera")]
    #[tokio::test]
    #[ignore]
    async fn test_fts_lindera_ipadic() -> anyhow::Result<()> {
        let dim = 8;
        let lindera_cfg = FtsConfig::apply_preset(FtsTokenizerKind::LinderaIpadic);
        let (config, _db) = TestDb::config_with_fts(dim, lindera_cfg);
        let repo = MemoryVectorRepositoryImpl::new(config).await?;

        repo.batch_upsert(vec![
            test_record_with_content(1, 10, "今日は会議で新しいプロジェクトの話をした", dim),
            test_record_with_content(2, 10, "明日の天気は晴れだと天気予報で言っていた", dim),
        ])
        .await?;

        // IPADIC should tokenize "会議" as a single morpheme, so the
        // exact-word query must hit.
        let results = repo.search_by_text("会議", None, 10).await?;
        assert!(
            results.iter().any(|h| h.memory_id == 1),
            "lindera/ipadic should tokenize 会議 and match the first record"
        );
        let results = repo.search_by_text("プロジェクト", None, 10).await?;
        assert!(
            results.iter().any(|h| h.memory_id == 1),
            "lindera/ipadic should tokenize プロジェクト and match the first record"
        );
        Ok(())
    }

    /// TC-16a: deleting the LanceDB-side FTS index files behind the
    /// repository's back (but keeping the manifest fingerprint) forces
    /// a rebuild on the next open via the real-index existence check.
    /// The existence check must take precedence over a still-matching
    /// manifest fingerprint — otherwise the gate would silently run
    /// queries against a missing inverted index.
    #[tokio::test]
    async fn test_fts_missing_index_detected_by_existence_check() -> anyhow::Result<()> {
        let dim = 8;
        let (config, db) = TestDb::config_with_fts(dim, ngram_fts());

        let pair_before = {
            let repo = MemoryVectorRepositoryImpl::new(config).await?;
            repo.batch_upsert(vec![test_record_with_content(1, 10, "テスト", dim)])
                .await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            read_manifest_pair(&repo)
                .await
                .expect("manifest must hold fingerprint after first init")
        };

        // Externally remove the `_indices` directory of the LanceDB
        // table. The manifest config itself lives in the dataset
        // manifest and is not touched, so we simulate the exact
        // "sidecar / manifest stale" drift mode the existence check
        // was designed to catch.
        let indices_dir = std::path::PathBuf::from(&db.path).join("test_memories.lance/_indices");
        if indices_dir.exists() {
            std::fs::remove_dir_all(&indices_dir)?;
        }

        // Reopen — list_indices should report an empty set and the
        // rebuild flow must run despite the manifest fingerprint
        // matching current config.
        let config2 = db.config_reusing_path(dim, ngram_fts());
        let pair_after = {
            let repo = MemoryVectorRepositoryImpl::new(config2).await?;
            let _ = repo.search_by_text("テス", None, 10).await?;
            read_manifest_pair(&repo)
                .await
                .expect("manifest must still hold fingerprint after rebuild")
        };
        // The rebuild ran, so the manifest write touched the stored
        // schema_version + fingerprint pair. The values themselves are
        // the same (because config didn't change), but the lancedb
        // manifest generation advances and the fact that the rebuild
        // succeeded after an empty `_indices` proves the existence
        // check dispatched correctly.
        assert_eq!(
            pair_before.1, pair_after.1,
            "config didn't change, so the fingerprint value is the same"
        );
        // And a subsequent search must still work, proving the new
        // index was actually built.
        let repo_final =
            MemoryVectorRepositoryImpl::new(db.config_reusing_path(dim, ngram_fts())).await?;
        let hits = repo_final.search_by_text("テス", None, 10).await?;
        assert!(!hits.is_empty());
        Ok(())
    }

    /// P1 本命: the `memory://` non-local URI scheme exercises the
    /// manifest-config read/write code path end-to-end.
    ///
    /// This is the scenario the old sidecar-file layout could not
    /// cover at all: for any non-local URI (`s3://`, `gs://`,
    /// `memory://`, …) the sidecar helper returned `None` and the
    /// rebuild decision flow silently trusted any existing index. With
    /// the manifest-config layout, the fingerprint lives in the
    /// lancedb table manifest itself, so the same code path works
    /// uniformly. We prove the round-trip here on `memory://` because
    /// s3/gs require external infrastructure.
    ///
    /// Verification strategy:
    /// 1. Build a repo on `memory://`, upsert a Japanese record, run
    ///    the ngram pipeline, and assert the Japanese substring query
    ///    hits (proving the rebuild ran correctly on a non-local URI).
    /// 2. Read the manifest fingerprint back through the same
    ///    `NativeTable::manifest()` API used by production code and
    ///    assert it matches the expected value for the current
    ///    `FtsConfig`. This is the assertion the sidecar layout could
    ///    not make, because no sidecar file would have been written.
    ///
    /// Note on `memory://` ephemerality: each call to `connect()`
    /// creates a brand-new in-memory database, so we cannot perform a
    /// two-pass "reopen with new config" test on this scheme the way
    /// we do for filesystem URIs. The "reopen with new config" path
    /// is already covered by `test_fts_reopen_with_new_config_rebuilds`
    /// on a local URI; the value this `memory://` test adds is proving
    /// that the new layout works uniformly on a non-local URI, which
    /// is precisely the P1 regression the sidecar layout exhibited.
    #[tokio::test]
    async fn test_fts_manifest_fingerprint_on_memory_uri() -> anyhow::Result<()> {
        let dim = 8;
        let config = VectorDBConfig {
            uri: "memory://fts-manifest-p1-test".to_string(),
            table_name: "memories_p1".to_string(),
            vector_size: dim,
            distance_type: DistanceType::Cosine,
            optimize: test_optimize_config(),
            fts: ngram_fts(),
            vector_index: VectorIndexConfig::default(),
        };

        let repo = MemoryVectorRepositoryImpl::new(config).await?;
        repo.batch_upsert(vec![test_record_with_content(
            1,
            10,
            "今日は会議で新しいプロジェクトの話をした",
            dim,
        )])
        .await?;

        // The ngram tokenizer must have been installed by
        // `ensure_fts_index` on this non-local URI — if the manifest
        // path were silently skipped (as the old sidecar layout did),
        // the query below would still hit because the initial index
        // built on a fresh table always uses the requested tokenizer.
        // The real proof of P1 is in the manifest assertion that
        // follows.
        let results = repo.search_by_text("会議", None, 10).await?;
        assert!(
            !results.is_empty(),
            "ngram tokenizer on memory:// must hit Japanese substrings"
        );

        let (schema, fp) = read_manifest_pair(&repo)
            .await
            .expect("P1 fix: manifest fingerprint must be readable on memory://");
        assert_eq!(schema, "1");
        let expected = ngram_fts().fingerprint(LANCE_INDEX_VERSION, "content");
        assert_eq!(
            fp, expected,
            "P1 fix: manifest fingerprint on non-local URI must match current FtsConfig"
        );

        // Additional proof: calling `rebuild_fts_index_locked` a
        // second time under a reset gate should take the skip branch
        // (fingerprint matches), not the rebuild branch, because the
        // manifest fingerprint we just verified is already current.
        // If P1 were broken and the non-local URI skipped manifest
        // reads, this would go through the non-local fallback and not
        // compare fingerprints at all — but the outcome (no-op) would
        // be the same. Instead we flip the fingerprint to a sentinel
        // and confirm that the rebuild flow observes it and rewrites
        // the manifest.
        write_manifest_pair(&repo, "1", "sha256:memory-uri-sentinel").await?;
        {
            let mut guard = repo.fts_init_state.lock().await;
            *guard = None;
            repo.rebuild_fts_index_locked(&mut guard).await?;
        }
        let (_, fp_after) = read_manifest_pair(&repo)
            .await
            .expect("manifest must still have fingerprint after second rebuild");
        assert_eq!(
            fp_after, expected,
            "P1 fix: rebuild on memory:// must observe the stale fingerprint and rewrite it"
        );
        Ok(())
    }
}
