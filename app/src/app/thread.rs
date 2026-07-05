use anyhow::Result;
use async_trait::async_trait;
use infra::error::LlmMemoryError;
use infra::infra::media_object::rdb::{MediaObjectRepository, MediaObjectRepositoryImpl};
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl, UseMemoryRepository};
use infra::infra::memory_rating::rdb::{
    MemoryRatingRepository, MemoryRatingRepositoryImpl, UseMemoryRatingRepository,
};
use infra::infra::thread::rdb::{
    ThreadRepository, ThreadRepositoryImpl, ThreadSort, UseThreadRepository,
};
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use infra::infra::thread_label::rows::LabelWithCountRow;
use infra::infra::thread_memory::rdb::{
    ThreadMemoryRepository, ThreadMemoryRepositoryImpl, UseThreadMemoryRepository,
};
use infra_utils::infra::rdb::UseRdbPool;
use memory_utils::cache::stretto::UseMemoryCache;
use memory_utils::lock::RwLockWithKey;
use protobuf::llm_memory::data::{
    MediaObjectId, Memory, MemoryData, MemoryId, MessageRole, Thread, ThreadData, ThreadId, UserId,
};
use std::collections::HashSet;
use std::{sync::Arc, time::Duration};
use stretto::TokioCache;

/// Common time-range and sort options for the thread list endpoints
/// (`find_thread_list_by_user_id`, `find_threads_by_labels`).
/// `Default` matches the pre-P8 hard-coded behaviour: no time filter and
/// `updated_at DESC` ordering. Adding new optional knobs to this struct is
/// preferred over growing the function signature again.
#[derive(Debug, Clone, Copy, Default)]
pub struct ThreadListOptions {
    pub created_after: Option<i64>,
    pub created_before: Option<i64>,
    pub updated_after: Option<i64>,
    pub updated_before: Option<i64>,
    pub sort: ThreadSort,
}

impl ThreadListOptions {
    /// True iff any of the four time-range bounds is populated.
    fn has_time_filter(&self) -> bool {
        self.created_after.is_some()
            || self.created_before.is_some()
            || self.updated_after.is_some()
            || self.updated_before.is_some()
    }

    /// True iff `thread`'s timestamps satisfy every populated bound. The
    /// bound semantics match the proto contract: `*_after` is strict
    /// (`>`), `*_before` is inclusive (`<=`).
    fn matches_time_range(&self, thread: &Thread) -> bool {
        let Some(d) = thread.data.as_ref() else {
            return false;
        };
        if self
            .created_after
            .is_some_and(|after| d.created_at <= after)
        {
            return false;
        }
        if self
            .created_before
            .is_some_and(|before| d.created_at > before)
        {
            return false;
        }
        if self
            .updated_after
            .is_some_and(|after| d.updated_at <= after)
        {
            return false;
        }
        if self
            .updated_before
            .is_some_and(|before| d.updated_at > before)
        {
            return false;
        }
        true
    }
}

/// Sort threads in-place per `ThreadSort`. Snowflake `id` is used as a
/// stable tiebreaker for every variant so paginated callers see a
/// deterministic order even when timestamps collide.
fn sort_threads_in_place(threads: &mut [Thread], sort: ThreadSort) {
    use std::cmp::Reverse;
    let updated_at = |t: &Thread| t.data.as_ref().map(|d| d.updated_at).unwrap_or(0);
    let created_at = |t: &Thread| t.data.as_ref().map(|d| d.created_at).unwrap_or(0);
    let id = |t: &Thread| t.id.as_ref().map(|i| i.value).unwrap_or(0);
    match sort {
        ThreadSort::UpdatedDesc => threads.sort_by_key(|t| Reverse((updated_at(t), id(t)))),
        ThreadSort::UpdatedAsc => threads.sort_by_key(|t| (updated_at(t), id(t))),
        ThreadSort::CreatedDesc => threads.sort_by_key(|t| Reverse((created_at(t), id(t)))),
        ThreadSort::CreatedAsc => threads.sort_by_key(|t| (created_at(t), id(t))),
        ThreadSort::IdDesc => threads.sort_by_key(|t| Reverse(id(t))),
    }
}

/// Default upper bound on ancestor-traversal depth. Chosen to comfortably
/// exceed realistic conversation + ROLE_SYSTEM chain lengths while still
/// cutting off runaway parent_ids cycles.
///
/// Semantics: `max_depth = N` means "walk at most N hops away from the
/// starting memory in the parent direction". `max_depth = 1` therefore
/// returns the starting memory *and* its direct parents. The starting
/// memory itself never counts toward the hop budget.
pub const DEFAULT_ANCESTOR_MAX_DEPTH: u32 = 1024;

/// Hard upper bound for caller-supplied `max_depth` values. The gRPC handler
/// clamps client requests to this ceiling to avoid a crafted request that
/// asks the server to walk an unbounded chain.
pub const HARD_ANCESTOR_MAX_DEPTH: u32 = 4096;

/// Validate that `Thread.metadata` is a syntactically well-formed JSON
/// document. The proto contract accepts any JSON value; storage decides
/// the rest (PostgreSQL canonicalises via JSONB, SQLite stores text
/// verbatim). Validating at the app boundary keeps the two backends in
/// sync — without this, malformed JSON would surface as
/// `invalid_argument` on Postgres (JSONB parse error) and as a silently
/// stored garbage string on SQLite, splitting the same RPC into a
/// success/failure pair depending on backend.
fn validate_metadata(metadata: Option<&str>) -> Result<()> {
    let Some(raw) = metadata else { return Ok(()) };
    if raw.is_empty() {
        return Err(LlmMemoryError::InvalidArgument(
            "thread.metadata must be a JSON document, not an empty string".to_string(),
        )
        .into());
    }
    serde_json::from_str::<serde_json::Value>(raw).map_err(|e| {
        LlmMemoryError::InvalidArgument(format!("thread.metadata is not valid JSON: {e}"))
    })?;
    Ok(())
}

/// Validate and normalize label strings.
fn validate_labels(labels: &[String]) -> Result<Vec<String>> {
    let mut seen = HashSet::with_capacity(labels.len());
    let mut validated = Vec::with_capacity(labels.len());
    for label in labels {
        let trimmed = label.trim().to_string();
        if trimmed.is_empty() {
            anyhow::bail!("Label must not be empty");
        }
        if trimmed.len() > 512 {
            anyhow::bail!("Label must not exceed 512 characters");
        }
        if seen.insert(trimmed.clone()) {
            validated.push(trimmed);
        }
    }
    Ok(validated)
}

/// A Memory paired with its position in a thread's `thread_memory` junction.
/// Returned by `ThreadApp::resolve_ancestor_closure`.
#[derive(Debug, Clone)]
pub struct MemoryWithPosition {
    pub memory: Memory,
    pub position: i32,
}

/// Outcome of `update_memory_parent_ids_with_guards`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateMemoryParentsOutcome {
    Rewired,
    Skipped(UpdateParentsSkipReason),
}

/// Reason a guarded `update_memory_parent_ids` call was a no-op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateParentsSkipReason {
    /// Memory is attached to more than one thread, blocked unless
    /// the caller passes `force_overwrite_when_shared`.
    SharedMemory,
    /// Memory's current `parent_ids` is non-empty, blocked unless
    /// the caller passes `force_overwrite_when_non_empty`.
    AlreadyHasParents,
}

/// Per-input record consumed by `ThreadApp::add_memories_batch`.
///
/// `memory.parent_ids` MUST be empty; references to parents are
/// expressed via `parent_external_ids` and resolved server-side.
#[derive(Debug, Clone)]
pub struct BatchMemoryInput {
    pub memory: MemoryData,
    pub parent_external_ids: Vec<String>,
}

/// Per-input outcome returned from `ThreadApp::add_memories_batch`.
#[derive(Debug, Clone)]
pub struct AddMemoryOutcome {
    pub memory_id: MemoryId,
    pub created: bool,
    pub position: i32,
    pub existing_parent_ids_empty: bool,
    pub resolved_parent_ids: Vec<MemoryId>,
}

/// How `add_memories_batch` should resolve the target thread.
#[derive(Debug, Clone)]
pub enum BatchThreadTarget {
    /// Use an already-known thread by id.
    ExistingThreadId(ThreadId),
    /// Upsert a thread keyed by `(user_id, channel)`. New thread metadata
    /// (description, default_system_memory_id, ...) is taken from
    /// `thread_data` only when creating a new row.
    UpsertByChannel(ThreadData),
}

/// Inputs passed to `ThreadApp::add_memories_batch`. Keeping the bag of
/// flags in a struct keeps the trait signature stable as Phase 2 adds
/// more knobs.
#[derive(Debug, Clone)]
pub struct AddMemoriesBatchInput {
    pub thread_target: BatchThreadTarget,
    pub memories: Vec<BatchMemoryInput>,
    pub upsert_by_external_id: bool,
    /// 0 means "no override".
    pub thread_updated_at_override: i64,
    pub labels: Vec<String>,
}

/// `Some((media_object.kind, storage_backend))` of the row-locked,
/// ref-bumped media_object linked to a freshly inserted memory, else
/// `None` (text-only). Lets the post-commit dispatch evaluate the media
/// (image) axis without re-reading the media_object.
pub type MediaDispatchHint = Option<(i32, String)>;

/// A newly INSERTed memory plus its media dispatch hint, handed to the
/// gRPC layer for post-commit embedding dispatch.
pub type NewMemoryForEmbedding = (MemoryId, MemoryData, MediaDispatchHint);

/// Result returned from `ThreadApp::add_memories_batch`.
#[derive(Debug, Clone)]
pub struct AddMemoriesBatchOutput {
    pub thread_id: ThreadId,
    /// false means an existing thread (matched by channel) was reused.
    pub thread_created: bool,
    pub outcomes: Vec<AddMemoryOutcome>,
    /// Memories that need (re-)embedding after commit: newly INSERTed
    /// ones plus those whose content was overwritten via
    /// `upsert_by_external_id`. Used by the gRPC layer to fire embedding
    /// dispatches after commit.
    pub new_memories_for_embedding: Vec<NewMemoryForEmbedding>,
}

/// Result of walking back the `parent_ids` chain from a starting memory.
///
/// `ordered_memories` contains every Memory reachable from the start node
/// (including the start itself), deduplicated and sorted by
/// `thread_memory.position` ascending. `system_memory` is the Memory with
/// `role == ROLE_SYSTEM` that has the largest position inside the closure,
/// or `None` if the closure contains no ROLE_SYSTEM entry at all.
#[derive(Debug, Clone, Default)]
pub struct AncestorClosure {
    pub ordered_memories: Vec<MemoryWithPosition>,
    pub system_memory: Option<MemoryWithPosition>,
}

#[async_trait]
pub trait ThreadApp:
    UseThreadRepository
    + UseThreadMemoryRepository
    + UseMemoryRepository
    + UseMemoryRatingRepository
    + super::memory::UseThreadFilterResolver
    + UseMemoryCache<Arc<String>, Thread>
    + Send
    + Sync
    + Sized
    + 'static
{
    fn thread_vector_app(&self) -> Option<&super::thread_vector::ThreadVectorAppImpl> {
        None
    }

    /// Media wiring (image memory feature). `None` = non-media
    /// deployment / env-less unit test, then add/delete leave
    /// `media_object` untouched (original behaviour).
    fn media_subsystem(&self) -> Option<&super::memory::MediaSubsystem> {
        None
    }

    fn thread_embedding_dispatcher(
        &self,
    ) -> Option<&Arc<infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher>> {
        None
    }

    async fn create_thread(&self, thread: &ThreadData) -> Result<ThreadId> {
        validate_metadata(thread.metadata.as_deref())?;
        let db = self.thread_repository().db_pool();
        let mut tx = db.begin().await.map_err(LlmMemoryError::DBError)?;
        lock_default_system_memory_scope_tx(&mut tx, thread.default_system_memory_id).await?;
        // Phase 2: validate / normalise default_system_memory_id (must be a
        // ROLE_SYSTEM Memory, or NULL/Some(0)).
        let normalized_default = validate_default_system_memory_id_tx(
            self.memory_repository(),
            &mut tx,
            thread.default_system_memory_id,
        )
        .await?;
        let mut to_insert = thread.clone();
        to_insert.default_system_memory_id = normalized_default;
        let id = self
            .thread_repository()
            .create(&mut *tx, &to_insert)
            .await?;

        // Anchor the default system memory in the junction so that
        // delete_thread's orphan check (NOT EXISTS thread_memory) does
        // not physically delete a system memory that another thread may
        // still reference via default_system_memory_id.
        if let Some(default_id) = normalized_default {
            let now = command_utils::util::datetime::now_millis();
            self.thread_memory_repository()
                .insert_or_ignore_auto_position_tx(&mut *tx, id.value, default_id, now)
                .await?;
        }

        if !to_insert.labels.is_empty() {
            let validated = validate_labels(&to_insert.labels)?;
            let now = command_utils::util::datetime::now_millis();
            for label in &validated {
                self.thread_label_repository()
                    .add_labels_tx(&mut *tx, id.value, label, now)
                    .await?;
            }
        }

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        // Fire-and-forget: dispatch thread description embedding job
        if let Some(dispatcher) = self.thread_embedding_dispatcher()
            && let Some(desc) = &thread.description
        {
            infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher::spawn_dispatch(
                dispatcher, id.value, desc,
            );
        }

        // NOTE: LanceDB scalar sync (labels) is deferred until the
        // embedding job completes and the LanceDB record exists; the
        // next update_thread or label RPC will pick it up.

        Ok(id)
    }

    async fn update_thread(&self, id: &ThreadId, thread: &Option<ThreadData>) -> Result<bool> {
        if let Some(w) = thread {
            validate_metadata(w.metadata.as_deref())?;
            let pool = self.thread_repository().db_pool();
            let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;
            lock_default_system_memory_scope_tx(&mut tx, w.default_system_memory_id).await?;

            // Reject user_id changes. This service treats `thread.user_id`
            // as thread metadata chosen at creation time, so mutating it
            // later would make historical filtering semantics ambiguous.
            let existing = self
                .thread_repository()
                .find_row_for_update_tx(&mut *tx, id)
                .await?
                .ok_or_else(|| {
                    LlmMemoryError::NotFound(format!("thread not found: {}", id.value))
                })?;
            let new_user_id = w.user_id.map(|u| u.value).unwrap_or(0);
            let old_user_id = existing.user_id;
            if new_user_id != old_user_id {
                return Err(LlmMemoryError::InvalidArgument(format!(
                    "cannot change thread user_id from {old_user_id} to {new_user_id}: \
                     thread user_id is immutable once created"
                ))
                .into());
            }

            // Phase 2: same validation as create_thread.
            let normalized_default = validate_default_system_memory_id_tx(
                self.memory_repository(),
                &mut tx,
                w.default_system_memory_id,
            )
            .await?;
            let mut to_update = w.clone();
            to_update.default_system_memory_id = normalized_default;
            let updated = self
                .thread_repository()
                .update(&mut *tx, id, &to_update)
                .await?;
            if !updated {
                return Err(LlmMemoryError::NotFound(format!(
                    "thread {} was deleted by a concurrent transaction",
                    id.value
                ))
                .into());
            }

            // Anchor the new default in the junction so delete_thread's
            // orphan check knows it is still referenced.
            //
            // Note: we intentionally do NOT detach the old default when
            // the default_system_memory_id changes or is cleared. Past
            // messages may still reference the old ROLE_SYSTEM memory via
            // parent_ids, and the junction is the sole source of truth
            // for ResolveAncestorClosure / FindMemoriesByThreadId.
            // Removing it would silently break history reconstruction.
            // The old default stays in the junction and is cleaned up
            // only when the thread itself is deleted (orphan check).
            if let Some(default_id) = normalized_default {
                let now = command_utils::util::datetime::now_millis();
                self.thread_memory_repository()
                    .insert_or_ignore_auto_position_tx(&mut *tx, id.value, default_id, now)
                    .await?;
            }

            // Replace labels (PUT semantics: always replace, empty = clear all)
            {
                let validated = if !w.labels.is_empty() {
                    Some(validate_labels(&w.labels)?)
                } else {
                    None
                };
                let now = command_utils::util::datetime::now_millis();
                self.thread_label_repository()
                    .delete_by_thread_tx(&mut *tx, id.value)
                    .await?;
                if let Some(validated) = validated {
                    for label in &validated {
                        self.thread_label_repository()
                            .add_labels_tx(&mut *tx, id.value, label, now)
                            .await?;
                    }
                }
            }

            tx.commit().await.map_err(LlmMemoryError::DBError)?;
            let k = Arc::new(Self::find_cache_key(&id.value));
            let _ = self.delete_cache(&k).await;

            // Best-effort vector sync after update
            {
                let desc_changed = w.description != existing.description;
                if desc_changed {
                    let new_desc = w.description.as_deref().unwrap_or("");
                    if new_desc.is_empty() {
                        // Description cleared — remove stale LanceDB record
                        if let Some(tva) = self.thread_vector_app()
                            && let Err(e) = tva.delete_thread_vector(id.value).await
                        {
                            tracing::warn!(
                                "delete_thread_vector after description cleared failed: {e}"
                            );
                        }
                    } else if let Some(dispatcher) = self.thread_embedding_dispatcher() {
                        // Description changed to non-empty — re-generate embedding
                        infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher::spawn_dispatch(
                            dispatcher,
                            id.value,
                            new_desc,
                        );
                    }
                }
                // Sync scalar columns (labels, channel, etc.) unless the
                // vector record was just deleted (empty description).
                let vector_deleted =
                    desc_changed && w.description.as_deref().unwrap_or("").is_empty();
                if !vector_deleted
                    && let Some(tva) = self.thread_vector_app()
                    && let Err(e) = tva.sync_thread_scalars(id.value).await
                {
                    tracing::warn!("sync_thread_scalars after update_thread failed: {e}");
                }
            }

            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete a thread and its exclusive memories.
    /// Returns `(deleted, exclusive_memory_ids)` so callers can cascade to external stores.
    async fn delete_thread(&self, id: &ThreadId) -> Result<(bool, Vec<i64>)> {
        let pool = self.thread_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        // 0. Lock the thread row first. On PostgreSQL this is a row-level
        //    FOR UPDATE lock that blocks concurrent add_memory from
        //    reading and inserting into a thread that is about to be
        //    deleted. On SQLite this is a plain SELECT (single-writer
        //    semantics already serialise writes).
        let _thread = self
            .thread_repository()
            .find_row_for_update_tx(&mut *tx, id)
            .await?;

        // 1. Get all memory IDs for this thread from thread_memory
        let all_memory_ids = self
            .thread_memory_repository()
            .find_memory_ids_by_thread_tx(&mut *tx, id.value)
            .await?;

        // 2. Lock the current member memories before removing the junction
        //    rows. PostgreSQL uses `FOR UPDATE` under the hood so a sibling
        //    AddMemory that tries to attach one of these memories into
        //    another thread must wait until this delete transaction has
        //    either deleted the row or committed the detach.
        let candidate_ids: Vec<i64> = all_memory_ids.iter().map(|mid| mid.value).collect();
        let locked_candidates = self
            .memory_repository()
            .find_by_ids_for_update_tx(&mut tx, &candidate_ids)
            .await?;
        // memory_id -> media_object_id from the row-locked pre-delete
        // rows, used after the orphan delete to decrement only the media
        // of memories that were actually deleted (a shared memory keeps
        // its media reference).
        let media_by_memory: std::collections::HashMap<i64, i64> = locked_candidates
            .iter()
            .filter_map(|m| {
                let mid = m.id.as_ref()?.value;
                let moid = m.data.as_ref()?.media_object_id?.value;
                Some((mid, moid))
            })
            .collect();

        // 3. Delete thread_memory junction entries first (safe for future FK constraints)
        self.thread_memory_repository()
            .delete_by_thread_tx(&mut *tx, id.value)
            .await?;

        // 4. Delete only those candidate memories that are still orphaned
        //    *at delete time*. This avoids relying on a stale exclusivity
        //    snapshot taken before another transaction could share the same
        //    memory through `thread_memory`.
        let exclusive_ids = self
            .memory_repository()
            .delete_orphaned_by_ids_tx(&mut tx, &candidate_ids)
            .await?;
        for mid in &exclusive_ids {
            self.memory_rating_repository()
                .delete_by_memory_id_tx(&mut *tx, *mid)
                .await?;
        }

        // 4a. Decrement media_object.ref_count for every deleted memory
        //     that referenced one, in this same tx — without it the
        //     image leaks (ref_count never returns to 0, GC never
        //     reclaims). Only the actually-deleted ids are decremented;
        //     a shared memory (not in exclusive_ids) keeps its media
        //     reference. The claim winner's storage+row delete runs
        //     after commit. Inert when media is not wired.
        let mut post_delete: Vec<(i64, infra::infra::media_object::rdb::MediaObjectRow)> =
            Vec::new();
        if let Some(media) = self.media_subsystem() {
            for mid in &exclusive_ids {
                let Some(&moid) = media_by_memory.get(mid) else {
                    continue;
                };
                media
                    .repository()
                    .find_by_id_for_update_tx(&mut tx, moid)
                    .await?;
                if let Some(job) =
                    super::memory::decr_and_maybe_claim(media.repository(), &mut tx, moid).await?
                {
                    post_delete.push(job);
                }
            }
        }

        // 4b. Delete thread_label junction entries.
        self.thread_label_repository()
            .delete_by_thread_tx(&mut *tx, id.value)
            .await?;

        // 5. Delete the thread row itself.
        let result = self.thread_repository().delete_tx(&mut *tx, id).await?;

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        // Post-commit: the claim winner runs the storage+row delete for
        // each media_object whose ref_count reached 0 (best-effort;
        // finish_delete marks deleted-failed on error and the GC retries).
        if let Some(media) = self.media_subsystem() {
            for (moid, row) in post_delete {
                let _ = media.finalizer().finish_delete(moid, &row).await;
            }
        }

        // 6. Clear memory caches (both exclusive and shared - shared might have stale thread refs)
        for mid in &all_memory_ids {
            let k = Arc::new(memory_cache_key(&mid.value));
            let _ = self.delete_memory_cache(&k).await;
        }

        // 7. Clear thread cache
        let k = Arc::new(Self::find_cache_key(&id.value));
        let _ = self.delete_cache(&k).await;

        // 8. Best-effort delete thread vector from LanceDB
        if let Some(tva) = self.thread_vector_app()
            && let Err(e) = tva.delete_thread_vector(id.value).await
        {
            tracing::warn!(
                "best-effort vector delete failed for thread {}: {e}",
                id.value
            );
        }

        Ok((result, exclusive_ids))
    }

    fn find_cache_key(id: &i64) -> String {
        ["thread_id:", &id.to_string()].join("")
    }

    /// Find a thread by ID, with optional cache.
    ///
    /// `hydrate_labels_single` is called on every hit (including cache hits)
    /// because labels can change independently of thread data via add_labels/remove_labels.
    /// This trades one extra DB query per call for label correctness.
    async fn find_thread(&self, id: &ThreadId, ttl: Option<&Duration>) -> Result<Option<Thread>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_cache_key(&id.value));
        let thread = self
            .with_cache_if_some(&k, ttl, || async {
                self.thread_repository().find(id).await
            })
            .await?;
        match thread {
            Some(mut t) => {
                self.hydrate_labels_single(&mut t).await?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    async fn find_thread_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<Thread>>
    where
        Self: Send + 'static,
    {
        let mut threads = self.thread_repository().find_list(limit, offset).await?;
        self.hydrate_labels_batch(&mut threads).await?;
        Ok(threads)
    }

    async fn find_thread_list_by_user_id(
        &self,
        user_id: UserId,
        limit: Option<&i32>,
        offset: Option<&i64>,
        opts: ThreadListOptions,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<Thread>>
    where
        Self: Send + 'static,
    {
        let mut threads = self
            .thread_repository()
            .find_by_user_id(
                user_id,
                limit,
                offset,
                opts.created_after,
                opts.created_before,
                opts.updated_after,
                opts.updated_before,
                opts.sort,
            )
            .await?;
        self.hydrate_labels_batch(&mut threads).await?;
        Ok(threads)
    }

    /// Hydrate labels for a single thread.
    async fn hydrate_labels_single(&self, thread: &mut Thread) -> Result<()> {
        if let (Some(id), Some(data)) = (&thread.id, &mut thread.data) {
            data.labels = self
                .thread_label_repository()
                .find_labels_by_thread(id.value)
                .await?;
        }
        Ok(())
    }

    /// Hydrate labels for a batch of threads (avoids N+1 queries).
    async fn hydrate_labels_batch(&self, threads: &mut [Thread]) -> Result<()> {
        let thread_ids: Vec<i64> = threads
            .iter()
            .filter_map(|t| t.id.as_ref().map(|id| id.value))
            .collect();
        if thread_ids.is_empty() {
            return Ok(());
        }
        let label_rows = self
            .thread_label_repository()
            .find_labels_by_thread_ids(&thread_ids)
            .await?;
        let mut labels_map: std::collections::HashMap<i64, Vec<String>> =
            std::collections::HashMap::new();
        for row in label_rows {
            labels_map.entry(row.thread_id).or_default().push(row.label);
        }
        for thread in threads.iter_mut() {
            if let (Some(id), Some(data)) = (&thread.id, &mut thread.data) {
                data.labels = labels_map.remove(&id.value).unwrap_or_default();
            }
        }
        Ok(())
    }

    // ===== Label operations =====

    /// Core logic for adding labels within an existing transaction.
    /// Does NOT update Thread.updated_at or commit.
    async fn add_labels_core_tx(
        &self,
        tx: &mut infra_utils::infra::rdb::RdbTransaction<'_>,
        thread_id: &ThreadId,
        labels: &[String],
        now: i64,
    ) -> Result<()> {
        let _thread = self
            .thread_repository()
            .find_row_for_update_tx(&mut **tx, thread_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("thread not found: {}", thread_id.value))
            })?;

        for label in labels {
            self.thread_label_repository()
                .add_labels_tx(&mut **tx, thread_id.value, label, now)
                .await?;
        }
        Ok(())
    }

    /// Add labels to a thread (idempotent).
    /// All operations run in a single transaction to prevent orphan labels
    /// from concurrent delete_thread.
    async fn add_labels(&self, thread_id: &ThreadId, labels: &[String]) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }
        let validated = validate_labels(labels)?;

        let pool = self.thread_repository().db_pool();
        let now = command_utils::util::datetime::now_millis();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        self.add_labels_core_tx(&mut tx, thread_id, &validated, now)
            .await?;

        self.thread_repository()
            .update_updated_at_tx(&mut *tx, thread_id, now)
            .await?;

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        let k = Arc::new(Self::find_cache_key(&thread_id.value));
        let _ = self.delete_cache(&k).await;

        if let Some(tva) = self.thread_vector_app()
            && let Err(e) = tva.sync_thread_scalars(thread_id.value).await
        {
            tracing::warn!("sync_thread_scalars after add_labels failed: {e}");
        }

        Ok(())
    }

    /// Add labels without updating Thread.updated_at.
    /// Used by the importer to control timestamps explicitly.
    async fn add_labels_only(&self, thread_id: &ThreadId, labels: &[String]) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }
        let validated = validate_labels(labels)?;

        let pool = self.thread_repository().db_pool();
        let now = command_utils::util::datetime::now_millis();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        self.add_labels_core_tx(&mut tx, thread_id, &validated, now)
            .await?;

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        let k = Arc::new(Self::find_cache_key(&thread_id.value));
        let _ = self.delete_cache(&k).await;

        if let Some(tva) = self.thread_vector_app()
            && let Err(e) = tva.sync_thread_scalars(thread_id.value).await
        {
            tracing::warn!("sync_thread_scalars after add_labels_only failed: {e}");
        }

        Ok(())
    }

    /// Remove labels from a thread (idempotent).
    /// All operations run in a single transaction to prevent orphan labels
    /// from concurrent delete_thread.
    async fn remove_labels(&self, thread_id: &ThreadId, labels: &[String]) -> Result<()> {
        if labels.is_empty() {
            return Ok(());
        }

        let trimmed: Vec<String> = labels.iter().map(|l| l.trim().to_string()).collect();
        let pool = self.thread_repository().db_pool();
        let now = command_utils::util::datetime::now_millis();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        // Lock thread row — prevents concurrent deletion
        let _thread = self
            .thread_repository()
            .find_row_for_update_tx(&mut *tx, thread_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("thread not found: {}", thread_id.value))
            })?;

        // Delete labels within the same transaction
        self.thread_label_repository()
            .remove_labels_tx(&mut *tx, thread_id.value, &trimmed)
            .await?;

        // Bump updated_at
        self.thread_repository()
            .update_updated_at_tx(&mut *tx, thread_id, now)
            .await?;

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        let k = Arc::new(Self::find_cache_key(&thread_id.value));
        let _ = self.delete_cache(&k).await;

        // Best-effort sync labels to LanceDB
        if let Some(tva) = self.thread_vector_app()
            && let Err(e) = tva.sync_thread_scalars(thread_id.value).await
        {
            tracing::warn!("sync_thread_scalars after remove_labels failed: {e}");
        }

        Ok(())
    }

    /// Get labels for a thread.
    async fn find_labels(&self, thread_id: &ThreadId) -> Result<Vec<String>> {
        self.thread_label_repository()
            .find_labels_by_thread(thread_id.value)
            .await
    }

    /// Find threads by label match.
    ///
    /// When `opts` carries no time-range filter and the default sort, the
    /// SQL `LIMIT/OFFSET` is applied directly inside the labels query —
    /// that path matches the pre-P8 fast path one-for-one.
    ///
    /// When time filtering or a non-default sort is requested, the
    /// pagination contract requires considering matches beyond the first
    /// SQL slice (otherwise pages would be under-filled or skip valid
    /// rows). We then over-fetch up to `MEMORY_THREAD_FILTER_INTERMEDIATE_HARD_LIMIT`
    /// candidates without `LIMIT/OFFSET`, apply the time-range filter and
    /// sort in-process, and slice the final `limit/offset` window. A
    /// candidate set that overflows the cap is rejected with
    /// `FailedPrecondition` so the caller is told to narrow the labels
    /// rather than silently receiving a truncated page.
    #[allow(clippy::too_many_arguments)]
    async fn find_threads_by_labels(
        &self,
        labels: &[String],
        match_all: bool,
        user_id: Option<i64>,
        limit: Option<i32>,
        offset: Option<i64>,
        opts: ThreadListOptions,
    ) -> Result<Vec<Thread>> {
        let needs_post_processing = opts.has_time_filter() || opts.sort != ThreadSort::default();

        // Fetch IDs. Fast path lets the labels SQL paginate; the post-
        // processing path over-fetches so the in-process slice is correct.
        let ids = if needs_post_processing {
            let cap = self.thread_filter_config().intermediate_hard_limit;
            let fetch_limit = cap.saturating_add(1).min(i32::MAX as i64) as i32;
            let raw = self
                .thread_label_repository()
                .find_thread_ids_by_labels(labels, match_all, user_id, Some(fetch_limit), None)
                .await?;
            if raw.len() as i64 > cap {
                return Err(LlmMemoryError::FailedPrecondition(format!(
                    "find_threads_by_labels candidate set exceeded \
                     MEMORY_THREAD_FILTER_INTERMEDIATE_HARD_LIMIT ({cap}). \
                     Tighten the label set or remove the time-range / sort \
                     options to use the SQL fast path."
                ))
                .into());
            }
            raw
        } else {
            self.thread_label_repository()
                .find_thread_ids_by_labels(labels, match_all, user_id, limit, offset)
                .await?
        };

        // Batch-fetch threads and labels
        let (threads, label_rows) = tokio::try_join!(
            self.thread_repository().find_by_ids(&ids),
            self.thread_label_repository()
                .find_labels_by_thread_ids(&ids),
        )?;

        let mut labels_map: std::collections::HashMap<i64, Vec<String>> =
            std::collections::HashMap::new();
        for row in label_rows {
            labels_map.entry(row.thread_id).or_default().push(row.label);
        }

        // Build a map for O(1) lookup while preserving the original ordering
        let mut thread_map: std::collections::HashMap<i64, Thread> = threads
            .into_iter()
            .filter_map(|t| t.id.as_ref().map(|id| (id.value, t.clone())))
            .collect();

        let mut result = Vec::with_capacity(ids.len());
        for id in &ids {
            if let Some(mut t) = thread_map.remove(id) {
                if let Some(ref mut data) = t.data {
                    data.labels = labels_map.remove(id).unwrap_or_default();
                }
                result.push(t);
            }
        }

        if !needs_post_processing {
            return Ok(result);
        }

        // The labels SQL ordered by `MAX(updated_at) DESC, tl.thread_id DESC`
        // over the full candidate set above. Apply the post-processing in
        // (filter → sort → paginate) order so pages stay correct.
        // Re-sort unconditionally (even when `opts.sort == default`) so
        // the post-processing path uses the same `(updated_at, id)`
        // tiebreaker as `find_thread_list_by_user_id`'s SQL ORDER BY —
        // otherwise tied `updated_at` rows could shuffle between the
        // two endpoints.
        if opts.has_time_filter() {
            result.retain(|t| opts.matches_time_range(t));
        }
        sort_threads_in_place(&mut result, opts.sort);
        let start = offset.unwrap_or(0).max(0) as usize;
        if start >= result.len() {
            return Ok(Vec::new());
        }
        let end = limit
            .map(|l| start.saturating_add(l.max(0) as usize).min(result.len()))
            .unwrap_or(result.len());
        Ok(result[start..end].to_vec())
    }

    /// Get distinct labels with usage count.
    ///
    /// (P9) The four optional `created_*` / `updated_*` bounds (epoch ms)
    /// pass through to the SQL layer, where they restrict the underlying
    /// `thread` rows before label aggregation. Bound semantics: `*_after`
    /// is strict (`>`), `*_before` is inclusive (`<=`); same convention as
    /// `FindThreadListByLabels` (P8).
    #[allow(clippy::too_many_arguments)]
    async fn find_distinct_labels(
        &self,
        user_id: Option<i64>,
        limit: Option<i32>,
        offset: Option<i64>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
    ) -> Result<Vec<LabelWithCountRow>> {
        self.thread_label_repository()
            .find_distinct_labels(
                user_id,
                limit,
                offset,
                created_after,
                created_before,
                updated_after,
                updated_before,
            )
            .await
    }

    /// Search labels by substring.
    ///
    /// (P9) Same time-range parameters and semantics as
    /// `find_distinct_labels`.
    #[allow(clippy::too_many_arguments)]
    async fn search_labels(
        &self,
        query: &str,
        user_id: Option<i64>,
        limit: Option<i32>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
    ) -> Result<Vec<LabelWithCountRow>> {
        self.thread_label_repository()
            .search_labels(
                query,
                user_id,
                limit,
                created_after,
                created_before,
                updated_after,
                updated_before,
            )
            .await
    }

    /// Find co-occurring labels.
    ///
    /// (P9) Time-range bounds restrict the inner candidate set (threads
    /// that match all `labels`) before co-occurrence aggregation, so the
    /// returned `thread_count` reflects the filtered population.
    #[allow(clippy::too_many_arguments)]
    async fn find_co_occurring_labels(
        &self,
        labels: &[String],
        user_id: Option<i64>,
        limit: Option<i32>,
        offset: Option<i64>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
    ) -> Result<Vec<LabelWithCountRow>> {
        self.thread_label_repository()
            .find_co_occurring_labels(
                labels,
                user_id,
                limit,
                offset,
                created_after,
                created_before,
                updated_after,
                updated_before,
            )
            .await
    }

    /// Accessor for the thread_label_repository (used in trait default methods).
    fn thread_label_repository(&self) -> &ThreadLabelRepositoryImpl;

    /// Default TTL accessor for label operations.
    fn label_default_ttl(&self) -> Duration;

    async fn add_memory(&self, thread_id: &ThreadId, memory: &MemoryData) -> Result<MemoryId>;

    /// Add a memory without updating Thread.updated_at.
    /// Used by the importer to control timestamps explicitly.
    async fn add_memory_only(&self, thread_id: &ThreadId, memory: &MemoryData) -> Result<MemoryId>;

    /// Idempotent batch insertion of memories under a thread.
    ///
    /// Resolves the target thread (existing id, or upsert by channel),
    /// validates pre-flight constraints (user_id, intra-batch refs,
    /// non-empty parent_ids) and runs the entire batch in a single
    /// transaction. `default_system_memory_id` auto-injection is
    /// disabled so the importer's parent graph is preserved verbatim.
    /// Returns the per-input outcomes plus the list of newly inserted
    /// memories so the gRPC layer can fire embedding dispatches after
    /// commit. Spec §3.2.
    async fn add_memories_batch(
        &self,
        input: AddMemoriesBatchInput,
    ) -> Result<AddMemoriesBatchOutput>;

    /// Find a memory by its external_id (for deduplication on import).
    async fn find_memory_by_external_id(&self, external_id: &str) -> Result<Option<Memory>> {
        self.memory_repository()
            .find_by_external_id(external_id)
            .await
    }

    /// Find threads by channel and user_id.
    async fn find_by_channel_and_user_id(
        &self,
        channel: &str,
        user_id: &UserId,
    ) -> Result<Vec<Thread>> {
        let threads = self
            .thread_repository()
            .find_by_channel_and_user_id(channel, user_id)
            .await?;
        // Enrich with labels (same pattern as find_list)
        let ids: Vec<i64> = threads
            .iter()
            .filter_map(|t| t.id.as_ref().map(|id| id.value))
            .collect();
        if ids.is_empty() {
            return Ok(threads);
        }
        let label_rows = self
            .thread_label_repository()
            .find_labels_by_thread_ids(&ids)
            .await?;
        let mut labels_map: std::collections::HashMap<i64, Vec<String>> =
            std::collections::HashMap::new();
        for row in label_rows {
            labels_map.entry(row.thread_id).or_default().push(row.label);
        }
        let result = threads
            .into_iter()
            .map(|mut t| {
                if let (Some(id), Some(data)) = (&t.id, &mut t.data) {
                    data.labels = labels_map.remove(&id.value).unwrap_or_default();
                }
                t
            })
            .collect();
        Ok(result)
    }

    /// Re-wire parent_ids with optional guards. Returns the rewire
    /// outcome so the gRPC handler can map "skipped due to guard" to
    /// `(rewired = false, skip_reason)` rather than a status error.
    /// Spec §3.3.
    async fn update_memory_parent_ids_with_guards(
        &self,
        thread_id: &ThreadId,
        memory_id: &MemoryId,
        new_parent_ids: &[MemoryId],
        force_overwrite_when_shared: bool,
        force_overwrite_when_non_empty: bool,
    ) -> Result<UpdateMemoryParentsOutcome> {
        let pool = self.thread_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        // Lock thread row
        let _thread = self
            .thread_repository()
            .find_row_for_update_tx(&mut *tx, thread_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("thread not found: {}", thread_id.value))
            })?;

        // Verify memory belongs to this thread
        if !self
            .thread_memory_repository()
            .contains_tx(&mut *tx, thread_id.value, memory_id.value)
            .await?
        {
            return Err(LlmMemoryError::NotFound(format!(
                "memory {} is not attached to thread {}",
                memory_id.value, thread_id.value
            ))
            .into());
        }

        // Lock the target memory row and confirm it actually exists. A
        // dangling junction (memory deleted but `thread_memory` row
        // lingers) would otherwise let the rewire commit a no-op write
        // silently — the spec requires NotFound in that case.
        let existing = self
            .memory_repository()
            .find_by_ids_for_update_tx(&mut tx, &[memory_id.value])
            .await?;
        let existing_data = existing
            .into_iter()
            .next()
            .and_then(|m| m.data)
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("memory not found: {}", memory_id.value))
            })?;

        // GUARD 1: shared memory across multiple threads.
        if !force_overwrite_when_shared {
            let count = self
                .thread_memory_repository()
                .count_refs_tx(&mut *tx, memory_id.value)
                .await?;
            if count != 1 {
                return Ok(UpdateMemoryParentsOutcome::Skipped(
                    UpdateParentsSkipReason::SharedMemory,
                ));
            }
        }

        // GUARD 2: existing parent_ids non-empty.
        if !force_overwrite_when_non_empty && !existing_data.parent_ids.is_empty() {
            return Ok(UpdateMemoryParentsOutcome::Skipped(
                UpdateParentsSkipReason::AlreadyHasParents,
            ));
        }

        // Validate parent existence before writing
        let parent_id_values: Vec<i64> = new_parent_ids.iter().map(|p| p.value).collect();
        let existing_parents = if parent_id_values.is_empty() {
            Vec::new()
        } else {
            self.memory_repository()
                .find_by_ids_for_update_tx(&mut tx, &parent_id_values)
                .await?
        };
        let existing_ids: HashSet<i64> = existing_parents
            .iter()
            .filter_map(|m| m.id.as_ref().map(|i| i.value))
            .collect();

        // Write only validated parent_ids to avoid dangling references
        let validated_parents: Vec<MemoryId> = new_parent_ids
            .iter()
            .filter(|p| existing_ids.contains(&p.value))
            .copied()
            .collect();
        self.memory_repository()
            .update_parent_ids(&mut *tx, memory_id, &validated_parents)
            .await?;

        // Attach validated parents to junction table
        let now = command_utils::util::datetime::now_millis();
        for parent in &validated_parents {
            self.thread_memory_repository()
                .insert_or_ignore_auto_position_tx(&mut *tx, thread_id.value, parent.value, now)
                .await?;
        }

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        // Invalidate caches
        let k = Arc::new(Self::find_cache_key(&thread_id.value));
        let _ = self.delete_cache(&k).await;
        let mk = Arc::new(crate::app::memory_cache_key(&memory_id.value));
        let _ = self.cache().try_remove(&mk).await;

        Ok(UpdateMemoryParentsOutcome::Rewired)
    }

    /// Check if a memory belongs to a specific thread (via thread_memory junction).
    async fn is_memory_in_thread(
        &self,
        thread_id: &ThreadId,
        memory_id: &MemoryId,
    ) -> Result<bool> {
        let pool = self.thread_memory_repository().db_pool();
        self.thread_memory_repository()
            .contains_tx(pool, thread_id.value, memory_id.value)
            .await
    }

    /// Count how many threads reference a memory (via thread_memory junction).
    async fn count_threads_for_memory(&self, memory_id: &MemoryId) -> Result<i64> {
        let pool = self.thread_memory_repository().db_pool();
        self.thread_memory_repository()
            .count_refs_tx(pool, memory_id.value)
            .await
    }

    /// Walk back the `parent_ids` chain from `start_memory_id` and return the
    /// ancestor closure together with the effective ROLE_SYSTEM Memory (if
    /// any).
    ///
    /// Algorithm (BFS, batch queries):
    /// 1. Start with `frontier = {start_memory_id}`, `visited = {}`.
    /// 2. Each round, batch-resolve the frontier through
    ///    `find_by_ids_with_position_tx(thread_id, frontier)`. Rows that are
    ///    not registered in `thread_memory` under `thread_id` are silently
    ///    dropped by the INNER JOIN and logged at debug level.
    /// 3. Add resolved ids to `visited` and collect their memories.
    /// 4. Build the next frontier from the parent_ids of the resolved rows,
    ///    skipping anything already in `visited` so cycles short-circuit
    ///    silently — the closure is still correct because every reachable
    ///    node has already been collected.
    /// 5. Stop when the next frontier is empty. If `max_depth` is exceeded
    ///    before that, return `InvalidArgument` — this also serves as a
    ///    safety net for extremely deep (or cycle-amplified) graphs.
    ///
    /// Note: true cycles (A → B → A) are *not* reported as errors because
    /// the visited set naturally absorbs them: the second encounter of A
    /// is filtered out of the frontier and the loop terminates once no
    /// new ids remain. The "possible cycle" message in the `max_depth`
    /// error is a hint, not a guarantee — the depth limit can also be hit
    /// by a legitimately deep chain.
    ///
    /// Once the visited set is complete, it is re-queried in one shot under
    /// the same repeatable-read snapshot and sorted ASC. The ROLE_SYSTEM with
    /// the largest position wins.
    ///
    /// Errors:
    /// - `NotFound` if the start memory is not attached to `thread_id` in
    ///   the junction.
    /// - `InvalidArgument` on `max_depth == 0`, excessive depth (which may
    ///   indicate a cycle), or a `max_depth` above
    ///   `HARD_ANCESTOR_MAX_DEPTH` (callers that want the server default
    ///   should pass `None`).
    async fn resolve_ancestor_closure(
        &self,
        thread_id: &ThreadId,
        start_memory_id: &MemoryId,
        max_depth: Option<u32>,
    ) -> Result<AncestorClosure> {
        let depth_limit = match max_depth {
            None => DEFAULT_ANCESTOR_MAX_DEPTH,
            Some(0) => {
                return Err(LlmMemoryError::InvalidArgument(
                    "max_depth must be at least 1".to_string(),
                )
                .into());
            }
            Some(n) if n > HARD_ANCESTOR_MAX_DEPTH => {
                return Err(LlmMemoryError::InvalidArgument(format!(
                    "max_depth {n} exceeds server hard limit {HARD_ANCESTOR_MAX_DEPTH}"
                ))
                .into());
            }
            Some(n) => n,
        };

        let pool = self.thread_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;
        configure_repeatable_read_if_supported(&mut tx).await?;

        let mut visited: HashSet<i64> = HashSet::new();
        let mut frontier: HashSet<i64> = HashSet::from([start_memory_id.value]);
        // `depth` counts hops already walked in the parent direction. The
        // starting memory itself is at depth 0 (no hop consumed), its direct
        // parents are at depth 1, grand-parents at depth 2, and so on. The
        // cap `depth > depth_limit` is therefore a strict-greater comparison
        // so that `max_depth = N` resolves "the starting memory plus up to N
        // hops of parents", matching the documentation on
        // `DEFAULT_ANCESTOR_MAX_DEPTH`.
        let mut depth: u32 = 0;

        // BFS expand. We exit the loop the moment the frontier is empty
        // (either because we ran out of ancestors or because every
        // next-level id was already visited).
        while !frontier.is_empty() {
            if depth > depth_limit {
                return Err(LlmMemoryError::InvalidArgument(format!(
                    "parent_ids traversal exceeded max_depth={depth_limit}: possible cycle"
                ))
                .into());
            }

            // Drop already-visited ids so a cycle short-circuits without
            // issuing a redundant round trip.
            let chunk: Vec<i64> = frontier
                .iter()
                .copied()
                .filter(|id| !visited.contains(id))
                .collect();
            if chunk.is_empty() {
                break;
            }

            let rows = self
                .memory_repository()
                .find_by_ids_with_position_tx(&mut tx, thread_id.value, &chunk, false)
                .await?;

            // On the very first round, require that the start memory is
            // actually in the junction under this thread. Everything else
            // we silently skip (with a debug log) so that a parent_id
            // referencing a Memory that lives under another thread does
            // not fail the whole traversal.
            if depth == 0 {
                let found_start = rows
                    .iter()
                    .any(|(m, _)| m.id.as_ref().map(|id| id.value) == Some(start_memory_id.value));
                if !found_start {
                    return Err(LlmMemoryError::NotFound(format!(
                        "start memory {} is not attached to thread {}",
                        start_memory_id.value, thread_id.value
                    ))
                    .into());
                }
            } else {
                let missing = chunk.len() - rows.len();
                if missing > 0 {
                    tracing::debug!(
                        thread_id = thread_id.value,
                        missing_count = missing,
                        "parent_ids traversal skipped {} memories that are not attached to this thread",
                        missing
                    );
                }
            }

            let mut next_frontier: HashSet<i64> = HashSet::new();
            for (mem, _pos) in &rows {
                let id = mem.id.as_ref().map(|i| i.value).unwrap_or(0);
                if id == 0 {
                    continue;
                }
                visited.insert(id);
                if let Some(data) = mem.data.as_ref() {
                    for parent in &data.parent_ids {
                        if !visited.contains(&parent.value) {
                            next_frontier.insert(parent.value);
                        }
                    }
                }
            }
            frontier = next_frontier;
            depth = depth.saturating_add(1);
        }

        // Re-resolve the whole visited set in one shot so ordered_memories
        // is internally consistent under the same repeatable-read snapshot.
        let visited_ids: Vec<i64> = visited.into_iter().collect();
        let ordered = self
            .memory_repository()
            .find_by_ids_with_position_tx(&mut tx, thread_id.value, &visited_ids, true)
            .await?;
        // Read-only traversal; commit to return the pool lease promptly.
        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        let ordered_memories: Vec<MemoryWithPosition> = ordered
            .into_iter()
            .map(|(memory, position)| MemoryWithPosition { memory, position })
            .collect();

        // The largest-position ROLE_SYSTEM wins. Ties should not occur in
        // practice because `thread_memory_thread_position` is UNIQUE, but
        // if they ever do, `max_by_key` picks the last one seen which is
        // a stable choice given the sorted input.
        let system_memory = ordered_memories
            .iter()
            .filter(|m| {
                m.memory
                    .data
                    .as_ref()
                    .map(|d| d.role == MessageRole::RoleSystem as i32)
                    .unwrap_or(false)
            })
            .max_by_key(|m| m.position)
            .cloned();

        Ok(AncestorClosure {
            ordered_memories,
            system_memory,
        })
    }

    async fn find_memories_by_thread_id(
        &self,
        thread_id: &ThreadId,
        limit: Option<&i32>,
        offset: Option<&i64>,
        roles: &[i32],
        content_types: &[i32],
    ) -> Result<Vec<Memory>> {
        let mut list = self
            .memory_repository()
            .find_by_thread_id(thread_id.value, limit, offset, roles, content_types, true)
            .await?;
        // Same two-stage media model as Find: hydrate the cacheable half
        // here; the gRPC layer adds the per-response presigned URL. No-op
        // when the media subsystem is not wired (non-media / env-less).
        super::memory::hydrate_media_list(self.media_subsystem(), &mut list).await?;
        Ok(list)
    }

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static,
    {
        self.thread_repository()
            .count_list_tx(self.thread_repository().db_pool())
            .await
    }

    // Helper to delete memory cache entries
    async fn delete_memory_cache(&self, key: &Arc<String>) -> bool;
}

use super::memory_cache_key;

pub struct ThreadAppImpl {
    thread_repository: ThreadRepositoryImpl,
    thread_memory_repository: ThreadMemoryRepositoryImpl,
    thread_label_repository: ThreadLabelRepositoryImpl,
    memory_repository: MemoryRepositoryImpl,
    memory_rating_repository: MemoryRatingRepositoryImpl,
    /// Cached resolve-pipeline config so the time-range / sort branch in
    /// `find_threads_by_labels` can bound the candidate fetch without
    /// re-parsing env on every request. Shared with `MemoryAppImpl` /
    /// `MemoryVectorAppImpl` via the same `MEMORY_THREAD_FILTER_*` knobs.
    thread_filter_config: super::thread_filter_resolver::ThreadFilterConfig,
    thread_cache: TokioCache<Arc<String>, Thread>,
    memory_cache: TokioCache<Arc<String>, Memory>,
    key_lock: RwLockWithKey<Arc<String>>,
    default_ttl: Duration,
    embedding_dispatcher:
        Option<Arc<infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher>>,
    thread_embedding_dispatcher:
        Option<Arc<infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher>>,
    /// Read once from env (immutable) so the add_memory dispatch path does
    /// not re-parse it, mirroring `thread_filter_config`. When a memory
    /// carries `media_object_id` and the media repository is wired, the
    /// batch path also evaluates the media axis (image dispatch).
    image_search_mode: infra::infra::embedding_dispatch::ImageSearchMode,
    thread_vector_app: Option<Arc<super::thread_vector::ThreadVectorAppImpl>>,
    /// Media wiring (shared `MediaSubsystem` type — the thread path bumps
    /// ref_count on batch import and decrements + finalizes on delete,
    /// the same repository+finalizer pair `MemoryApp` uses). `None` =
    /// non-media deployment / env-less unit tests (then add/delete leave
    /// `media_object` untouched, as before).
    media: Option<super::memory::MediaSubsystem>,
}

impl ThreadAppImpl {
    const DEFAULT_TTL_SEC: u64 = 60;
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        thread_repository: ThreadRepositoryImpl,
        thread_memory_repository: ThreadMemoryRepositoryImpl,
        thread_label_repository: ThreadLabelRepositoryImpl,
        memory_repository: MemoryRepositoryImpl,
        memory_rating_repository: MemoryRatingRepositoryImpl,
        thread_cache: TokioCache<Arc<String>, Thread>,
        memory_cache: TokioCache<Arc<String>, Memory>,
        embedding_dispatcher: Option<
            Arc<infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher>,
        >,
        thread_embedding_dispatcher: Option<
            Arc<infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher>,
        >,
        thread_vector_app: Option<Arc<super::thread_vector::ThreadVectorAppImpl>>,
    ) -> Self {
        Self {
            thread_repository,
            thread_memory_repository,
            thread_label_repository,
            memory_repository,
            memory_rating_repository,
            thread_filter_config: super::thread_filter_resolver::ThreadFilterConfig::from_env(),
            thread_cache,
            memory_cache,
            key_lock: RwLockWithKey::new(16 * 1024),
            default_ttl: Duration::from_secs(Self::DEFAULT_TTL_SEC),
            embedding_dispatcher,
            thread_embedding_dispatcher,
            image_search_mode: infra::infra::embedding_dispatch::ImageSearchMode::from_env(),
            thread_vector_app,
            media: None,
        }
    }

    /// Wire the media subsystem (image memory feature). Called from the
    /// DI module with a sibling `MediaObjectRepositoryImpl` (same pool as
    /// `memory_repository`, so ref_count moves stay in the memory tx) and
    /// the shared `MediaApp` finalizer. The repository keeps
    /// `media_object.ref_count` correct on batch import (increment) and
    /// thread delete (decrement); the finalizer runs the post-commit
    /// storage+row delete when a ref_count hits 0.
    pub fn with_media(
        mut self,
        media_object_repository: MediaObjectRepositoryImpl,
        media_finalizer: Arc<crate::app::media::MediaAppImpl>,
    ) -> Self {
        self.media = Some(super::memory::MediaSubsystem::new(
            media_object_repository,
            media_finalizer,
        ));
        self
    }

    pub fn thread_label_repository(&self) -> &ThreadLabelRepositoryImpl {
        &self.thread_label_repository
    }

    /// Core logic for adding a memory to a thread, executed within an
    /// existing transaction. Does NOT update Thread.updated_at or commit.
    /// Returns `(memory_id, possibly-mutated memory data, now, media)`
    /// where `media = Some((kind, storage_backend))` of the row-locked
    /// `media_object` when one was referenced and ref-bumped (so the
    /// post-commit dispatch can evaluate the media axis), else `None`.
    ///
    /// `skip_default_system_inject = true` disables the
    /// `default_system_memory_id` auto-injection so callers (notably
    /// the batch importer) can preserve the source transcript's parent
    /// graph verbatim. Spec §3.2.2.
    async fn add_memory_core_tx(
        &self,
        tx: &mut infra_utils::infra::rdb::RdbTransaction<'_>,
        thread_id: &ThreadId,
        memory: &MemoryData,
        skip_default_system_inject: bool,
    ) -> Result<(MemoryId, MemoryData, i64, MediaDispatchHint)> {
        let thread = self
            .thread_repository()
            .find_row_for_update_tx(&mut **tx, thread_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("thread not found: {}", thread_id.value))
            })?;

        let mut memory = memory.clone();

        // ROLE_SYSTEM parent validation and default_system_memory_id injection.
        let supplied_parent_ids: Vec<i64> = memory.parent_ids.iter().map(|p| p.value).collect();
        let parent_memories = if supplied_parent_ids.is_empty() {
            Vec::new()
        } else {
            self.memory_repository()
                .find_by_ids_for_update_tx(tx, &supplied_parent_ids)
                .await?
        };

        let explicit_system_parent_count = parent_memories
            .iter()
            .filter(|m| {
                m.data
                    .as_ref()
                    .map(|d| d.role == MessageRole::RoleSystem as i32)
                    .unwrap_or(false)
            })
            .count();
        if explicit_system_parent_count > 1 {
            return Err(LlmMemoryError::RuntimeError(format!(
                "parent_ids contains {} ROLE_SYSTEM memories; at most 1 is allowed",
                explicit_system_parent_count
            ))
            .into());
        }

        let validated_default_id =
            if !skip_default_system_inject && explicit_system_parent_count == 0 {
                let default_id = validate_default_system_memory_id_tx(
                    self.memory_repository(),
                    tx,
                    thread.default_system_memory_id,
                )
                .await?;
                if let Some(default_id) = default_id {
                    memory.parent_ids.insert(0, MemoryId { value: default_id });
                } else {
                    tracing::debug!(
                        thread_id = thread_id.value,
                        "thread has no default_system_memory_id and no explicit ROLE_SYSTEM parent"
                    );
                }
                default_id
            } else {
                tracing::debug!(
                    thread_id = thread_id.value,
                    skip_default_system_inject,
                    "skipping default_system_memory_id injection"
                );
                None
            };

        let now = command_utils::util::datetime::now_millis();

        // Attach existing parents to thread_memory junction.
        let mut existing_parent_ids: HashSet<i64> = parent_memories
            .iter()
            .filter_map(|m| m.id.as_ref().map(|i| i.value))
            .collect();
        if let Some(default_id) = validated_default_id {
            existing_parent_ids.insert(default_id);
        }
        for parent in &memory.parent_ids {
            if !existing_parent_ids.contains(&parent.value) {
                tracing::debug!(
                    thread_id = thread_id.value,
                    parent_id = parent.value,
                    "parent_ids entry points at a memory that does not exist; skipping junction attach"
                );
                continue;
            }
            self.thread_memory_repository()
                .insert_or_ignore_auto_position_tx(&mut **tx, thread_id.value, parent.value, now)
                .await?;
        }

        // A media-bearing memory must bump media_object.ref_count in the
        // SAME tx as the memory INSERT, else the imported image stays
        // orphaned (ref_count=0) and the deferred GC reclaims a
        // still-referenced object. Inert when the media repository is not
        // wired (non-media deployment / env-less unit test).
        let mut media_dispatch: MediaDispatchHint = None;
        if let (Some(mid), Some(media)) =
            (memory.media_object_id.map(|m| m.value), self.media.as_ref())
        {
            let bump = media.repository().lock_and_incr_ref_tx(tx, mid).await?;
            media_dispatch = Some((bump.kind, bump.storage_backend));
        }

        let memory_id = self.memory_repository().create(&mut **tx, &memory).await?;

        self.thread_memory_repository()
            .insert_auto_position_tx(&mut **tx, thread_id.value, memory_id.value, now)
            .await?;

        Ok((memory_id, memory, now, media_dispatch))
    }

    /// Post-commit side effects for embedding dispatch.
    ///
    /// `media` is `Some((media_object.kind, storage_backend))` of the
    /// ref-bumped media_object when the memory carries one (so
    /// `dispatch_kinds` evaluates the media/image axis too), else `None`
    /// (text-only — same as the pre-image-memory behaviour, backward
    /// compatible). The ref_count itself is bumped in `add_memory_core_tx`
    /// (same tx as the INSERT); this only fires the dispatch.
    fn dispatch_embedding_if_enabled(
        &self,
        memory_id: &MemoryId,
        memory: &MemoryData,
        media: MediaDispatchHint,
    ) {
        let (media_kind, media_backend) = match media {
            Some((k, b)) => (Some(k), Some(b)),
            None => (None, None),
        };
        if let Some(dispatcher) = &self.embedding_dispatcher
            && let Some(target) =
                infra::infra::memory_vector::dispatcher::DispatchTarget::from_memory(
                    memory_id.value,
                    &memory.content,
                    memory.role,
                    memory.content_type,
                    memory.media_object_id.map(|m| m.value),
                    media_kind,
                    media_backend.as_deref(),
                    self.image_search_mode,
                )
        {
            infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher::spawn_dispatch(
                dispatcher, target,
            );
        }
    }

    /// Public re-export for the gRPC layer's `add_memories_batch` post-
    /// commit dispatch loop. `media` comes from
    /// `AddMemoriesBatchOutput.new_memories_for_embedding`'s third tuple
    /// element so an imported image dispatches the media axis (not just
    /// text); `None` keeps the text-only path.
    pub fn dispatch_embedding_for_batch_item(
        &self,
        memory_id: &MemoryId,
        memory: &MemoryData,
        media: MediaDispatchHint,
    ) {
        self.dispatch_embedding_if_enabled(memory_id, memory, media);
    }
}

impl UseThreadRepository for ThreadAppImpl {
    fn thread_repository(&self) -> &ThreadRepositoryImpl {
        &self.thread_repository
    }
}

impl UseThreadMemoryRepository for ThreadAppImpl {
    fn thread_memory_repository(&self) -> &ThreadMemoryRepositoryImpl {
        &self.thread_memory_repository
    }
}

impl UseMemoryRepository for ThreadAppImpl {
    fn memory_repository(&self) -> &MemoryRepositoryImpl {
        &self.memory_repository
    }
}

impl UseMemoryRatingRepository for ThreadAppImpl {
    fn memory_rating_repository(&self) -> &MemoryRatingRepositoryImpl {
        &self.memory_rating_repository
    }
}

impl super::memory::UseThreadFilterResolver for ThreadAppImpl {
    fn thread_filter_config(&self) -> &super::thread_filter_resolver::ThreadFilterConfig {
        &self.thread_filter_config
    }
}

#[async_trait]
impl ThreadApp for ThreadAppImpl {
    fn thread_label_repository(&self) -> &ThreadLabelRepositoryImpl {
        &self.thread_label_repository
    }

    fn label_default_ttl(&self) -> Duration {
        self.default_ttl
    }

    fn thread_vector_app(&self) -> Option<&super::thread_vector::ThreadVectorAppImpl> {
        self.thread_vector_app.as_deref()
    }

    fn media_subsystem(&self) -> Option<&super::memory::MediaSubsystem> {
        self.media.as_ref()
    }

    fn thread_embedding_dispatcher(
        &self,
    ) -> Option<&Arc<infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher>> {
        self.thread_embedding_dispatcher.as_ref()
    }

    async fn delete_memory_cache(&self, key: &Arc<String>) -> bool {
        let _ = self.memory_cache.remove(key).await;
        self.memory_cache.wait().await.is_ok()
    }

    async fn add_memory(&self, thread_id: &ThreadId, memory: &MemoryData) -> Result<MemoryId> {
        let pool = self.thread_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        let (memory_id, memory, now, media) = self
            .add_memory_core_tx(&mut tx, thread_id, memory, false)
            .await?;

        self.thread_repository()
            .update_updated_at_tx(&mut *tx, thread_id, now)
            .await?;

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        let k = Arc::new(Self::find_cache_key(&thread_id.value));
        let _ = self.delete_cache(&k).await;

        self.dispatch_embedding_if_enabled(&memory_id, &memory, media);

        Ok(memory_id)
    }

    /// Add a memory without updating Thread.updated_at.
    /// Used by the importer to control timestamps explicitly.
    /// Embedding dispatch is intentionally skipped; use `redispatch_embeddings`
    /// after bulk import completes.
    async fn add_memory_only(&self, thread_id: &ThreadId, memory: &MemoryData) -> Result<MemoryId> {
        let pool = self.thread_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        let (memory_id, _memory, _now, _media) = self
            .add_memory_core_tx(&mut tx, thread_id, memory, false)
            .await?;

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        let k = Arc::new(Self::find_cache_key(&thread_id.value));
        let _ = self.delete_cache(&k).await;

        Ok(memory_id)
    }

    async fn add_memories_batch(
        &self,
        input: AddMemoriesBatchInput,
    ) -> Result<AddMemoriesBatchOutput> {
        let AddMemoriesBatchInput {
            thread_target,
            memories,
            upsert_by_external_id,
            thread_updated_at_override,
            labels,
        } = input;

        if memories.is_empty() {
            return Err(
                LlmMemoryError::InvalidArgument("memories must not be empty".to_string()).into(),
            );
        }

        // ----- Phase 0: pre-scan (no transaction yet) -----
        let mut all_batch_eids: HashSet<String> = HashSet::with_capacity(memories.len());
        for (idx, item) in memories.iter().enumerate() {
            if !item.memory.parent_ids.is_empty() {
                return Err(LlmMemoryError::InvalidArgument(format!(
                    "memories[{idx}].memory.parent_ids must be empty; \
                     express parent references via parent_external_ids"
                ))
                .into());
            }
            if let Some(eid) = item.memory.external_id.as_deref()
                && !eid.is_empty()
                && !all_batch_eids.insert(eid.to_string())
            {
                return Err(LlmMemoryError::InvalidArgument(format!(
                    "duplicate external_id within batch: {eid}"
                ))
                .into());
            }
        }
        // Validate every memory carries a non-zero user_id (proto3
        // default = 0 = "unset").
        for (idx, item) in memories.iter().enumerate() {
            let uid = item.memory.user_id.map(|u| u.value).unwrap_or(0);
            if uid == 0 {
                return Err(LlmMemoryError::InvalidArgument(format!(
                    "memories[{idx}].memory.user_id is required (must be non-zero)"
                ))
                .into());
            }
        }

        // ----- Phase 1: transaction begin + thread resolution -----
        let pool = self.thread_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        let (thread_id, thread_created, target_user_id) = match thread_target {
            BatchThreadTarget::ExistingThreadId(tid) => {
                let row = self
                    .thread_repository()
                    .find_row_for_update_tx(&mut *tx, &tid)
                    .await?
                    .ok_or_else(|| {
                        LlmMemoryError::NotFound(format!("thread not found: {}", tid.value))
                    })?;
                (tid, false, row.user_id)
            }
            BatchThreadTarget::UpsertByChannel(thread_data) => {
                let user_id = thread_data.user_id.ok_or_else(|| {
                    LlmMemoryError::InvalidArgument("thread_data.user_id is required".to_string())
                })?;
                if user_id.value == 0 {
                    return Err(LlmMemoryError::InvalidArgument(
                        "thread_data.user_id must be non-zero".to_string(),
                    )
                    .into());
                }
                let channel = thread_data.channel.clone().unwrap_or_default();
                if channel.is_empty() {
                    return Err(LlmMemoryError::InvalidArgument(
                        "thread_data.channel is required for upsert".to_string(),
                    )
                    .into());
                }
                // Validate metadata up-front. Even though the existing-thread
                // branch ignores it, callers that hit a fresh upsert route it
                // straight to INSERT and we'd otherwise surface the failure
                // as a Postgres-only DB error.
                validate_metadata(thread_data.metadata.as_deref())?;
                // `AddMemoriesBatch` is meant for source-faithful import:
                // the auto default_system_memory_id machinery would
                // attach a memory under position 0 in `thread_memory`,
                // shifting every subsequently appended import entry and
                // surfacing a non-source memory through
                // `FindMemoriesByThreadId`. Reject the field rather
                // than silently ignoring it so callers notice. Spec
                // §3.2.2.
                if thread_data
                    .default_system_memory_id
                    .is_some_and(|id| id != 0)
                {
                    return Err(LlmMemoryError::InvalidArgument(
                        "thread_data.default_system_memory_id is not supported for \
                         AddMemoriesBatch; assign it via ThreadService.Update after import"
                            .to_string(),
                    )
                    .into());
                }
                let existing = self
                    .thread_repository()
                    .find_by_channel_and_user_id_tx(&mut *tx, &channel, &user_id)
                    .await?;
                match existing.len() {
                    0 => {
                        // Create a new thread inside the same transaction.
                        // `default_system_memory_id` was rejected above, so
                        // the new row has no default and the junction starts
                        // empty — the import entries get position 0..N.
                        let mut to_insert = thread_data.clone();
                        to_insert.default_system_memory_id = None;
                        let new_id = self
                            .thread_repository()
                            .create(&mut *tx, &to_insert)
                            .await?;
                        (new_id, true, user_id.value)
                    }
                    1 => {
                        let existing_thread = existing.into_iter().next().unwrap();
                        let tid = existing_thread.id.ok_or_else(|| {
                            LlmMemoryError::RuntimeError(
                                "thread row missing id after channel lookup".to_string(),
                            )
                        })?;
                        // Lock the row for update so subsequent inserts
                        // observe a stable target.
                        //
                        // Existing-thread metadata is intentionally preserved:
                        // AddMemoriesBatch is called multiple times per import
                        // (once per chunk). If chunk 1 wrote `metadata` and
                        // chunk 2 arrives with `metadata = None`, overwriting
                        // here would erase the state. Mutations go through
                        // ThreadService.Update only.
                        let _ = self
                            .thread_repository()
                            .find_row_for_update_tx(&mut *tx, &tid)
                            .await?;
                        let owner = existing_thread
                            .data
                            .as_ref()
                            .and_then(|d| d.user_id)
                            .map(|u| u.value)
                            .unwrap_or(user_id.value);
                        (tid, false, owner)
                    }
                    _ => {
                        return Err(LlmMemoryError::FailedPrecondition(format!(
                            "multiple threads for channel ({channel}, user {})",
                            user_id.value
                        ))
                        .into());
                    }
                }
            }
        };

        // ----- Phase 1 (cont.): user_id alignment between target thread and each memory -----
        for (idx, item) in memories.iter().enumerate() {
            let uid = item.memory.user_id.map(|u| u.value).unwrap_or(0);
            if uid != target_user_id {
                return Err(LlmMemoryError::PermissionDenied(format!(
                    "memories[{idx}].memory.user_id={uid} does not match \
                     target thread owner user_id={target_user_id}"
                ))
                .into());
            }
        }

        // ----- Phase 1 (cont.): seed eid_map from a single bulk JOIN
        //     restricted to the external_ids actually referenced by this
        //     batch (parent_external_ids ∪ each memory's own external_id).
        //     The JOIN against `thread_memory` returns the attach position
        //     in the same round trip, so the per-memory `find_position_tx`
        //     is no longer necessary. `target_user_id` filtering defends
        //     against past cross-user attaches; rows with a mismatched
        //     owner are dropped from the eid_map but still indexed so the
        //     cross-thread / cross-user guards can reject explicitly. -----
        let mut wanted_eids: HashSet<String> = HashSet::with_capacity(memories.len());
        for item in &memories {
            for eid in &item.parent_external_ids {
                if !eid.is_empty() {
                    wanted_eids.insert(eid.clone());
                }
            }
            if let Some(eid) = item.memory.external_id.as_deref()
                && !eid.is_empty()
            {
                wanted_eids.insert(eid.to_string());
            }
        }
        let wanted_vec: Vec<String> = wanted_eids.into_iter().collect();
        let bulk_existing = self
            .memory_repository()
            .find_by_external_ids_with_position_tx(&mut tx, thread_id.value, &wanted_vec)
            .await?;

        // Slim record kept per external_id so we don't keep `Memory`
        // (content + metadata) alive for every batch entry. Carries
        // exactly what the upsert / rewire decisions read.
        struct ExistingInfo {
            memory_id: MemoryId,
            owner_user_id: i64,
            parent_ids_empty: bool,
            attached_position: Option<i32>,
            // Stored metadata that drives re-embedding dispatch on a
            // content-only upsert: the contract overwrites `content` but
            // keeps role / content_type / media, so the dispatch decision
            // (`DispatchTarget::from_memory`) must use these stored values,
            // not the reimport input's (which may carry a different
            // content_type / role and wrongly skip the text axis).
            role: i32,
            content_type: i32,
            media_object_id: Option<i64>,
        }
        let mut existing_by_eid: std::collections::HashMap<String, ExistingInfo> =
            std::collections::HashMap::with_capacity(bulk_existing.len());
        for (m, attached_position) in bulk_existing {
            let Some(id) = m.id else { continue };
            let Some(data) = m.data else { continue };
            let Some(eid) = data.external_id else {
                continue;
            };
            existing_by_eid.insert(
                eid,
                ExistingInfo {
                    memory_id: id,
                    owner_user_id: data.user_id.map(|u| u.value).unwrap_or(0),
                    parent_ids_empty: data.parent_ids.is_empty(),
                    attached_position,
                    role: data.role,
                    content_type: data.content_type,
                    media_object_id: data.media_object_id.map(|m| m.value),
                },
            );
        }

        // eid_map: only memories already attached to THIS thread under the
        // target user can be parent-resolved against the batch.
        let mut eid_map: std::collections::HashMap<String, (MemoryId, bool)> =
            std::collections::HashMap::with_capacity(existing_by_eid.len());
        for (eid, info) in &existing_by_eid {
            if info.owner_user_id == target_user_id && info.attached_position.is_some() {
                eid_map.insert(eid.clone(), (info.memory_id, info.parent_ids_empty));
            }
        }

        // ----- Phase 2: process each memory in input order -----
        let mut outcomes: Vec<AddMemoryOutcome> = Vec::with_capacity(memories.len());
        let mut new_memories_for_embedding: Vec<NewMemoryForEmbedding> = Vec::new();
        // memory_ids whose content was overwritten via `upsert_by_external_id`.
        // Their `memory_cache` entries must be invalidated post-commit so a
        // previously-cached `MemoryApp::find_memory` does not serve stale
        // content until TTL.
        let mut overwritten_memory_ids: Vec<MemoryId> = Vec::new();
        // Forward references inside the batch (e.g. Codex reordered
        // rollouts where `function_call_output` precedes `function_call`
        // in entry order) can only be resolved once every entry has its
        // memory_id assigned. We record them here and patch the inserted
        // memory's `parent_ids` plus the outcome in a Phase 2.5 pass.
        struct ForwardRef {
            outcome_idx: usize,
            target_eids: Vec<String>,
        }
        let mut forward_refs: Vec<ForwardRef> = Vec::new();

        for (idx, item) in memories.into_iter().enumerate() {
            let BatchMemoryInput {
                memory,
                parent_external_ids,
            } = item;

            // Resolve parent_external_ids order-preserving + dedup.
            // Forward references that point at a later entry in this
            // batch are deferred to Phase 2.5; only refs that point
            // outside the batch and outside the eid_map are dropped.
            let mut resolved_ids: Vec<MemoryId> = Vec::new();
            let mut seen_resolved: HashSet<i64> = HashSet::new();
            let mut pending: Vec<String> = Vec::new();
            for eid in &parent_external_ids {
                if let Some((mid, _)) = eid_map.get(eid) {
                    if seen_resolved.insert(mid.value) {
                        resolved_ids.push(*mid);
                    }
                } else if all_batch_eids.contains(eid) {
                    pending.push(eid.clone());
                } else {
                    tracing::warn!(
                        thread_id = thread_id.value,
                        external_id = eid,
                        index = idx,
                        "parent_external_id could not be resolved; dropping"
                    );
                }
            }
            if !pending.is_empty() {
                forward_refs.push(ForwardRef {
                    outcome_idx: outcomes.len(),
                    target_eids: pending,
                });
            }

            // Decide create vs reuse based on external_id and the upsert flag.
            let item_eid = memory
                .external_id
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string());
            let existing_for_eid = item_eid.as_ref().and_then(|eid| existing_by_eid.get(eid));

            match (existing_for_eid, upsert_by_external_id) {
                (Some(info), false) => {
                    return Err(LlmMemoryError::AlreadyExists(format!(
                        "memory.external_id collision (id={}); \
                         set upsert_by_external_id to reuse",
                        info.memory_id.value
                    ))
                    .into());
                }
                (Some(info), true) => {
                    if info.owner_user_id != target_user_id {
                        return Err(LlmMemoryError::PermissionDenied(format!(
                            "external_id existing memory belongs to user {} \
                             but target thread is owned by {target_user_id}",
                            info.owner_user_id
                        ))
                        .into());
                    }
                    // `attached_position` doubles as the attach probe: `None`
                    // means the memory exists under this user but lives in a
                    // different thread, which we reject explicitly to avoid
                    // silently cross-thread sharing.
                    let position = info.attached_position.ok_or_else(|| {
                        LlmMemoryError::FailedPrecondition(format!(
                            "external_id memory id={} exists in a different thread",
                            info.memory_id.value
                        ))
                    })?;
                    let existing_id = info.memory_id;
                    let existing_parent_ids_empty = info.parent_ids_empty;
                    if let Some(eid) = item_eid {
                        eid_map.insert(eid, (existing_id, existing_parent_ids_empty));
                    }

                    // Contract (`upsert_by_external_id`): overwrite the
                    // existing memory's content with the incoming value.
                    // Scoped to `content` via `update_content_only` so the
                    // branch's parent/ownership/position invariants stay
                    // intact. We re-dispatch embedding for the new content
                    // so the vector index does not drift from the stored
                    // content on reimport.
                    self.memory_repository()
                        .update_content_only(&mut *tx, &existing_id, &memory.content)
                        .await?;
                    // The dispatch decision (`DispatchTarget::from_memory`)
                    // keys on role / content_type / media, which the
                    // content-only update does NOT change. Use the STORED
                    // metadata with the new content; using the reimport
                    // input's metadata could wrongly skip the text axis
                    // (e.g. input content_type=TOOL) and leave the vector
                    // stale.
                    let dispatch_memory = MemoryData {
                        content: memory.content,
                        role: info.role,
                        content_type: info.content_type,
                        media_object_id: info.media_object_id.map(|value| MediaObjectId { value }),
                        ..memory
                    };
                    new_memories_for_embedding.push((existing_id, dispatch_memory, None));
                    // Invalidate the shared memory_cache post-commit (the RDB
                    // content just changed under the same id).
                    overwritten_memory_ids.push(existing_id);

                    outcomes.push(AddMemoryOutcome {
                        memory_id: existing_id,
                        created: false,
                        position,
                        existing_parent_ids_empty,
                        resolved_parent_ids: resolved_ids,
                    });
                }
                (None, _) => {
                    let mut to_insert = memory;
                    to_insert.parent_ids = resolved_ids.clone();
                    let (new_id, materialized, _now, media) = self
                        .add_memory_core_tx(
                            &mut tx, &thread_id, &to_insert,
                            /* skip_default_system_inject */ true,
                        )
                        .await?;
                    let position = self
                        .thread_memory_repository()
                        .find_position_tx(&mut *tx, thread_id.value, new_id.value)
                        .await?
                        .unwrap_or(0);
                    if let Some(eid) = item_eid {
                        eid_map.insert(eid, (new_id, false));
                    }
                    new_memories_for_embedding.push((new_id, materialized, media));
                    outcomes.push(AddMemoryOutcome {
                        memory_id: new_id,
                        created: true,
                        position,
                        existing_parent_ids_empty: false,
                        resolved_parent_ids: resolved_ids,
                    });
                }
            }
        }

        // ----- Phase 2.5: resolve in-batch forward references -----
        // After every entry has its memory_id assigned, walk the
        // deferred refs and patch each affected memory's parent_ids
        // (plus the corresponding outcome). The junction is kept
        // consistent by re-attaching every parent under the same
        // thread; `insert_or_ignore_auto_position_tx` is idempotent.
        if !forward_refs.is_empty() {
            let now = command_utils::util::datetime::now_millis();
            for fr in forward_refs {
                let outcome = &mut outcomes[fr.outcome_idx];
                // Created outcomes are the only ones eligible to grow
                // their parent_ids in-place: existing memories use the
                // separate UpdateMemoryParents RPC with its own guards.
                if !outcome.created {
                    continue;
                }
                let mut additions: Vec<MemoryId> = Vec::new();
                let mut seen: HashSet<i64> = outcome
                    .resolved_parent_ids
                    .iter()
                    .map(|m| m.value)
                    .collect();
                for eid in &fr.target_eids {
                    if let Some((mid, _)) = eid_map.get(eid)
                        && seen.insert(mid.value)
                    {
                        additions.push(*mid);
                    }
                }
                if additions.is_empty() {
                    continue;
                }
                let mut new_parent_ids = outcome.resolved_parent_ids.clone();
                new_parent_ids.extend_from_slice(&additions);
                self.memory_repository()
                    .update_parent_ids(&mut *tx, &outcome.memory_id, &new_parent_ids)
                    .await?;
                for parent in &additions {
                    self.thread_memory_repository()
                        .insert_or_ignore_auto_position_tx(
                            &mut *tx,
                            thread_id.value,
                            parent.value,
                            now,
                        )
                        .await?;
                }
                outcome.resolved_parent_ids = new_parent_ids;
            }
        }

        // ----- Phase 3: finishing touches (labels, updated_at override) -----
        if !labels.is_empty() {
            let validated = validate_labels(&labels)?;
            let now = command_utils::util::datetime::now_millis();
            self.add_labels_core_tx(&mut tx, &thread_id, &validated, now)
                .await?;
        }
        // Thread `updated_at` bump. Two sources can move it forward:
        //   * `thread_updated_at_override` (explicit high-watermark from the
        //     import source), and
        //   * a content overwrite via `upsert_by_external_id`, which changed
        //     thread content in place and so must surface the thread in
        //     `updated_after` / sort-by-updated views (mirrors how a plain
        //     add_memory bumps the thread).
        // Both fold into a single `max(current, ...)` so we never move
        // `updated_at` backwards — differential imports / `summarize-after`
        // windows rely on it as a monotonic high-watermark.
        let overwrite_bump = if overwritten_memory_ids.is_empty() {
            0
        } else {
            command_utils::util::datetime::now_millis()
        };
        let updated_at_target = thread_updated_at_override.max(overwrite_bump);
        if updated_at_target > 0 {
            let current = self
                .thread_repository()
                .find_row_for_update_tx(&mut *tx, &thread_id)
                .await?
                .map(|t| t.updated_at)
                .unwrap_or(0);
            let new_value = current.max(updated_at_target);
            if new_value != current {
                self.thread_repository()
                    .update_updated_at_tx(&mut *tx, &thread_id, new_value)
                    .await?;
            }
        }

        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        // Cache invalidation.
        let k = Arc::new(Self::find_cache_key(&thread_id.value));
        let _ = self.delete_cache(&k).await;
        // Drop stale memory_cache entries for any content overwritten via
        // `upsert_by_external_id`, else `MemoryApp::find_memory` (sharing
        // this cache) would serve the pre-update content until TTL.
        for mid in &overwritten_memory_ids {
            let mk = Arc::new(memory_cache_key(&mid.value));
            let _ = self.delete_memory_cache(&mk).await;
        }

        // Best-effort vector scalar sync (labels / updated_at). Mirrors
        // the existing behavior of add_labels_only / update_thread so
        // search filters that look up these fields in LanceDB stay in
        // step with the RDB after a batch import.
        {
            let labels_changed = !labels.is_empty();
            // `updated_at` may have moved from the explicit override OR from a
            // content overwrite bump — both must trigger the scalar sync.
            let timestamps_changed = updated_at_target > 0;
            if (labels_changed || timestamps_changed)
                && let Some(tva) = self.thread_vector_app()
                && let Err(e) = tva.sync_thread_scalars(thread_id.value).await
            {
                tracing::warn!("sync_thread_scalars after add_memories_batch failed: {e}");
            }
        }

        Ok(AddMemoriesBatchOutput {
            thread_id,
            thread_created,
            outcomes,
            new_memories_for_embedding,
        })
    }
}

impl UseMemoryCache<Arc<String>, Thread> for ThreadAppImpl {
    fn cache(&self) -> &TokioCache<Arc<String>, Thread> {
        &self.thread_cache
    }

    fn default_ttl(&self) -> Option<&Duration> {
        Some(&self.default_ttl)
    }

    fn key_lock(&self) -> &RwLockWithKey<Arc<String>> {
        &self.key_lock
    }
}

/// Validate `thread.default_system_memory_id` against the rules established
/// in Phase 2 of the system-prompt-as-memory migration.
///
/// Behaviour:
/// - `None`            → `Ok(None)` (no default)
/// - `Some(0)`         → `Ok(None)` (proto convention: 0 means "no default")
/// - `Some(non-zero)`  → fetch the Memory from the same transaction and
///   require (1) it exists and (2) its `role` is `ROLE_SYSTEM`. Missing rows
///   map to `LlmMemoryError::NotFound`; the role failure maps to
///   `LlmMemoryError::InvalidArgument`.
///
/// We run inside the caller's transaction so that newly-created memories from
/// the same Thread.Create/Update call (rare but possible) are visible.
async fn validate_default_system_memory_id_tx(
    memory_repo: &MemoryRepositoryImpl,
    tx: &mut infra_utils::infra::rdb::RdbTransaction<'_>,
    default_id: Option<i64>,
) -> Result<Option<i64>> {
    let Some(id) = default_id else {
        return Ok(None);
    };
    if id == 0 {
        // proto convention: Some(0) is equivalent to None.
        return Ok(None);
    }
    let mems = memory_repo.find_by_ids_for_update_tx(tx, &[id]).await?;
    let Some(mem) = mems.into_iter().next() else {
        return Err(LlmMemoryError::NotFound(format!(
            "default_system_memory_id refers to a memory that does not exist: id={id}"
        ))
        .into());
    };
    let data = mem.data.ok_or_else(|| {
        LlmMemoryError::InvalidArgument(format!(
            "default_system_memory_id={id}: returned memory has no data"
        ))
    })?;
    if data.role != MessageRole::RoleSystem as i32 {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "default_system_memory_id={id}: target memory must be ROLE_SYSTEM (role=3), got role={}",
            data.role
        ))
        .into());
    }
    Ok(Some(id))
}

/// Serialize all create/update/delete operations that target the same
/// default system memory across transactions.
///
/// PostgreSQL row locks alone are insufficient because `update_thread`
/// normally locks `thread` before `memory`, while `delete_memory` must also
/// observe and clear every referencing thread. Taking a transaction-scoped
/// advisory lock keyed by memory id forces a single lock order for "default
/// system memory membership" changes without relying on per-process mutexes.
pub(crate) async fn lock_default_system_memory_scope_tx(
    _tx: &mut infra_utils::infra::rdb::RdbTransaction<'_>,
    default_id: Option<i64>,
) -> Result<()> {
    let Some(_memory_id) = default_id.filter(|id| *id != 0) else {
        return Ok(());
    };
    #[cfg(feature = "postgres")]
    {
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(_memory_id)
            .execute(&mut **_tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
    }
    Ok(())
}

async fn configure_repeatable_read_if_supported(
    _tx: &mut infra_utils::infra::rdb::RdbTransaction<'_>,
) -> Result<()> {
    #[cfg(feature = "postgres")]
    {
        sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .execute(&mut **_tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
    }
    Ok(())
}

#[cfg(test)]
mod test {
    //! Phase 2 of system-prompt-as-memory migration:
    //! Exercises `ThreadApp::add_memory`'s new auto-injection / parent-validation
    //! behaviour around `default_system_memory_id` and ROLE_SYSTEM parents.
    //!
    //! Tests share the single DB instance provided by `infra_utils::infra::test`
    //! and clean up their own rows after execution so they can run in
    //! `--test-threads=1` mode without colliding with each other.

    use super::*;
    use crate::app::thread::{ThreadApp, ThreadAppImpl};
    use anyhow::{Context, Result};

    #[test]
    fn validate_metadata_accepts_well_formed_json() {
        validate_metadata(None).expect("None passes");
        validate_metadata(Some(r#"{"git":{"last_commit":"abc"}}"#)).expect("object passes");
        validate_metadata(Some(r#"[1, 2, 3]"#)).expect("array passes");
        validate_metadata(Some("42")).expect("scalar number passes");
        validate_metadata(Some("\"plain string\"")).expect("scalar string passes");
        validate_metadata(Some("null")).expect("null passes");
    }

    #[test]
    fn validate_metadata_rejects_malformed_input() {
        // Backend-independent rejection: would surface as a JSONB parse
        // error on Postgres and as a silently-stored garbage string on
        // SQLite without this guard, splitting the same RPC into a
        // success/failure pair depending on the deployment.
        for bad in [
            "",                // empty
            "{",               // truncated
            "{\"k\": }",       // missing value
            "{'k': 'v'}",      // single quotes
            "not json at all", // bare token
            "{\"k\": \"v\",}", // trailing comma
        ] {
            let err = validate_metadata(Some(bad)).expect_err(&format!("must reject {bad:?}"));
            let downcast = err
                .downcast_ref::<LlmMemoryError>()
                .expect("InvalidArgument variant");
            assert!(
                matches!(downcast, LlmMemoryError::InvalidArgument(_)),
                "expected InvalidArgument for {bad:?}, got {downcast:?}"
            );
        }
    }
    use infra::infra::memory::rdb::MemoryRepositoryImpl;
    use infra::infra::memory_rating::rdb::MemoryRatingRepositoryImpl;
    use infra::infra::thread::rdb::ThreadRepositoryImpl;
    use infra::infra::thread_memory::rdb::ThreadMemoryRepositoryImpl;
    use infra_utils::infra::rdb::RdbPool;
    use memory_utils::cache::stretto::{MemoryCacheConfig, new_memory_cache};
    use protobuf::llm_memory::data::{ContentType, MediaObjectId, Memory, MessageRole, Thread};

    fn build_app(pool: &'static RdbPool) -> ThreadAppImpl {
        let id_gen = infra::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen.clone(), pool);
        let thread_memory_repo = ThreadMemoryRepositoryImpl::new(pool);
        let memory_repo = MemoryRepositoryImpl::new(id_gen.clone(), pool);
        let memory_rating_repo = MemoryRatingRepositoryImpl::new(id_gen, pool);
        let cache_config = MemoryCacheConfig::default();
        let thread_cache = new_memory_cache::<Arc<String>, Thread>(&cache_config);
        let memory_cache = new_memory_cache::<Arc<String>, Memory>(&cache_config);
        ThreadAppImpl::new(
            thread_repo,
            thread_memory_repo,
            ThreadLabelRepositoryImpl::new(pool),
            memory_repo,
            memory_rating_repo,
            thread_cache,
            memory_cache,
            None,
            None,
            None,
        )
    }

    /// Build a `ThreadAppImpl` and a `MemoryAppImpl` that share the same
    /// `memory_cache` handle, mirroring the production DI wiring. Used to
    /// prove cross-app cache invalidation after a content upsert.
    fn build_thread_and_memory_app_sharing_cache(
        pool: &'static RdbPool,
    ) -> (ThreadAppImpl, crate::app::memory::MemoryAppImpl) {
        let id_gen = infra::test_helper::shared_id_generator();
        let cache_config = MemoryCacheConfig::default();
        let thread_cache = new_memory_cache::<Arc<String>, Thread>(&cache_config);
        let memory_cache = new_memory_cache::<Arc<String>, Memory>(&cache_config);

        let thread_app = ThreadAppImpl::new(
            ThreadRepositoryImpl::new(id_gen.clone(), pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            MemoryRepositoryImpl::new(id_gen.clone(), pool),
            MemoryRatingRepositoryImpl::new(id_gen.clone(), pool),
            thread_cache.clone(),
            memory_cache.clone(),
            None,
            None,
            None,
        );
        let memory_app = crate::app::memory::MemoryAppImpl::new(
            MemoryRepositoryImpl::new(id_gen.clone(), pool),
            MemoryRatingRepositoryImpl::new(id_gen.clone(), pool),
            ThreadRepositoryImpl::new(id_gen.clone(), pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            thread_cache,
            memory_cache,
            None,
        );
        (thread_app, memory_app)
    }

    fn setup_pool() -> impl std::future::Future<Output = &'static RdbPool> {
        use infra_utils::infra::test::setup_test_rdb_from;
        // `app` crate's cwd during `cargo test -p app` is `app/`, so the
        // migration directory is two levels up from there: `../infra/sql/...`.
        // This matches the existing pattern in `app::memory_vector::test`.
        async {
            if cfg!(feature = "postgres") {
                let pool = setup_test_rdb_from("../infra/sql/postgres").await;
                sqlx::query("TRUNCATE TABLE thread_memory CASCADE;")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("TRUNCATE TABLE memory CASCADE;")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("TRUNCATE TABLE thread CASCADE;")
                    .execute(pool)
                    .await
                    .unwrap();
                pool
            } else {
                let pool = setup_test_rdb_from("../infra/sql/sqlite").await;
                sqlx::query("DELETE FROM thread_memory;")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("DELETE FROM memory;")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("DELETE FROM thread;")
                    .execute(pool)
                    .await
                    .unwrap();
                pool
            }
        }
    }

    /// Insert a ROLE_SYSTEM Memory directly through the repository, returning its id.
    /// Used by tests that need a real system-prompt memory without constructing
    /// a full app-level create_memory path.
    async fn insert_system_memory(app: &ThreadAppImpl, user_id: i64, content: &str) -> Result<i64> {
        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: user_id }),
            content: content.to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleSystem as i32,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let pool = app.memory_repository().db_pool();
        let mut tx = pool.begin().await.context("begin")?;
        let id = app.memory_repository().create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;
        Ok(id.value)
    }

    async fn create_thread_with_default(
        app: &ThreadAppImpl,
        user_id: i64,
        default_system_memory_id: Option<i64>,
    ) -> Result<ThreadId> {
        let data = ThreadData {
            default_system_memory_id,
            user_id: Some(UserId { value: user_id }),
            description: Some("phase2 add_memory test".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 0,
            updated_at: 0,
            labels: vec![],
            metadata: None,
        };
        let pool = app.thread_repository().db_pool();
        let mut tx = pool.begin().await.context("begin")?;
        let id = app.thread_repository().create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;
        Ok(id)
    }

    fn user_message(content: &str, user_id: i64, parent_ids: Vec<MemoryId>) -> MemoryData {
        MemoryData {
            parent_ids,
            user_id: Some(UserId { value: user_id }),
            content: content.to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleUser as i32,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        }
    }

    /// default_system_memory_id が設定されていて、クライアントが parent_ids に
    /// ROLE_SYSTEM を含めなかった場合 — デフォルトが parent_ids 先頭に自動注入される。
    async fn _test_add_memory_injects_default_system(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 42i64;
        let system_mem_id = insert_system_memory(&app, user_id, "be helpful").await?;
        let thread_id = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;

        let new_memory = user_message("hello", user_id, vec![]);
        let created_id = app.add_memory(&thread_id, &new_memory).await?;

        let stored = app
            .memory_repository()
            .find(&created_id, false)
            .await?
            .expect("memory should exist");
        let data = stored.data.expect("data");
        assert_eq!(
            data.parent_ids.len(),
            1,
            "default system memory should be injected"
        );
        assert_eq!(data.parent_ids[0].value, system_mem_id);
        // Thread membership is now tracked through `thread_memory`; verify
        // the junction has a row for this memory under the expected thread.
        let memory_ids = app
            .thread_memory_repository()
            .find_memory_ids_by_thread_tx(app.thread_repository().db_pool(), thread_id.value)
            .await?;
        assert!(
            memory_ids.iter().any(|m| m.value == created_id.value),
            "newly added memory should appear in thread_memory junction"
        );
        Ok(())
    }

    async fn _test_find_memories_by_thread_id_hydrates_thread_ids(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 43i64;
        let thread_a = create_thread_with_default(&app, user_id, None).await?;
        let thread_b = create_thread_with_default(&app, user_id, None).await?;

        let created_id = app
            .add_memory(&thread_a, &user_message("shared", user_id, vec![]))
            .await?;
        app.thread_memory_repository()
            .insert_auto_position_tx(pool, thread_b.value, created_id.value, 0)
            .await?;

        let memories = app
            .find_memories_by_thread_id(&thread_a, None, None, &[], &[])
            .await?;
        let data = memories
            .iter()
            .find(|m| m.id.as_ref().is_some_and(|id| id.value == created_id.value))
            .and_then(|m| m.data.as_ref())
            .expect("created memory should be returned with data");
        let thread_ids: Vec<i64> = data.thread_ids.iter().map(|id| id.value).collect();
        let mut expected = vec![thread_a.value, thread_b.value];
        expected.sort_unstable();
        assert_eq!(thread_ids, expected);
        Ok(())
    }

    /// default_system_memory_id が None の thread に memory を追加しても、
    /// parent_ids は client 指定のままで自動注入されない。
    async fn _test_add_memory_no_default_no_injection(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 43i64;
        let thread_id = create_thread_with_default(&app, user_id, None).await?;

        let new_memory = user_message("hello without default", user_id, vec![]);
        let created_id = app.add_memory(&thread_id, &new_memory).await?;

        let stored = app
            .memory_repository()
            .find(&created_id, false)
            .await?
            .expect("memory should exist");
        let data = stored.data.expect("data");
        assert!(
            data.parent_ids.is_empty(),
            "no default → parent_ids should stay empty"
        );
        Ok(())
    }

    /// クライアントが明示的に ROLE_SYSTEM parent を指定した場合、
    /// thread.default_system_memory_id は *注入されない*。
    async fn _test_add_memory_explicit_system_parent_preserved(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 44i64;
        let default_mem_id = insert_system_memory(&app, user_id, "default prompt").await?;
        let explicit_mem_id = insert_system_memory(&app, user_id, "explicit override").await?;
        let thread_id = create_thread_with_default(&app, user_id, Some(default_mem_id)).await?;

        let new_memory = user_message(
            "hello with explicit",
            user_id,
            vec![MemoryId {
                value: explicit_mem_id,
            }],
        );
        let created_id = app.add_memory(&thread_id, &new_memory).await?;

        let stored = app
            .memory_repository()
            .find(&created_id, false)
            .await?
            .expect("memory should exist");
        let data = stored.data.expect("data");
        assert_eq!(data.parent_ids.len(), 1, "no injection expected");
        assert_eq!(
            data.parent_ids[0].value, explicit_mem_id,
            "client-supplied explicit parent must be preserved"
        );
        Ok(())
    }

    /// parent_ids に複数の ROLE_SYSTEM Memory が含まれていたら拒否される。
    async fn _test_add_memory_multiple_system_parents_rejected(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 45i64;
        let sys1 = insert_system_memory(&app, user_id, "sys1").await?;
        let sys2 = insert_system_memory(&app, user_id, "sys2").await?;
        let thread_id = create_thread_with_default(&app, user_id, None).await?;

        let new_memory = user_message(
            "hello with two systems",
            user_id,
            vec![MemoryId { value: sys1 }, MemoryId { value: sys2 }],
        );
        let result = app.add_memory(&thread_id, &new_memory).await;
        assert!(
            result.is_err(),
            "multiple ROLE_SYSTEM parents should be rejected"
        );
        let err = format!("{:?}", result.unwrap_err());
        assert!(
            err.contains("ROLE_SYSTEM"),
            "error should mention ROLE_SYSTEM, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn run_test_add_memory_injects_default_system() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_injects_default_system(pool).await
        })
    }

    #[test]
    fn run_test_find_memories_by_thread_id_hydrates_thread_ids() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_memories_by_thread_id_hydrates_thread_ids(pool).await
        })
    }

    #[test]
    fn run_test_add_memory_no_default_no_injection() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_no_default_no_injection(pool).await
        })
    }

    #[test]
    fn run_test_add_memory_explicit_system_parent_preserved() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_explicit_system_parent_preserved(pool).await
        })
    }

    #[test]
    fn run_test_add_memory_multiple_system_parents_rejected() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_multiple_system_parents_rejected(pool).await
        })
    }

    /// `delete_thread` must NOT cascade-delete a ROLE_SYSTEM Memory that is
    /// only referenced through `parent_ids` / `default_system_memory_id`.
    ///
    /// Design rationale: system prompts are created as free-standing
    /// ROLE_SYSTEM memories (`thread_id = None`, not registered in the
    /// `thread_memory` junction). They can therefore be shared across
    /// multiple threads. `delete_thread` walks the junction table to find
    /// "exclusive" memories, so system prompts referenced only via
    /// `parent_ids` stay untouched and can be reused by other threads.
    ///
    /// This test pins that invariant so future refactors (Phase 3/4) cannot
    /// accidentally start deleting shared system prompts.
    async fn _test_delete_thread_preserves_shared_system_memory(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 46i64;
        let system_mem_id = insert_system_memory(&app, user_id, "shared system prompt").await?;

        // Two threads sharing the same default system memory.
        let thread_a = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;
        let thread_b = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;

        // Add a user message to each thread; each one pulls the shared
        // system memory into its own parent_ids via auto-injection.
        let msg_a = user_message("hello from A", user_id, vec![]);
        let msg_a_id = app.add_memory(&thread_a, &msg_a).await?;
        let msg_b = user_message("hello from B", user_id, vec![]);
        let msg_b_id = app.add_memory(&thread_b, &msg_b).await?;

        // Sanity: the user messages actually reference the system memory.
        let stored_a = app
            .memory_repository()
            .find(&msg_a_id, false)
            .await?
            .expect("msg_a should exist");
        assert_eq!(
            stored_a
                .data
                .as_ref()
                .map(|d| d.parent_ids.len())
                .unwrap_or(0),
            1,
            "default system memory should be injected into thread A's user message"
        );

        // Delete thread A.
        let (deleted, exclusive_ids) = app.delete_thread(&thread_a).await?;
        assert!(deleted);
        // The user message is exclusive to thread A and must be in the
        // cascade set; the shared system memory must NOT be.
        assert!(
            exclusive_ids.contains(&msg_a_id.value),
            "thread A's user message should be in the exclusive delete list, got {exclusive_ids:?}"
        );
        assert!(
            !exclusive_ids.contains(&system_mem_id),
            "shared ROLE_SYSTEM memory must not be cascade-deleted, got {exclusive_ids:?}"
        );

        // Verify the system memory is still retrievable and still works for
        // thread B.
        let system_after = app
            .memory_repository()
            .find(
                &MemoryId {
                    value: system_mem_id,
                },
                false,
            )
            .await?;
        assert!(
            system_after.is_some(),
            "shared ROLE_SYSTEM memory should survive thread A deletion"
        );

        // And that thread B's user message is untouched.
        let msg_b_after = app.memory_repository().find(&msg_b_id, false).await?;
        assert!(
            msg_b_after.is_some(),
            "thread B's message should be unaffected by thread A deletion"
        );

        // Clean up thread B.
        let (_, _) = app.delete_thread(&thread_b).await?;
        Ok(())
    }

    #[test]
    fn run_test_delete_thread_preserves_shared_system_memory() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_thread_preserves_shared_system_memory(pool).await
        })
    }

    // ----- Phase 2 review #1 / #2: validation tests for
    // `default_system_memory_id` -----

    /// Helper that mirrors `create_thread_with_default` but goes through the
    /// **public** `ThreadApp::create_thread` so that the new validation logic
    /// is exercised. The legacy helper bypasses validation by talking to the
    /// repository directly, which is what the existing add_memory tests rely
    /// on for setup convenience.
    async fn create_thread_via_app(
        app: &ThreadAppImpl,
        user_id: i64,
        default_system_memory_id: Option<i64>,
    ) -> Result<ThreadId> {
        let data = ThreadData {
            default_system_memory_id,
            user_id: Some(UserId { value: user_id }),
            description: Some("phase2 validation test".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 0,
            updated_at: 0,
            labels: vec![],
            metadata: None,
        };
        app.create_thread(&data).await
    }

    /// #1: `Some(0)` must be normalised to `None` at create time, and
    /// AddMemory must not inject `MemoryId { value: 0 }` into parent_ids.
    async fn _test_create_thread_normalizes_zero_default(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 47i64;

        let thread_id = create_thread_via_app(&app, user_id, Some(0)).await?;

        // Round-trip the thread to verify the column was stored as NULL
        // (not 0).
        let stored = app
            .thread_repository()
            .find(&thread_id)
            .await?
            .expect("thread should exist");
        assert_eq!(
            stored
                .data
                .as_ref()
                .and_then(|d| d.default_system_memory_id),
            None,
            "Some(0) must be normalised to None on create"
        );

        // AddMemory must not auto-inject MemoryId { value: 0 }.
        let new_memory = user_message("hello", user_id, vec![]);
        let created_id = app.add_memory(&thread_id, &new_memory).await?;
        let memory = app
            .memory_repository()
            .find(&created_id, false)
            .await?
            .expect("memory should exist");
        assert!(
            memory.data.as_ref().unwrap().parent_ids.is_empty(),
            "no default → no auto-injection, even when client originally sent Some(0)"
        );
        Ok(())
    }

    /// #1: same normalisation must apply to `update_thread`.
    async fn _test_update_thread_normalizes_zero_default(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 48i64;
        let real_system_id = insert_system_memory(&app, user_id, "real default").await?;

        // Start with a real default to make sure update sets it back to None
        // when given Some(0).
        let thread_id = create_thread_via_app(&app, user_id, Some(real_system_id)).await?;

        let updated = ThreadData {
            default_system_memory_id: Some(0),
            user_id: Some(UserId { value: user_id }),
            description: Some("phase2 validation test".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 0,
            updated_at: 0,
            labels: vec![],
            metadata: None,
        };
        app.update_thread(&thread_id, &Some(updated)).await?;

        let stored = app
            .thread_repository()
            .find(&thread_id)
            .await?
            .expect("thread should exist");
        assert_eq!(
            stored
                .data
                .as_ref()
                .and_then(|d| d.default_system_memory_id),
            None,
            "Some(0) on update must clear the default"
        );
        Ok(())
    }

    /// #2: a `default_system_memory_id` pointing at a non-existent Memory id
    /// must be rejected.
    async fn _test_create_thread_rejects_missing_default(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 49i64;

        let bogus_id = 9_999_999_999i64;
        let result = create_thread_via_app(&app, user_id, Some(bogus_id)).await;
        assert!(result.is_err(), "missing memory id must be rejected");
        let err = format!("{:?}", result.unwrap_err());
        assert!(
            err.contains("does not exist") || err.contains("NotFound"),
            "error should explain missing memory; got: {err}"
        );
        Ok(())
    }

    /// #2: a `default_system_memory_id` whose target is not ROLE_SYSTEM must
    /// be rejected.
    async fn _test_create_thread_rejects_non_system_role_default(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 50i64;

        // Insert a ROLE_USER memory to use as a misconfigured default.
        let user_mem = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: user_id }),
            content: "not a system prompt".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleUser as i32,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let pool_ = app.memory_repository().db_pool();
        let mut tx = pool_.begin().await.context("begin")?;
        let user_mem_id = app
            .memory_repository()
            .create(&mut *tx, &user_mem)
            .await?
            .value;
        tx.commit().await.context("commit")?;

        let result = create_thread_via_app(&app, user_id, Some(user_mem_id)).await;
        assert!(result.is_err(), "non-system role must be rejected");
        let err = format!("{:?}", result.unwrap_err());
        assert!(
            err.contains("ROLE_SYSTEM"),
            "error should mention ROLE_SYSTEM; got: {err}"
        );
        Ok(())
    }

    /// #2: a `default_system_memory_id` belonging to a different user is
    /// accepted as long as it points at a ROLE_SYSTEM memory.
    async fn _test_create_thread_accepts_cross_user_default(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let owner_id = 51i64;
        let other_user_id = 52i64;

        // System memory created by *other_user_id*.
        let other_system_id =
            insert_system_memory(&app, other_user_id, "belongs to other user").await?;

        let thread_id = create_thread_via_app(&app, owner_id, Some(other_system_id)).await?;
        let stored = app
            .thread_repository()
            .find(&thread_id)
            .await?
            .expect("thread should exist");
        assert_eq!(
            stored
                .data
                .as_ref()
                .and_then(|d| d.default_system_memory_id),
            Some(other_system_id),
            "cross-user ROLE_SYSTEM default must round-trip unchanged"
        );
        Ok(())
    }

    /// #2: happy path — a valid same-user ROLE_SYSTEM memory id is accepted.
    async fn _test_create_thread_accepts_valid_default(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 53i64;
        let system_mem_id = insert_system_memory(&app, user_id, "valid default").await?;

        let thread_id = create_thread_via_app(&app, user_id, Some(system_mem_id)).await?;
        let stored = app
            .thread_repository()
            .find(&thread_id)
            .await?
            .expect("thread should exist");
        assert_eq!(
            stored
                .data
                .as_ref()
                .and_then(|d| d.default_system_memory_id),
            Some(system_mem_id),
            "valid default must round-trip unchanged"
        );
        Ok(())
    }

    #[test]
    fn run_test_create_thread_normalizes_zero_default() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_create_thread_normalizes_zero_default(pool).await
        })
    }

    #[test]
    fn run_test_update_thread_normalizes_zero_default() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_update_thread_normalizes_zero_default(pool).await
        })
    }

    #[test]
    fn run_test_create_thread_rejects_missing_default() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_create_thread_rejects_missing_default(pool).await
        })
    }

    #[test]
    fn run_test_create_thread_rejects_non_system_role_default() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_create_thread_rejects_non_system_role_default(pool).await
        })
    }

    #[test]
    fn run_test_create_thread_accepts_cross_user_default() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_create_thread_accepts_cross_user_default(pool).await
        })
    }

    #[test]
    fn run_test_create_thread_accepts_valid_default() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_create_thread_accepts_valid_default(pool).await
        })
    }

    /// AddMemory must revalidate an auto-injected default system memory in
    /// the same transaction that performs the junction attach. If the
    /// thread points at a deleted default, the call must fail instead of
    /// planting a dangling row into `thread_memory`.
    async fn _test_add_memory_rejects_stale_default_system_memory(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use infra::infra::thread_memory::rdb::ThreadMemoryRepository;

        let app = build_app(pool);
        let user_id = 54i64;
        let default_id = insert_system_memory(&app, user_id, "temp default").await?;
        let thread_id = create_thread_via_app(&app, user_id, Some(default_id)).await?;

        // Simulate a stale thread configuration: the thread still points at
        // the old default id, but the underlying memory row has been
        // deleted in the meantime.
        let deleted = app
            .memory_repository()
            .delete(&MemoryId { value: default_id })
            .await?;
        assert!(deleted, "the default memory must be deleted for this test");

        let result = app
            .add_memory(
                &thread_id,
                &user_message("hello after stale default", user_id, vec![]),
            )
            .await;
        assert!(
            result.is_err(),
            "stale default_system_memory_id must be rejected"
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("does not exist") || err.contains("default_system_memory_id"),
            "error should mention the stale default, got: {err}"
        );

        // create_thread now pre-registers the default system memory in the
        // junction (Phase 4 orphan-safety). The stale default's junction row
        // persists even though the underlying memory row was deleted — that
        // is expected; the invariant is that add_memory's own transaction
        // must not leave *new* rows behind on failure. The only pre-existing
        // row is the one for the (now stale) default_system_memory_id.
        let members = app
            .thread_memory_repository()
            .find_memory_ids_by_thread_tx(app.thread_repository().db_pool(), thread_id.value)
            .await?;
        let member_ids: Vec<i64> = members.iter().map(|m| m.value).collect();
        assert!(
            member_ids == vec![default_id] || member_ids.is_empty(),
            "after failed add_memory only the pre-registered default may remain: {member_ids:?}"
        );

        let (_, _) = app.delete_thread(&thread_id).await?;
        Ok(())
    }

    #[test]
    fn run_test_add_memory_rejects_stale_default_system_memory() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_rejects_stale_default_system_memory(pool).await
        })
    }

    /// A single AddMemory with a duplicate external_id must surface as
    /// AlreadyExists, matching AddMemoriesBatch and keeping callers away
    /// from raw UNIQUE-constraint database errors.
    async fn _test_add_memory_duplicate_external_id_returns_already_exists(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 49i64;
        let thread_id = create_thread_with_default(&app, user_id, None).await?;

        let first = MemoryData {
            external_id: Some("add-memory-duplicate-eid".to_string()),
            ..user_message("first", user_id, vec![])
        };
        app.add_memory(&thread_id, &first).await?;

        let second = MemoryData {
            external_id: Some("add-memory-duplicate-eid".to_string()),
            ..user_message("second", user_id, vec![])
        };
        let err = app.add_memory(&thread_id, &second).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<LlmMemoryError>(),
                Some(LlmMemoryError::AlreadyExists(_))
            ),
            "expected AlreadyExists, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn run_test_add_memory_duplicate_external_id_returns_already_exists() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_duplicate_external_id_returns_already_exists(pool).await
        })
    }

    // ---- Phase 4: ThreadApp::resolve_ancestor_closure + add_memory junction attach ----

    /// Helper: look up a memory by id and unwrap its parent_ids as i64.
    async fn parent_ids_of(app: &ThreadAppImpl, id: i64) -> Result<Vec<i64>> {
        let mem = app
            .memory_repository()
            .find(&MemoryId { value: id }, false)
            .await?
            .expect("memory exists");
        Ok(mem
            .data
            .expect("data exists")
            .parent_ids
            .into_iter()
            .map(|p| p.value)
            .collect())
    }

    /// `add_memory` must register every parent_ids reference into the
    /// thread_memory junction in addition to the new message itself. This
    /// is the invariant that makes the Phase 4 ancestor traversal resolvable
    /// for shared ROLE_SYSTEM prompts.
    async fn _test_add_memory_registers_parent_ids_in_junction(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use infra::infra::thread_memory::rdb::ThreadMemoryRepository;

        let app = build_app(pool);
        let user_id = 80i64;
        let system_mem_id = insert_system_memory(&app, user_id, "system prompt A").await?;
        let thread_id = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;

        let msg = user_message("hello world", user_id, vec![]);
        let created_id = app.add_memory(&thread_id, &msg).await?;

        // The junction must now contain both the system prompt (via auto
        // injection into parent_ids) and the newly created user message.
        let members = app
            .thread_memory_repository()
            .find_memory_ids_by_thread_tx(app.thread_repository().db_pool(), thread_id.value)
            .await?;
        let member_ids: Vec<i64> = members.iter().map(|m| m.value).collect();
        assert!(
            member_ids.contains(&system_mem_id),
            "parent ROLE_SYSTEM memory must be attached to junction: {member_ids:?}"
        );
        assert!(
            member_ids.contains(&created_id.value),
            "new user message must be attached to junction: {member_ids:?}"
        );

        // Clean up the thread (exclusive memories only — the shared system
        // prompt must survive).
        let (_, _) = app.delete_thread(&thread_id).await?;
        // Manually drop the system memory since it's orphaned after thread
        // deletion (no other thread references it in this test).
        let _ = app
            .memory_repository()
            .delete(&MemoryId {
                value: system_mem_id,
            })
            .await;
        Ok(())
    }

    /// When the same shared ROLE_SYSTEM prompt is the default for two
    /// threads, the second thread's `add_memory` must be a silent no-op on
    /// the junction row for the prompt (the first thread already attached
    /// it). After deleting one thread, the other thread's conversation —
    /// including the shared system prompt — must survive untouched.
    async fn _test_add_memory_parent_ids_registration_idempotent(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use infra::infra::thread_memory::rdb::ThreadMemoryRepository;

        let app = build_app(pool);
        let user_id = 81i64;
        let system_mem_id = insert_system_memory(&app, user_id, "shared prompt").await?;
        let thread_a = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;
        let thread_b = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;

        let msg_a = app
            .add_memory(&thread_a, &user_message("hello from A", user_id, vec![]))
            .await?;
        let msg_b = app
            .add_memory(&thread_b, &user_message("hello from B", user_id, vec![]))
            .await?;

        // Both threads must see the system memory in their junction rows
        // after idempotent attach.
        for (tid, label) in [(thread_a, "A"), (thread_b, "B")] {
            let members = app
                .thread_memory_repository()
                .find_memory_ids_by_thread_tx(app.thread_repository().db_pool(), tid.value)
                .await?;
            assert!(
                members.iter().any(|m| m.value == system_mem_id),
                "thread {label} must have the shared system memory in its junction"
            );
        }

        // Deleting thread A must not cascade-delete the shared system
        // memory (it is non-exclusive per find_exclusive_memory_ids_tx).
        let (_, exclusive_a) = app.delete_thread(&thread_a).await?;
        assert!(
            exclusive_a.contains(&msg_a.value),
            "thread A's user message should be exclusive"
        );
        assert!(
            !exclusive_a.contains(&system_mem_id),
            "shared system memory must not be cascade-deleted"
        );

        // Thread B's junction must still carry system_mem_id and msg_b.
        let members_b = app
            .thread_memory_repository()
            .find_memory_ids_by_thread_tx(app.thread_repository().db_pool(), thread_b.value)
            .await?;
        let ids_b: Vec<i64> = members_b.iter().map(|m| m.value).collect();
        assert!(
            ids_b.contains(&system_mem_id),
            "system survived in B: {ids_b:?}"
        );
        assert!(ids_b.contains(&msg_b.value), "msg_b survived: {ids_b:?}");

        // Cleanup
        let (_, _) = app.delete_thread(&thread_b).await?;
        let _ = app
            .memory_repository()
            .delete(&MemoryId {
                value: system_mem_id,
            })
            .await;
        Ok(())
    }

    /// Resolving the ancestor closure from the latest message of a simple
    /// conversation returns every memory in the chain (including the
    /// ROLE_SYSTEM) sorted by position, and selects the single ROLE_SYSTEM
    /// as the effective system prompt.
    async fn _test_traversal_linear_chain(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 90i64;
        let system_mem_id = insert_system_memory(&app, user_id, "sys 1").await?;
        let thread_id = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;

        // msg1 inherits the default system via auto-injection.
        let msg1 = app
            .add_memory(&thread_id, &user_message("first user", user_id, vec![]))
            .await?;
        // msg2 explicitly references msg1 as its parent. The test helper
        // `user_message` takes a parent_ids vector directly.
        let msg2 = app
            .add_memory(
                &thread_id,
                &user_message("second user", user_id, vec![msg1]),
            )
            .await?;
        // Sanity:
        //   msg1: empty caller parent_ids + thread default -> [system_mem_id]
        //   msg2: caller passed [msg1] but no explicit ROLE_SYSTEM, so the
        //         thread default is injected at the head -> [system_mem_id, msg1]
        let parents_msg1 = parent_ids_of(&app, msg1.value).await?;
        assert_eq!(parents_msg1, vec![system_mem_id]);
        let parents_msg2 = parent_ids_of(&app, msg2.value).await?;
        assert_eq!(parents_msg2, vec![system_mem_id, msg1.value]);

        let closure = app
            .resolve_ancestor_closure(&thread_id, &msg2, None)
            .await?;

        // ordered_memories must include sys, msg1, msg2 in position order.
        // Phase 4 attaches parent_ids into the junction just before creating
        // the child message, so attach order is: system (0), msg1 (1), msg2 (2).
        let ids: Vec<i64> = closure
            .ordered_memories
            .iter()
            .map(|m| m.memory.id.as_ref().unwrap().value)
            .collect();
        assert_eq!(
            ids,
            vec![system_mem_id, msg1.value, msg2.value],
            "ancestor closure must be system, msg1, msg2 in position order"
        );
        for memory in &closure.ordered_memories {
            let thread_ids = memory
                .memory
                .data
                .as_ref()
                .expect("ordered memory data")
                .thread_ids
                .iter()
                .map(|id| id.value)
                .collect::<Vec<_>>();
            assert_eq!(
                thread_ids,
                vec![thread_id.value],
                "ancestor closure memories must include hydrated thread_ids"
            );
        }

        // system_memory resolves to the ROLE_SYSTEM row.
        let sys = closure
            .system_memory
            .as_ref()
            .expect("ROLE_SYSTEM must be selected");
        assert_eq!(sys.memory.id.as_ref().unwrap().value, system_mem_id);
        let sys_thread_ids = sys
            .memory
            .data
            .as_ref()
            .expect("system memory data")
            .thread_ids
            .iter()
            .map(|id| id.value)
            .collect::<Vec<_>>();
        assert_eq!(sys_thread_ids, vec![thread_id.value]);

        // Cleanup
        let (_, _) = app.delete_thread(&thread_id).await?;
        let _ = app
            .memory_repository()
            .delete(&MemoryId {
                value: system_mem_id,
            })
            .await;
        Ok(())
    }

    /// When the thread has no default system memory and no message in the
    /// chain carries a ROLE_SYSTEM parent, `system_memory` must be `None`
    /// while `ordered_memories` still reflects the conversation order.
    async fn _test_traversal_no_system_returns_none(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 91i64;
        let thread_id = create_thread_with_default(&app, user_id, None).await?;
        let msg1 = app
            .add_memory(&thread_id, &user_message("user msg 1", user_id, vec![]))
            .await?;
        let msg2 = app
            .add_memory(&thread_id, &user_message("user msg 2", user_id, vec![msg1]))
            .await?;

        let closure = app
            .resolve_ancestor_closure(&thread_id, &msg2, None)
            .await?;
        assert_eq!(closure.ordered_memories.len(), 2);
        assert!(
            closure.system_memory.is_none(),
            "no ROLE_SYSTEM in chain => system_memory must be None"
        );

        let (_, _) = app.delete_thread(&thread_id).await?;
        Ok(())
    }

    /// Passing `max_depth = Some(0)` must be rejected with InvalidArgument
    /// before any DB work is done.
    async fn _test_traversal_max_depth_zero_rejected(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 92i64;
        let thread_id = create_thread_with_default(&app, user_id, None).await?;
        let msg = app
            .add_memory(&thread_id, &user_message("only", user_id, vec![]))
            .await?;

        let result = app
            .resolve_ancestor_closure(&thread_id, &msg, Some(0))
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("max_depth"),
            "error must mention max_depth: {err}"
        );

        let (_, _) = app.delete_thread(&thread_id).await?;
        Ok(())
    }

    /// Starting a traversal from a memory that is not registered in the
    /// thread_memory junction under the given thread must return
    /// `NotFound`, not silently produce an empty closure.
    async fn _test_traversal_start_not_in_junction_errors(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 93i64;
        let thread_id = create_thread_with_default(&app, user_id, None).await?;
        // Create a free-standing memory that is NOT attached to the thread.
        let orphan = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: user_id }),
            content: "orphan".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleUser as i32,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = app.memory_repository().db_pool().begin().await?;
        let orphan_id = app.memory_repository().create(&mut *tx, &orphan).await?;
        tx.commit().await?;

        let result = app
            .resolve_ancestor_closure(&thread_id, &orphan_id, None)
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not attached"),
            "error must explain that start memory is not attached: {err}"
        );

        // Cleanup
        let _ = app.memory_repository().delete(&orphan_id).await;
        let (_, _) = app.delete_thread(&thread_id).await?;
        Ok(())
    }

    #[test]
    fn run_test_add_memory_registers_parent_ids_in_junction() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_registers_parent_ids_in_junction(pool).await
        })
    }

    #[test]
    fn run_test_add_memory_parent_ids_registration_idempotent() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_parent_ids_registration_idempotent(pool).await
        })
    }

    #[test]
    fn run_test_traversal_linear_chain() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_traversal_linear_chain(pool).await
        })
    }

    #[test]
    fn run_test_traversal_no_system_returns_none() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_traversal_no_system_returns_none(pool).await
        })
    }

    #[test]
    fn run_test_traversal_max_depth_zero_rejected() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_traversal_max_depth_zero_rejected(pool).await
        })
    }

    #[test]
    fn run_test_traversal_start_not_in_junction_errors() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_traversal_start_not_in_junction_errors(pool).await
        })
    }

    // ---- Phase 4 review: cross-user parent_ids are allowed ----

    /// `add_memory` accepts `parent_ids` whose memories carry a different
    /// `user_id` than the thread metadata. The parent still needs to exist
    /// so it can be attached to the junction and participate in traversal.
    async fn _test_add_memory_accepts_cross_user_parent_ids(pool: &'static RdbPool) -> Result<()> {
        use infra::infra::thread_memory::rdb::ThreadMemoryRepository;

        let app = build_app(pool);
        let alice = 95i64;
        let bob = 96i64;

        // Alice owns a memory (via direct repository insert so we have a
        // row that was never attached to any thread; the check would fail
        // identically even for an attached row, but this exercises the
        // simplest setup).
        let alice_memory = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: alice }),
            content: "alice's memory".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleUser as i32,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = app.memory_repository().db_pool().begin().await?;
        let alice_memory_id = app
            .memory_repository()
            .create(&mut *tx, &alice_memory)
            .await?;
        tx.commit().await?;

        // Bob owns his own thread with no default system prompt.
        let bob_thread_id = create_thread_with_default(&app, bob, None).await?;

        // Bob pulls Alice's memory into his thread via parent_ids.
        let cross_user = user_message("cross-user attempt", bob, vec![alice_memory_id]);
        let created_id = app.add_memory(&bob_thread_id, &cross_user).await?;

        // The junction should now contain both the referenced parent and the
        // newly created message.
        let bob_members = app
            .thread_memory_repository()
            .find_memory_ids_by_thread_tx(app.thread_repository().db_pool(), bob_thread_id.value)
            .await?;
        assert!(
            bob_members.iter().any(|m| m.value == alice_memory_id.value),
            "cross-user parent must be attached to the target thread: {bob_members:?}"
        );
        assert!(
            bob_members.iter().any(|m| m.value == created_id.value),
            "newly created message must be attached to the target thread: {bob_members:?}"
        );

        // Cleanup
        let _ = app.memory_repository().delete(&alice_memory_id).await;
        let (_, _) = app.delete_thread(&bob_thread_id).await?;
        Ok(())
    }

    #[test]
    fn run_test_add_memory_accepts_cross_user_parent_ids() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_accepts_cross_user_parent_ids(pool).await
        })
    }

    // ---- Phase 4 review: ghost parent_ids must not pollute the junction ----

    /// When `parent_ids` contains an id that does not exist in the
    /// `memory` table, `add_memory` must skip the junction attach for
    /// that id. Otherwise a client (buggy or malicious) could plant
    /// dangling rows in `thread_memory` just by listing a random id.
    ///
    /// The existing ROLE_SYSTEM / real parent must still land in the
    /// junction so that subsequent traversal works correctly.
    async fn _test_add_memory_skips_nonexistent_parent_ids(pool: &'static RdbPool) -> Result<()> {
        use infra::infra::thread_memory::rdb::ThreadMemoryRepository;

        let app = build_app(pool);
        let user_id = 110i64;
        // A real, owned memory that will serve as the legitimate parent.
        let real_parent = insert_system_memory(&app, user_id, "real system").await?;
        // Fabricated id that does NOT exist in the `memory` table.
        let ghost_id = 9_999_999_999_999_999_i64;
        let thread_id = create_thread_with_default(&app, user_id, None).await?;

        // Caller passes [real_parent, ghost_id] — the ghost must be
        // silently skipped, the real parent must be attached.
        let new_memory = user_message(
            "hello with a ghost parent",
            user_id,
            vec![
                MemoryId { value: real_parent },
                MemoryId { value: ghost_id },
            ],
        );
        let created_id = app.add_memory(&thread_id, &new_memory).await?;

        // Junction should contain exactly: real_parent (attached first),
        // and the new message (attached second). The ghost must NOT be
        // there.
        let members = app
            .thread_memory_repository()
            .find_memory_ids_by_thread_tx(app.thread_repository().db_pool(), thread_id.value)
            .await?;
        let ids: Vec<i64> = members.iter().map(|m| m.value).collect();
        assert!(
            ids.contains(&real_parent),
            "legitimate parent must be attached: {ids:?}"
        );
        assert!(
            ids.contains(&created_id.value),
            "new message must be attached: {ids:?}"
        );
        assert!(
            !ids.contains(&ghost_id),
            "ghost parent id must not appear in junction: {ids:?}"
        );
        assert_eq!(
            ids.len(),
            2,
            "junction should have exactly 2 members (real parent + new message): {ids:?}"
        );

        // Cleanup
        let (_, _) = app.delete_thread(&thread_id).await?;
        let _ = app
            .memory_repository()
            .delete(&MemoryId { value: real_parent })
            .await;
        Ok(())
    }

    #[test]
    fn run_test_add_memory_skips_nonexistent_parent_ids() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_add_memory_skips_nonexistent_parent_ids(pool).await
        })
    }

    // ---- Phase 4 review: max_depth semantics = parent hop count ----

    /// `max_depth = 1` must allow the traversal to reach the direct
    /// parents of the starting memory — the starting memory itself is
    /// at hop 0 (no budget consumed) and its immediate parents are at
    /// hop 1. Before the off-by-one fix, `max_depth = 1` erroneously
    /// returned only the starting memory.
    async fn _test_traversal_max_depth_one_allows_single_hop(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 111i64;
        let system_mem_id = insert_system_memory(&app, user_id, "grand system").await?;
        let thread_id = create_thread_with_default(&app, user_id, Some(system_mem_id)).await?;

        // msg1 inherits system as its parent (auto-injected).
        let msg1 = app
            .add_memory(&thread_id, &user_message("first", user_id, vec![]))
            .await?;
        // msg2 references msg1; because thread has a default system,
        // msg2.parent_ids ends up as [system_mem_id, msg1].
        let msg2 = app
            .add_memory(&thread_id, &user_message("second", user_id, vec![msg1]))
            .await?;

        // max_depth = 1: we should see msg2 (hop 0) and its direct parents
        // (hop 1 = system_mem_id + msg1). Grandparents (none in this chain
        // because msg1's parent is the same system_mem_id, already visited)
        // should not expand further.
        let closure = app
            .resolve_ancestor_closure(&thread_id, &msg2, Some(1))
            .await?;

        let ids: Vec<i64> = closure
            .ordered_memories
            .iter()
            .map(|m| m.memory.id.as_ref().unwrap().value)
            .collect();
        assert!(
            ids.contains(&msg2.value),
            "start memory must be present at max_depth=1: {ids:?}"
        );
        assert!(
            ids.contains(&msg1.value),
            "direct parent must be reachable at max_depth=1: {ids:?}"
        );
        assert!(
            ids.contains(&system_mem_id),
            "direct parent (system) must be reachable at max_depth=1: {ids:?}"
        );

        // Cleanup
        let (_, _) = app.delete_thread(&thread_id).await?;
        let _ = app
            .memory_repository()
            .delete(&MemoryId {
                value: system_mem_id,
            })
            .await;
        Ok(())
    }

    #[test]
    fn run_test_traversal_max_depth_one_allows_single_hop() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_traversal_max_depth_one_allows_single_hop(pool).await
        })
    }

    // ---- Cross-user regression: thread deletion must not destroy shared memories ----

    /// When Bob's thread references Alice's memory and Bob deletes his
    /// thread, Alice's memory must survive because it is still attached to
    /// Alice's thread (orphan check sees the remaining junction row).
    async fn _test_delete_thread_preserves_cross_user_memory(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let alice = 200i64;
        let bob = 201i64;

        let alice_system = insert_system_memory(&app, alice, "alice system").await?;
        let alice_thread = create_thread_with_default(&app, alice, Some(alice_system)).await?;
        let alice_msg = user_message("alice says hi", alice, vec![]);
        let alice_msg_id = app.add_memory(&alice_thread, &alice_msg).await?;

        // Bob creates his own thread and references Alice's message as a parent.
        let bob_thread = create_thread_with_default(&app, bob, None).await?;
        let bob_msg = user_message("bob references alice", bob, vec![alice_msg_id]);
        let _bob_msg_id = app.add_memory(&bob_thread, &bob_msg).await?;

        // Delete Bob's thread.
        let (deleted, exclusive_ids) = app.delete_thread(&bob_thread).await?;
        assert!(deleted, "Bob's thread should be deleted");

        // Alice's message must NOT be in the exclusive (cascade-deleted) set,
        // because it is still attached to Alice's thread.
        assert!(
            !exclusive_ids.contains(&alice_msg_id.value),
            "Alice's memory must not be cascade-deleted: {exclusive_ids:?}"
        );
        let alice_mem_after = app.memory_repository().find(&alice_msg_id, false).await?;
        assert!(
            alice_mem_after.is_some(),
            "Alice's memory must survive Bob's thread deletion"
        );

        // Cleanup
        let (_, _) = app.delete_thread(&alice_thread).await?;
        Ok(())
    }

    #[test]
    fn run_test_delete_thread_preserves_cross_user_memory() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_thread_preserves_cross_user_memory(pool).await
        })
    }

    // ---- Cross-user regression: delete_memory clears dangling default refs ----

    /// When Bob sets Alice's system memory as his thread's default, Alice
    /// must still be able to delete her own memory. The deletion should
    /// clear Bob's thread default rather than blocking.
    async fn _test_delete_memory_clears_cross_user_default(pool: &'static RdbPool) -> Result<()> {
        use crate::app::memory::{MemoryApp, MemoryAppImpl};
        use memory_utils::cache::stretto::{MemoryCacheConfig, new_memory_cache};

        let thread_app = build_app(pool);
        let alice = 210i64;
        let bob = 211i64;

        let alice_system = insert_system_memory(&thread_app, alice, "alice prompt").await?;

        // Bob creates a thread that uses Alice's system memory as its default.
        let bob_thread = create_thread_via_app(&thread_app, bob, Some(alice_system)).await?;

        // Warm the thread cache first so the delete path must actively
        // invalidate it instead of falling back to a cold repository read.
        let cached_before = thread_app
            .find_thread(&bob_thread, None)
            .await?
            .expect("Bob's thread should exist before delete");
        assert_eq!(
            cached_before
                .data
                .as_ref()
                .and_then(|d| d.default_system_memory_id),
            Some(alice_system)
        );

        // Build a MemoryAppImpl to call delete_memory.
        let id_gen = infra::test_helper::shared_id_generator();
        let cache_config = MemoryCacheConfig::default();
        let shared_thread_cache = new_memory_cache::<Arc<String>, Thread>(&cache_config);
        let shared_memory_cache = new_memory_cache::<Arc<String>, Memory>(&cache_config);
        let thread_key = Arc::new(ThreadAppImpl::find_cache_key(&bob_thread.value));
        shared_thread_cache
            .insert(thread_key, cached_before, 1)
            .await;
        shared_thread_cache
            .wait()
            .await
            .context("thread cache wait")?;
        let memory_app = MemoryAppImpl::new(
            MemoryRepositoryImpl::new(id_gen.clone(), pool),
            MemoryRatingRepositoryImpl::new(id_gen.clone(), pool),
            ThreadRepositoryImpl::new(id_gen.clone(), pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            shared_thread_cache.clone(),
            shared_memory_cache,
            None,
        );
        let thread_app = ThreadAppImpl::new(
            ThreadRepositoryImpl::new(id_gen.clone(), pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            MemoryRepositoryImpl::new(id_gen.clone(), pool),
            MemoryRatingRepositoryImpl::new(id_gen, pool),
            shared_thread_cache,
            new_memory_cache::<Arc<String>, Memory>(&cache_config),
            None,
            None,
            None,
        );

        // Alice deletes her system memory — must succeed.
        let deleted = memory_app
            .delete_memory(&MemoryId {
                value: alice_system,
            })
            .await?;
        assert!(deleted, "Alice must be able to delete her own memory");

        // Bob's thread should now have default_system_memory_id cleared even
        // when the thread was already cached.
        let bob_thread_after = thread_app
            .find_thread(&bob_thread, None)
            .await?
            .expect("Bob's thread should still exist");
        assert_eq!(
            bob_thread_after
                .data
                .as_ref()
                .and_then(|d| d.default_system_memory_id),
            None,
            "Bob's thread default should be cleared after Alice deletes her memory"
        );

        // Cleanup
        let (_, _) = thread_app.delete_thread(&bob_thread).await?;
        Ok(())
    }

    #[test]
    fn run_test_delete_memory_clears_cross_user_default() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_memory_clears_cross_user_default(pool).await
        })
    }

    // ===== Label hydration and cascade tests =====

    async fn _test_find_thread_hydrates_labels(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 100;

        // Create thread
        let thread_id = create_thread_with_default(&app, user_id, None).await?;

        // Add labels
        app.add_labels(
            &thread_id,
            &["rust".to_string(), "async".to_string(), "tokio".to_string()],
        )
        .await?;

        // find_thread should hydrate labels
        let found = app.find_thread(&thread_id, None).await?.expect("found");
        let data = found.data.as_ref().unwrap();
        assert_eq!(data.labels.len(), 3);
        assert!(data.labels.contains(&"rust".to_string()));
        assert!(data.labels.contains(&"async".to_string()));
        assert!(data.labels.contains(&"tokio".to_string()));

        // Cleanup
        let _ = app.delete_thread(&thread_id).await?;
        Ok(())
    }

    async fn _test_find_thread_list_hydrates_labels(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 101;

        let t1 = create_thread_with_default(&app, user_id, None).await?;
        let t2 = create_thread_with_default(&app, user_id, None).await?;

        app.add_labels(&t1, &["label-a".to_string()]).await?;
        app.add_labels(&t2, &["label-b".to_string(), "label-c".to_string()])
            .await?;

        // find_thread_list_by_user_id should hydrate labels for all threads
        let threads = app
            .find_thread_list_by_user_id(
                UserId { value: user_id },
                None,
                None,
                ThreadListOptions::default(),
                None,
            )
            .await?;
        assert!(threads.len() >= 2);

        for thread in &threads {
            let tid = thread.id.as_ref().unwrap().value;
            let labels = &thread.data.as_ref().unwrap().labels;
            if tid == t1.value {
                assert_eq!(labels, &["label-a"]);
            } else if tid == t2.value {
                assert_eq!(labels.len(), 2);
                assert!(labels.contains(&"label-b".to_string()));
                assert!(labels.contains(&"label-c".to_string()));
            }
        }

        // Cleanup
        let _ = app.delete_thread(&t1).await?;
        let _ = app.delete_thread(&t2).await?;
        Ok(())
    }

    async fn _test_delete_thread_cascades_labels(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 102;

        let thread_id = create_thread_with_default(&app, user_id, None).await?;
        app.add_labels(
            &thread_id,
            &["to-be-deleted".to_string(), "also-deleted".to_string()],
        )
        .await?;

        // Verify labels exist
        let labels = app.find_labels(&thread_id).await?;
        assert_eq!(labels.len(), 2);

        // Delete thread — should cascade to thread_label
        let (deleted, _) = app.delete_thread(&thread_id).await?;
        assert!(deleted);

        // Labels should be gone (query via repository directly since thread is deleted)
        let labels = app
            .thread_label_repository()
            .find_labels_by_thread(thread_id.value)
            .await?;
        assert!(labels.is_empty(), "labels should be cascade-deleted");

        Ok(())
    }

    #[test]
    fn run_test_find_thread_hydrates_labels() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_thread_hydrates_labels(pool).await
        })
    }

    #[test]
    fn run_test_find_thread_list_hydrates_labels() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_thread_list_hydrates_labels(pool).await
        })
    }

    #[test]
    fn run_test_delete_thread_cascades_labels() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_thread_cascades_labels(pool).await
        })
    }

    // ---- Labels in Create/Update ----

    async fn _test_create_thread_with_labels(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 200i64;
        let data = ThreadData {
            default_system_memory_id: None,
            user_id: Some(UserId { value: user_id }),
            description: Some("create with labels".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 0,
            updated_at: 0,
            labels: vec!["alpha".to_string(), "beta".to_string()],
            metadata: None,
        };
        let thread_id = app.create_thread(&data).await?;

        let found = app
            .find_thread(&thread_id, None)
            .await?
            .expect("thread exists");
        let mut labels = found.data.as_ref().unwrap().labels.clone();
        labels.sort();
        assert_eq!(labels, vec!["alpha", "beta"]);

        // Verify via direct label query too
        let mut direct = app.find_labels(&thread_id).await?;
        direct.sort();
        assert_eq!(direct, vec!["alpha", "beta"]);

        Ok(())
    }

    async fn _test_update_thread_replaces_labels(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 201i64;

        // Create with initial labels
        let data = ThreadData {
            default_system_memory_id: None,
            user_id: Some(UserId { value: user_id }),
            description: Some("update replaces labels".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 0,
            updated_at: 0,
            labels: vec!["a".to_string(), "b".to_string()],
            metadata: None,
        };
        let thread_id = app.create_thread(&data).await?;

        // Update with different labels — should replace
        let update_data = ThreadData {
            labels: vec!["c".to_string(), "d".to_string()],
            ..data.clone()
        };
        let ok = app.update_thread(&thread_id, &Some(update_data)).await?;
        assert!(ok);

        let mut labels = app.find_labels(&thread_id).await?;
        labels.sort();
        assert_eq!(labels, vec!["c", "d"]);

        // Update with empty labels — should clear all
        let clear_data = ThreadData {
            labels: vec![],
            ..data.clone()
        };
        let ok = app.update_thread(&thread_id, &Some(clear_data)).await?;
        assert!(ok);

        let labels = app.find_labels(&thread_id).await?;
        assert!(
            labels.is_empty(),
            "empty labels should clear all: {labels:?}"
        );

        // Verify via find_thread too
        let found = app
            .find_thread(&thread_id, None)
            .await?
            .expect("thread exists");
        assert!(
            found.data.as_ref().unwrap().labels.is_empty(),
            "find_thread should return empty labels after clear"
        );

        Ok(())
    }

    #[test]
    fn run_test_create_thread_with_labels() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_create_thread_with_labels(pool).await
        })
    }

    #[test]
    fn run_test_update_thread_replaces_labels() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_update_thread_replaces_labels(pool).await
        })
    }

    /// Pagination must consider rows beyond the requested SQL slice when
    /// the caller asks for a time-range filter or a non-default sort.
    /// The pre-fix code only fetched `limit` rows from the labels SQL and
    /// post-filtered them, which silently under-filled pages whenever the
    /// filter rejected rows that fell inside the SQL slice while valid
    /// matches existed beyond it. We trigger that exact pattern by
    /// staggering `updated_at` (drives the labels-SQL ordering) and
    /// `created_at` (drives the user-supplied filter) so they disagree
    /// on which rows belong on page 0.
    async fn _test_find_threads_by_labels_paginates_after_post_filter(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 4242i64;
        let label = "ptest".to_string();
        let id_gen = infra::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen.clone(), pool);

        // updated_at -> created_at schedule (rows listed in labels-SQL
        // `updated_at DESC` order):
        //   updated=300 created=100  (NO match: created_at <= 150)
        //   updated=250 created=200  (match)
        //   updated=200 created=50   (NO match)
        //   updated=150 created=250  (match)
        //   updated=100 created=300  (match)
        // With `created_after = Some(150)` three rows match, but they are
        // interleaved with non-matches in the SQL ordering — exactly the
        // shape that under-filled pages under the pre-fix code.
        let rows = [
            (300i64, 100i64),
            (250, 200),
            (200, 50),
            (150, 250),
            (100, 300),
        ];
        let mut created_ids = Vec::new();
        for (updated_at, created_at) in rows {
            let data = ThreadData {
                default_system_memory_id: None,
                user_id: Some(UserId { value: user_id }),
                description: Some(format!("u={updated_at} c={created_at}")),
                channel: None,
                embedding: None,
                embedding_dim: None,
                created_at,
                updated_at,
                labels: vec![],
                metadata: None,
            };
            let mut tx = pool.begin().await.context("begin")?;
            let id = thread_repo.create(&mut *tx, &data).await?;
            app.thread_label_repository()
                .add_labels_tx(&mut *tx, id.value, label.as_str(), created_at)
                .await?;
            tx.commit().await.context("commit")?;
            created_ids.push(id);
        }

        let opts = ThreadListOptions {
            created_after: Some(150),
            ..Default::default()
        };

        // limit=2 page 0 — must return updated_at=250 and updated_at=150
        // (the two newest matches in default `updated_at DESC` order).
        // The pre-fix code returned only updated_at=250 because it
        // dropped the non-matching updated_at=300 from a `limit=2` SQL
        // slice and never looked further.
        let page0 = app
            .find_threads_by_labels(
                std::slice::from_ref(&label),
                false,
                Some(user_id),
                Some(2),
                Some(0),
                opts,
            )
            .await?;
        assert_eq!(page0.len(), 2, "page 0 must be filled to limit=2");
        let page0_updated: Vec<i64> = page0
            .iter()
            .map(|t| t.data.as_ref().unwrap().updated_at)
            .collect();
        assert_eq!(page0_updated, vec![250, 150]);

        // limit=2 page 1 — one match remains (updated_at=100).
        let page1 = app
            .find_threads_by_labels(
                std::slice::from_ref(&label),
                false,
                Some(user_id),
                Some(2),
                Some(2),
                opts,
            )
            .await?;
        assert_eq!(page1.len(), 1);
        assert_eq!(page1[0].data.as_ref().unwrap().updated_at, 100);

        // page 2 — exhausted.
        let page2 = app
            .find_threads_by_labels(
                std::slice::from_ref(&label),
                false,
                Some(user_id),
                Some(2),
                Some(4),
                opts,
            )
            .await?;
        assert!(page2.is_empty());

        // Default opts (no time filter, default sort) must keep using the
        // SQL fast path — sanity check that the ordering is intact.
        let all = app
            .find_threads_by_labels(
                std::slice::from_ref(&label),
                false,
                Some(user_id),
                Some(10),
                Some(0),
                ThreadListOptions::default(),
            )
            .await?;
        assert_eq!(all.len(), 5);
        let all_updated: Vec<i64> = all
            .iter()
            .map(|t| t.data.as_ref().unwrap().updated_at)
            .collect();
        assert_eq!(all_updated, vec![300, 250, 200, 150, 100]);

        // cleanup
        for id in created_ids {
            let _ = app.delete_thread(&id).await;
        }
        Ok(())
    }

    #[test]
    fn run_test_find_threads_by_labels_paginates_after_post_filter() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_threads_by_labels_paginates_after_post_filter(pool).await
        })
    }

    /// When several threads share the same `updated_at`, label-based
    /// pagination must use the same tiebreaker as the user-scoped path
    /// (`updated_at DESC, id DESC`). Otherwise the two endpoints return
    /// different orderings for the tied rows and pages can shuffle or
    /// drop entries across boundaries.
    async fn _test_find_threads_by_labels_tiebreaker_matches_user_path(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 5252i64;
        let label = "tietest".to_string();
        let id_gen = infra::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen.clone(), pool);

        // Five threads, all with the same updated_at — only the snowflake
        // id can break the tie. Insertion order ensures distinct ids.
        let shared_updated_at = 1_000_000i64;
        let mut created_ids = Vec::new();
        for i in 0..5 {
            let data = ThreadData {
                default_system_memory_id: None,
                user_id: Some(UserId { value: user_id }),
                description: Some(format!("tie {i}")),
                channel: None,
                embedding: None,
                embedding_dim: None,
                created_at: shared_updated_at,
                updated_at: shared_updated_at,
                labels: vec![],
                metadata: None,
            };
            let mut tx = pool.begin().await.context("begin")?;
            let id = thread_repo.create(&mut *tx, &data).await?;
            app.thread_label_repository()
                .add_labels_tx(&mut *tx, id.value, label.as_str(), shared_updated_at)
                .await?;
            tx.commit().await.context("commit")?;
            created_ids.push(id);
        }

        // Reference order from the user-scoped SQL path
        // (updated_at DESC, id DESC).
        let user_path = app
            .find_thread_list_by_user_id(
                UserId { value: user_id },
                Some(&10),
                Some(&0),
                ThreadListOptions::default(),
                None,
            )
            .await?;
        let user_path_ids: Vec<i64> = user_path
            .iter()
            .filter_map(|t| t.id.as_ref().map(|i| i.value))
            .filter(|id| created_ids.iter().any(|c| c.value == *id))
            .collect();
        assert_eq!(user_path_ids.len(), 5, "all five tied rows must show up");

        // Fast path (default opts): label SQL must agree.
        let fast_path = app
            .find_threads_by_labels(
                std::slice::from_ref(&label),
                false,
                Some(user_id),
                Some(10),
                Some(0),
                ThreadListOptions::default(),
            )
            .await?;
        let fast_path_ids: Vec<i64> = fast_path
            .iter()
            .filter_map(|t| t.id.as_ref().map(|i| i.value))
            .collect();
        assert_eq!(
            fast_path_ids, user_path_ids,
            "label fast path must match user-path tiebreaker"
        );

        // Post-processing path (time filter forces `needs_post_processing`).
        // `updated_after` is strict (>), so use `shared_updated_at - 1` to
        // keep all five rows in the result.
        let opts_post = ThreadListOptions {
            updated_after: Some(shared_updated_at - 1),
            ..Default::default()
        };
        let post_path = app
            .find_threads_by_labels(
                std::slice::from_ref(&label),
                false,
                Some(user_id),
                Some(10),
                Some(0),
                opts_post,
            )
            .await?;
        let post_path_ids: Vec<i64> = post_path
            .iter()
            .filter_map(|t| t.id.as_ref().map(|i| i.value))
            .collect();
        assert_eq!(
            post_path_ids, user_path_ids,
            "label post-processing path must match user-path tiebreaker"
        );

        // Pagination must not shuffle tied rows across boundaries.
        let mut paged_ids: Vec<i64> = Vec::new();
        for offset in (0..5).step_by(2) {
            let page = app
                .find_threads_by_labels(
                    std::slice::from_ref(&label),
                    false,
                    Some(user_id),
                    Some(2),
                    Some(offset as i64),
                    opts_post,
                )
                .await?;
            for t in &page {
                if let Some(id) = t.id.as_ref() {
                    paged_ids.push(id.value);
                }
            }
        }
        assert_eq!(
            paged_ids, user_path_ids,
            "paginated label results must match user-path order on ties"
        );

        // cleanup
        for id in created_ids {
            let _ = app.delete_thread(&id).await;
        }
        Ok(())
    }

    #[test]
    fn run_test_find_threads_by_labels_tiebreaker_matches_user_path() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_threads_by_labels_tiebreaker_matches_user_path(pool).await
        })
    }

    // =====================================================================
    // Phase 1 (gRPC migration) — add_memories_batch / update_memory_parents
    // =====================================================================

    fn batch_memory_input(
        external_id: &str,
        user_id: i64,
        content: &str,
        ts: i64,
        parent_external_ids: Vec<String>,
    ) -> BatchMemoryInput {
        BatchMemoryInput {
            memory: MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: user_id }),
                content: content.to_string(),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: ts,
                updated_at: ts,
                role: MessageRole::RoleUser as i32,
                external_id: Some(external_id.to_string()),
                media_object_id: None,
                thread_ids: Vec::new(),
            },
            parent_external_ids,
        }
    }

    fn channel_upsert_input(
        user_id: i64,
        channel: &str,
        memories: Vec<BatchMemoryInput>,
        upsert_by_external_id: bool,
    ) -> AddMemoriesBatchInput {
        AddMemoriesBatchInput {
            thread_target: BatchThreadTarget::UpsertByChannel(ThreadData {
                default_system_memory_id: None,
                user_id: Some(UserId { value: user_id }),
                description: Some("batch import test".to_string()),
                channel: Some(channel.to_string()),
                embedding: None,
                embedding_dim: None,
                created_at: 0,
                updated_at: 0,
                labels: vec![],
                metadata: None,
            }),
            memories,
            upsert_by_external_id,
            thread_updated_at_override: 0,
            labels: vec![],
        }
    }

    /// New thread + memories created via channel upsert, parent reference
    /// resolved within the same batch (tool_call → tool_output pattern).
    async fn _test_batch_creates_thread_and_resolves_intra_batch_parent(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_001i64;
        let channel = "import:test:intra-batch";

        let input = channel_upsert_input(
            user_id,
            channel,
            vec![
                batch_memory_input("eid-call", user_id, "tool call", 1_000, vec![]),
                batch_memory_input(
                    "eid-out",
                    user_id,
                    "tool output",
                    1_001,
                    vec!["eid-call".to_string()],
                ),
            ],
            true,
        );

        let out = app.add_memories_batch(input).await?;
        assert!(out.thread_created);
        assert_eq!(out.outcomes.len(), 2);
        assert!(out.outcomes[0].created);
        assert!(out.outcomes[1].created);
        let call_id = out.outcomes[0].memory_id.value;
        // Server-resolved parent for memories[1] should match the call id.
        assert_eq!(out.outcomes[1].resolved_parent_ids.len(), 1);
        assert_eq!(out.outcomes[1].resolved_parent_ids[0].value, call_id);

        // The new memory's persisted parent_ids must include call id.
        let memory = app
            .memory_repository()
            .find(&out.outcomes[1].memory_id, false)
            .await?
            .unwrap();
        let stored_parents: Vec<i64> = memory
            .data
            .as_ref()
            .map(|d| d.parent_ids.iter().map(|p| p.value).collect())
            .unwrap_or_default();
        assert_eq!(stored_parents, vec![call_id]);

        // Cleanup
        let _ = app.delete_thread(&out.thread_id).await;
        Ok(())
    }

    /// `AddMemoriesBatch` upserts must NOT clobber an existing thread's
    /// `metadata` column. Imports run in chunks; later chunks typically
    /// arrive without metadata and overwriting would erase state set
    /// earlier (e.g. by `plain-git`'s `git.last_commit`). The current
    /// branch leaves metadata intact — the test pins that contract.
    async fn _test_batch_upsert_preserves_existing_metadata(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_010i64;
        let channel = "import:test:metadata-preserve";

        // Seed a thread with non-empty metadata via the direct create path
        // (AddMemoriesBatch never writes metadata on create either, so
        // the explicit create + update is the realistic state).
        let seed = ThreadData {
            default_system_memory_id: None,
            user_id: Some(UserId { value: user_id }),
            description: Some("metadata preserve seed".to_string()),
            channel: Some(channel.to_string()),
            embedding: None,
            embedding_dim: None,
            created_at: 1_000,
            updated_at: 1_000,
            labels: vec![],
            metadata: Some(r#"{"git":{"last_commit":"abc"}}"#.to_string()),
        };
        let thread_id = app.create_thread(&seed).await?;

        // Now run an AddMemoriesBatch upsert with metadata = None.
        let input = channel_upsert_input(
            user_id,
            channel,
            vec![batch_memory_input(
                "preserve-1",
                user_id,
                "msg",
                1_500,
                vec![],
            )],
            true,
        );
        let out = app.add_memories_batch(input).await?;
        // Existing thread must be reused (created = false) and the same id.
        assert!(!out.thread_created);
        assert_eq!(out.thread_id, thread_id);

        let stored = app
            .find_thread(&thread_id, None)
            .await?
            .expect("thread present");
        // JSON-equivalent comparison: PostgreSQL's JSONB canonicalises
        // whitespace and may reorder keys. The proto contract is
        // round-trip-as-JSON, not byte-identical.
        let raw = stored.data.unwrap().metadata.expect("metadata still set");
        let stored_json: serde_json::Value =
            serde_json::from_str(&raw).expect("metadata is valid JSON");
        let expected_json: serde_json::Value =
            serde_json::from_str(r#"{"git":{"last_commit":"abc"}}"#).unwrap();
        assert_eq!(
            stored_json, expected_json,
            "AddMemoriesBatch must not overwrite existing thread metadata"
        );
        Ok(())
    }

    /// Re-importing the same batch is idempotent: second pass returns
    /// `created = false` for every memory and does not create more rows.
    async fn _test_batch_is_idempotent_on_re_import(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_002i64;
        let channel = "import:test:idempotent";

        let make_input = || {
            channel_upsert_input(
                user_id,
                channel,
                vec![
                    batch_memory_input("idp-1", user_id, "msg1", 1_000, vec![]),
                    batch_memory_input("idp-2", user_id, "msg2", 1_001, vec![]),
                ],
                true,
            )
        };

        let first = app.add_memories_batch(make_input()).await?;
        assert!(first.thread_created);
        assert_eq!(first.outcomes.iter().filter(|o| o.created).count(), 2);

        let second = app.add_memories_batch(make_input()).await?;
        assert!(!second.thread_created);
        assert_eq!(second.outcomes.iter().filter(|o| o.created).count(), 0);
        assert_eq!(second.thread_id.value, first.thread_id.value);

        let _ = app.delete_thread(&first.thread_id).await;
        Ok(())
    }

    /// `upsert_by_external_id=true` overwrites the existing memory's
    /// content on an external_id collision (the documented contract) and
    /// queues it for re-embedding, while keeping the same memory_id and
    /// returning `created=false`.
    async fn _test_batch_upsert_overwrites_content_on_collision(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_021i64;
        let channel = "import:test:upsert-overwrite";

        let first = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                channel,
                vec![batch_memory_input(
                    "ovw-1",
                    user_id,
                    "original",
                    1_000,
                    vec![],
                )],
                true,
            ))
            .await?;
        let original_id = first.outcomes[0].memory_id;

        // Re-import the same external_id with new content.
        let second = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                channel,
                vec![batch_memory_input(
                    "ovw-1",
                    user_id,
                    "updated",
                    2_000,
                    vec![],
                )],
                true,
            ))
            .await?;

        // Same memory, reported as not created ...
        assert_eq!(second.outcomes.len(), 1);
        assert!(!second.outcomes[0].created);
        assert_eq!(second.outcomes[0].memory_id.value, original_id.value);
        // ... and queued for re-embedding so the vector index follows the
        // new content.
        assert!(
            second
                .new_memories_for_embedding
                .iter()
                .any(|(id, _, _)| id.value == original_id.value),
            "overwritten memory must be queued for re-embedding"
        );

        // The stored content is overwritten (contract honored).
        let stored = app
            .memory_repository()
            .find_by_external_id("ovw-1")
            .await?
            .expect("memory must exist after upsert");
        assert_eq!(
            stored.data.expect("memory data").content,
            "updated",
            "upsert_by_external_id must overwrite the existing content"
        );

        let _ = app.delete_thread(&first.thread_id).await;
        Ok(())
    }

    /// On a content-only upsert, the re-embedding dispatch must use the
    /// STORED role / content_type (not the reimport input's), so a divergent
    /// input metadata cannot make the dispatch skip the text axis and leave
    /// the vector stale.
    async fn _test_batch_upsert_redispatch_uses_stored_metadata(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_022i64;
        let channel = "import:test:upsert-stored-meta";

        // First import: a normal text memory (role=User, content_type=Text)
        // — text-dispatchable.
        let first = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                channel,
                vec![batch_memory_input(
                    "meta-1",
                    user_id,
                    "original",
                    1_000,
                    vec![],
                )],
                true,
            ))
            .await?;
        let original_id = first.outcomes[0].memory_id;

        // Re-import the same external_id with new content but a divergent
        // content_type (TOOL) that, if honored, would skip the text axis.
        let mut reimport = batch_memory_input("meta-1", user_id, "updated", 2_000, vec![]);
        reimport.memory.content_type = ContentType::Tool as i32;
        let second = app
            .add_memories_batch(channel_upsert_input(user_id, channel, vec![reimport], true))
            .await?;

        // The queued dispatch entry must carry the STORED metadata (Text,
        // role User) with the NEW content — not the reimport's TOOL type.
        let (_, dispatched, _) = second
            .new_memories_for_embedding
            .iter()
            .find(|(id, _, _)| id.value == original_id.value)
            .expect("overwritten memory must be queued for re-embedding");
        assert_eq!(
            dispatched.content_type,
            ContentType::Text as i32,
            "dispatch must use the stored content_type, not the reimport input's"
        );
        assert_eq!(dispatched.role, MessageRole::RoleUser as i32);
        assert_eq!(dispatched.content, "updated");

        let _ = app.delete_thread(&first.thread_id).await;
        Ok(())
    }

    /// A content upsert must invalidate the shared `memory_cache` so a
    /// previously-cached `MemoryApp::find_memory` does not keep serving the
    /// pre-update content until TTL.
    async fn _test_batch_upsert_invalidates_memory_cache(pool: &'static RdbPool) -> Result<()> {
        use crate::app::memory::MemoryApp;

        let (thread_app, memory_app) = build_thread_and_memory_app_sharing_cache(pool);
        let user_id = 70_023i64;
        let channel = "import:test:upsert-cache";

        let first = thread_app
            .add_memories_batch(channel_upsert_input(
                user_id,
                channel,
                vec![batch_memory_input(
                    "cache-1",
                    user_id,
                    "original",
                    1_000,
                    vec![],
                )],
                true,
            ))
            .await?;
        let mid = first.outcomes[0].memory_id;

        // Prime the shared memory_cache via MemoryApp::find_memory.
        let primed = memory_app.find_memory(&mid, None).await?;
        assert_eq!(
            primed.and_then(|m| m.data).map(|d| d.content),
            Some("original".to_string())
        );

        // Overwrite the content through the thread upsert path.
        thread_app
            .add_memories_batch(channel_upsert_input(
                user_id,
                channel,
                vec![batch_memory_input(
                    "cache-1",
                    user_id,
                    "updated",
                    2_000,
                    vec![],
                )],
                true,
            ))
            .await?;

        // The cache must now serve the fresh content (not the stale cache).
        let after = memory_app.find_memory(&mid, None).await?;
        assert_eq!(
            after.and_then(|m| m.data).map(|d| d.content),
            Some("updated".to_string()),
            "memory_cache must be invalidated so find_memory returns fresh content"
        );

        let _ = thread_app.delete_thread(&first.thread_id).await;
        Ok(())
    }

    /// A content overwrite via `upsert_by_external_id` must move the
    /// thread's `updated_at` forward (even without `thread_updated_at_override`)
    /// so the thread is not missed by `updated_after` / sort-by-updated views.
    async fn _test_batch_upsert_bumps_thread_updated_at(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_024i64;
        let channel = "import:test:upsert-bump-ts";

        let first = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                channel,
                vec![batch_memory_input(
                    "bump-1",
                    user_id,
                    "original",
                    1_000,
                    vec![],
                )],
                true,
            ))
            .await?;
        let baseline = app
            .thread_repository()
            .find(&first.thread_id)
            .await?
            .unwrap()
            .data
            .unwrap()
            .updated_at;

        // Ensure `now()` advances past the baseline millisecond.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Overwrite the content WITHOUT a thread_updated_at_override.
        app.add_memories_batch(channel_upsert_input(
            user_id,
            channel,
            vec![batch_memory_input(
                "bump-1",
                user_id,
                "updated",
                2_000,
                vec![],
            )],
            true,
        ))
        .await?;
        let after = app
            .thread_repository()
            .find(&first.thread_id)
            .await?
            .unwrap()
            .data
            .unwrap()
            .updated_at;

        assert!(
            after > baseline,
            "content overwrite must bump thread.updated_at (baseline={baseline}, after={after})"
        );

        let _ = app.delete_thread(&first.thread_id).await;
        Ok(())
    }

    /// A pure new-memory import (no overwrite, no override) does not run the
    /// overwrite bump, but a plain insert path already advances the thread
    /// elsewhere; here we only assert `updated_at` never moves backwards so
    /// the new bump source cannot regress the monotonic high-watermark.
    async fn _test_batch_updated_at_never_regresses(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_025i64;
        let channel = "import:test:no-regress-ts";

        let first = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                channel,
                vec![batch_memory_input("nr-1", user_id, "a", 1_000, vec![])],
                true,
            ))
            .await?;
        let baseline = app
            .thread_repository()
            .find(&first.thread_id)
            .await?
            .unwrap()
            .data
            .unwrap()
            .updated_at;

        // A past override on a NEW (non-colliding) memory must not pull
        // updated_at backwards.
        app.add_memories_batch(AddMemoriesBatchInput {
            thread_target: BatchThreadTarget::ExistingThreadId(first.thread_id),
            memories: vec![batch_memory_input("nr-2", user_id, "b", 0, vec![])],
            upsert_by_external_id: true,
            thread_updated_at_override: 1, // far in the past
            labels: vec![],
        })
        .await?;
        let after = app
            .thread_repository()
            .find(&first.thread_id)
            .await?
            .unwrap()
            .data
            .unwrap()
            .updated_at;
        assert!(
            after >= baseline,
            "updated_at must never regress (baseline={baseline}, after={after})"
        );

        let _ = app.delete_thread(&first.thread_id).await;
        Ok(())
    }

    /// `parent_ids` populated by the caller is a hard error — server is
    /// the only source of truth for parent resolution.
    async fn _test_batch_rejects_non_empty_parent_ids(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_003i64;

        let mut bad = batch_memory_input("eid-bad", user_id, "x", 0, vec![]);
        bad.memory.parent_ids = vec![MemoryId { value: 1 }];
        let input = channel_upsert_input(user_id, "import:test:bad-parent", vec![bad], true);

        let err = app.add_memories_batch(input).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("parent_ids must be empty"),
            "expected InvalidArgument about parent_ids, got: {msg}"
        );
        Ok(())
    }

    /// Forward intra-batch reference (memories[0] points at a later
    /// eid) must be resolved by the post-loop rewire pass — Codex
    /// reordered rollouts (`function_call_output` before
    /// `function_call`) rely on this so the importer does not have
    /// to topologically sort the entries beforehand.
    async fn _test_batch_resolves_forward_intra_batch_ref(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_004i64;

        let input = channel_upsert_input(
            user_id,
            "import:test:forward",
            vec![
                batch_memory_input(
                    "eid-front",
                    user_id,
                    "front",
                    0,
                    vec!["eid-back".to_string()],
                ),
                batch_memory_input("eid-back", user_id, "back", 1, vec![]),
            ],
            true,
        );
        let out = app.add_memories_batch(input).await?;
        assert_eq!(out.outcomes.len(), 2);
        let front_id = out.outcomes[0].memory_id;
        let back_id = out.outcomes[1].memory_id;

        // The outcome should reflect the late-resolved parent so the
        // importer's rewire-gating code sees the real link.
        assert_eq!(
            out.outcomes[0].resolved_parent_ids,
            vec![back_id],
            "front entry must end up with back's memory_id as its resolved parent"
        );
        // And the persisted memory row must actually carry the parent_id.
        let stored = app
            .memory_repository()
            .find(&front_id, false)
            .await?
            .unwrap();
        let parents: Vec<i64> = stored
            .data
            .as_ref()
            .map(|d| d.parent_ids.iter().map(|p| p.value).collect())
            .unwrap_or_default();
        assert_eq!(parents, vec![back_id.value]);

        let _ = app.delete_thread(&out.thread_id).await;
        Ok(())
    }

    /// `ThreadUpsertByChannel.thread_data.default_system_memory_id`
    /// must be rejected so a non-source memory is never auto-attached
    /// to the freshly created thread's junction (which would shift
    /// imported positions and surface a foreign memory through
    /// `FindMemoriesByThreadId`). Spec §3.2.2.
    async fn _test_batch_rejects_default_system_memory_id_on_upsert(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_012i64;
        let system_id = insert_system_memory(&app, user_id, "system prompt").await?;

        let mut input = channel_upsert_input(
            user_id,
            "import:test:reject-default",
            vec![batch_memory_input("rd-1", user_id, "x", 0, vec![])],
            true,
        );
        if let BatchThreadTarget::UpsertByChannel(td) = &mut input.thread_target {
            td.default_system_memory_id = Some(system_id);
        }

        let err = app.add_memories_batch(input).await.unwrap_err();
        let downcast = err.downcast_ref::<LlmMemoryError>();
        assert!(
            matches!(downcast, Some(LlmMemoryError::InvalidArgument(_))),
            "expected InvalidArgument, got: {err}"
        );
        Ok(())
    }

    #[test]
    fn run_test_batch_rejects_default_system_memory_id_on_upsert() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_rejects_default_system_memory_id_on_upsert(pool).await
        })
    }

    /// `upsert_by_external_id = false` with an existing external_id
    /// must surface as `LlmMemoryError::AlreadyExists` so the gRPC
    /// layer maps it to `Status::already_exists`.
    async fn _test_batch_rejects_collision_when_upsert_disabled(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_011i64;
        let channel = "import:test:collision-no-upsert";

        let first = channel_upsert_input(
            user_id,
            channel,
            vec![batch_memory_input(
                "collide-eid",
                user_id,
                "first",
                0,
                vec![],
            )],
            true,
        );
        let out = app.add_memories_batch(first).await?;

        let mut second = channel_upsert_input(
            user_id,
            channel,
            vec![batch_memory_input(
                "collide-eid",
                user_id,
                "second",
                1,
                vec![],
            )],
            false,
        );
        second.thread_target = BatchThreadTarget::ExistingThreadId(out.thread_id);
        let err = app.add_memories_batch(second).await.unwrap_err();
        let downcast = err.downcast_ref::<LlmMemoryError>();
        assert!(
            matches!(downcast, Some(LlmMemoryError::AlreadyExists(_))),
            "expected AlreadyExists, got: {err}"
        );

        let _ = app.delete_thread(&out.thread_id).await;
        Ok(())
    }

    #[test]
    fn run_test_batch_rejects_collision_when_upsert_disabled() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_rejects_collision_when_upsert_disabled(pool).await
        })
    }

    /// Memory user_id mismatch with the target thread owner is rejected
    /// with PermissionDenied.
    async fn _test_batch_rejects_cross_user_memory(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let owner = 70_005i64;
        let attacker = 70_006i64;

        let input = channel_upsert_input(
            owner,
            "import:test:cross-user",
            vec![batch_memory_input(
                "eid-cross",
                attacker,
                "stolen",
                0,
                vec![],
            )],
            true,
        );
        let err = app.add_memories_batch(input).await.unwrap_err();
        let downcast = err.downcast_ref::<LlmMemoryError>();
        assert!(
            matches!(downcast, Some(LlmMemoryError::PermissionDenied(_))),
            "expected PermissionDenied, got: {err}"
        );
        Ok(())
    }

    /// `default_system_memory_id` auto-injection must be DISABLED for
    /// batch imports — the new memory's parent_ids is exactly what the
    /// server resolved from `parent_external_ids` (here: empty).
    async fn _test_batch_does_not_inject_default_system(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_007i64;
        let system_id = insert_system_memory(&app, user_id, "system prompt").await?;

        // Create a thread with default_system_memory_id, then run a batch
        // import on it via existing_thread_id.
        let thread_id = create_thread_with_default(&app, user_id, Some(system_id)).await?;

        let input = AddMemoriesBatchInput {
            thread_target: BatchThreadTarget::ExistingThreadId(thread_id),
            memories: vec![batch_memory_input(
                "eid-no-inject",
                user_id,
                "user msg",
                1_000,
                vec![],
            )],
            upsert_by_external_id: true,
            thread_updated_at_override: 0,
            labels: vec![],
        };

        let out = app.add_memories_batch(input).await?;
        let stored = app
            .memory_repository()
            .find(&out.outcomes[0].memory_id, false)
            .await?
            .unwrap();
        let stored_parents: Vec<i64> = stored
            .data
            .as_ref()
            .map(|d| d.parent_ids.iter().map(|p| p.value).collect())
            .unwrap_or_default();
        assert!(
            stored_parents.is_empty(),
            "default_system_memory_id must not be auto-injected during batch import: \
             got parents {stored_parents:?}"
        );

        let _ = app.delete_thread(&thread_id).await;
        Ok(())
    }

    /// `thread_updated_at_override` clamps via max(current, override):
    /// future overrides apply, past overrides are ignored so existing
    /// updated_at never moves backwards. Spec §3.2.4.
    async fn _test_batch_thread_updated_at_override(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_008i64;
        let channel = "import:test:override-ts";

        // First call creates the thread with `updated_at = now()` from
        // `fill_timestamps`. The override only matters relative to that
        // baseline.
        let initial = channel_upsert_input(
            user_id,
            channel,
            vec![batch_memory_input(
                "eid-base",
                user_id,
                "x",
                500_000,
                vec![],
            )],
            true,
        );
        let out = app.add_memories_batch(initial).await?;
        let baseline = app
            .thread_repository()
            .find(&out.thread_id)
            .await?
            .unwrap()
            .data
            .unwrap()
            .updated_at;

        // Override with a future timestamp → applied (max-of-two).
        let future = baseline + 10_000;
        let mut bump = AddMemoriesBatchInput {
            thread_target: BatchThreadTarget::ExistingThreadId(out.thread_id),
            memories: vec![batch_memory_input("eid-bump", user_id, "y", 0, vec![])],
            upsert_by_external_id: true,
            thread_updated_at_override: future,
            labels: vec![],
        };
        bump.upsert_by_external_id = true;
        let _ = app.add_memories_batch(bump).await?;
        let after_future = app
            .thread_repository()
            .find(&out.thread_id)
            .await?
            .unwrap()
            .data
            .unwrap()
            .updated_at;
        assert_eq!(after_future, future);

        // Override with a past timestamp → ignored (max-of-two retains
        // the live value, preventing differential-import / summarize
        // windows from being silently broken).
        let past = future - 1_000_000;
        let regress = AddMemoriesBatchInput {
            thread_target: BatchThreadTarget::ExistingThreadId(out.thread_id),
            memories: vec![batch_memory_input("eid-past", user_id, "z", 0, vec![])],
            upsert_by_external_id: true,
            thread_updated_at_override: past,
            labels: vec![],
        };
        let _ = app.add_memories_batch(regress).await?;
        let after_past = app
            .thread_repository()
            .find(&out.thread_id)
            .await?
            .unwrap()
            .data
            .unwrap()
            .updated_at;
        assert_eq!(
            after_past, future,
            "past override must not move updated_at backwards"
        );

        let _ = app.delete_thread(&out.thread_id).await;
        Ok(())
    }

    /// `update_memory_parent_ids_with_guards`:
    ///   - Default flags + non-empty existing parents → skip with reason.
    ///   - Force flag → overwrites.
    async fn _test_update_parents_guard_already_has(pool: &'static RdbPool) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_009i64;
        let channel = "import:test:guard-non-empty";

        // Create a thread with two memories: parent + child(parent=parent).
        let create = channel_upsert_input(
            user_id,
            channel,
            vec![
                batch_memory_input("guard-parent", user_id, "p", 0, vec![]),
                batch_memory_input(
                    "guard-child",
                    user_id,
                    "c",
                    1,
                    vec!["guard-parent".to_string()],
                ),
            ],
            true,
        );
        let out = app.add_memories_batch(create).await?;
        let thread_id = out.thread_id;
        let parent_id = out.outcomes[0].memory_id;
        let child_id = out.outcomes[1].memory_id;

        // Insert a second potential parent to use as the "new" parent.
        let create2 = AddMemoriesBatchInput {
            thread_target: BatchThreadTarget::ExistingThreadId(thread_id),
            memories: vec![batch_memory_input("guard-other", user_id, "o", 2, vec![])],
            upsert_by_external_id: true,
            thread_updated_at_override: 0,
            labels: vec![],
        };
        let out2 = app.add_memories_batch(create2).await?;
        let other_id = out2.outcomes[0].memory_id;

        // Default flags + child already has parent_id → skip.
        let outcome = app
            .update_memory_parent_ids_with_guards(&thread_id, &child_id, &[other_id], false, false)
            .await?;
        match outcome {
            UpdateMemoryParentsOutcome::Skipped(UpdateParentsSkipReason::AlreadyHasParents) => {}
            other => panic!("expected skip ALREADY_HAS_PARENTS, got {other:?}"),
        }
        // child.parent_ids unchanged.
        let stored = app
            .memory_repository()
            .find(&child_id, false)
            .await?
            .unwrap();
        let parents: Vec<i64> = stored
            .data
            .as_ref()
            .map(|d| d.parent_ids.iter().map(|p| p.value).collect())
            .unwrap_or_default();
        assert_eq!(parents, vec![parent_id.value]);

        // force_overwrite_when_non_empty=true → rewires.
        let outcome2 = app
            .update_memory_parent_ids_with_guards(&thread_id, &child_id, &[other_id], false, true)
            .await?;
        assert_eq!(outcome2, UpdateMemoryParentsOutcome::Rewired);
        let stored2 = app
            .memory_repository()
            .find(&child_id, false)
            .await?
            .unwrap();
        let parents2: Vec<i64> = stored2
            .data
            .as_ref()
            .map(|d| d.parent_ids.iter().map(|p| p.value).collect())
            .unwrap_or_default();
        assert_eq!(parents2, vec![other_id.value]);

        let _ = app.delete_thread(&thread_id).await;
        Ok(())
    }

    /// Dangling junction (memory deleted but `thread_memory` row lingers)
    /// must surface as NotFound rather than silently committing a no-op
    /// rewire. Spec §3.3 requires NotFound when the memory does not
    /// exist.
    async fn _test_update_parents_dangling_junction_is_not_found(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app(pool);
        let user_id = 70_010i64;
        let channel = "import:test:dangling-junction";

        let create = channel_upsert_input(
            user_id,
            channel,
            vec![batch_memory_input("dang-mem", user_id, "x", 0, vec![])],
            true,
        );
        let out = app.add_memories_batch(create).await?;
        let thread_id = out.thread_id;
        let memory_id = out.outcomes[0].memory_id;

        // Delete the memory row directly while leaving the junction row
        // in place (simulates a stale entry). Pick the placeholder for
        // the active backend so this test works under both sqlite (`?`)
        // and postgres (`$1`).
        let pool_ref = app.thread_repository().db_pool();
        let mut tx = pool_ref.begin().await?;
        #[cfg(feature = "postgres")]
        let delete_sql = "DELETE FROM memory WHERE id = $1";
        #[cfg(not(feature = "postgres"))]
        let delete_sql = "DELETE FROM memory WHERE id = ?";
        sqlx::query(delete_sql)
            .bind(memory_id.value)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let result = app
            .update_memory_parent_ids_with_guards(&thread_id, &memory_id, &[], false, false)
            .await;
        let err = result.unwrap_err();
        let downcast = err.downcast_ref::<LlmMemoryError>();
        assert!(
            matches!(downcast, Some(LlmMemoryError::NotFound(_))),
            "expected NotFound for dangling junction, got: {err}"
        );

        let _ = app.delete_thread(&thread_id).await;
        Ok(())
    }

    #[test]
    fn run_test_update_parents_dangling_junction_is_not_found() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_update_parents_dangling_junction_is_not_found(pool).await
        })
    }

    #[test]
    fn run_test_batch_creates_thread_and_resolves_intra_batch_parent() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_creates_thread_and_resolves_intra_batch_parent(pool).await
        })
    }

    #[test]
    fn run_test_batch_is_idempotent_on_re_import() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_is_idempotent_on_re_import(pool).await
        })
    }

    #[test]
    fn run_test_batch_upsert_overwrites_content_on_collision() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_upsert_overwrites_content_on_collision(pool).await
        })
    }

    #[test]
    fn run_test_batch_upsert_redispatch_uses_stored_metadata() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_upsert_redispatch_uses_stored_metadata(pool).await
        })
    }

    #[test]
    fn run_test_batch_upsert_invalidates_memory_cache() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_upsert_invalidates_memory_cache(pool).await
        })
    }

    #[test]
    fn run_test_batch_upsert_bumps_thread_updated_at() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_upsert_bumps_thread_updated_at(pool).await
        })
    }

    #[test]
    fn run_test_batch_updated_at_never_regresses() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_updated_at_never_regresses(pool).await
        })
    }

    #[test]
    fn run_test_batch_upsert_preserves_existing_metadata() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_upsert_preserves_existing_metadata(pool).await
        })
    }

    #[test]
    fn run_test_batch_rejects_non_empty_parent_ids() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_rejects_non_empty_parent_ids(pool).await
        })
    }

    #[test]
    fn run_test_batch_resolves_forward_intra_batch_ref() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_resolves_forward_intra_batch_ref(pool).await
        })
    }

    #[test]
    fn run_test_batch_rejects_cross_user_memory() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_rejects_cross_user_memory(pool).await
        })
    }

    #[test]
    fn run_test_batch_does_not_inject_default_system() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_does_not_inject_default_system(pool).await
        })
    }

    #[test]
    fn run_test_batch_thread_updated_at_override() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_thread_updated_at_override(pool).await
        })
    }

    #[test]
    fn run_test_update_parents_guard_already_has() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_update_parents_guard_already_has(pool).await
        })
    }

    // =====================================================================
    // Image memory: AddMemoriesBatch media_object ref_count + dispatch.
    //
    // agent-chat-import goes exclusively through AddMemoriesBatch. Before
    // this wiring the batch path persisted media_object_id as a column but
    // never bumped media_object.ref_count, so imported images stayed
    // orphaned (ref_count=0, gc_state=1) and the deferred GC deleted them.
    // =====================================================================

    fn build_app_with_media(pool: &'static RdbPool) -> ThreadAppImpl {
        use infra::infra::media_storage::{StorageBackend, inline::InlineMediaStorage};
        let id_gen = infra::test_helper::shared_id_generator();
        let finalizer = Arc::new(crate::app::media::MediaAppImpl::new(
            MediaObjectRepositoryImpl::new(id_gen.clone(), pool),
            Arc::new(StorageBackend::Inline(InlineMediaStorage::new())),
            id_gen.clone(),
            "memories/".to_string(),
            900,
            20 * 1024 * 1024,
        ));
        build_app(pool).with_media(MediaObjectRepositoryImpl::new(id_gen, pool), finalizer)
    }

    /// Seed a confirmed image media_object (ref_count=0, gc_state=orphan)
    /// the same way `app::memory::tests::seed_confirmed_media` does.
    async fn seed_confirmed_image(pool: &'static RdbPool, sha: &str) -> i64 {
        use infra::infra::media_object::rdb::MediaObjectReservation;
        let id_gen = infra::test_helper::shared_id_generator();
        let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), pool);
        let id = id_gen.generate_id().unwrap();
        let now = command_utils::util::datetime::now_millis();
        let mut tx = pool.begin().await.unwrap();
        repo.insert_reservation_tx(
            &mut *tx,
            &MediaObjectReservation {
                id,
                kind: 2,
                media_type: "image/png".to_string(),
                byte_size: Some(10),
                sha256: sha.to_string(),
                width: None,
                height: None,
                duration_ms: None,
                alt: None,
                created_at: now,
                updated_at: now,
            },
        )
        .await
        .unwrap();
        repo.confirm_reservation_tx(&mut *tx, id, "memories/ab/cd/x", "s3", Some(10), None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    }

    async fn _test_batch_media_bumps_ref_count_and_returns_dispatch(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use infra::infra::media_object::rdb::{GC_ACTIVE, MediaObjectRepository};
        let app = build_app_with_media(pool);
        let user_id = 70_900i64;
        let mid = seed_confirmed_image(pool, "sha-batch-media-active").await;

        let mut item = batch_memory_input("eid-img", user_id, "screenshot", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let input = channel_upsert_input(user_id, "import:test:media-active", vec![item], true);

        let out = app.add_memories_batch(input).await?;
        assert_eq!(out.outcomes.len(), 1);
        assert!(out.outcomes[0].created);

        // ref_count bumped to 1 and promoted to active (NOT orphaned).
        let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let row = repo.find_by_id(mid).await?.expect("media row");
        assert_eq!(row.ref_count, 1, "batch import must bump ref_count");
        assert_eq!(
            row.gc_state, GC_ACTIVE,
            "ref-bumped media must be active, not orphan (else GC deletes it)"
        );

        // The dispatch tuple (kind, backend) is carried out for the
        // post-commit media-axis dispatch.
        let (_id, _mem, media) = &out.new_memories_for_embedding[0];
        assert_eq!(
            media.as_ref().map(|(k, b)| (*k, b.as_str())),
            Some((2, "s3")),
            "media kind/backend must be returned for image dispatch"
        );
        Ok(())
    }

    #[test]
    fn run_test_batch_media_bumps_ref_count_and_returns_dispatch() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_media_bumps_ref_count_and_returns_dispatch(pool).await
        })
    }

    async fn _test_batch_no_media_wiring_stays_text_only(pool: &'static RdbPool) -> Result<()> {
        use infra::infra::media_object::rdb::{GC_ORPHAN, MediaObjectRepository};
        // build_app (no .with_media) = non-media deployment / env-less
        // unit-test backward-compat: media_object_id is persisted but
        // ref_count is NOT touched and no media tuple is returned.
        let app = build_app(pool);
        let user_id = 70_901i64;
        let mid = seed_confirmed_image(pool, "sha-batch-no-wiring").await;

        let mut item = batch_memory_input("eid-img2", user_id, "shot", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let input = channel_upsert_input(user_id, "import:test:no-wiring", vec![item], true);

        let out = app.add_memories_batch(input).await?;
        assert!(out.outcomes[0].created);

        let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let row = repo.find_by_id(mid).await?.expect("media row");
        assert_eq!(row.ref_count, 0, "no media wiring => ref_count untouched");
        assert_eq!(row.gc_state, GC_ORPHAN);
        let (_id, _mem, media) = &out.new_memories_for_embedding[0];
        assert!(media.is_none(), "no media wiring => text-only dispatch");
        Ok(())
    }

    #[test]
    fn run_test_batch_no_media_wiring_stays_text_only() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_no_media_wiring_stays_text_only(pool).await
        })
    }

    async fn _test_batch_nonexistent_media_rolls_back(pool: &'static RdbPool) -> Result<()> {
        let app = build_app_with_media(pool);
        let user_id = 70_902i64;
        let mut item = batch_memory_input("eid-bad", user_id, "x", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: 999_999_999 });
        let input = channel_upsert_input(user_id, "import:test:bad-media", vec![item], true);

        let err = app.add_memories_batch(input).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<LlmMemoryError>(),
                Some(LlmMemoryError::InvalidArgument(_))
            ),
            "missing media_object must be InvalidArgument: {err}"
        );
        // tx rolled back: no thread / memory created for this channel.
        let threads = app
            .find_by_channel_and_user_id("import:test:bad-media", &UserId { value: user_id })
            .await?;
        assert!(threads.is_empty(), "failed batch must not create a thread");
        Ok(())
    }

    #[test]
    fn run_test_batch_nonexistent_media_rolls_back() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_batch_nonexistent_media_rolls_back(pool).await
        })
    }

    // delete_thread must decrement media_object.ref_count for the
    // memories it deletes, else an imported image leaks (ref_count
    // never returns to 0, GC never reclaims).

    async fn _test_delete_thread_decrements_and_frees_media(pool: &'static RdbPool) -> Result<()> {
        use infra::infra::media_object::rdb::MediaObjectRepository;
        let app = build_app_with_media(pool);
        let user_id = 70_910i64;
        let mid = seed_confirmed_image(pool, "sha-del-thread-frees").await;

        let mut item = batch_memory_input("eid-del", user_id, "shot", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let out = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                "import:test:del-frees",
                vec![item],
                true,
            ))
            .await?;
        let thread_id = out.thread_id;
        let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        assert_eq!(repo.find_by_id(mid).await?.unwrap().ref_count, 1);

        let (deleted, exclusive) = app.delete_thread(&thread_id).await?;
        assert!(deleted);
        assert!(!exclusive.is_empty(), "the media memory was exclusive");
        // ref_count hit 0 → claimed → finalized (inline backend row
        // delete). The media_object must be gone, not a leaked orphan.
        assert!(
            repo.find_by_id(mid).await?.is_none(),
            "media_object must be freed when its only memory's thread is deleted"
        );
        Ok(())
    }

    #[test]
    fn run_test_delete_thread_decrements_and_frees_media() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_thread_decrements_and_frees_media(pool).await
        })
    }

    async fn _test_delete_thread_shared_media_keeps_object(pool: &'static RdbPool) -> Result<()> {
        use infra::infra::media_object::rdb::MediaObjectRepository;
        let app = build_app_with_media(pool);
        let user_id = 70_911i64;
        let mid = seed_confirmed_image(pool, "sha-del-thread-shared").await;

        // Two memories in two threads reference the same media_object
        // (ref_count=2). Deleting one thread must drop it to 1, NOT 0.
        let mut a = batch_memory_input("eid-shared-a", user_id, "a", 1_000, vec![]);
        a.memory.media_object_id = Some(MediaObjectId { value: mid });
        let out_a = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                "import:test:shared-a",
                vec![a],
                true,
            ))
            .await?;
        let mut b = batch_memory_input("eid-shared-b", user_id, "b", 1_000, vec![]);
        b.memory.media_object_id = Some(MediaObjectId { value: mid });
        app.add_memories_batch(channel_upsert_input(
            user_id,
            "import:test:shared-b",
            vec![b],
            true,
        ))
        .await?;

        let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        assert_eq!(repo.find_by_id(mid).await?.unwrap().ref_count, 2);

        let (deleted, _) = app.delete_thread(&out_a.thread_id).await?;
        assert!(deleted);
        let row = repo
            .find_by_id(mid)
            .await?
            .expect("shared media must survive: still referenced by thread B");
        assert_eq!(row.ref_count, 1, "only the deleted thread's ref drops");
        Ok(())
    }

    #[test]
    fn run_test_delete_thread_shared_media_keeps_object() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_thread_shared_media_keeps_object(pool).await
        })
    }

    async fn _test_delete_thread_no_media_wiring_untouched(pool: &'static RdbPool) -> Result<()> {
        use infra::infra::media_object::rdb::MediaObjectRepository;
        // build_app (no media wiring): delete_thread must not touch
        // media_object at all (original behaviour / backward compat).
        let app = build_app(pool);
        let user_id = 70_912i64;
        let mid = seed_confirmed_image(pool, "sha-del-thread-nowire").await;

        let mut item = batch_memory_input("eid-nowire", user_id, "x", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let out = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                "import:test:del-nowire",
                vec![item],
                true,
            ))
            .await?;

        let (deleted, _) = app.delete_thread(&out.thread_id).await?;
        assert!(deleted);
        let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let row = repo.find_by_id(mid).await?.expect("media row untouched");
        assert_eq!(
            row.ref_count, 0,
            "no media wiring: add never bumped, delete never decremented"
        );
        Ok(())
    }

    #[test]
    fn run_test_delete_thread_no_media_wiring_untouched() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_thread_no_media_wiring_untouched(pool).await
        })
    }

    async fn _test_delete_thread_tolerates_dangling_media(pool: &'static RdbPool) -> Result<()> {
        use infra::infra::media_object::rdb::MediaObjectRepository;
        let app = build_app_with_media(pool);
        let user_id = 70_913i64;
        let mid = seed_confirmed_image(pool, "sha-del-thread-dangling").await;

        let mut item = batch_memory_input("eid-dangle", user_id, "x", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let out = app
            .add_memories_batch(channel_upsert_input(
                user_id,
                "import:test:del-dangling",
                vec![item],
                true,
            ))
            .await?;

        // Simulate a prior GC / migration that removed the media_object
        // row but left memory.media_object_id pointing at it (dangling).
        {
            let pool_ref = app.thread_repository().db_pool();
            let mut tx = pool_ref.begin().await?;
            #[cfg(feature = "postgres")]
            let del = "DELETE FROM media_object WHERE id = $1";
            #[cfg(not(feature = "postgres"))]
            let del = "DELETE FROM media_object WHERE id = ?";
            sqlx::query(del).bind(mid).execute(&mut *tx).await?;
            tx.commit().await?;
        }
        let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        assert!(repo.find_by_id(mid).await?.is_none(), "media row removed");

        // delete_thread must NOT roll back on the dangling pointer.
        let (deleted, exclusive) = app.delete_thread(&out.thread_id).await?;
        assert!(
            deleted,
            "thread with a dangling media ref must be deletable"
        );
        assert!(!exclusive.is_empty());
        Ok(())
    }

    #[test]
    fn run_test_delete_thread_tolerates_dangling_media() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_delete_thread_tolerates_dangling_media(pool).await
        })
    }

    // =====================================================================
    // FindMemoriesByThreadId media enrich: a thread history fetch must
    // carry the cacheable half of Memory.media for image memories, the
    // same two-stage model as Find. (The gRPC layer adds the presign.)
    // =====================================================================

    async fn seed_unresolvable_image(pool: &'static RdbPool, sha: &str) -> i64 {
        use infra::infra::media_object::rdb::MediaObjectRepository;
        let id_gen = infra::test_helper::shared_id_generator();
        let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), pool);
        let id = id_gen.generate_id().unwrap();
        let mut tx = pool.begin().await.unwrap();
        repo.insert_unresolvable_tx(
            &mut *tx,
            id,
            2,
            "image/png",
            Some(10),
            sha,
            None,
            None,
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        id
    }

    async fn _test_find_memories_by_thread_id_hydrates_media(pool: &'static RdbPool) -> Result<()> {
        let app = build_app_with_media(pool);
        let user_id = 71_400i64;
        let mid = seed_confirmed_image(pool, "sha-thread-hydrate").await;
        let mut item = batch_memory_input("eid-th-img", user_id, "a screenshot", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let input = channel_upsert_input(user_id, "import:test:th-hydrate", vec![item], true);
        let out = app.add_memories_batch(input).await?;

        let list = app
            .find_memories_by_thread_id(&out.thread_id, None, None, &[], &[])
            .await?;
        let m = list
            .iter()
            .find(|m| {
                m.data
                    .as_ref()
                    .and_then(|d| d.media_object_id)
                    .map(|x| x.value)
                    == Some(mid)
            })
            .expect("image memory in thread");
        let media = m.media.as_ref().expect("media must be hydrated");
        assert!(!media.unresolved, "confirmed s3 media is resolvable");
        assert!(
            media.presigned_url.is_none(),
            "presign is the gRPC layer's job"
        );
        assert_eq!(media.metadata.as_ref().unwrap().media_type, "image/png");
        Ok(())
    }

    #[test]
    fn run_test_find_memories_by_thread_id_hydrates_media() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_memories_by_thread_id_hydrates_media(pool).await
        })
    }

    async fn _test_find_memories_by_thread_id_no_media_subsystem_none(
        pool: &'static RdbPool,
    ) -> Result<()> {
        // build_app (no .with_media) = non-media deployment / env-less:
        // media stays None even for an image memory (backward compatible).
        let app = build_app(pool);
        let user_id = 71_401i64;
        let mid = seed_confirmed_image(pool, "sha-thread-nomedia").await;
        let mut item = batch_memory_input("eid-th-nm", user_id, "shot", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let input = channel_upsert_input(user_id, "import:test:th-nomedia", vec![item], true);
        let out = app.add_memories_batch(input).await?;

        let list = app
            .find_memories_by_thread_id(&out.thread_id, None, None, &[], &[])
            .await?;
        let m = list
            .iter()
            .find(|m| {
                m.data
                    .as_ref()
                    .and_then(|d| d.media_object_id)
                    .map(|x| x.value)
                    == Some(mid)
            })
            .expect("memory in thread");
        assert!(
            m.media.is_none(),
            "no media subsystem => media stays None (backward compatible)"
        );
        Ok(())
    }

    #[test]
    fn run_test_find_memories_by_thread_id_no_media_subsystem_none() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_memories_by_thread_id_no_media_subsystem_none(pool).await
        })
    }

    async fn _test_find_memories_by_thread_id_unresolvable_no_panic(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let app = build_app_with_media(pool);
        let user_id = 71_402i64;
        let mid = seed_unresolvable_image(pool, "sha-thread-unresolvable").await;
        let mut item = batch_memory_input("eid-th-un", user_id, "elided", 1_000, vec![]);
        item.memory.media_object_id = Some(MediaObjectId { value: mid });
        let input = channel_upsert_input(user_id, "import:test:th-unres", vec![item], true);
        let out = app.add_memories_batch(input).await?;

        let list = app
            .find_memories_by_thread_id(&out.thread_id, None, None, &[], &[])
            .await?;
        let m = list
            .iter()
            .find(|m| {
                m.data
                    .as_ref()
                    .and_then(|d| d.media_object_id)
                    .map(|x| x.value)
                    == Some(mid)
            })
            .expect("memory in thread");
        let media = m.media.as_ref().expect("metadata still hydrated");
        assert!(media.unresolved, "unresolvable => unresolved=true");
        assert!(media.presigned_url.is_none());
        Ok(())
    }

    #[test]
    fn run_test_find_memories_by_thread_id_unresolvable_no_panic() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_memories_by_thread_id_unresolvable_no_panic(pool).await
        })
    }
}
