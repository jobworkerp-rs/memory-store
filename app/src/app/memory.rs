use anyhow::Result;
use async_trait::async_trait;
use infra::error::LlmMemoryError;
use infra::infra::media_object::rdb::{
    GC_ACTIVE, GC_ORPHAN, MediaObjectRepository, MediaObjectRepositoryImpl, MediaObjectRow,
};
use infra::infra::memory::rdb::{
    CreatedAtRange, MemoryRepository, MemoryRepositoryImpl, MemorySort, UpdatedAtRange,
    UseMemoryRepository,
};
use infra::infra::memory_rating::rdb::{
    MemoryRatingRepository, MemoryRatingRepositoryImpl, UseMemoryRatingRepository,
};
use infra::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl, UseThreadRepository};
use infra::infra::thread_label::rdb::{ThreadLabelRepositoryImpl, UseThreadLabelRepository};
use infra::infra::thread_memory::rdb::{
    ThreadMemoryRepository, ThreadMemoryRepositoryImpl, UseThreadMemoryRepository,
};
use infra_utils::infra::rdb::UseRdbPool;
use memory_utils::cache::stretto::UseMemoryCache;
use memory_utils::lock::RwLockWithKey;
use protobuf::llm_memory::data::{
    Memory, MemoryData, MemoryId, Thread, ThreadSearchFilter, UserId,
};
use std::{sync::Arc, time::Duration};
use stretto::AsyncCache;

use crate::app::memory_vector::{RepresentativeThreadInfo, enrich_memories_with_thread_info};
use crate::app::thread_filter_resolver::{self, ThreadFilterConfig};

/// Capability bound for app-layer types that need the shared
/// thread_filter resolve config. The resolver itself is a free function
/// in [`thread_filter_resolver`]; this trait only exposes the cached
/// per-app config so callers don't reach into env on the hot path.
pub trait UseThreadFilterResolver {
    fn thread_filter_config(&self) -> &ThreadFilterConfig;
}

#[derive(Debug, Clone)]
pub struct MemoryCondition {
    pub roles: Vec<i32>,
    pub content_types: Vec<i32>,
    pub user_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub updated_at_range: UpdatedAtRange,
    pub created_at_range: CreatedAtRange,
    pub external_id: Option<String>,
    /// LIKE prefix match against `external_id`. The infra layer escapes
    /// `%` / `_` / `\` before issuing the SQL. Mutually exclusive with
    /// `external_id` — the gRPC handler rejects requests that set both.
    pub external_id_prefix: Option<String>,
    /// Resolved server-side into a memory_id allow-list before the SQL
    /// runs (see `thread_filter_resolver`); `None` skips the resolve.
    pub thread_filter: Option<ThreadSearchFilter>,
}

#[derive(Debug, Clone)]
pub struct MemoryListCondition {
    pub limit: Option<i32>,
    pub offset: Option<i64>,
    pub filter: MemoryCondition,
    pub sort: MemorySort,
}

#[async_trait]
pub trait MemoryApp:
    UseMemoryRepository
    + UseMemoryRatingRepository
    + UseThreadRepository
    + UseThreadMemoryRepository
    + UseThreadLabelRepository
    + UseThreadFilterResolver
    + UseMemoryCache<Arc<String>, Memory>
    + Send
    + Sync
    + Sized
    + 'static
{
    async fn create_memory(&self, memory: &MemoryData) -> Result<MemoryId>;

    async fn update_memory(
        &self,
        id: &MemoryId,
        memory: &Option<MemoryData>,
    ) -> Result<UpdateOutcome>;

    /// The media subsystem, if wired (image memory feature). `None` keeps
    /// the env-less unit tests / non-media deployments on the original
    /// no-media behaviour.
    fn media_subsystem(&self) -> Option<&MediaSubsystem> {
        None
    }

    async fn delete_thread_cache_by_id(&self, thread_id: i64) -> Result<()>;

    async fn delete_memory(&self, id: &MemoryId) -> Result<bool> {
        let db = self.memory_repository().db_pool();
        let mut tx = db.begin().await.map_err(LlmMemoryError::DBError)?;
        super::thread::lock_default_system_memory_scope_tx(&mut tx, Some(id.value)).await?;
        // Lock the referencing thread rows before the memory row so that the
        // row-lock order matches `update_thread` (`thread` -> `memory`) on
        // PostgreSQL. The advisory lock taken above prevents another
        // transaction from adding a new reference to this memory while this
        // delete is in flight, so the locked id list is complete.
        let affected_thread_ids = self
            .thread_repository()
            .find_thread_ids_by_default_system_memory_for_update_tx(&mut *tx, id.value)
            .await?;
        // With the thread rows fixed, locking the memory row now serializes
        // against sibling validations that still need to read the memory as a
        // default candidate.
        let locked = self
            .memory_repository()
            .find_by_ids_for_update_tx(&mut tx, &[id.value])
            .await?;
        if locked.is_empty() {
            // Already deleted or never existed — nothing to do.
            tx.commit().await.map_err(LlmMemoryError::DBError)?;
            return Ok(false);
        }
        // The media_object this memory referenced (image memory feature).
        // Taken from the row-locked pre-delete row.
        let media_object_id: Option<i64> = locked
            .first()
            .and_then(|m| m.data.as_ref())
            .and_then(|d| d.media_object_id.map(|x| x.value));
        // Detach any threads that reference this memory as their default
        // system prompt. Because cross-user default references are allowed,
        // blocking deletion on a ref-count would let a third party
        // permanently prevent the memory owner from deleting their own data.
        // Clearing the default is safe: affected threads will simply have no
        // default system prompt until one is reassigned.
        self.thread_repository()
            .clear_default_system_memory_tx(&mut *tx, id.value)
            .await?;
        self.memory_rating_repository()
            .delete_by_memory_id_tx(&mut *tx, id.value)
            .await?;
        self.thread_memory_repository()
            .delete_by_memory_tx(&mut *tx, id.value)
            .await?;
        // media_object ref_count decrement in the SAME tx (design 2/3
        // §7.5.4). The claim winner runs the storage->DB delete after
        // commit. Skipped entirely when the media subsystem is not wired
        // (non-media deployments / env-less tests) — original behaviour.
        let mut post_delete: Option<(i64, MediaObjectRow)> = None;
        if let (Some(moid), Some(media)) = (media_object_id, self.media_subsystem()) {
            // Primary defense: row-lock the media_object (design §6.3).
            media
                .repository
                .find_by_id_for_update_tx(&mut tx, moid)
                .await?;
            post_delete = decr_and_maybe_claim(&media.repository, &mut tx, moid).await?;
        }
        let deleted = self.memory_repository().delete_tx(&mut *tx, id).await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        // Post-commit: claim winner runs storage->DB delete (best-effort;
        // finish_delete marks 5->2 on failure and the GC retries).
        if let (Some((moid, row)), Some(media)) = (post_delete, self.media_subsystem()) {
            let _ = media.finalizer.finish_delete(moid, &row).await;
        }
        // Rating cache entries (keyed by rating_id) are not cleared here because
        // this app has no access to the rating cache; 60s TTL provides acceptable staleness.
        let k = Arc::new(Self::find_cache_key(&id.value));
        let _ = self.delete_cache(&k).await;
        for thread_id in affected_thread_ids {
            let _ = self.delete_thread_cache_by_id(thread_id).await;
        }
        Ok(deleted)
    }

    /// Atomically replace ONLY `content` (+ updated_at) and invalidate
    /// the cache, WITHOUT spawning the embedding dispatcher. Backs
    /// `MemoryService.UpdateContentNoDispatch`; its whole reason to exist
    /// is breaking the caption -> Update -> re-dispatch loop (design 2/3
    /// §7.5.3 / spec §4.2.1.1). Does NOT touch media_object_id, so a
    /// caption write cannot drop the media reference.
    async fn update_content_no_dispatch(&self, id: &MemoryId, content: &str) -> Result<bool> {
        let pool = self.memory_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;
        let ok = self
            .memory_repository()
            .update_content_only(&mut *tx, id, content)
            .await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        // Post-commit cache invalidation: without this a same-process
        // Find could return stale content for up to the cache TTL (Find
        // 30s / FindList 5s / app default 60s).
        let k = Arc::new(Self::find_cache_key(&id.value));
        let _ = self.delete_cache(&k).await;
        // Intentionally NO dispatcher spawn.
        Ok(ok)
    }

    fn find_cache_key(id: &i64) -> String {
        super::memory_cache_key(id)
    }

    async fn find_memory(&self, id: &MemoryId, ttl: Option<&Duration>) -> Result<Option<Memory>>
    where
        Self: Send + 'static,
    {
        let k = Arc::new(Self::find_cache_key(&id.value));
        let mut m = self
            .with_cache_if_some(&k, ttl, || async {
                let mut m = self.memory_repository().find(id, false).await?;
                // Hydrate the cacheable media metadata (no presigned URL —
                // the gRPC layer issues that fresh per response). Cached so
                // the search/N+1 path does not re-hit the DB (design §7.5.5).
                if let Some(mem) = m.as_mut() {
                    hydrate_media_metadata(self.media_subsystem(), mem).await?;
                }
                Ok(m)
            })
            .await?;
        if let Some(mem) = m.as_mut() {
            self.memory_repository()
                .fill_thread_ids(std::slice::from_mut(mem))
                .await?;
        }
        Ok(m)
    }
    fn find_list_cache_key(limit: Option<&i32>, offset: Option<&i64>) -> String {
        if let Some(l) = limit {
            [
                "memory_list:",
                l.to_string().as_str(),
                ":",
                offset.unwrap_or(&0i64).to_string().as_str(),
            ]
            .join("")
        } else {
            Self::find_all_list_cache_key()
        }
    }
    fn find_all_list_cache_key() -> String {
        "memory_list:all".to_string()
    }

    async fn find_memory_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<Memory>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        // let k = Arc::new(Self::find_list_cache_key(limit, offset));
        // self.with_cache(&k, ttl, || async {
        let mut memories = self
            .memory_repository()
            .find_list(limit, offset, true)
            .await?;
        hydrate_media_list(self.media_subsystem(), &mut memories).await?;
        Ok(memories)
        // })
        // .await
    }

    async fn find_recent_list_by_user_id(
        &self,
        user_id: UserId,
        limit: Option<&i32>,
        updated_after: Option<&i64>,
        updated_before: Option<&i64>,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<Memory>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        // let k = Arc::new(Self::find_list_cache_key(limit, offset));
        // self.with_cache(&k, ttl, || async {
        let mut memories = self
            .memory_repository()
            .find_recent_list_by_user_id(
                user_id,
                limit,
                UpdatedAtRange {
                    updated_after: updated_after.copied(),
                    updated_before: updated_before.copied(),
                },
                true,
            )
            .await?;
        hydrate_media_list(self.media_subsystem(), &mut memories).await?;
        Ok(memories)
        // })
        // .await
    }

    async fn find_memory_all_list(&self, _ttl: Option<&Duration>) -> Result<Vec<Memory>>
    where
        Self: Send + 'static,
    {
        // TODO list cache
        // let k = Arc::new(Self::find_all_list_cache_key());
        // self.with_cache(&k, ttl, || async {
        let mut memories = self.memory_repository().find_list(None, None, true).await?;
        hydrate_media_list(self.media_subsystem(), &mut memories).await?;
        Ok(memories)
        // })
        // .await
    }

    async fn count(&self) -> Result<i64>
    where
        Self: Send + 'static,
    {
        // TODO cache
        self.memory_repository()
            .count_list_tx(self.memory_repository().db_pool())
            .await
    }

    /// Conditional list query replacing `SystemPromptService.FindList`.
    /// `condition.filter.thread_filter` is resolved to a memory_id
    /// allow-list via [`thread_filter_resolver`] before the SQL runs;
    /// see that module for the three-state outcome (`Ok(None)` /
    /// `Ok(Some(empty))` / `Ok(Some(ids))`) and the precondition errors.
    ///
    /// Each returned row carries representative-thread metadata so the
    /// gRPC layer can emit `MemoryListEntry` (mirroring the search-hit
    /// path's `MemorySearchResult` shape). ROLE_SYSTEM and orphan
    /// protections are enforced by [`enrich_memories_with_thread_info`].
    async fn find_memory_list_by_condition(
        &self,
        condition: &MemoryListCondition,
        _ttl: Option<&Duration>,
    ) -> Result<Vec<(Memory, RepresentativeThreadInfo)>>
    where
        Self: Send + 'static,
    {
        let memory_id_constraint =
            resolve_thread_filter_constraint(self, condition.filter.thread_filter.as_ref()).await?;
        if let Some(ids) = memory_id_constraint.as_ref()
            && ids.is_empty()
        {
            return Ok(Vec::new());
        }
        // List caching is intentionally skipped — same policy as
        // `find_memory_list`. See the TODO in that method.
        let memories = self
            .memory_repository()
            .find_list_by_condition(
                condition.limit.as_ref(),
                condition.offset.as_ref(),
                &condition.filter.roles,
                &condition.filter.content_types,
                condition.filter.user_id,
                condition.filter.thread_id,
                condition.filter.updated_at_range,
                condition.filter.created_at_range,
                condition.filter.external_id.as_deref(),
                condition.filter.external_id_prefix.as_deref(),
                memory_id_constraint.as_deref(),
                condition.sort,
                true,
            )
            .await?;
        let enriched = enrich_memories_with_thread_info(
            self.thread_repository(),
            self.thread_memory_repository(),
            memories,
        )
        .await?;
        let (mut memories, infos): (Vec<_>, Vec<_>) = enriched.into_iter().unzip();
        for m in memories.iter_mut() {
            hydrate_media_metadata(self.media_subsystem(), m).await?;
        }
        Ok(memories.into_iter().zip(infos).collect())
    }

    /// Conditional count replacing `SystemPromptService.Count`.
    /// `thread_filter` is resolved the same way as in
    /// [`find_memory_list_by_condition`].
    async fn count_memory_by_condition(&self, filter: &MemoryCondition) -> Result<i64>
    where
        Self: Send + 'static,
    {
        let memory_id_constraint =
            resolve_thread_filter_constraint(self, filter.thread_filter.as_ref()).await?;
        if let Some(ids) = memory_id_constraint.as_ref()
            && ids.is_empty()
        {
            return Ok(0);
        }
        self.memory_repository()
            .count_by_condition(
                &filter.roles,
                &filter.content_types,
                filter.user_id,
                filter.thread_id,
                filter.updated_at_range,
                filter.created_at_range,
                filter.external_id.as_deref(),
                filter.external_id_prefix.as_deref(),
                memory_id_constraint.as_deref(),
            )
            .await
    }
}

/// Shared helper for `find_memory_list_by_condition` / `count_memory_by_condition`.
/// Returns the resolved memory_id allow-list, or `None` if the caller
/// passed no thread_filter.
///
/// Allow-lists larger than SQLite's `SQLITE_MAX_VARIABLE_NUMBER`
/// (default 999) used to be rejected here as a precondition error.
/// The infra layer now renders the IDs as inline integer literals
/// (see `format_i64_in_list` in `infra::infra::memory::rdb`), so the
/// allow-list never consumes bound-parameter slots and the only
/// remaining cap is the resolver's own
/// `MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS` (default 50_000).
async fn resolve_thread_filter_constraint<A: MemoryApp>(
    app: &A,
    thread_filter: Option<&ThreadSearchFilter>,
) -> Result<Option<Vec<i64>>> {
    let Some(tf) = thread_filter else {
        return Ok(None);
    };
    let resolved = thread_filter_resolver::resolve_memory_ids_from_thread_filter(
        app.thread_filter_config(),
        app.thread_label_repository(),
        app.thread_repository(),
        app.thread_memory_repository(),
        tf,
        // memory-side user_id is currently advisory; the resolver does not
        // re-apply it (see `_memory_user_id` doc on the resolver).
        None,
    )
    .await?;
    Ok(resolved)
}
pub struct MemoryAppImpl {
    memory_repository: MemoryRepositoryImpl,
    memory_rating_repository: MemoryRatingRepositoryImpl,
    thread_repository: ThreadRepositoryImpl,
    thread_memory_repository: ThreadMemoryRepositoryImpl,
    thread_label_repository: ThreadLabelRepositoryImpl,
    /// Held per-app so the per-request hot path doesn't re-parse env.
    /// Shared with `MemoryVectorAppImpl` via the same env knobs to keep
    /// the LanceDB and RDB resolve paths in lockstep.
    thread_filter_config: ThreadFilterConfig,
    thread_cache: AsyncCache<Arc<String>, Thread>,
    memory_cache: AsyncCache<Arc<String>, Memory>,
    key_lock: RwLockWithKey<Arc<String>>,
    default_ttl: Duration,
    embedding_dispatcher:
        Option<Arc<infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher>>,
    /// Read once from `MEMORY_IMAGE_SEARCH_MODE` (env is immutable) so the
    /// create/update dispatch hot path does not re-parse it, mirroring
    /// `thread_filter_config`. Decides whether a media-bearing memory
    /// also gets a `DispatchKind::Media` job.
    image_search_mode: infra::infra::embedding_dispatch::ImageSearchMode,
    /// `None` keeps non-media deployments / env-less tests on the
    /// original no-media behaviour; `Some` wires the ref_count paths.
    media: Option<MediaSubsystem>,
}

/// Result of `MemoryApp::update_memory`. `updated` is the original
/// boolean contract (`true` = a row was updated). `stale_image_cleanup`
/// is `true` when the update removed an image attachment (image →
/// no-media, or image → a non-image media) — the embedding dispatch only
/// re-runs the *text* pipeline in that case, so the memory's old
/// `image`/`caption` LanceDB rows would otherwise be orphaned and keep
/// producing stale `SearchByMedia` / `SearchSemantic` hits. The gRPC
/// layer reacts to this flag with a best-effort LanceDB cascade, mirroring
/// the `MemoryService.delete` "RDB is source of truth, vector store is
/// cascaded" pattern (review P2 #1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct UpdateOutcome {
    pub updated: bool,
    pub stale_image_cleanup: bool,
}

/// The media wiring of a `MemoryAppImpl`. Both halves are always present
/// together (set by `with_media`), so they live in one struct rather
/// than two correlated `Option`s — the "both or neither" invariant is in
/// the type, not a runtime check. Public only because it is the return
/// type of the `pub trait MemoryApp::media_subsystem`; the fields stay
/// private so it cannot be constructed/destructured outside this module.
pub struct MediaSubsystem {
    /// Sibling `MediaObjectRepositoryImpl` (same pool) so create/update/
    /// delete can do the ref_count diff in the SAME tx as the memory row
    /// — a separate tx would break the TOCTOU primary defense (row lock).
    repository: MediaObjectRepositoryImpl,
    /// Shared `MediaApp` used post-commit to run the storage→DB delete
    /// (`finish_delete`) when a ref_count hits 0. `Arc` so the gRPC layer
    /// can share the very same instance (no double storage backend).
    finalizer: Arc<crate::app::media::MediaAppImpl>,
}

impl MediaSubsystem {
    pub(crate) fn new(
        repository: MediaObjectRepositoryImpl,
        finalizer: Arc<crate::app::media::MediaAppImpl>,
    ) -> Self {
        Self {
            repository,
            finalizer,
        }
    }
    pub(crate) fn repository(&self) -> &MediaObjectRepositoryImpl {
        &self.repository
    }
    pub(crate) fn finalizer(&self) -> &Arc<crate::app::media::MediaAppImpl> {
        &self.finalizer
    }
}

impl MemoryAppImpl {
    const DEFAULT_TTL_SEC: u64 = 60;
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        memory_repository: MemoryRepositoryImpl,
        memory_rating_repository: MemoryRatingRepositoryImpl,
        thread_repository: ThreadRepositoryImpl,
        thread_memory_repository: ThreadMemoryRepositoryImpl,
        thread_label_repository: ThreadLabelRepositoryImpl,
        thread_cache: AsyncCache<Arc<String>, Thread>,
        memory_cache: AsyncCache<Arc<String>, Memory>,
        embedding_dispatcher: Option<
            Arc<infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher>,
        >,
    ) -> Self {
        Self {
            memory_repository,
            memory_rating_repository,
            thread_repository,
            thread_memory_repository,
            thread_label_repository,
            thread_filter_config: ThreadFilterConfig::from_env(),
            thread_cache,
            memory_cache,
            key_lock: RwLockWithKey::new(16 * 1024), // XXX  fix it
            default_ttl: Duration::from_secs(Self::DEFAULT_TTL_SEC),
            embedding_dispatcher,
            image_search_mode: infra::infra::embedding_dispatch::ImageSearchMode::from_env(),
            media: None,
        }
    }

    /// Wire the media subsystem (image memory feature). Called from the
    /// DI module after the `MediaApp` Arc is built. Without this the
    /// create/update/delete media ref_count paths are inert (a memory may
    /// still carry `media_object_id` but no ref_count is tracked) — only
    /// the env-less unit tests rely on that.
    pub fn with_media(
        mut self,
        media_object_repository: MediaObjectRepositoryImpl,
        media_finalizer: Arc<crate::app::media::MediaAppImpl>,
    ) -> Self {
        self.media = Some(MediaSubsystem {
            repository: media_object_repository,
            finalizer: media_finalizer,
        });
        self
    }

    /// Test-only override for the image-search mode (production reads it
    /// once from `MEMORY_IMAGE_SEARCH_MODE` in `new`). The stale-image
    /// cleanup decision depends on it via `media_axis_dispatchable`;
    /// setting the mode via env in a test would race other tests sharing
    /// the process, so this builder keeps those tests deterministic
    /// (mirrors `MemoryVectorAppImpl::with_image_search_mode`).
    #[cfg(test)]
    pub fn with_image_search_mode(
        mut self,
        mode: infra::infra::embedding_dispatch::ImageSearchMode,
    ) -> Self {
        self.image_search_mode = mode;
        self
    }

    /// Evaluate `dispatch_kinds` and, if any pipeline applies, spawn the
    /// fire-and-forget dispatch (text → text workflow, media → image
    /// workflow). No-op when no dispatcher is wired (env-less tests) or
    /// no kind applies. `update_content_no_dispatch` deliberately does
    /// NOT call this — that is the whole point of that method (it breaks
    /// the caption→Update→re-dispatch loop).
    #[allow(clippy::too_many_arguments)]
    fn spawn_embedding_dispatch(
        &self,
        memory_id: i64,
        content: &str,
        role: i32,
        content_type: i32,
        media_object_id: Option<i64>,
        media_kind: Option<i32>,
        media_storage_backend: Option<&str>,
    ) {
        let Some(dispatcher) = &self.embedding_dispatcher else {
            return;
        };
        if let Some(target) = infra::infra::memory_vector::dispatcher::DispatchTarget::from_memory(
            memory_id,
            content,
            role,
            content_type,
            media_object_id,
            media_kind,
            media_storage_backend,
            self.image_search_mode,
        ) {
            infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher::spawn_dispatch(
                dispatcher, target,
            );
        }
    }
}

impl UseMemoryRepository for MemoryAppImpl {
    fn memory_repository(&self) -> &MemoryRepositoryImpl {
        &self.memory_repository
    }
}

impl UseMemoryRatingRepository for MemoryAppImpl {
    fn memory_rating_repository(&self) -> &MemoryRatingRepositoryImpl {
        &self.memory_rating_repository
    }
}

impl UseThreadRepository for MemoryAppImpl {
    fn thread_repository(&self) -> &ThreadRepositoryImpl {
        &self.thread_repository
    }
}

impl UseThreadLabelRepository for MemoryAppImpl {
    fn thread_label_repository(&self) -> &ThreadLabelRepositoryImpl {
        &self.thread_label_repository
    }
}

impl UseThreadFilterResolver for MemoryAppImpl {
    fn thread_filter_config(&self) -> &ThreadFilterConfig {
        &self.thread_filter_config
    }
}

impl UseThreadMemoryRepository for MemoryAppImpl {
    fn thread_memory_repository(&self) -> &ThreadMemoryRepositoryImpl {
        &self.thread_memory_repository
    }
}

#[async_trait]
impl MemoryApp for MemoryAppImpl {
    fn media_subsystem(&self) -> Option<&MediaSubsystem> {
        self.media.as_ref()
    }

    async fn delete_thread_cache_by_id(&self, thread_id: i64) -> Result<()> {
        let key = Arc::new(["thread_id:", &thread_id.to_string()].join(""));
        self.thread_cache
            .try_remove(&key)
            .await
            .map_err(|e| e.into())
    }

    /// Create. If `media_object_id` is set AND the media subsystem is
    /// wired, the ref_count increment runs in the SAME tx as the memory
    /// INSERT: a row-lock (primary TOCTOU defense) then `incr_ref_tx`
    /// (the §6.3.1 guarded SQL). A missing target or a {2,5} reject rolls
    /// the whole tx back with InvalidArgument (design 2/3 §7.5.1).
    async fn create_memory(&self, memory: &MemoryData) -> Result<MemoryId> {
        let db = self.memory_repository().db_pool();
        let mut tx = db.begin().await.map_err(LlmMemoryError::DBError)?;

        // The locked media_object's kind/backend, carried out of the tx
        // so the post-commit dispatch can pick the embedding axis without
        // a second SELECT.
        let mut media_kind: Option<i32> = None;
        let mut media_backend: Option<String> = None;
        if let (Some(mid), Some(media)) = (
            memory.media_object_id.map(|m| m.value),
            self.media_subsystem(),
        ) {
            let bump = media.repository.lock_and_incr_ref_tx(&mut tx, mid).await?;
            media_kind = Some(bump.kind);
            media_backend = Some(bump.storage_backend);
        }

        let id = self.memory_repository().create(&mut *tx, memory).await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        self.spawn_embedding_dispatch(
            id.value,
            &memory.content,
            memory.role,
            memory.content_type,
            memory.media_object_id.map(|m| m.value),
            media_kind,
            media_backend.as_deref(),
        );

        Ok(id)
    }

    /// Update with the 5-transition ref_count diff (design 2/3 §7.5.2 /
    /// spec §4.2.1.1). The old media_object_id comes from the
    /// row-locked pre-update row; A/B are locked id-ascending to avoid
    /// deadlock; a rejected B rolls back A's decr too.
    async fn update_memory(
        &self,
        id: &MemoryId,
        memory: &Option<MemoryData>,
    ) -> Result<UpdateOutcome> {
        let Some(w) = memory else {
            return Ok(UpdateOutcome::default());
        };
        let pool = self.memory_repository().db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;

        // Post-commit storage delete jobs for media that hit ref_count=0.
        let mut post_delete: Vec<(i64, MediaObjectRow)> = Vec::new();
        // New media's kind/backend, carried out of the tx for dispatch.
        let mut media_kind: Option<i32> = None;
        let mut media_backend: Option<String> = None;
        // True when this update drops an image attachment (image → none,
        // or image → a non-image media). The text-only re-dispatch below
        // cannot evict the memory's old `image`/`caption` vector rows, so
        // the gRPC layer must cascade-clear them (review P2 #1).
        let mut stale_image_cleanup = false;

        if let Some(media) = self.media_subsystem() {
            let media_repo = &media.repository;
            // Old media_object_id from the row-locked pre-update row
            // (same pattern as delete_memory's find_by_ids_for_update_tx).
            let prev = self
                .memory_repository()
                .find_by_ids_for_update_tx(&mut tx, &[id.value])
                .await?;
            if prev.is_empty() {
                // The target memory does not exist. Bail out BEFORE any
                // ref_count change: otherwise (None, Some(b)) below would
                // incr_ref a media nothing references and the 0-row
                // UPDATE would still commit, orphaning ref_count=1
                // forever. tx drops -> ROLLBACK (no-op). Mirrors
                // delete_memory's `locked.is_empty()` early return.
                return Ok(UpdateOutcome::default());
            }
            let old_mid: Option<i64> = prev
                .first()
                .and_then(|m| m.data.as_ref())
                .and_then(|d| d.media_object_id.map(|x| x.value));
            let new_mid: Option<i64> = w.media_object_id.map(|x| x.value);

            match (old_mid, new_mid) {
                (None, None) => {}
                (Some(a), Some(b)) if a == b => {
                    // ref_count unchanged, but the public Update still
                    // re-evaluates dispatch_kinds, so the (possibly still
                    // image) media must be dispatched. Read-lock its row
                    // to carry kind/backend out.
                    if let Some(row) = media_repo.find_by_id_for_update_tx(&mut tx, b).await? {
                        media_kind = Some(row.kind);
                        media_backend = Some(row.storage_backend.clone());
                    }
                }
                (None, Some(b)) => {
                    let Some(row) = media_repo.find_by_id_for_update_tx(&mut tx, b).await? else {
                        return Err(LlmMemoryError::InvalidArgument(format!(
                            "media_object not found: {b}"
                        ))
                        .into());
                    };
                    media_kind = Some(row.kind);
                    media_backend = Some(row.storage_backend.clone());
                    if !media_repo.incr_ref_tx(&mut *tx, b).await? {
                        return Err(LlmMemoryError::InvalidArgument(format!(
                            "media_object {b} not found or being deleted"
                        ))
                        .into());
                    }
                }
                (Some(a), None) => {
                    // The old media is being detached entirely. If it was
                    // an image, its image/caption vector rows must be
                    // cascade-cleared (the text-only re-dispatch below
                    // won't touch them).
                    if let Some(arow) = media_repo.find_by_id_for_update_tx(&mut tx, a).await? {
                        stale_image_cleanup = is_image_kind(arow.kind);
                    }
                    if let Some(job) = decr_and_maybe_claim(media_repo, &mut tx, a).await? {
                        post_delete.push(job);
                    }
                }
                (Some(a), Some(b)) => {
                    // Lock A and B id-ascending (deadlock avoidance,
                    // same discipline as delete_memory's thread->memory
                    // lock order).
                    let (lo, hi) = if a < b { (a, b) } else { (b, a) };
                    let lo_row = media_repo.find_by_id_for_update_tx(&mut tx, lo).await?;
                    let hi_row = media_repo.find_by_id_for_update_tx(&mut tx, hi).await?;
                    // Pick A's and B's rows out of (lo,hi) by id.
                    let (a_row, b_row) = if a == lo {
                        (lo_row, hi_row)
                    } else {
                        (hi_row, lo_row)
                    };
                    // Old image swapped for a media that will NOT run the
                    // Media pipeline. "Non-image" is not enough: an image
                    // whose backend is unresolvable/inline, or any media
                    // when image mode is off, also produces no new
                    // image/caption rows. In all those cases the old
                    // image/caption vectors are orphaned and must be
                    // cascade-cleared. Only when the NEW media is itself
                    // Media-dispatchable do we skip cleanup (its own
                    // replace_kinds=["image","caption"] dispatch overwrites
                    // them). Uses the same predicate as `dispatch_kinds`'
                    // Media axis so the two never drift.
                    let old_is_image = a_row.as_ref().map(|r| is_image_kind(r.kind));
                    if let Some(brow) = b_row {
                        media_kind = Some(brow.kind);
                        media_backend = Some(brow.storage_backend.clone());
                        let new_media_dispatchable =
                            infra::infra::embedding_dispatch::media_axis_dispatchable(
                                Some(brow.kind),
                                Some(brow.storage_backend.as_str()),
                                self.image_search_mode,
                            );
                        if old_is_image == Some(true) && !new_media_dispatchable {
                            stale_image_cleanup = true;
                        }
                    }
                    let a_job = decr_and_maybe_claim(media_repo, &mut tx, a).await?;
                    if !media_repo.incr_ref_tx(&mut *tx, b).await? {
                        // tx drops -> ROLLBACK: A's decr/claim is undone
                        // too (spec §4.2.1.1).
                        return Err(LlmMemoryError::InvalidArgument(format!(
                            "media_object {b} not found or being deleted"
                        ))
                        .into());
                    }
                    if let Some(job) = a_job {
                        post_delete.push(job);
                    }
                }
            }
        }

        let updated = self.memory_repository().update(&mut *tx, id, w).await?;
        if !updated {
            // 0 rows updated. With the media subsystem wired this is
            // unreachable (the `prev.is_empty()` guard above already
            // returned) — so a 0-row UPDATE here while ref_count was
            // touched is a concurrent-delete race; dropping `tx` rolls
            // the ref_count change back. Without media it just means the
            // memory does not exist. Either way: no commit, no
            // post-delete/cache/dispatch, return false.
            return Ok(UpdateOutcome::default());
        }
        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        // Post-commit: the claim winner runs storage->DB delete. Errors
        // are best-effort (finish_delete marks 5->2 and the GC retries);
        // the memory update already committed.
        if let Some(media) = self.media_subsystem() {
            for (mid, row) in post_delete {
                let _ = media.finalizer.finish_delete(mid, &row).await;
            }
        }

        let k = Arc::new(Self::find_cache_key(&id.value));
        let _ = self.delete_cache(&k).await;

        self.spawn_embedding_dispatch(
            id.value,
            &w.content,
            w.role,
            w.content_type,
            w.media_object_id.map(|x| x.value),
            media_kind,
            media_backend.as_deref(),
        );

        Ok(UpdateOutcome {
            updated: true,
            stale_image_cleanup,
        })
    }
}

/// A `media_object.kind` of `IMAGE`. `kind` shares the `ContentType`
/// enum (`IMAGE = 2`); this is the same predicate `dispatch_kinds` uses
/// to gate the Media pipeline, kept in sync so "what dispatches as image"
/// and "what needs image-vector cleanup on detach" agree.
fn is_image_kind(kind: i32) -> bool {
    kind == protobuf::llm_memory::data::ContentType::Image as i32
}

/// Decrement `media_object_id`'s ref_count inside `tx` and, if it reached
/// 0 and the row is in {active, orphan}, claim it for deletion ({0,1}->5
/// CAS). Returns the `(media_object_id, row)` the caller must pass to
/// `finish_delete` AFTER commit. A row in {unresolvable, promoting} is
/// never claimed — the row and its gc_state stay. Shared by every
/// memory-delete path (single delete, A/B update swap, thread cascade)
/// so the decr+claim invariant lives in one place.
///
/// `decr_ref_tx`'s `WHERE id=? AND ref_count>0` returns no row for two
/// distinct cases, disambiguated here by a presence read:
/// - the media_object row is **absent** — a dangling
///   `memory.media_object_id` left by a prior GC / migration that
///   removed the object but not the pointer. This is legitimate
///   pre-existing data; the decrement is a no-op (`Ok(None)`) so the
///   memory/thread can still be deleted.
/// - the row **exists** with `ref_count` already 0 — a genuine
///   double-decr / underflow, kept as an invariant violation.
pub(crate) async fn decr_and_maybe_claim(
    media_repo: &MediaObjectRepositoryImpl,
    tx: &mut sqlx::Transaction<'_, infra_utils::infra::rdb::Rdb>,
    media_object_id: i64,
) -> Result<Option<(i64, MediaObjectRow)>> {
    let Some(r) = media_repo.decr_ref_tx(&mut **tx, media_object_id).await? else {
        if media_repo.find_by_id(media_object_id).await?.is_none() {
            // Dangling pointer: nothing to decrement, do not block the
            // delete.
            return Ok(None);
        }
        return Err(LlmMemoryError::RuntimeError(format!(
            "media_object {media_object_id} ref_count underflow / \
             double-decr (invariant violation)"
        ))
        .into());
    };
    if r.ref_count == 0 && matches!(r.gc_state, GC_ACTIVE | GC_ORPHAN) {
        // Snapshot the row (locked above) for the post-commit
        // finish_delete; claim_deleting flips {0,1}->5 so a concurrent
        // incr_ref is structurally rejected (design 2/3 §6.3.1).
        if media_repo
            .claim_deleting_tx(&mut **tx, media_object_id)
            .await?
        {
            let row = media_repo
                .find_by_id_for_update_tx(tx, media_object_id)
                .await?
                .ok_or_else(|| {
                    LlmMemoryError::RuntimeError(format!(
                        "media_object {media_object_id} vanished after \
                         claim_deleting (invariant violation)"
                    ))
                })?;
            return Ok(Some((media_object_id, row)));
        }
    }
    Ok(None)
}

/// Fill `Memory.media` with the cacheable metadata (no presigned URL —
/// that is issued per-response by the gRPC layer). No-op when the media
/// subsystem is not wired or the memory has no `media_object_id`. A
/// missing media_object row leaves `media = None` (the memory still
/// renders; the dangling id is a separate integrity concern).
///
/// Reused by the vector-search (`memory_vector::enrich_hits`) and
/// thread-history (`thread::find_memories_by_thread_id`) paths so all
/// read paths share one hydrate model; the gRPC layer then adds the
/// short-lived presigned URL the same way for every path.
pub(crate) async fn hydrate_media_metadata(
    media: Option<&MediaSubsystem>,
    memory: &mut Memory,
) -> Result<()> {
    let Some(media) = media else {
        return Ok(());
    };
    let Some(mid) = memory
        .data
        .as_ref()
        .and_then(|d| d.media_object_id.map(|x| x.value))
    else {
        return Ok(());
    };
    match media.finalizer.media_payload_metadata(mid).await {
        Ok(payload) => memory.media = Some(payload),
        Err(e) => {
            // Only a dangling media_object_id (row deleted out from under
            // us = NotFound) is tolerated: it must not fail the whole
            // Find, and `media = None` for it is correct. Any other error
            // (DBError, decode, ...) is a real failure — propagate it so
            // it is NOT silently turned into a successful empty response
            // and (worse) cached for the find_memory TTL.
            match e.downcast_ref::<LlmMemoryError>() {
                Some(LlmMemoryError::NotFound(_)) => {
                    tracing::warn!(
                        "media_object {mid} referenced by memory but not \
                         found during enrich (dangling id): {e}"
                    );
                }
                _ => return Err(e),
            }
        }
    }
    Ok(())
}

/// Apply [`hydrate_media_metadata`] to every memory in a list. N+1 over
/// the media rows; the per-memory cost is gated to media-bearing rows
/// inside `hydrate_media_metadata`. Bulk/parallel fetch is tracked as a
/// follow-up (see `docs/image-memory-open-issues.md`). Shared by the
/// list-shaped read paths (FindList and thread history).
pub(crate) async fn hydrate_media_list(
    media: Option<&MediaSubsystem>,
    memories: &mut [Memory],
) -> Result<()> {
    for m in memories.iter_mut() {
        hydrate_media_metadata(media, m).await?;
    }
    Ok(())
}

impl UseMemoryCache<Arc<String>, Memory> for MemoryAppImpl {
    fn cache(&self) -> &AsyncCache<Arc<String>, Memory> {
        &self.memory_cache
    }

    #[doc = " default cache ttl"]
    fn default_ttl(&self) -> Option<&Duration> {
        Some(&self.default_ttl)
    }

    fn key_lock(&self) -> &RwLockWithKey<Arc<String>> {
        &self.key_lock
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infra::infra::media_object::rdb::{GC_DELETING, GC_UNRESOLVABLE, MediaObjectReservation};
    use infra::infra::media_storage::StorageBackend;
    use infra::infra::media_storage::inline::InlineMediaStorage;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
    use protobuf::llm_memory::data::MediaObjectId;

    async fn pool() -> &'static RdbPool {
        #[cfg(feature = "postgres")]
        {
            setup_test_rdb_from("../infra/sql/postgres").await
        }
        #[cfg(not(feature = "postgres"))]
        {
            setup_test_rdb_from("../infra/sql/sqlite").await
        }
    }

    fn cache<V: Send + Sync + 'static>() -> AsyncCache<Arc<String>, V> {
        memory_utils::cache::stretto::new_memory_cache::<Arc<String>, V>(
            &memory_utils::cache::stretto::MemoryCacheConfig::default(),
        )
    }

    /// A MemoryApp wired with the inline media backend so the
    /// create/update/delete ref_count paths are exercised end to end.
    /// Returns the app plus a sibling MediaApp for seeding media_objects.
    fn app(pool: &'static RdbPool) -> (MemoryAppImpl, Arc<crate::app::media::MediaAppImpl>) {
        let id_gen = infra::test_helper::shared_id_generator();
        let media_app = Arc::new(crate::app::media::MediaAppImpl::new(
            MediaObjectRepositoryImpl::new(id_gen.clone(), pool),
            Arc::new(StorageBackend::Inline(InlineMediaStorage::new())),
            id_gen.clone(),
            "memories/".to_string(),
            900,
            20 * 1024 * 1024,
        ));
        let app = MemoryAppImpl::new(
            MemoryRepositoryImpl::new(id_gen.clone(), pool),
            MemoryRatingRepositoryImpl::new(id_gen.clone(), pool),
            ThreadRepositoryImpl::new(id_gen.clone(), pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            cache(),
            cache(),
            None,
        )
        .with_media(
            MediaObjectRepositoryImpl::new(id_gen, pool),
            media_app.clone(),
        );
        (app, media_app)
    }

    /// Seed a confirmed orphan media_object (storage_uri set, gc_state=1)
    /// directly via the repository and return its id.
    async fn seed_confirmed_media(pool: &'static RdbPool, sha: &str) -> i64 {
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
        repo.confirm_reservation_tx(&mut *tx, id, "s3://b/k", "s3", Some(10), None)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        id
    }

    /// A confirmed NON-image media (`kind=URL`, `storage_backend=url`).
    /// Used to assert that detaching / replacing a *non-image* media does
    /// NOT request the image-vector cascade clear (only image kinds do).
    async fn seed_confirmed_nonimage_media(pool: &'static RdbPool, uri: &str) -> i64 {
        let id_gen = infra::test_helper::shared_id_generator();
        let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), pool);
        let id = id_gen.generate_id().unwrap();
        let mut tx = pool.begin().await.unwrap();
        repo.insert_url_tx(
            &mut *tx,
            id,
            protobuf::llm_memory::data::ContentType::Url as i32,
            "text/html",
            None,
            None,
            None,
            uri,
            None,
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        id
    }

    /// A `kind=IMAGE` media whose `storage_backend=unresolvable` (elided
    /// source, no bytes). It is an image by kind but NOT Media-dispatchable
    /// (`media_axis_dispatchable` excludes unresolvable). Used to prove
    /// that swapping an image for an *unembeddable image* still triggers
    /// the stale cascade (review P2 follow-up: kind alone is insufficient).
    async fn seed_unresolvable_image_media(pool: &'static RdbPool, sha: &str) -> i64 {
        let id_gen = infra::test_helper::shared_id_generator();
        let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), pool);
        let id = id_gen.generate_id().unwrap();
        let mut tx = pool.begin().await.unwrap();
        repo.insert_unresolvable_tx(
            &mut *tx,
            id,
            protobuf::llm_memory::data::ContentType::Image as i32,
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

    fn mem(content: &str, media: Option<i64>) -> MemoryData {
        MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: content.to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: 1, // ROLE_USER
            external_id: None,
            media_object_id: media.map(|value| MediaObjectId { value }),
            thread_ids: Vec::new(),
        }
    }

    async fn ref_count(pool: &'static RdbPool, mid: i64) -> i64 {
        let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        repo.find_by_id(mid).await.unwrap().unwrap().ref_count
    }

    // ---- Create (§7.5.1 / spec §4.2.1) ----

    #[test]
    fn create_with_confirmed_media_increments_ref_to_active() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_c1").await;
            let id = a.create_memory(&mem("hi", Some(mid))).await?;
            assert_eq!(ref_count(p, mid).await, 1);
            let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), p);
            // confirmed orphan -> active (gc 1->0).
            assert_eq!(repo.find_by_id(mid).await?.unwrap().gc_state, GC_ACTIVE);
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn create_with_nonexistent_media_rolls_back() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let err = a
                .create_memory(&mem("hi", Some(999_999_999)))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LlmMemoryError>(),
                    Some(LlmMemoryError::InvalidArgument(_))
                ),
                "nonexistent media_object_id must be InvalidArgument: {err}"
            );
            // The memory must NOT have been inserted (whole tx rolled back).
            let all = a.find_memory_all_list(None).await?;
            assert!(
                !all.iter()
                    .any(|m| m.data.as_ref().and_then(|d| d.media_object_id).is_some()),
                "no memory should reference the missing media"
            );
            Ok(())
        })
    }

    #[test]
    fn create_with_deleting_media_rejected() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_c_del").await;
            // Drive it to gc_state=5 (deleting claim).
            let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), p);
            let mut tx = p.begin().await?;
            assert!(repo.claim_deleting_tx(&mut *tx, mid).await?);
            tx.commit().await?;
            let err = a.create_memory(&mem("x", Some(mid))).await.unwrap_err();
            assert!(matches!(
                err.downcast_ref::<LlmMemoryError>(),
                Some(LlmMemoryError::InvalidArgument(_))
            ));
            assert_eq!(
                repo.find_by_id(mid).await?.unwrap().gc_state,
                GC_DELETING,
                "gc_state untouched"
            );
            Ok(())
        })
    }

    // ---- Update 5-transition (§7.5.2 / spec §4.2.1.1) ----

    #[test]
    fn update_none_to_some_increments() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let id = a.create_memory(&mem("body", None)).await?;
            let mid = seed_confirmed_media(p, "sha_u1").await;
            assert!(
                a.update_memory(&id, &Some(mem("body", Some(mid))))
                    .await?
                    .updated
            );
            assert_eq!(ref_count(p, mid).await, 1);
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn update_some_to_none_decrements_and_deletes() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_u2").await;
            let id = a.create_memory(&mem("body", Some(mid))).await?;
            assert_eq!(ref_count(p, mid).await, 1);
            // Some(A) -> None : ref 1->0, claim, finish_delete removes row.
            assert!(
                a.update_memory(&id, &Some(mem("body", None)))
                    .await?
                    .updated
            );
            let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), p);
            assert!(
                repo.find_by_id(mid).await?.is_none(),
                "media_object physically deleted at ref_count=0"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    // ---- stale_image_cleanup flag (review P2 #1 + follow-up) ----
    //
    // The flag must be set exactly when an Update orphans the memory's
    // old image/caption vector rows: the old media was an image AND the
    // new media will NOT run the Media pipeline (so its own
    // replace_kinds=["image","caption"] dispatch won't overwrite them).
    // "New is not Media-dispatchable" covers THREE cases, not just
    // non-image: (a) non-image kind, (b) image kind but
    // storage_backend ∈ {unresolvable, inline}, (c) any media when image
    // mode is off. It must NOT fire when the new media IS Media-
    // dispatchable (image → embeddable image with mode on) nor when no
    // image was attached. `image_search_mode` is set explicitly because
    // the test process has no `MEMORY_IMAGE_SEARCH_MODE` env.

    #[test]
    fn update_image_to_none_requests_stale_image_cleanup() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let a = a.with_image_search_mode(
                infra::infra::embedding_dispatch::ImageSearchMode::Multimodal,
            );
            let mid = seed_confirmed_media(p, "sha_p2_img_none").await; // kind=IMAGE
            let id = a.create_memory(&mem("body", Some(mid))).await?;
            let outcome = a.update_memory(&id, &Some(mem("body", None))).await?;
            assert!(outcome.updated);
            assert!(
                outcome.stale_image_cleanup,
                "image → none MUST request the image-vector cascade clear"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn update_image_to_nonimage_requests_stale_image_cleanup() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let a = a.with_image_search_mode(
                infra::infra::embedding_dispatch::ImageSearchMode::Multimodal,
            );
            let img = seed_confirmed_media(p, "sha_p2_img_swap").await; // kind=IMAGE
            let url = seed_confirmed_nonimage_media(p, "https://e.invalid/x").await; // kind=URL
            let id = a.create_memory(&mem("body", Some(img))).await?;
            let outcome = a.update_memory(&id, &Some(mem("body", Some(url)))).await?;
            assert!(outcome.updated);
            assert!(
                outcome.stale_image_cleanup,
                "image → non-image media MUST request the cascade clear"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    /// Review P2 follow-up: image → an UNEMBEDDABLE image (kind=IMAGE but
    /// storage_backend=unresolvable). The new media is an image by kind
    /// yet the Media pipeline will NOT run for it (no bytes), so no new
    /// image/caption rows replace the old ones — the old vectors are
    /// orphaned and the cascade MUST fire. This is the case the
    /// kind-only check `!is_image_kind(new.kind)` silently missed.
    #[test]
    fn update_image_to_unresolvable_image_requests_cleanup() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let a = a.with_image_search_mode(
                infra::infra::embedding_dispatch::ImageSearchMode::Multimodal,
            );
            let img = seed_confirmed_media(p, "sha_p2_img_unres_src").await; // s3 IMAGE
            let unres = seed_unresolvable_image_media(p, "sha_p2_img_unres_dst").await; // unresolvable IMAGE
            let id = a.create_memory(&mem("body", Some(img))).await?;
            let outcome = a
                .update_memory(&id, &Some(mem("body", Some(unres))))
                .await?;
            assert!(outcome.updated);
            assert!(
                outcome.stale_image_cleanup,
                "image → unresolvable image (kind=IMAGE but not Media-\
                 dispatchable) MUST still cascade-clear: kind alone is \
                 insufficient, backend/mode matter"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    /// image → image with image mode OFF: even though the new media is a
    /// proper embeddable image, `mode=none` means the Media pipeline
    /// never runs, so the old image/caption rows would be orphaned →
    /// cascade MUST fire. Proves the mode conjunct of
    /// `media_axis_dispatchable` is honoured (not just kind/backend).
    #[test]
    fn update_image_to_image_with_mode_off_requests_cleanup() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let a =
                a.with_image_search_mode(infra::infra::embedding_dispatch::ImageSearchMode::None);
            let img_a = seed_confirmed_media(p, "sha_p2_modeoff_a").await;
            let img_b = seed_confirmed_media(p, "sha_p2_modeoff_b").await;
            let id = a.create_memory(&mem("body", Some(img_a))).await?;
            let outcome = a
                .update_memory(&id, &Some(mem("body", Some(img_b))))
                .await?;
            assert!(outcome.updated);
            assert!(
                outcome.stale_image_cleanup,
                "image → image with mode=none MUST cascade-clear: no Media \
                 dispatch runs to replace the old image/caption rows"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn update_image_to_image_does_not_request_cleanup() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            // Image mode ON so the NEW image IS Media-dispatchable: its
            // own replace_kinds=["image","caption"] dispatch overwrites
            // the old rows, so no separate cascade is needed.
            let a = a.with_image_search_mode(
                infra::infra::embedding_dispatch::ImageSearchMode::Multimodal,
            );
            let img_a = seed_confirmed_media(p, "sha_p2_imgA").await;
            let img_b = seed_confirmed_media(p, "sha_p2_imgB").await;
            let id = a.create_memory(&mem("body", Some(img_a))).await?;
            let outcome = a
                .update_memory(&id, &Some(mem("body", Some(img_b))))
                .await?;
            assert!(outcome.updated);
            assert!(
                !outcome.stale_image_cleanup,
                "image → embeddable image (mode on) must NOT cascade-clear: \
                 the image pipeline re-dispatches and replaces image/caption \
                 via replace_kinds"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn update_nonimage_to_none_does_not_request_cleanup() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let url = seed_confirmed_nonimage_media(p, "https://e.invalid/y").await;
            let id = a.create_memory(&mem("body", Some(url))).await?;
            let outcome = a.update_memory(&id, &Some(mem("body", None))).await?;
            assert!(outcome.updated);
            assert!(
                !outcome.stale_image_cleanup,
                "non-image → none has no image vectors to clear"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn update_text_only_no_media_does_not_request_cleanup() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let id = a.create_memory(&mem("original", None)).await?;
            let outcome = a.update_memory(&id, &Some(mem("edited", None))).await?;
            assert!(outcome.updated);
            assert!(
                !outcome.stale_image_cleanup,
                "text-only update with no media never cascades"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn update_a_to_b_rejected_rolls_back_a_decr() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let a_mid = seed_confirmed_media(p, "sha_uA").await;
            let id = a.create_memory(&mem("body", Some(a_mid))).await?;
            assert_eq!(ref_count(p, a_mid).await, 1);
            // B is a deleting (gc=5) media -> incr_ref rejects it.
            let b_mid = seed_confirmed_media(p, "sha_uB").await;
            let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), p);
            let mut tx = p.begin().await?;
            assert!(repo.claim_deleting_tx(&mut *tx, b_mid).await?);
            tx.commit().await?;

            let err = a
                .update_memory(&id, &Some(mem("body", Some(b_mid))))
                .await
                .unwrap_err();
            assert!(matches!(
                err.downcast_ref::<LlmMemoryError>(),
                Some(LlmMemoryError::InvalidArgument(_))
            ));
            // A's decr must have been rolled back together with the tx.
            assert_eq!(
                ref_count(p, a_mid).await,
                1,
                "A.ref_count must be restored when B is rejected (spec §4.2.1.1)"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    // ---- update_content_no_dispatch (§7.5.3) ----

    #[test]
    fn no_dispatch_updates_content_only_and_refreshes_cache() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_nd").await;
            let id = a.create_memory(&mem("original", Some(mid))).await?;
            // Warm the cache with a long TTL.
            let ttl = Duration::from_secs(300);
            let before = a.find_memory(&id, Some(&ttl)).await?.unwrap();
            assert_eq!(before.data.as_ref().unwrap().content, "original");

            assert!(a.update_content_no_dispatch(&id, "caption").await?);
            // Cache invalidated -> a TTL-still-valid Find returns fresh
            // content, and media_object_id is preserved.
            let after = a.find_memory(&id, Some(&ttl)).await?.unwrap();
            let d = after.data.as_ref().unwrap();
            assert_eq!(d.content, "caption", "content fresh despite TTL");
            assert_eq!(
                d.media_object_id,
                Some(MediaObjectId { value: mid }),
                "media_object_id must NOT be dropped"
            );
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn no_dispatch_missing_id_returns_false() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            assert!(
                !a.update_content_no_dispatch(&MemoryId { value: 999_999_999 }, "x")
                    .await?
            );
            Ok(())
        })
    }

    // ---- Delete (§7.5.4) ----

    #[test]
    fn delete_shared_media_keeps_row() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_del_shared").await;
            let id1 = a.create_memory(&mem("m1", Some(mid))).await?;
            let id2 = a.create_memory(&mem("m2", Some(mid))).await?;
            assert_eq!(ref_count(p, mid).await, 2);
            a.delete_memory(&id1).await?;
            // ref 2->1 : row survives.
            assert_eq!(ref_count(p, mid).await, 1);
            a.delete_memory(&id2).await?;
            let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), p);
            assert!(
                repo.find_by_id(mid).await?.is_none(),
                "row deleted only when the last reference is gone"
            );
            Ok(())
        })
    }

    #[test]
    fn delete_no_media_unaffected() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let id = a.create_memory(&mem("plain", None)).await?;
            assert!(a.delete_memory(&id).await?);
            assert!(a.find_memory(&id, None).await?.is_none());
            Ok(())
        })
    }

    // ---- Find enrich (§7.5.5 / §4.2.2.0) ----

    #[test]
    fn find_includes_media_payload() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_find").await;
            let id = a.create_memory(&mem("cap", Some(mid))).await?;
            let m = a.find_memory(&id, None).await?.unwrap();
            let payload = m.media.expect("media payload present");
            assert_eq!(payload.metadata.unwrap().media_type, "image/png");
            assert!(!payload.unresolved);
            // Cacheable half only — presigned URL is filled by the gRPC layer.
            assert!(payload.presigned_url.is_none());
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    #[test]
    fn find_unresolvable_sets_unresolved_no_panic() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            // Seed an unresolvable media (elided-style) directly.
            let id_gen = infra::test_helper::shared_id_generator();
            let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), p);
            let mid = id_gen.generate_id()?;
            let now = command_utils::util::datetime::now_millis();
            let mut tx = p.begin().await?;
            repo.insert_reservation_tx(
                &mut *tx,
                &MediaObjectReservation {
                    id: mid,
                    kind: 2,
                    media_type: "image/png".to_string(),
                    byte_size: Some(1),
                    sha256: "sha_unres".to_string(),
                    width: None,
                    height: None,
                    duration_ms: None,
                    alt: None,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
            sqlx::query::<infra_utils::infra::rdb::Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET gc_state = {GC_UNRESOLVABLE}, \
                 storage_backend = 'unresolvable' WHERE id = {mid}"
            )))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            // create references gc=3 (legitimate, §9.4) — gc_state preserved.
            let id = a.create_memory(&mem("", Some(mid))).await?;
            let m = a.find_memory(&id, None).await?.unwrap();
            let payload = m.media.expect("media payload present even if unresolved");
            assert!(payload.unresolved, "unresolved=true (no panic, normal)");
            assert!(payload.presigned_url.is_none());
            // metadata still useful (sha256 for manual recovery).
            assert!(payload.metadata.unwrap().sha256.is_some());
            a.delete_memory(&id).await?;
            Ok(())
        })
    }

    // ---- Public-surface boundary (spec §4.2.1.1) ----

    fn workspace_file(rel: &str) -> String {
        // CARGO_MANIFEST_DIR is the `app/` crate dir; the workspace root
        // is its parent.
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        std::fs::read_to_string(root.join(rel)).unwrap_or_default()
    }

    /// UpdateContentNoDispatch must NOT be exposed to agents via the RAG
    /// tool manifest (misuse only staleness, but the boundary is a spec
    /// requirement, §4.2.1.1).
    #[test]
    fn rag_manifest_excludes_update_content_no_dispatch() {
        let manifest = workspace_file("workflows/rag-tools-manifest.yaml");
        assert!(!manifest.is_empty(), "rag-tools-manifest.yaml should exist");
        assert!(
            !manifest.contains("UpdateContentNoDispatch")
                && !manifest
                    .to_lowercase()
                    .contains("update_content_no_dispatch"),
            "UpdateContentNoDispatch must stay off the RAG manifest"
        );
    }

    /// The llm-memory-plugin export surface must not call
    /// UpdateContentNoDispatch (no external reach via jobworkerp plugins).
    #[test]
    fn plugin_export_excludes_update_content_no_dispatch() {
        let lib = workspace_file("llm-memory-plugin/src/lib.rs");
        assert!(!lib.is_empty(), "llm-memory-plugin/src/lib.rs should exist");
        assert!(
            !lib.contains("UpdateContentNoDispatch") && !lib.contains("update_content_no_dispatch"),
            "llm-memory-plugin must not reach UpdateContentNoDispatch"
        );
    }

    // ---- Regression: review findings ----

    /// High: updating a NON-existent memory with media_object_id=Some(b)
    /// must NOT incr_ref b (which would orphan ref_count=1 forever). The
    /// update returns Ok(false) and b.ref_count stays 0.
    #[test]
    fn update_missing_memory_with_media_does_not_orphan_refcount() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_orphan_guard").await;
            assert_eq!(ref_count(p, mid).await, 0);

            let outcome = a
                .update_memory(
                    &MemoryId { value: 999_999_999 },
                    &Some(mem("body", Some(mid))),
                )
                .await?;
            assert!(!outcome.updated, "update on a missing memory returns false");
            assert_eq!(
                ref_count(p, mid).await,
                0,
                "ref_count must NOT be incremented for a non-existent memory"
            );
            Ok(())
        })
    }

    /// Medium: a dangling media_object_id (row gone) is tolerated —
    /// find_memory returns the memory with media=None instead of
    /// failing. A real DBError would propagate, but that path cannot be
    /// injected here without a fault hook; the NotFound branch is the
    /// one this change narrows the swallow to.
    #[test]
    fn find_with_dangling_media_id_returns_memory_without_media() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, _) = app(p);
            let mid = seed_confirmed_media(p, "sha_dangling").await;
            let id = a.create_memory(&mem("has media", Some(mid))).await?;
            // Delete the media row out from under the memory to simulate the
            // dangling-reference race the enrich path must survive.
            let repo = MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), p);
            let mut tx = p.begin().await?;
            sqlx::query::<infra_utils::infra::rdb::Rdb>(sqlx::AssertSqlSafe(format!(
                "DELETE FROM media_object WHERE id = {mid}"
            )))
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;
            assert!(repo.find_by_id(mid).await?.is_none());

            let m = a.find_memory(&id, None).await?.expect("memory still found");
            assert!(
                m.media.is_none(),
                "dangling media_object_id surfaces as media=None, not an error"
            );
            assert_eq!(
                m.data.and_then(|d| d.media_object_id).map(|x| x.value),
                Some(mid)
            );
            let _ = a.delete_memory(&id).await;
            Ok(())
        })
    }
}
