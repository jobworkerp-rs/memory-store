use anyhow::Result;
use infra::error::LlmMemoryError;
use infra::infra::media_object::rdb::{MediaObjectRepository, MediaObjectRepositoryImpl};
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use infra::infra::memory_vector::dispatcher::{
    DispatchError, DispatchKind, DispatchTarget, EmbeddingDispatch, EmbeddingJobDispatcher,
};
use infra::infra::memory_vector::record::{MemoryVectorRecord, vector_kind};
use infra::infra::memory_vector::repository::{
    AggregationStrategy, HybridOptions, HybridStrategy, IndexStats, MemoryVectorRepositoryImpl,
    VectorSearchHit, aggregate_scores,
};
use infra::infra::memory_vector::safe_filter::SafeFilter;
use infra::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
use infra::infra::thread_label::rdb::ThreadLabelRepositoryImpl;
// Trait kept in scope for tests (`add_labels_tx`); production resolver code
// has been moved to `crate::app::thread_filter_resolver`.
#[cfg(test)]
use infra::infra::thread_label::rdb::ThreadLabelRepository;
use infra::infra::thread_memory::rdb::{ThreadMemoryRepository, ThreadMemoryRepositoryImpl};
use infra_utils::infra::rdb::UseRdbPool;
use protobuf::llm_memory::data::{
    Memory, MemoryId, MessageRole, ThreadId, ThreadSearchFilter, UserId,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// Shared P5 thread_filter resolver — same env knobs and algorithm as
// the P8 RDB list/Count path.
use crate::app::thread_filter_resolver::{self, ThreadFilterConfig};

/// Which LanceDB query path the caller is about to drive. The
/// `build_search_filter` ceiling logic is route-aware: FTS-bearing
/// routes (`Fts`, `Hybrid`) go through `only_if(SQL)` and must obey the
/// `FTS_MAX_*` limits; pure ANN (`Vector`) is bounded by the upstream
/// `MAX_THREAD_IDS` and only emits a `prefilter_threshold` warning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchKind {
    Vector,
    Fts,
    Hybrid,
}

/// Planner output for `build_search_filter_with_cfg`. Encodes which of
/// three runtime paths the caller must take so the FTS approximate-mode
/// two-phase pipeline can stay cleanly separated from the strict path.
pub(crate) enum FilterPlan {
    /// Thread filter resolved to zero rows — caller short-circuits to an
    /// empty result without touching LanceDB.
    ShortCircuit,
    /// Strict path: AND the resolved `IN (...)` into the base filter and
    /// hand a single SafeFilter to the LanceDB query (`None` means no
    /// thread_filter at all, `Some(filter)` means narrowed).
    Strict(Option<SafeFilter>),
    /// Approximate path: caller fetches `limit * K` candidates with
    /// `base` only, then locally intersects with `allowed`. Only emitted
    /// when `kind.uses_fts()` AND `cfg.fts_approximate_mode` AND
    /// N > `fts_max_inline_ids` AND N ≤ `fts_max_total_ids`.
    Approximate {
        base: Option<SafeFilter>,
        allowed: Vec<i64>,
    },
}

// SafeFilter does not implement Debug, so format opaquely. We only
// surface the variant name (and allowed.len for Approximate) which is
// the relevant signal for tests / logs.
impl std::fmt::Debug for FilterPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FilterPlan::ShortCircuit => write!(f, "FilterPlan::ShortCircuit"),
            FilterPlan::Strict(opt) => write!(
                f,
                "FilterPlan::Strict({})",
                if opt.is_some() { "Some(_)" } else { "None" }
            ),
            FilterPlan::Approximate { base, allowed } => write!(
                f,
                "FilterPlan::Approximate {{ base: {}, allowed.len: {} }}",
                if base.is_some() { "Some(_)" } else { "None" },
                allowed.len()
            ),
        }
    }
}

impl SearchKind {
    fn uses_fts(self) -> bool {
        matches!(self, SearchKind::Fts | SearchKind::Hybrid)
    }
    fn uses_ann(self) -> bool {
        matches!(self, SearchKind::Vector | SearchKind::Hybrid)
    }
}

/// Mode for `MemoryVectorAppImpl::count_search_matches` (P2,
/// Phase 5-1). Phase 5-1 implements `FilterOnly` and `Text`; `Vector`
/// and `Hybrid` return `LlmMemoryError::Unimplemented` until Phase 5-2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CountMode {
    FilterOnly,
    Text,
    Vector,
    Hybrid,
}

impl CountMode {
    /// Map a wire `CountSearchMode` value (the i32 carried on the proto
    /// field) to the app-level `CountMode`. Returns `None` for
    /// `UNSPECIFIED` (= 0) so the caller can reject the request with
    /// `invalid_argument` per the proto contract. Unknown values are
    /// also `None` (`prost` decodes unknown enum tags by preserving the
    /// raw i32, so this projects future proto extensions safely).
    ///
    /// Takes `i32` rather than the generated enum type because the
    /// gRPC frontend rebuilds its own copy of `CountSearchMode` (see
    /// `grpc-admin/src/protobuf.rs`); both copies share the same wire
    /// numbering, so going through i32 keeps app independent of which
    /// crate's generated code the caller holds.
    pub fn from_proto_i32(value: i32) -> Option<Self> {
        use protobuf::llm_memory::service::CountSearchMode;
        match CountSearchMode::try_from(value).ok()? {
            CountSearchMode::Unspecified => None,
            CountSearchMode::FilterOnly => Some(Self::FilterOnly),
            CountSearchMode::Text => Some(Self::Text),
            CountSearchMode::Vector => Some(Self::Vector),
            CountSearchMode::Hybrid => Some(Self::Hybrid),
        }
    }

    /// Reverse of `from_proto_i32`: project the app-level mode back
    /// into the i32 carried on the wire response.
    pub fn to_proto_i32(self) -> i32 {
        use protobuf::llm_memory::service::CountSearchMode;
        match self {
            Self::FilterOnly => CountSearchMode::FilterOnly as i32,
            Self::Text => CountSearchMode::Text as i32,
            Self::Vector => CountSearchMode::Vector as i32,
            Self::Hybrid => CountSearchMode::Hybrid as i32,
        }
    }
}

/// Input for `count_search_matches`. The lifetime ties the request to
/// the proto buffers held by the gRPC service layer so the app does not
/// need to clone them — callers pass `req.filter.as_ref()` and
/// `req.query_text.as_deref()` directly.
///
/// Phase 5-2 added `query_vectors`, `aggregation`, and `hybrid_options`
/// for VECTOR / HYBRID modes. They are ignored by FILTER_ONLY / TEXT.
/// `aggregation` is accepted for API symmetry with `search_by_vector`
/// but does not affect Count membership — multi-vector Count returns
/// the size of the union of per-branch id sets, which is independent
/// of how the aggregation strategy would re-rank them.
pub struct CountSearchInput<'a> {
    pub mode: CountMode,
    pub filter: Option<&'a protobuf::llm_memory::data::MemorySearchFilter>,
    pub query_text: Option<&'a str>,
    pub query_vectors: &'a [Vec<f32>],
    pub aggregation: Option<AggregationStrategy>,
    pub hybrid_options: Option<&'a HybridOptions>,
}

#[derive(Debug, Clone, Copy)]
pub struct CountSearchOutput {
    pub total: u64,
    pub is_truncated: bool,
    pub mode: CountMode,
}

/// Search result item with Memory entity, relevance scores, and the
/// representative-thread metadata required by SPEC §5.5.1
/// `[parent thread name] [N/M] [role]`. The five P1 fields are
/// `Option`-typed: callers map `None` to proto-level `unset` directly.
///
/// Unset semantics:
/// - All five `None` → ROLE_SYSTEM protection OR memory has no thread
///   OR representative thread row was concurrently deleted (orphan).
/// - First four `Some`, `thread_description` `None` → thread row exists
///   but its `description` column is NULL — UI uses placeholder but
///   keeps jump UI enabled.
pub struct MemorySearchResultItem {
    pub memory: Memory,
    pub score: f32,
    pub distance: f32,
    pub position: Option<i32>,
    pub thread_total: Option<i32>,
    pub thread_id: Option<ThreadId>,
    pub thread_owner_user_id: Option<UserId>,
    pub thread_description: Option<String>,
    /// Server-computed highlight ranges against `memory.data.content`.
    /// Empty for vector-only searches (no query text), for
    /// `include_content = false` (offsets into a stripped payload
    /// would mislead the client), and for non-text content types.
    pub highlights: Vec<protobuf::llm_memory::data::HighlightField>,
    /// Image memory Phase 4 (N-row de-dup). After memory_id de-dup the
    /// winning row's metadata is kept here so the gRPC layer can fill
    /// `MemorySearchResult.matched_*`. None on the fusion/aggregation
    /// paths where no single row is attributable.
    pub matched_vector_kind: Option<String>,
    pub matched_begin_position: Option<i32>,
    pub matched_end_position: Option<i32>,
    pub matched_content: Option<String>,
    /// `ScoreSource` (proto enum as i32). Decided by the query's origin,
    /// NOT the matched row's kind (design 1/3 §2.6.4 / 3/3 §13.3):
    /// SearchByVector keeps SCORE_VECTOR, HybridSearch keeps
    /// SCORE_HYBRID, SearchSemantic/SearchByMedia map to
    /// SCORE_{TEXT,IMAGE,CAPTION}_EMBED per the matched row's kind.
    pub score_source: i32,
}

/// Where a search's query vector came from — determines `score_source`
/// (design 3/3 §13.3 score_source boundary). The matched row's kind is
/// always reflected in `matched_vector_kind`; only this enum decides
/// whether `score_source` stays generic or becomes kind-specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOrigin {
    /// SearchByVector — client-supplied vector, no model-space
    /// guarantee. `score_source = SCORE_VECTOR` regardless of hit kind.
    ExternalVector,
    /// SearchByText (BM25). `score_source = SCORE_TEXT`.
    Bm25Text,
    /// HybridSearch — fused. `score_source = SCORE_HYBRID`.
    Hybrid,
    /// SearchSemantic — server embedded the text query in the same
    /// model space. `score_source` follows the matched row's kind.
    ServerText,
    /// SearchByMedia — server embedded the media in the same space.
    /// `score_source` follows the matched row's kind.
    ServerImage,
}

impl QueryOrigin {
    /// Resolve the proto `ScoreSource` (as i32) for a hit, given the
    /// matched row's `vector_kind`. Generic origins ignore the kind;
    /// server-embedded origins map text/image/caption to the
    /// kind-specific `SCORE_*_EMBED`.
    fn score_source(self, matched_vector_kind: Option<&str>) -> i32 {
        use protobuf::llm_memory::data::ScoreSource;
        let s = match self {
            QueryOrigin::ExternalVector => ScoreSource::ScoreVector,
            QueryOrigin::Bm25Text => ScoreSource::ScoreText,
            QueryOrigin::Hybrid => ScoreSource::ScoreHybrid,
            QueryOrigin::ServerText | QueryOrigin::ServerImage => match matched_vector_kind {
                Some(vector_kind::IMAGE) => ScoreSource::ScoreImageEmbed,
                Some(vector_kind::CAPTION) => ScoreSource::ScoreCaptionEmbed,
                // text or unknown/None → text-embed (the query was
                // produced in the embed space; default to text).
                _ => ScoreSource::ScoreTextEmbed,
            },
        };
        s as i32
    }
}

/// Image memory Phase 4 (design 3/3 §13.3): collapse N-row ANN hits to
/// one entry per memory_id, keeping the max-score row and its
/// matched-row metadata. Pure (no LanceDB/RDB) so it is unit-tested
/// directly. Input order is the ANN rank; output preserves the first
/// (best-ranked) appearance order after max-score aggregation, so a
/// later equal-or-lower hit never displaces the winner.
pub(crate) fn dedup_vector_hits(hits: Vec<VectorSearchHit>) -> Vec<VectorSearchHit> {
    let mut order: Vec<i64> = Vec::new();
    let mut best: HashMap<i64, VectorSearchHit> = HashMap::new();
    for hit in hits {
        match best.get_mut(&hit.memory_id) {
            None => {
                order.push(hit.memory_id);
                best.insert(hit.memory_id, hit);
            }
            Some(existing) => {
                if hit.score > existing.score {
                    *existing = hit;
                }
            }
        }
    }
    order
        .into_iter()
        .filter_map(|id| best.remove(&id))
        .collect()
}

/// App layer for vector search operations.
/// Wraps MemoryRepositoryImpl (RDB) and MemoryVectorRepositoryImpl (LanceDB).
///
/// `thread_memory_repo` is needed for thread-scoped validation of
/// `get_surrounding_memories` — we reject requests where the pivot memory
/// is not registered under the requested thread, rather than silently
/// return empty neighbour lists. The check and the surrounding lookup run
/// inside the same repeatable-read snapshot on PostgreSQL.
///
/// `thread_repo` and `thread_label_repo` were added in Phase 5-1 (P5) to
/// resolve `MemorySearchFilter.thread_filter` — labels go through
/// `thread_label_repo`, the remaining seven conditions go through
/// `thread_repo::find_thread_ids_by_filter`, and the app layer intersects
/// the two sets before passing the resulting memory_ids into the vector
/// query. See `resolve_memory_ids_from_thread_filter` for the algorithm.
pub struct MemoryVectorAppImpl {
    memory_repo: MemoryRepositoryImpl,
    vector_repo: MemoryVectorRepositoryImpl,
    thread_memory_repo: ThreadMemoryRepositoryImpl,
    thread_repo: ThreadRepositoryImpl,
    thread_label_repo: ThreadLabelRepositoryImpl,
    thread_filter_config: ThreadFilterConfig,
    embedding_dispatcher: Option<Arc<EmbeddingJobDispatcher>>,
    /// Read once from env (immutable) so redispatch does not re-parse it.
    /// Decides whether redispatch also drives the image pipeline.
    image_search_mode: infra::infra::embedding_dispatch::ImageSearchMode,
    /// `Some` lets `redispatch_embeddings` resolve each memory's linked
    /// `media_object` (kind / storage_backend) so `dispatch_kinds` can
    /// decide the Media axis. `None` (env-less tests / non-media
    /// deployments) keeps redispatch text-only — backward compatible.
    media_object_repo: Option<MediaObjectRepositoryImpl>,
    /// `Some` lets search results carry the cacheable half of
    /// `Memory.media` (metadata + `unresolved`), the same hydrate the
    /// Find path does. Independent of `media_object_repo` (that one only
    /// feeds the redispatch Media axis). `None` (env-less tests /
    /// non-media deployments) leaves search results without media —
    /// backward compatible.
    media: Option<super::memory::MediaSubsystem>,
}

impl MemoryVectorAppImpl {
    pub fn new(
        memory_repo: MemoryRepositoryImpl,
        vector_repo: MemoryVectorRepositoryImpl,
        thread_memory_repo: ThreadMemoryRepositoryImpl,
        thread_repo: ThreadRepositoryImpl,
        thread_label_repo: ThreadLabelRepositoryImpl,
        embedding_dispatcher: Option<Arc<EmbeddingJobDispatcher>>,
    ) -> Self {
        Self {
            memory_repo,
            vector_repo,
            thread_memory_repo,
            thread_repo,
            thread_label_repo,
            thread_filter_config: ThreadFilterConfig::from_env(),
            embedding_dispatcher,
            image_search_mode: infra::infra::embedding_dispatch::ImageSearchMode::from_env(),
            media_object_repo: None,
            media: None,
        }
    }

    /// Wire the media_object repository so `redispatch_embeddings` can
    /// resolve each memory's linked media (kind / storage_backend) and
    /// evaluate the Media dispatch axis. Without this, redispatch stays
    /// text-only (env-less tests / non-media deployments).
    pub fn with_media_resolver(mut self, media_object_repo: MediaObjectRepositoryImpl) -> Self {
        self.media_object_repo = Some(media_object_repo);
        self
    }

    /// Wire the media subsystem so search results carry the cacheable
    /// half of `Memory.media` (metadata + `unresolved`), mirroring the
    /// Find path. Independent of `with_media_resolver` (that wires only
    /// the redispatch Media axis repo). `None` (env-less tests /
    /// non-media deployments) leaves search results without media, as
    /// before this wiring.
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

    fn media_subsystem(&self) -> Option<&super::memory::MediaSubsystem> {
        self.media.as_ref()
    }

    /// Test-only override for the image-search mode (production reads it
    /// once from `MEMORY_IMAGE_SEARCH_MODE` in `new`). Setting it via env
    /// in a test would race other tests sharing the process; this builder
    /// keeps the Media-axis redispatch tests env-independent.
    #[cfg(test)]
    pub fn with_image_search_mode(
        mut self,
        mode: infra::infra::embedding_dispatch::ImageSearchMode,
    ) -> Self {
        self.image_search_mode = mode;
        self
    }

    pub fn vector_repo(&self) -> &MemoryVectorRepositoryImpl {
        &self.vector_repo
    }

    /// Startup dimension probe. Embeds a tiny
    /// fixed text via the mm-embedding worker and compares the runner's
    /// reported dimension to the configured `MEMORY_VECTOR_SIZE` (the
    /// LanceDB FixedSizeList width).
    ///
    /// - `Ok(())` — match, or the probe could not run (no dispatcher /
    ///   jobworkerp unreachable / probe error): a transient infra
    ///   problem must NOT block startup before jobworkerp is up; the
    ///   first real embedding still surfaces a true mismatch via the
    ///   FixedSizeList INSERT. A warn is logged so it is visible.
    /// - `Err` — the probe ran AND the dimensions disagree: a genuine
    ///   model/config drift. The caller fails fast (panic) so every
    ///   subsequent embedding is not silently rejected.
    pub async fn verify_embedding_dimension(&self) -> Result<()> {
        let Some(dispatcher) = self.embedding_dispatcher.as_ref() else {
            tracing::warn!(
                "embedding dimension probe skipped: no embedding dispatcher \
                 configured (auto-embedding disabled)"
            );
            return Ok(());
        };
        let configured = self.vector_repo.vector_size();
        match dispatcher.query_embed_text("dimension probe").await {
            Ok(emb) => {
                if emb.dimension != configured {
                    // Surface as `StartupError::EmbeddingDimensionMismatch`
                    // (wrapped in `anyhow::Error`) so the caller in
                    // `grpc-admin/src/front/server.rs` can downcast and
                    // route into the structured `fatal()` path — agent-app
                    // matches on the `code` field, not the message text.
                    return Err(anyhow::Error::new(
                        infra::infra::startup_error::StartupError::EmbeddingDimensionMismatch {
                            expected_dim: u32::try_from(configured).unwrap_or(u32::MAX),
                            actual_dim: u32::try_from(emb.dimension).unwrap_or(u32::MAX),
                            runner_name: emb.model_name.as_deref().unwrap_or("unknown").to_string(),
                        },
                    ));
                }
                tracing::info!(
                    "embedding dimension probe ok: {} (model={})",
                    configured,
                    emb.model_name.as_deref().unwrap_or("unknown")
                );
                Ok(())
            }
            Err(e) => {
                // jobworkerp may legitimately not be up yet at memories
                // startup. Do not block — log and let the first real
                // embedding catch a true mismatch via the FixedSizeList.
                tracing::warn!(
                    "embedding dimension probe could not run (jobworkerp \
                     unreachable or worker not registered yet): {e}. \
                     Skipping the startup check; a real mismatch will \
                     still be rejected at the first embedding INSERT."
                );
                Ok(())
            }
        }
    }

    // ===== Embedding management =====

    /// Upsert embedding: fetch Memory from RDB, then write to LanceDB
    pub async fn upsert_embedding(
        &self,
        memory_id: i64,
        embedding: &[f32],
        embedding_model: Option<&str>,
    ) -> Result<()> {
        let id = MemoryId { value: memory_id };
        let memory = self
            .memory_repo
            .find(&id, false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("memory not found: {}", memory_id))?;
        let data = memory
            .data
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("memory has no data: {}", memory_id))?;
        let record =
            MemoryVectorRecord::from_memory_data(memory_id, data, embedding, embedding_model);
        self.vector_repo.upsert(&record).await
    }

    /// Batch upsert: fetch all Memories from RDB in one call, build records, batch write
    pub async fn batch_upsert_embeddings(
        &self,
        items: Vec<(i64, Vec<f32>, Option<String>)>,
    ) -> Result<(u32, u32, Vec<String>)> {
        if items.is_empty() {
            return Ok((0, 0, Vec::new()));
        }

        let ids: Vec<i64> = items.iter().map(|(id, _, _)| *id).collect();
        let memories = self.memory_repo.find_by_ids(&ids, false).await?;
        let memory_map: HashMap<i64, Memory> = memories
            .into_iter()
            .filter_map(|m| {
                let id_val = m.id.as_ref()?.value;
                Some((id_val, m))
            })
            .collect();

        let mut records = Vec::new();
        let mut errors = Vec::new();
        let mut failure = 0u32;

        for (memory_id, embedding, model) in &items {
            match memory_map.get(memory_id) {
                Some(memory) => match memory.data.as_ref() {
                    Some(data) => {
                        records.push(MemoryVectorRecord::from_memory_data(
                            *memory_id,
                            data,
                            embedding,
                            model.as_deref(),
                        ));
                    }
                    None => {
                        failure += 1;
                        errors.push(format!("memory_id={}: has no data", memory_id));
                    }
                },
                None => {
                    failure += 1;
                    errors.push(format!("memory_id={}: not found in RDB", memory_id));
                }
            }
        }

        let success = self.vector_repo.batch_upsert(records).await? as u32;
        Ok((success, failure, errors))
    }

    /// N-row `rows` BatchUpsertEmbeddings path.
    /// Writes every `(memory_id, vector_kind, chunk_index)` row for one
    /// memory, deleting the `replace_kinds` rows first so a smaller new
    /// chunk set leaves no stale rows and the text / image pipelines stay
    /// isolated (each replaces only its own kinds).
    ///
    /// `rows` is `(vector_kind, chunk_index, begin, end, content,
    /// embedding)` per chunk — proto-free so the layer boundary stays
    /// clean. Non-content columns come from the RDB `MemoryData` so the
    /// row is consistent with the memory. Returns `(success, failure,
    /// errors)` mirroring the legacy path.
    #[allow(clippy::type_complexity)]
    pub async fn batch_upsert_embeddings_rows(
        &self,
        memory_id: i64,
        embedding_model: Option<&str>,
        replace_kinds: &[String],
        rows: Vec<(String, i32, i32, i32, String, Vec<f32>)>,
    ) -> Result<(u32, u32, Vec<String>)> {
        if replace_kinds.is_empty() {
            return Err(LlmMemoryError::InvalidArgument(
                "batch_upsert_embeddings_rows: replace_kinds must not be empty".to_string(),
            )
            .into());
        }
        // The memory must exist so the row carries consistent user_id /
        // role / content_type / metadata (the search-side columns).
        let memories = self.memory_repo.find_by_ids(&[memory_id], false).await?;
        let Some(data) = memories
            .into_iter()
            .find(|m| m.id.as_ref().map(|i| i.value) == Some(memory_id))
            .and_then(|m| m.data)
        else {
            return Ok((
                0,
                rows.len() as u32,
                vec![format!("memory_id={memory_id}: not found in RDB")],
            ));
        };

        let mut records = Vec::with_capacity(rows.len());
        for (vector_kind, chunk_index, begin, end, content, embedding) in rows {
            records.push(MemoryVectorRecord::from_chunk_with_content(
                memory_id,
                &data,
                &embedding,
                embedding_model,
                &vector_kind,
                chunk_index,
                begin,
                end,
                content,
            ));
        }
        let replace_refs: Vec<&str> = replace_kinds.iter().map(String::as_str).collect();
        let success = self
            .vector_repo
            .replace_kinds_upsert(memory_id, &replace_refs, records)
            .await? as u32;
        Ok((success, 0, Vec::new()))
    }

    pub fn thread_filter_config(&self) -> &ThreadFilterConfig {
        &self.thread_filter_config
    }

    // ===== Thread filter resolve (P5) =====

    /// Thin wrapper around the shared `thread_filter_resolver` so existing
    /// vector-side call sites keep their `self.resolve_memory_ids_from_thread_filter(...)`
    /// signature. The actual algorithm lives in
    /// `crate::app::thread_filter_resolver` so the P8 RDB list/Count path
    /// can share the same implementation.
    pub async fn resolve_memory_ids_from_thread_filter(
        &self,
        filter: &ThreadSearchFilter,
        memory_user_id: Option<i64>,
    ) -> Result<Option<Vec<i64>>> {
        thread_filter_resolver::resolve_memory_ids_from_thread_filter(
            &self.thread_filter_config,
            &self.thread_label_repo,
            &self.thread_repo,
            &self.thread_memory_repo,
            filter,
            memory_user_id,
        )
        .await
    }

    // ===== Search =====

    /// Resolve `thread_filter` into a `SafeFilter` chunk and AND it onto
    /// `base`. Returns:
    /// * `Ok(None)` — caller should short-circuit the search to an empty
    ///   result (the thread_filter matched zero threads).
    /// * `Ok(Some(filter))` — pass `filter` to the LanceDB query.
    ///
    /// The FTS-specific `MAX_INLINE_IDS` / `MAX_TOTAL_IDS` ceilings only
    /// apply to routes that go through LanceDB's `only_if(SQL)` for FTS
    /// (`Fts` and `Hybrid`). Pure ANN (`Vector`) is bounded by
    /// `MAX_THREAD_IDS` upstream in `resolve_memory_ids_from_thread_filter`
    /// and intentionally bypasses the FTS limits — `prefilter_threshold`
    /// only emits a warn for ANN since flipping to LanceDB `postfilter()`
    /// is a follow-up PR.
    fn build_search_filter(
        &self,
        base: Option<&SafeFilter>,
        thread_filter_ids: Option<Vec<i64>>,
        kind: SearchKind,
    ) -> Result<FilterPlan> {
        build_search_filter_with_cfg(&self.thread_filter_config, base, thread_filter_ids, kind)
    }

    /// Resolve the thread filter into memory_ids and combine it with the
    /// base filter for a `SearchKind::Vector` query. `Ok(None)` means the
    /// filter short-circuits to "no results" (the caller returns an empty
    /// vec). Shared by `search_by_vector`'s single-vector path and the
    /// server-embedded path so the thread-resolve + 3-variant FilterPlan
    /// match is not maintained twice. Vector search never yields
    /// `FilterPlan::Approximate` (`SearchKind::Vector` is not
    /// `uses_fts()`), so that variant is a programmer error.
    async fn resolve_vector_search_filter(
        &self,
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
    ) -> Result<Option<Option<SafeFilter>>> {
        let thread_ids = match thread_filter {
            Some(tf) => {
                self.resolve_memory_ids_from_thread_filter(tf, memory_user_id)
                    .await?
            }
            None => None,
        };
        match self.build_search_filter(filter, thread_ids, SearchKind::Vector)? {
            FilterPlan::ShortCircuit => Ok(None),
            FilterPlan::Strict(f) => Ok(Some(f)),
            FilterPlan::Approximate { .. } => Err(anyhow::anyhow!(
                "internal error: approximate plan is not valid for SearchKind::Vector"
            )),
        }
    }

    /// Vector similarity search with RDB hydration.
    /// Supports multi-vector search with aggregation when multiple vectors are provided.
    #[allow(clippy::too_many_arguments)]
    pub async fn search_by_vector(
        &self,
        query_vectors: &[Vec<f32>],
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
        limit: usize,
        include_content: bool,
        aggregation: Option<AggregationStrategy>,
    ) -> Result<Vec<MemorySearchResultItem>> {
        if query_vectors.is_empty() {
            anyhow::bail!("query_vectors must not be empty");
        }

        let Some(combined) = self
            .resolve_vector_search_filter(filter, thread_filter, memory_user_id)
            .await?
        else {
            return Ok(Vec::new());
        };
        let combined_ref = combined.as_ref();

        if query_vectors.len() == 1 {
            return self
                .search_with_overfetch(
                    limit,
                    include_content,
                    None,
                    // Client-supplied vector → no model-space guarantee,
                    // score_source stays SCORE_VECTOR (design 1/3 §2.6.4).
                    QueryOrigin::ExternalVector,
                    |fetch_limit| {
                        self.vector_repo.search_by_vector(
                            &query_vectors[0],
                            combined_ref,
                            fetch_limit,
                        )
                    },
                )
                .await;
        }

        // Multi-vector search: run all vectors in parallel, then aggregate.
        // Uses overfetch + retry to compensate for stale LanceDB records (same
        // principle as search_with_overfetch, but adapted for parallel aggregation).
        let strategy = aggregation.unwrap_or(AggregationStrategy::Average);
        let overfetch_limit = ((limit as f64 * 1.5).ceil() as usize).max(limit + 1);
        let futures: Vec<_> = query_vectors
            .iter()
            .map(|vec| {
                self.vector_repo
                    .search_by_vector(vec, combined_ref, overfetch_limit)
            })
            .collect();
        let all_hits = futures::future::try_join_all(futures).await?;
        let mut aggregated = aggregate_scores(&all_hits, strategy);
        aggregated.truncate(limit);
        let mut results = self
            .enrich_hits(
                aggregated,
                include_content,
                None,
                QueryOrigin::ExternalVector,
            )
            .await?;

        if results.len() < limit {
            // Retry with larger fetch to compensate for stale records
            let retry_limit = limit * 3;
            if retry_limit > overfetch_limit {
                let retry_futures: Vec<_> = query_vectors
                    .iter()
                    .map(|vec| {
                        self.vector_repo
                            .search_by_vector(vec, combined_ref, retry_limit)
                    })
                    .collect();
                let retry_hits = futures::future::try_join_all(retry_futures).await?;
                let mut retry_aggregated = aggregate_scores(&retry_hits, strategy);
                retry_aggregated.truncate(limit);
                results = self
                    .enrich_hits(
                        retry_aggregated,
                        include_content,
                        None,
                        QueryOrigin::ExternalVector,
                    )
                    .await?;
                results.truncate(limit);
            }
            if results.len() < limit {
                tracing::warn!(
                    "multi-vector search: requested {} results but only {} found after RDB hydration (possible stale index)",
                    limit,
                    results.len()
                );
            }
        } else {
            results.truncate(limit);
        }
        Ok(results)
    }

    /// Semantic (vector) text search: the server embeds `query_text` in
    /// the SAME model space as the stored vectors (via the mm-embedding
    /// worker), then ANN-searches all vector_kinds and de-dups by
    /// memory_id. `score_source` becomes the matched row's
    /// kind-specific `SCORE_*_EMBED` because the query is server-embedded
    /// in the stored-vector model space — unlike `SearchByVector`, whose
    /// client-supplied vector has no model-space guarantee.
    #[allow(clippy::too_many_arguments)]
    pub async fn search_semantic(
        &self,
        query_text: &str,
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
        limit: usize,
        include_content: bool,
    ) -> Result<Vec<MemorySearchResultItem>> {
        if query_text.trim().is_empty() {
            anyhow::bail!("query_text must not be empty");
        }
        self.embed_then_vector_search(
            "semantic search",
            QueryOrigin::ServerText,
            |d| async move { d.query_embed_text(query_text).await },
            filter,
            thread_filter,
            memory_user_id,
            limit,
            include_content,
        )
        .await
    }

    /// Media (vector) search: the caller has already resolved the query
    /// media to a fetchable URL (the gRPC layer does the Resolve +
    /// unresolved/FailedPrecondition handling so the layer boundary stays
    /// clean). The server embeds the image in the same model space and
    /// ANN-searches all vector_kinds.
    #[allow(clippy::too_many_arguments)]
    pub async fn search_by_media_url(
        &self,
        image_url: &str,
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
        limit: usize,
        include_content: bool,
    ) -> Result<Vec<MemorySearchResultItem>> {
        if image_url.trim().is_empty() {
            anyhow::bail!("image_url must not be empty");
        }
        self.embed_then_vector_search(
            "media search",
            QueryOrigin::ServerImage,
            |d| async move { d.query_embed_image_url(image_url).await },
            filter,
            thread_filter,
            memory_user_id,
            limit,
            include_content,
        )
        .await
    }

    /// Shared body of SearchSemantic / SearchByMedia: require the
    /// embedding dispatcher, run `embed` (server-side query embedding in
    /// the stored-vector model space), take the first vector, then run
    /// the server-embedded vector search. `what` names the operation for
    /// the "unavailable" error.
    #[allow(clippy::too_many_arguments)]
    async fn embed_then_vector_search<'a, F, Fut>(
        &'a self,
        what: &str,
        query_origin: QueryOrigin,
        embed: F,
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
        limit: usize,
        include_content: bool,
    ) -> Result<Vec<MemorySearchResultItem>>
    where
        F: FnOnce(&'a EmbeddingJobDispatcher) -> Fut,
        Fut: std::future::Future<Output = Result<infra::infra::embedding_dispatch::QueryEmbedding>>,
    {
        let dispatcher = self.embedding_dispatcher.as_ref().ok_or_else(|| {
            LlmMemoryError::InvalidArgument(format!(
                "{what} is unavailable: embedding worker not configured"
            ))
        })?;
        let emb = embed(dispatcher).await?;
        // A short query is one chunk / one image; if the runner still
        // split it, the first vector is the best whole-query
        // representative.
        let query_vec =
            emb.vectors.into_iter().next().ok_or_else(|| {
                anyhow::anyhow!("embedding worker returned no vector for the query")
            })?;
        self.vector_search_server_embedded(
            query_vec,
            query_origin,
            filter,
            thread_filter,
            memory_user_id,
            limit,
            include_content,
        )
        .await
    }

    /// Shared tail of SearchSemantic / SearchByMedia: build the filter
    /// (same as SearchByVector's single-vector path), run the staged
    /// over-fetch ANN + memory_id de-dup, and tag the result with the
    /// server-embedded `query_origin` so `score_source` is kind-specific.
    #[allow(clippy::too_many_arguments)]
    async fn vector_search_server_embedded(
        &self,
        query_vec: Vec<f32>,
        query_origin: QueryOrigin,
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
        limit: usize,
        include_content: bool,
    ) -> Result<Vec<MemorySearchResultItem>> {
        let Some(combined) = self
            .resolve_vector_search_filter(filter, thread_filter, memory_user_id)
            .await?
        else {
            return Ok(Vec::new());
        };
        let combined_ref = combined.as_ref();
        self.search_with_overfetch(limit, include_content, None, query_origin, |fetch_limit| {
            self.vector_repo
                .search_by_vector(&query_vec, combined_ref, fetch_limit)
        })
        .await
    }

    /// Full-text search with RDB hydration
    pub async fn search_by_text(
        &self,
        query_text: &str,
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
        limit: usize,
        include_content: bool,
    ) -> Result<Vec<MemorySearchResultItem>> {
        let thread_ids = match thread_filter {
            Some(tf) => {
                self.resolve_memory_ids_from_thread_filter(tf, memory_user_id)
                    .await?
            }
            None => None,
        };
        match self.build_search_filter(filter, thread_ids, SearchKind::Fts)? {
            FilterPlan::ShortCircuit => Ok(Vec::new()),
            FilterPlan::Strict(combined) => {
                let combined_ref = combined.as_ref();
                self.search_with_overfetch(
                    limit,
                    include_content,
                    Some(query_text),
                    QueryOrigin::Bm25Text,
                    |fetch_limit| {
                        self.vector_repo
                            .search_by_text(query_text, combined_ref, fetch_limit)
                    },
                )
                .await
            }
            FilterPlan::Approximate { base, allowed } => {
                self.two_phase_fts_search(
                    limit,
                    include_content,
                    base,
                    allowed,
                    Some(query_text),
                    QueryOrigin::Bm25Text,
                    |b, n| async move {
                        self.vector_repo
                            .search_by_text(query_text, b.as_ref(), n)
                            .await
                    },
                )
                .await
            }
        }
    }

    /// Hybrid search (vector + FTS) with RDB hydration
    #[allow(clippy::too_many_arguments)]
    pub async fn hybrid_search(
        &self,
        query_vectors: &[Vec<f32>],
        query_text: &str,
        filter: Option<&SafeFilter>,
        thread_filter: Option<&ThreadSearchFilter>,
        memory_user_id: Option<i64>,
        limit: usize,
        options: &HybridOptions,
        include_content: bool,
    ) -> Result<Vec<MemorySearchResultItem>> {
        if query_vectors.is_empty() {
            anyhow::bail!("query_vectors must not be empty");
        }
        if query_vectors.len() > 1 {
            anyhow::bail!(
                "multi-vector search is not yet supported; pass exactly one query vector"
            );
        }
        let thread_ids = match thread_filter {
            Some(tf) => {
                self.resolve_memory_ids_from_thread_filter(tf, memory_user_id)
                    .await?
            }
            None => None,
        };
        match self.build_search_filter(filter, thread_ids, SearchKind::Hybrid)? {
            FilterPlan::ShortCircuit => Ok(Vec::new()),
            FilterPlan::Strict(combined) => {
                let combined_ref = combined.as_ref();
                self.search_with_overfetch(
                    limit,
                    include_content,
                    Some(query_text),
                    QueryOrigin::Hybrid,
                    |fetch_limit| {
                        self.vector_repo.hybrid_search(
                            &query_vectors[0],
                            query_text,
                            combined_ref,
                            fetch_limit,
                            options,
                        )
                    },
                )
                .await
            }
            FilterPlan::Approximate { base, allowed } => {
                self.two_phase_fts_search(
                    limit,
                    include_content,
                    base,
                    allowed,
                    Some(query_text),
                    QueryOrigin::Hybrid,
                    |b, n| async move {
                        self.vector_repo
                            .hybrid_search(&query_vectors[0], query_text, b.as_ref(), n, options)
                            .await
                    },
                )
                .await
            }
        }
    }

    // ===== Count search (P2, Phase 5-1) =====

    /// Count matches for a `CountSearchMatches` RPC.
    ///
    /// Phase 5-1 implements `FilterOnly` and `Text` only. Mode-specific
    /// behaviour:
    ///
    /// - **FilterOnly**: backed by `Table::count_rows(Filter::Sql)` on
    ///   LanceDB so the result is exact (`is_truncated = false`).
    ///   `filter = None` is allowed (cross-user header count) and emits
    ///   an INFO log so operators can spot unintended global counts.
    /// - **Text**: backed by `full_text_search().execute()` with a
    ///   `hard_cap + 1` limit (`MEMORY_FTS_COUNT_HARD_CAP`, default
    ///   50_000). When the stream goes past the cap, returns
    ///   `(hard_cap, true)`. `query_text` is required and rejected
    ///   when empty (both unset and empty string are invalid).
    /// - **Vector / Hybrid**: returns `LlmMemoryError::Unimplemented`
    ///   until Phase 5-2.
    ///
    /// Thread-filter handling reuses
    /// `resolve_memory_ids_from_thread_filter` (P5). When the resolved
    /// memory_id set exceeds `fts_max_inline_ids` the FilterOnly path
    /// chunks `count_rows` calls (1000 ids per chunk) and sums the
    /// totals — `find_memory_ids_by_thread_ids` is `SELECT DISTINCT`
    /// upstream so chunk boundaries cannot double-count. The Text path
    /// rejects with `failed_precondition` instead, because BM25 cannot
    /// be chunked while keeping the `_score > 0` row count consistent
    /// (see spec §P5 3-b).
    pub async fn count_search_matches(
        &self,
        input: CountSearchInput<'_>,
    ) -> Result<CountSearchOutput> {
        let memory_user_id = input.filter.and_then(|f| f.user_id);
        let thread_filter = input.filter.and_then(|f| f.thread_filter.as_ref());
        let base_filter = input.filter.and_then(SafeFilter::from_proto_filter);

        // Resolve the thread_filter into a memory_id set up-front. The
        // search path runs the same resolve, so any new MAX / hard-cap
        // logic flows through one code path.
        let thread_memory_ids = match thread_filter {
            Some(tf) => {
                self.resolve_memory_ids_from_thread_filter(tf, memory_user_id)
                    .await?
            }
            None => None,
        };

        match input.mode {
            CountMode::FilterOnly => {
                // INFO log so operators can correlate cross-user header
                // counts (filter intentionally absent) against
                // expectations. Spec §P2 line 720-727: this is allowed
                // and not WARN-worthy because it is a legitimate use
                // case under MemorySearchFilter.user_id = optional.
                if input.filter.is_none() {
                    tracing::info!(
                        target: "memory_vector::count",
                        "FILTER_ONLY count without filter: counting all memories across users",
                    );
                }
                self.count_filter_only(base_filter.as_ref(), thread_memory_ids)
                    .await
            }
            CountMode::Text => {
                let q = input.query_text.filter(|s| !s.is_empty()).ok_or_else(|| {
                    LlmMemoryError::InvalidArgument(
                        "query_text is required and must not be empty for CountSearchMode::TEXT"
                            .to_string(),
                    )
                })?;
                self.count_text(q, base_filter.as_ref(), thread_memory_ids)
                    .await
            }
            CountMode::Vector => {
                self.count_vector(
                    input.query_vectors,
                    input.aggregation,
                    base_filter.as_ref(),
                    thread_memory_ids,
                )
                .await
            }
            CountMode::Hybrid => {
                let q = input.query_text.filter(|s| !s.is_empty()).ok_or_else(|| {
                    LlmMemoryError::InvalidArgument(
                        "query_text is required and must not be empty for CountSearchMode::HYBRID"
                            .to_string(),
                    )
                })?;
                self.count_hybrid(
                    input.query_vectors,
                    q,
                    input.hybrid_options,
                    base_filter.as_ref(),
                    thread_memory_ids,
                )
                .await
            }
        }
    }

    async fn count_vector(
        &self,
        query_vectors: &[Vec<f32>],
        aggregation: Option<AggregationStrategy>,
        base_filter: Option<&SafeFilter>,
        thread_memory_ids: Option<Vec<i64>>,
    ) -> Result<CountSearchOutput> {
        if query_vectors.is_empty() {
            return Err(LlmMemoryError::InvalidArgument(
                "query_vectors is required and must not be empty for CountSearchMode::VECTOR"
                    .to_string(),
            )
            .into());
        }
        // `aggregation` is accepted for API symmetry but does not change
        // the count: aggregation strategies only re-rank a fixed
        // membership set, and Count returns set size.
        let _ = aggregation;

        let cfg = &self.thread_filter_config;
        let hard_cap = cfg.count_vector_hard_cap;

        let combined: Option<SafeFilter> = match thread_memory_ids {
            None => base_filter.cloned(),
            Some(ids) if ids.is_empty() => {
                return Ok(CountSearchOutput {
                    total: 0,
                    is_truncated: false,
                    mode: CountMode::Vector,
                });
            }
            Some(ids) => {
                // ANN cannot be chunked cheaply; reject above the absolute
                // ceiling and warn (without chunking) above the inline
                // ceiling so the operator can adjust thread_filter scope.
                if ids.len() > cfg.fts_max_total_ids {
                    return Err(LlmMemoryError::FailedPrecondition(format!(
                        "thread_filter resolved to {} memory_ids for \
                         CountSearchMode::VECTOR, exceeding \
                         MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS = {}.",
                        ids.len(),
                        cfg.fts_max_total_ids,
                    ))
                    .into());
                }
                if ids.len() > cfg.fts_max_inline_ids {
                    tracing::warn!(
                        target: "memory_vector::count",
                        n = ids.len(),
                        inline_ceiling = cfg.fts_max_inline_ids,
                        "VECTOR count thread_filter exceeds inline ceiling — \
                         post-filter ANN cost may be high",
                    );
                }
                let in_filter = SafeFilter::in_i64_list("memory_id", &ids)?;
                Some(match base_filter.cloned() {
                    Some(b) => b.and(in_filter),
                    None => in_filter,
                })
            }
        };

        if query_vectors.len() == 1 {
            let (total, is_truncated) = self
                .vector_repo
                .count_by_vector(&query_vectors[0], combined.as_ref(), hard_cap)
                .await?;
            return Ok(CountSearchOutput {
                total,
                is_truncated,
                mode: CountMode::Vector,
            });
        }

        // Multi-vector: count the union of per-branch ANN id sets.
        // Aggregation strategy is irrelevant for membership, so this
        // path computes a single union irrespective of `aggregation`.
        let overfetch_cap = ((hard_cap as f64 * 1.5).ceil() as u64).max(hard_cap + 1);
        let combined_ref = combined.as_ref();
        let futures: Vec<_> = query_vectors
            .iter()
            .map(|v| {
                self.vector_repo
                    .collect_vector_ids_capped(v, combined_ref, overfetch_cap)
            })
            .collect();
        let branches = futures::future::try_join_all(futures).await?;
        let mut union: std::collections::HashSet<i64> = std::collections::HashSet::new();
        let mut any_branch_truncated = false;
        for (ids, truncated) in branches {
            any_branch_truncated |= truncated;
            union.extend(ids);
        }
        let merged = union.len() as u64;
        let total = merged.min(hard_cap);
        let is_truncated = any_branch_truncated || merged > hard_cap;
        Ok(CountSearchOutput {
            total,
            is_truncated,
            mode: CountMode::Vector,
        })
    }

    async fn count_hybrid(
        &self,
        query_vectors: &[Vec<f32>],
        query_text: &str,
        hybrid_options: Option<&HybridOptions>,
        base_filter: Option<&SafeFilter>,
        thread_memory_ids: Option<Vec<i64>>,
    ) -> Result<CountSearchOutput> {
        if query_vectors.is_empty() {
            return Err(LlmMemoryError::InvalidArgument(
                "query_vectors is required and must not be empty for CountSearchMode::HYBRID"
                    .to_string(),
            )
            .into());
        }
        if query_vectors.len() != 1 {
            // Mirror search-side restriction: hybrid_search accepts a
            // single query_vector only.
            return Err(LlmMemoryError::InvalidArgument(
                "CountSearchMode::HYBRID accepts exactly one query_vector".to_string(),
            )
            .into());
        }
        let options = hybrid_options.cloned().unwrap_or(HybridOptions {
            strategy: HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: None,
        });

        let cfg = &self.thread_filter_config;
        let vector_hard_cap = cfg.count_vector_hard_cap;
        let fts_hard_cap = cfg.fts_count_hard_cap;

        // BM25 cannot be chunked while keeping its row count consistent,
        // and HYBRID's text branch participates in the union — so the
        // TEXT-mode inline ceiling is the binding constraint here too.
        let combined: Option<SafeFilter> = match thread_memory_ids {
            None => base_filter.cloned(),
            Some(ids) if ids.is_empty() => {
                return Ok(CountSearchOutput {
                    total: 0,
                    is_truncated: false,
                    mode: CountMode::Hybrid,
                });
            }
            Some(ids) => {
                if ids.len() > cfg.fts_max_inline_ids {
                    return Err(LlmMemoryError::FailedPrecondition(format!(
                        "thread_filter resolved to {} memory_ids for \
                         CountSearchMode::HYBRID, exceeding \
                         MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS = {}. \
                         BM25 cannot be chunked while remaining consistent — \
                         narrow the thread_filter or use FILTER_ONLY.",
                        ids.len(),
                        cfg.fts_max_inline_ids,
                    ))
                    .into());
                }
                let in_filter = SafeFilter::in_i64_list("memory_id", &ids)?;
                Some(match base_filter.cloned() {
                    Some(b) => b.and(in_filter),
                    None => in_filter,
                })
            }
        };

        let (total, is_truncated) = self
            .vector_repo
            .count_by_hybrid(
                &query_vectors[0],
                query_text,
                combined.as_ref(),
                &options,
                vector_hard_cap,
                fts_hard_cap,
            )
            .await?;
        Ok(CountSearchOutput {
            total,
            is_truncated,
            mode: CountMode::Hybrid,
        })
    }

    async fn count_filter_only(
        &self,
        base_filter: Option<&SafeFilter>,
        thread_memory_ids: Option<Vec<i64>>,
    ) -> Result<CountSearchOutput> {
        let cfg = &self.thread_filter_config;

        match thread_memory_ids {
            // No thread_filter: a single LanceDB count_rows pass.
            None => {
                let (total, is_truncated) = self.vector_repo.count_by_filter(base_filter).await?;
                Ok(CountSearchOutput {
                    total,
                    is_truncated,
                    mode: CountMode::FilterOnly,
                })
            }
            // thread_filter resolved to zero memory_ids — short-circuit.
            Some(ids) if ids.is_empty() => Ok(CountSearchOutput {
                total: 0,
                is_truncated: false,
                mode: CountMode::FilterOnly,
            }),
            Some(ids) => {
                if ids.len() > cfg.fts_max_total_ids {
                    return Err(LlmMemoryError::FailedPrecondition(format!(
                        "thread_filter resolved to {} memory_ids for \
                         CountSearchMode::FILTER_ONLY, exceeding \
                         MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS = {}.",
                        ids.len(),
                        cfg.fts_max_total_ids,
                    ))
                    .into());
                }
                // Chunk size 1000 (50 chunks × 1000 = 50_000 default
                // upper bound). Spec §P5 3-a.
                const COUNT_CHUNK_SIZE: usize = 1_000;
                let mut total: u64 = 0;
                for chunk in ids.chunks(COUNT_CHUNK_SIZE) {
                    let in_filter = SafeFilter::in_i64_list("memory_id", chunk)?;
                    let combined = match base_filter.cloned() {
                        Some(b) => b.and(in_filter),
                        None => in_filter,
                    };
                    let (chunk_total, _) =
                        self.vector_repo.count_by_filter(Some(&combined)).await?;
                    total = total.saturating_add(chunk_total);
                }
                Ok(CountSearchOutput {
                    total,
                    is_truncated: false,
                    mode: CountMode::FilterOnly,
                })
            }
        }
    }

    async fn count_text(
        &self,
        query_text: &str,
        base_filter: Option<&SafeFilter>,
        thread_memory_ids: Option<Vec<i64>>,
    ) -> Result<CountSearchOutput> {
        let cfg = &self.thread_filter_config;
        let hard_cap = cfg.fts_count_hard_cap;

        let combined: Option<SafeFilter> = match thread_memory_ids {
            None => base_filter.cloned(),
            Some(ids) if ids.is_empty() => {
                return Ok(CountSearchOutput {
                    total: 0,
                    is_truncated: false,
                    mode: CountMode::Text,
                });
            }
            Some(ids) => {
                // BM25 cannot be chunked while keeping the row count
                // consistent (chunked _score > 0 rows ≠ overall hit
                // count because IDF differs per chunk). Reject when
                // the resolved set exceeds the inline ceiling — spec
                // §P5 3-b.
                if ids.len() > cfg.fts_max_inline_ids {
                    return Err(LlmMemoryError::FailedPrecondition(format!(
                        "thread_filter resolved to {} memory_ids for \
                         CountSearchMode::TEXT, exceeding \
                         MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS = {}. \
                         BM25 cannot be chunked while remaining consistent — \
                         narrow the thread_filter or use FILTER_ONLY.",
                        ids.len(),
                        cfg.fts_max_inline_ids,
                    ))
                    .into());
                }
                let in_filter = SafeFilter::in_i64_list("memory_id", &ids)?;
                Some(match base_filter.cloned() {
                    Some(b) => b.and(in_filter),
                    None => in_filter,
                })
            }
        };

        let (total, is_truncated) = self
            .vector_repo
            .count_by_text(query_text, combined.as_ref(), hard_cap)
            .await?;
        Ok(CountSearchOutput {
            total,
            is_truncated,
            mode: CountMode::Text,
        })
    }

    /// Backward-compat resolver for `GetSurroundingMemories` callers that
    /// cannot populate the new `thread_id` field. Walks the thread_memory
    /// junction for `memory_id` and returns the single owning thread_id.
    ///
    /// Returned errors are distinguishable by message so the caller (and
    /// humans reading gRPC logs) can tell apart three states:
    /// - memory row does not exist → `NotFound("memory not found: X")`.
    ///   Symmetric with the existence check in `get_surrounding_memories`.
    /// - memory exists but has 0 junction rows → `NotFound("memory X is
    ///   not attached to any thread; pass thread_id explicitly")`.
    /// - memory exists with 2+ junction rows → `InvalidArgument` (shared
    ///   ROLE_SYSTEM prompt or forked conversation — picking one silently
    ///   would be non-deterministic).
    ///
    /// Intended for the Phase 4 wire-compat shim only: new clients should
    /// always pass `thread_id` explicitly and hit the fast path in
    /// `get_surrounding_memories`.
    pub async fn resolve_single_thread_for_memory(&self, memory_id: i64) -> Result<i64> {
        // Confirm the memory row exists before consulting the junction so
        // "memory was never created" is not silently reported as
        // "not attached to any thread". Uses the same lookup as the
        // existence guard in `get_surrounding_memories` for consistent
        // error wording across both paths.
        let id = MemoryId { value: memory_id };
        if self.memory_repo.find(&id, false).await?.is_none() {
            return Err(LlmMemoryError::NotFound(format!("memory not found: {memory_id}")).into());
        }

        let pool = self.memory_repo.db_pool();
        let threads = self
            .thread_memory_repo
            .find_threads_by_memory_tx(pool, memory_id)
            .await?;
        match threads.len() {
            0 => Err(LlmMemoryError::NotFound(format!(
                "memory {memory_id} is not attached to any thread; pass thread_id explicitly"
            ))
            .into()),
            1 => Ok(threads[0]),
            _ => Err(LlmMemoryError::InvalidArgument(format!(
                "memory {memory_id} is attached to multiple threads; thread_id is required to disambiguate"
            ))
            .into()),
        }
    }

    /// Get surrounding memories using `thread_memory.position` for ordering.
    /// Uses bounded queries to avoid loading all records in the thread.
    ///
    /// `thread_id` is required from the caller because a Memory may belong
    /// to more than one thread (shared ROLE_SYSTEM prompts, and eventually
    /// forked conversations). The caller selects which thread's position
    /// axis to walk.
    ///
    /// If `memory_id` is not registered under `thread_id` in the
    /// `thread_memory` junction this method returns a `NotFound` error —
    /// otherwise `find_surrounding` would resolve the pivot position to
    /// NULL, return no neighbours, and happily hand back a target row
    /// together with empty before/after lists, which is silently wrong for
    /// the caller's intent.
    pub async fn get_surrounding_memories(
        &self,
        memory_id: i64,
        thread_id: i64,
        before_count: usize,
        after_count: usize,
    ) -> Result<SurroundingMemoriesResult> {
        let pool = self.memory_repo.db_pool();
        let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;
        configure_repeatable_read_if_supported(&mut tx).await?;

        let target_id = MemoryId { value: memory_id };
        let mut target = self
            .memory_repo
            .find_row_tx(&mut *tx, &target_id)
            .await?
            .map(|row| row.to_proto())
            .ok_or_else(|| LlmMemoryError::NotFound(format!("memory not found: {memory_id}")))?;
        if target.data.is_none() {
            return Err(LlmMemoryError::InvalidArgument(format!(
                "memory has no data: {memory_id}"
            ))
            .into());
        }

        // Reject the request early if the pivot memory does not belong to
        // the requested thread.
        let attached = self
            .thread_memory_repo
            .contains_tx(&mut *tx, thread_id, memory_id)
            .await?;
        if !attached {
            return Err(LlmMemoryError::NotFound(format!(
                "memory {memory_id} is not attached to thread {thread_id}"
            ))
            .into());
        }

        // Fetch one extra to detect has_more
        let (mut before, mut after) = self
            .memory_repo
            .find_surrounding_tx(
                &mut tx,
                thread_id,
                memory_id,
                (before_count + 1) as i64,
                (after_count + 1) as i64,
                true,
            )
            .await?;
        self.memory_repo
            .fill_thread_ids_tx(&mut tx, std::slice::from_mut(&mut target))
            .await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        let has_more_before = before.len() > before_count;
        let has_more_after = after.len() > after_count;
        // before is in chronological order (oldest first); drop the oldest extra element
        if has_more_before {
            before.remove(0);
        }
        after.truncate(after_count);

        Ok(SurroundingMemoriesResult {
            target,
            before,
            after,
            has_more_before,
            has_more_after,
        })
    }

    /// Re-enqueue embedding jobs for memories selected from RDB.
    ///
    /// Intended for operational recovery after manually dropping a LanceDB
    /// table. The dispatcher path is reused so the rebuild follows the same
    /// embedding workflow as normal write traffic.
    ///
    /// Returns `(dispatched, skipped, failed, duration_ms)`:
    /// - `dispatched` — memory rows whose embedding job was successfully
    ///   enqueued to jobworkerp.
    /// - `skipped` — rows intentionally bypassed (non-embeddable content
    ///   type, empty content, missing id/data, or dispatcher reporting
    ///   `Ok(None)`).
    /// - `failed` — rows for which `dispatch()` returned a
    ///   `DispatchError`. If the first failure is `DispatchError::Init`
    ///   the loop breaks early because every subsequent dispatch would
    ///   fail for the same reason; the un-touched remainder is left out
    ///   of all three counters.
    pub async fn redispatch_embeddings(
        &self,
        user_id: Option<i64>,
        thread_id: Option<i64>,
        batch_size: Option<u32>,
        kinds: &[DispatchKind],
    ) -> Result<(u32, u32, u32, i64)> {
        let dispatcher = self.embedding_dispatcher.as_ref().ok_or_else(|| {
            LlmMemoryError::InvalidArgument(
                "embedding dispatcher is not configured; cannot redispatch embeddings".to_string(),
            )
        })?;
        self.redispatch_embeddings_with(dispatcher.as_ref(), user_id, thread_id, batch_size, kinds)
            .await
    }

    /// Inner loop extracted from [`Self::redispatch_embeddings`] so unit
    /// tests can inject a stub dispatcher. The trait receiver is `&dyn`
    /// to avoid polluting every call site with generics; the hot path
    /// still goes through the inherent `EmbeddingJobDispatcher::dispatch`
    /// via the blanket impl in `dispatcher.rs`.
    pub(crate) async fn redispatch_embeddings_with(
        &self,
        dispatcher: &dyn EmbeddingDispatch,
        user_id: Option<i64>,
        thread_id: Option<i64>,
        batch_size: Option<u32>,
        kinds: &[DispatchKind],
    ) -> Result<(u32, u32, u32, i64)> {
        let page_size = batch_size.unwrap_or(500).clamp(1, 5000) as i32;
        // Keyset pagination: track the last seen id and fetch rows with
        // id > cursor. This avoids the O(offset) scan and row-skip /
        // duplicate issues that OFFSET pagination suffers under
        // concurrent writes.
        let mut cursor: i64 = 0;
        let mut dispatched = 0u32;
        let mut skipped = 0u32;
        let mut failed = 0u32;
        let mut aborted_due_to_init = false;
        let start = std::time::Instant::now();

        'outer: loop {
            let memories = self
                .memory_repo
                .find_list_by_condition_after_id(page_size, cursor, &[], user_id, thread_id)
                .await?;
            if memories.is_empty() {
                break;
            }

            // Resolve every linked media_object for this page in ONE
            // query (kind / storage_backend feed dispatch_kinds' Media
            // axis), instead of a per-memory find_by_id (N+1). Absent
            // resolver (env-less tests) → text-only; a dangling
            // media_object_id is simply missing from the map.
            let media_by_id: HashMap<i64, (i32, String)> = match &self.media_object_repo {
                Some(repo) => {
                    let ids: Vec<i64> = memories
                        .iter()
                        .filter_map(|m| {
                            m.data
                                .as_ref()
                                .and_then(|d| d.media_object_id.map(|x| x.value))
                        })
                        .collect();
                    if ids.is_empty() {
                        HashMap::new()
                    } else {
                        repo.find_by_ids(&ids)
                            .await?
                            .into_iter()
                            .map(|r| (r.id, (r.kind, r.storage_backend)))
                            .collect()
                    }
                }
                None => HashMap::new(),
            };

            for memory in &memories {
                let Some(memory_id) = memory.id.as_ref().map(|id| id.value) else {
                    skipped += 1;
                    continue;
                };
                let Some(data) = memory.data.as_ref() else {
                    skipped += 1;
                    continue;
                };

                let mid = data.media_object_id.map(|m| m.value);
                let (media_kind, media_backend) = match mid.and_then(|m| media_by_id.get(&m)) {
                    Some((kind, backend)) => (Some(*kind), Some(backend.as_str())),
                    None => (None, None),
                };

                let Some(mut target) = DispatchTarget::from_memory(
                    memory_id,
                    &data.content,
                    data.role,
                    data.content_type,
                    mid,
                    media_kind,
                    media_backend,
                    self.image_search_mode,
                ) else {
                    // No pipeline applies (role not allowed / tool-only /
                    // empty content with no embeddable media).
                    skipped += 1;
                    continue;
                };

                // Apply the requested kinds filter. Empty `kinds` means
                // "all kinds" (backward compatible). A non-empty filter
                // keeps only the requested kinds; if nothing remains the
                // memory is skipped.
                if !kinds.is_empty() {
                    target.kinds.retain(|k| kinds.contains(k));
                    if target.kinds.is_empty() {
                        skipped += 1;
                        continue;
                    }
                }

                let mut row_dispatched = false;
                let mut row_failed = false;
                let mut init_failed = false;
                for (kind, r) in target
                    .kinds
                    .iter()
                    .copied()
                    .zip(dispatcher.dispatch_target(&target).await)
                {
                    match r {
                        Ok(Some(_)) => row_dispatched = true,
                        // Ok(None): the dispatcher skipped this kind (e.g.
                        // text content empty after truncation). Counted
                        // below as skipped only if NO kind dispatched and
                        // NO kind errored.
                        Ok(None) => {}
                        Err(e) => {
                            failed += 1;
                            row_failed = true;
                            tracing::error!(
                                memory_id,
                                ?kind,
                                "redispatch_embeddings: dispatch failed: {e}"
                            );
                            // Init failure is global (lazy jobworkerp
                            // client unbuildable); aborting keeps the
                            // failed counter honest instead of hammering
                            // every remaining row with the same error.
                            if matches!(e, DispatchError::Init(_)) {
                                init_failed = true;
                                break;
                            }
                        }
                    }
                }
                if row_dispatched {
                    dispatched += 1;
                } else if !row_failed {
                    // No kind enqueued and no error: treat as skipped
                    // (e.g. every kind returned Ok(None)). A row that
                    // errored is already counted in `failed`, not here.
                    skipped += 1;
                }
                if init_failed {
                    aborted_due_to_init = true;
                    break 'outer;
                }
            }

            // Advance the cursor to the largest id in this page (keyset
            // pagination). The result set is ordered by id ASC so the last
            // element carries the maximum id.
            if let Some(last) = memories.last().and_then(|m| m.id.as_ref()) {
                cursor = last.value;
            } else {
                break;
            }
        }

        if aborted_due_to_init {
            tracing::warn!(
                "redispatch_embeddings: aborted after init failure (dispatched={dispatched}, skipped={skipped}, failed={failed})"
            );
        }
        Ok((
            dispatched,
            skipped,
            failed,
            start.elapsed().as_millis() as i64,
        ))
    }

    // ===== Index management =====

    /// Clean up orphaned LanceDB records (present in LanceDB but not in RDB).
    /// Loads all LanceDB memory_ids into memory (O(N) where N = total vector records).
    /// For millions of records, consider chunked streaming in the future.
    pub async fn rebuild_index(&self) -> Result<(u32, u32, i64)> {
        let start = std::time::Instant::now();

        let lance_ids = self.vector_repo.get_all_memory_ids().await?;
        if lance_ids.is_empty() {
            return Ok((0, 0, start.elapsed().as_millis() as i64));
        }

        let rdb_memories = self.memory_repo.find_by_ids(&lance_ids, false).await?;
        let rdb_id_set: std::collections::HashSet<i64> = rdb_memories
            .iter()
            .filter_map(|m| m.id.as_ref().map(|id| id.value))
            .collect();

        let mut cleaned = 0u32;
        for &lance_id in &lance_ids {
            if !rdb_id_set.contains(&lance_id) {
                if let Err(e) = self.vector_repo.delete(lance_id).await {
                    tracing::error!("Failed to clean orphan memory_id={}: {e}", lance_id);
                }
                cleaned += 1;
            }
        }

        let indexed = lance_ids.len() as u32 - cleaned;
        let duration_ms = start.elapsed().as_millis() as i64;
        tracing::info!(
            "RebuildIndex: {} records intact, {} orphans cleaned, {}ms",
            indexed,
            cleaned,
            duration_ms
        );
        Ok((indexed, cleaned, duration_ms))
    }

    pub async fn get_stats(&self) -> Result<IndexStats> {
        self.vector_repo.get_stats().await
    }

    /// Delete vector entry for a memory (called on Memory delete)
    pub async fn delete_vector(&self, memory_id: i64) -> Result<()> {
        self.vector_repo.delete(memory_id).await.map(|_| ())
    }

    /// Delete vector entries for specific memory IDs (shared-memory-aware cascade)
    pub async fn delete_vectors_by_memory_ids(&self, memory_ids: &[i64]) -> Result<()> {
        self.vector_repo.delete_by_memory_ids(memory_ids).await
    }

    /// Drop only this memory's `image` / `caption` vector rows, leaving
    /// `text` intact. Called by the gRPC `update` handler when an Update
    /// detached an image (image → no-media, or image → a non-image
    /// media): the text-only embedding re-dispatch cannot evict these
    /// rows, so without this cascade they linger and keep producing stale
    /// `SearchByMedia` / `SearchSemantic` hits (review P2 #1). Empty
    /// `records` makes `replace_kinds_upsert` a pure delete of the listed
    /// kinds — the same primitive the image workflow uses with
    /// `replace_kinds=["image","caption"]`.
    pub async fn clear_image_vectors(&self, memory_id: i64) -> Result<()> {
        self.vector_repo
            .replace_kinds_upsert(memory_id, &["image", "caption"], Vec::new())
            .await
            .map(|_| ())
    }

    // ===== Internal helpers =====

    /// Search with staged over-fetch + hard cap.
    ///
    /// Image memory Phase 4 (design 3/3 §13.3.1, spec §3.5.3 / §4.4.6,
    /// REQUIRED behaviour): with the N-row schema one memory owns many
    /// chunk rows, so a *fixed-ratio* over-fetch can be wholly consumed
    /// by a few long memories and fall short of `limit` after the
    /// memory_id de-dup inside `enrich_hits`. This mirrors the FTS
    /// approximate-mode fix: reuse the same `approximate_fetch_limit`
    /// pure fn to grow the fetch per attempt (K, 2K, 3K…) and stop at
    /// `vector_dedup_max_fetch`. Hitting the cap with fewer than `limit`
    /// de-dup'd memories is NORMAL (safety > completeness) — a shortfall
    /// `warn` is emitted, same policy as `two_phase_fts_search`. The
    /// retry condition is evaluated AFTER de-dup + RDB hydration so a
    /// long memory's chunks crowding the window triggers a widen, not a
    /// silent short result.
    async fn search_with_overfetch<F, Fut>(
        &self,
        limit: usize,
        include_content: bool,
        // `None` keeps highlights empty (vector-only paths); `Some` makes
        // `enrich_hits` re-tokenize the content for the BM25 / hybrid paths.
        query_text: Option<&str>,
        query_origin: QueryOrigin,
        search_fn: F,
    ) -> Result<Vec<MemorySearchResultItem>>
    where
        F: Fn(usize) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Vec<VectorSearchHit>>>,
    {
        let cfg = &self.thread_filter_config;
        let mut results: Vec<MemorySearchResultItem> = Vec::new();
        let mut last_fetch = 0usize;
        for attempt in 1..=3usize {
            let fetch_limit = approximate_fetch_limit(
                limit,
                cfg.vector_dedup_overfetch_k,
                attempt,
                cfg.vector_dedup_max_fetch,
            );
            // Stop early once a wider attempt cannot pull more rows than
            // the previous pass (cap pinned) — re-running de-dup + RDB
            // hydration on the identical hit set is wasted work.
            if attempt > 1 && fetch_limit <= last_fetch {
                break;
            }
            last_fetch = fetch_limit;
            let hits = search_fn(fetch_limit).await?;
            results = self
                .enrich_hits(hits, include_content, query_text, query_origin)
                .await?;
            if results.len() >= limit {
                break;
            }
        }
        if results.len() < limit {
            tracing::warn!(
                "vector de-dup short of limit: requested={} unique={} \
                 max_fetch={} (N-row schema: a few long memories may \
                 dominate the ANN window; safety > completeness)",
                limit,
                results.len(),
                cfg.vector_dedup_max_fetch,
            );
        }
        results.truncate(limit);
        Ok(results)
    }

    /// Approximate-mode FTS pipeline (spec §P5「FTS 経路」近似モード):
    /// `search_fn` is invoked WITHOUT the thread_filter so LanceDB never
    /// sees a giant `IN (...)`; the returned candidates are intersected
    /// against `allowed` in-process. The fetch size grows linearly with
    /// the retry counter (`limit * K`, `limit * 2K`, `limit * 3K`) to
    /// match the spec's "最大 3 回" guarantee. If `limit` is still
    /// unmet after the third pass we surface a warn but return whatever
    /// matched — silent precision loss is the documented trade-off the
    /// caller opted into via `MEMORY_THREAD_FILTER_FTS_APPROXIMATE_MODE`.
    // Image memory Phase 4 added `query_origin` (score_source boundary,
    // design 3/3 §13.3); the pipeline genuinely needs all of these
    // inputs, same as the sibling `hybrid_search` which also opts out.
    #[allow(clippy::too_many_arguments)]
    async fn two_phase_fts_search<F, Fut>(
        &self,
        limit: usize,
        include_content: bool,
        base: Option<SafeFilter>,
        allowed: Vec<i64>,
        // Always `Some` in practice (this pipeline only runs for
        // SearchKind::Text / Hybrid), but kept `Option<&str>` to
        // share the signature shape with `search_with_overfetch`.
        query_text: Option<&str>,
        query_origin: QueryOrigin,
        search_fn: F,
    ) -> Result<Vec<MemorySearchResultItem>>
    where
        // Take `base` by value into the future so the closure does not
        // borrow `&self`-derived state — that lets us avoid an HRTB on
        // the closure (which doesn't compose with stable Rust as of 2026
        // for closures returning futures).
        F: Fn(Option<SafeFilter>, usize) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Vec<VectorSearchHit>>>,
    {
        let cfg = &self.thread_filter_config;
        let allowed_set: HashSet<i64> = allowed.iter().copied().collect();
        // Spec contract: at most three attempts with K, 2K, 3K. The
        // retry condition is evaluated *after* RDB hydration so that
        // stale LanceDB rows (embedding present, RDB row missing —
        // dropped by `enrich_hits`) cause us to widen and retry rather
        // than silently return a short result.
        let mut results: Vec<MemorySearchResultItem> = Vec::new();
        let mut last_fetch = 0usize;
        for attempt in 1..=3usize {
            let fetch_limit =
                approximate_fetch_limit(limit, cfg.fts_overfetch_k, attempt, cfg.fts_max_total_ids);
            // Skip retry if it wouldn't actually fetch more rows than
            // the previous pass (e.g. attempt 2 capped at the same
            // `fts_max_total_ids` as attempt 1). Without this guard
            // we'd burn an RDB hydration round on identical hits.
            if attempt > 1 && fetch_limit <= last_fetch {
                break;
            }
            last_fetch = fetch_limit;
            let hits = search_fn(base.clone(), fetch_limit).await?;
            let filtered: Vec<VectorSearchHit> = hits
                .into_iter()
                .filter(|h| allowed_set.contains(&h.memory_id))
                .collect();
            results = self
                .enrich_hits(filtered, include_content, query_text, query_origin)
                .await?;
            if results.len() >= limit {
                break;
            }
        }
        if results.len() < limit {
            tracing::warn!(
                "FTS approximate-mode pipeline returned {} matches after over-fetching up to {} candidates \
                 (requested limit = {}, allowed set size = {}, fts_max_total_ids = {}); \
                 consider tightening the thread_filter",
                results.len(),
                last_fetch,
                limit,
                allowed_set.len(),
                cfg.fts_max_total_ids,
            );
        }
        results.truncate(limit);
        Ok(results)
    }

    /// Hydrate search hits with full Memory data from RDB, attach P1
    /// representative-thread metadata, and (when `query_text` is set)
    /// compute P3 highlight ranges over `memory.data.content`. The 3
    /// IN-bulk SQLs guarantee N+1 is not introduced regardless of hit
    /// count, and highlights are calculated in-process from the same
    /// hydrated content so no extra RDB / LanceDB round-trip is needed.
    ///
    /// `query_text` semantics:
    ///   - `None`  → highlights stay empty for every hit. Used by
    ///     `search_by_vector` (vector-only path with no query string).
    ///   - `Some(q)` → highlights are computed against `q` using the
    ///     server-side BM25 tokenizer (`MemoryVectorRepositoryImpl::fts_config()`).
    ///     When `include_content == false` the call is a no-op because
    ///     offsets are meaningless without the surrounding text.
    async fn enrich_hits(
        &self,
        hits: Vec<VectorSearchHit>,
        include_content: bool,
        query_text: Option<&str>,
        query_origin: QueryOrigin,
    ) -> Result<Vec<MemorySearchResultItem>> {
        if hits.is_empty() {
            return Ok(Vec::new());
        }
        // Image memory Phase 4: a memory now owns N vector rows, so the
        // ANN result can list the same memory_id multiple times. Collapse
        // to one hit per memory (max score, winner's matched-row kept)
        // BEFORE RDB hydration so we neither double-hydrate nor emit
        // duplicate result rows (design 3/3 §13.3). Pure helper, unit
        // tested separately.
        let hits = dedup_vector_hits(hits);
        let ids: Vec<i64> = hits.iter().map(|h| h.memory_id).collect();
        let memories = self.memory_repo.find_by_ids(&ids, true).await?;
        let memory_map: HashMap<i64, Memory> = memories
            .into_iter()
            .filter_map(|m| {
                let id_val = m.id.as_ref()?.value;
                Some((id_val, m))
            })
            .collect();

        let (positions_map, summaries, totals) = fetch_representative_thread_maps(
            &self.thread_repo,
            &self.thread_memory_repo,
            ids.iter().filter_map(|id| memory_map.get(id)),
        )
        .await?;

        // One fts_config snapshot for the whole batch — the tokenizer
        // kind and ngram knobs do not change per-hit. `include_content
        // == false` strips the content body, so highlight offsets into
        // a stripped payload would mislead the client; we surface
        // empty highlights instead.
        let fts_config_for_highlights = query_text
            .filter(|q| !q.is_empty() && include_content)
            .map(|q| (q, self.vector_repo.fts_config()));

        let mut results = Vec::new();
        for hit in hits {
            if let Some(mut memory) = memory_map.get(&hit.memory_id).cloned() {
                // Compute highlights before stripping the body — otherwise
                // the offsets would point into a payload the client never
                // receives. The `include_content` guard on
                // `fts_config_for_highlights` already short-circuits this
                // branch when the body will be cleared, but doing the
                // computation first makes the ordering invariant explicit.
                let info = resolve_representative_thread_info(
                    hit.memory_id,
                    &positions_map,
                    &summaries,
                    &totals,
                );
                let highlights = compute_memory_highlights(&memory, fts_config_for_highlights);
                if !include_content && let Some(ref mut data) = memory.data {
                    data.content.clear();
                }
                let score_source = query_origin.score_source(hit.vector_kind.as_deref());
                results.push(MemorySearchResultItem {
                    memory,
                    score: hit.score,
                    distance: hit.distance,
                    position: info.position,
                    thread_total: info.thread_total,
                    thread_id: info.thread_id,
                    thread_owner_user_id: info.thread_owner_user_id,
                    thread_description: info.thread_description,
                    highlights,
                    matched_vector_kind: hit.vector_kind,
                    matched_begin_position: hit.begin_position,
                    matched_end_position: hit.end_position,
                    matched_content: hit.matched_content,
                    score_source,
                });
            } else {
                tracing::warn!(
                    "LanceDB hit memory_id={} not found in RDB (stale index)",
                    hit.memory_id
                );
            }
        }
        // Same two-stage media model as the Find path: fill the cacheable
        // half here; the gRPC layer issues the short-lived presigned URL
        // per response. No-op when the media subsystem is not wired or a
        // hit has no linked media. A real error (DBError) fails the whole
        // search rather than silently returning media-less results.
        for item in results.iter_mut() {
            super::memory::hydrate_media_metadata(self.media_subsystem(), &mut item.memory).await?;
        }
        Ok(results)
    }
}

/// Bulk-fetch the three SQL maps needed to populate
/// [`RepresentativeThreadInfo`] for a batch of memories. ROLE_SYSTEM
/// memories are filtered out before any SQL runs — proto contract:
/// their representative-thread fields are ALWAYS unset to avoid routing
/// a cross-user system memory to a thread owned by someone else.
///
/// Empty input (or "all ROLE_SYSTEM") short-circuits to three empty
/// maps without touching the DB.
pub(crate) async fn fetch_representative_thread_maps<'a>(
    thread_repo: &impl ThreadRepository,
    thread_memory_repo: &impl ThreadMemoryRepository,
    memories: impl IntoIterator<Item = &'a Memory>,
) -> Result<(
    HashMap<i64, (i64, i32)>,
    HashMap<i64, infra::infra::thread::rdb::ThreadSummary>,
    HashMap<i64, i32>,
)> {
    let non_system_ids: Vec<i64> = memories
        .into_iter()
        .filter_map(|m| {
            let id = m.id.as_ref()?.value;
            let data = m.data.as_ref()?;
            (data.role != MessageRole::RoleSystem as i32).then_some(id)
        })
        .collect();

    if non_system_ids.is_empty() {
        return Ok((HashMap::new(), HashMap::new(), HashMap::new()));
    }

    let positions_rows = thread_memory_repo
        .find_positions_for_memories(&non_system_ids)
        .await?;
    let rep_thread_ids: Vec<i64> = positions_rows
        .iter()
        .map(|(_mid, tid, _pos)| *tid)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    // Concurrent: wall-clock cost is the slower of the two rather than
    // the sum. Cannot fan out with `find_positions_for_memories` because
    // both depend on rep_thread_ids derived from its result.
    let (summaries, totals) = tokio::try_join!(
        thread_repo.find_thread_summaries(&rep_thread_ids),
        thread_memory_repo.count_by_thread_ids(&rep_thread_ids),
    )?;
    let positions_map: HashMap<i64, (i64, i32)> = positions_rows
        .into_iter()
        .map(|(mid, tid, pos)| (mid, (tid, pos)))
        .collect();
    Ok((positions_map, summaries, totals))
}

/// Attach representative-thread metadata to each memory in `memories`.
/// Used by the filter-only list path
/// (`MemoryService.FindListByCondition`) so its response shape matches
/// the search-hit path's `MemorySearchResult`. Input order is preserved.
pub(crate) async fn enrich_memories_with_thread_info(
    thread_repo: &impl ThreadRepository,
    thread_memory_repo: &impl ThreadMemoryRepository,
    memories: Vec<Memory>,
) -> Result<Vec<(Memory, RepresentativeThreadInfo)>> {
    if memories.is_empty() {
        return Ok(Vec::new());
    }
    let (positions_map, summaries, totals) =
        fetch_representative_thread_maps(thread_repo, thread_memory_repo, memories.iter()).await?;
    Ok(memories
        .into_iter()
        .map(|m| {
            // Memory without an id should be impossible post-fetch; fall back
            // to UNSET rather than substituting a sentinel that could collide
            // with a real thread row.
            let info = match m.id.as_ref() {
                Some(id) => resolve_representative_thread_info(
                    id.value,
                    &positions_map,
                    &summaries,
                    &totals,
                ),
                None => RepresentativeThreadInfo::UNSET,
            };
            (m, info)
        })
        .collect())
}

/// "Representative thread" = the thread with the smallest `thread_id`
/// the memory is attached to (see `find_positions_for_memories`).
/// All-`None` is a contract sentinel for ROLE_SYSTEM and orphan rows;
/// the all-or-none invariant is enforced by
/// [`resolve_representative_thread_info`].
pub struct RepresentativeThreadInfo {
    pub position: Option<i32>,
    pub thread_total: Option<i32>,
    pub thread_id: Option<ThreadId>,
    pub thread_owner_user_id: Option<UserId>,
    pub thread_description: Option<String>,
}

impl RepresentativeThreadInfo {
    pub(crate) const UNSET: Self = Self {
        position: None,
        thread_total: None,
        thread_id: None,
        thread_owner_user_id: None,
        thread_description: None,
    };
}

/// Wrap `infra::memory_vector::highlight::compute_highlight_field`
/// with the `Memory.data.content` accessor so the per-hit loop in
/// `enrich_hits` stays a one-liner.
///
/// Suppresses highlights for non-text rows so the wire contract in
/// `memory_vector.proto:104` ("non-text content type ⇒ empty
/// highlights") is honoured even for rows that bypassed
/// `EmbeddingJobDispatcher::is_embeddable`. Two paths still seed
/// non-text rows into the index: (1) legacy rows from before the
/// embeddable filter was tightened, and (2) direct `UpsertEmbedding`
/// RPC calls. `MemoryVectorRecord::from_memory_data` copies
/// `data.content` verbatim regardless of `content_type`, so without
/// this gate a tool-output / URL row that wins a BM25 hit would
/// produce client-visible ranges into payload the proto says clients
/// must not receive. We fail closed: anything that is not exactly
/// `ContentType::Text` (including out-of-range values from a future
/// proto extension or a malformed import) is treated as non-text.
pub(crate) fn compute_memory_highlights(
    memory: &Memory,
    query_and_cfg: Option<(&str, &infra::infra::memory_vector::config::FtsConfig)>,
) -> Vec<protobuf::llm_memory::data::HighlightField> {
    use infra::infra::memory_vector::highlight::compute_highlight_field;
    use protobuf::llm_memory::data::{ContentType, HighlightSource};

    let Some(data) = memory.data.as_ref() else {
        return Vec::new();
    };
    if !matches!(
        ContentType::try_from(data.content_type),
        Ok(ContentType::Text)
    ) {
        return Vec::new();
    }
    compute_highlight_field(
        Some(data.content.as_str()),
        query_and_cfg,
        HighlightSource::Content,
    )
}

/// Project the three SQL maps into a single per-memory
/// `RepresentativeThreadInfo` value. Keeping the orphan-thread gate in
/// one place (rather than inlined in `enrich_hits` /
/// `enrich_memories_with_thread_info`) makes the half-state impossible
/// to introduce by accident — every code path here either fully
/// populates or fully returns `UNSET`.
///
/// The three input maps come from three independent SQLs run without a
/// single repeatable-read snapshot (`find_positions_for_memories` +
/// `find_thread_summaries` + `count_by_thread_ids`). Concurrent writes
/// can therefore drop a row from one map while leaving the others
/// populated. Two race patterns to guard:
///
/// 1. **Thread row deleted** (`thread` row gone, `thread_memory`
///    survives): `summaries.get(rep_tid) == None`. Treated as orphan.
/// 2. **Last attachment detached** (`thread_memory.count == 0`,
///    `thread` row still alive): `totals.get(rep_tid) == None` because
///    `count_by_thread_ids` GROUP BYs and emits no row for empty
///    threads. We treat this as orphan too — surfacing `position`
///    snapshotted from the earlier query while `thread_total` is unset
///    would render `[N/M]` with a missing M, violating the all-or-none
///    representative-thread contract.
pub(crate) fn resolve_representative_thread_info(
    memory_id: i64,
    positions_map: &HashMap<i64, (i64, i32)>,
    summaries: &HashMap<i64, infra::infra::thread::rdb::ThreadSummary>,
    totals: &HashMap<i64, i32>,
) -> RepresentativeThreadInfo {
    let Some(&(rep_tid, pos)) = positions_map.get(&memory_id) else {
        return RepresentativeThreadInfo::UNSET;
    };
    let Some(summary) = summaries.get(&rep_tid) else {
        tracing::warn!(
            "representative-thread enrich: orphan thread detected (thread row missing) — \
             memory_id={memory_id}, rep_thread_id={rep_tid} (cascade leak?)"
        );
        return RepresentativeThreadInfo::UNSET;
    };
    let Some(total) = totals.get(&rep_tid).copied() else {
        // Race: the representative thread lost its last `thread_memory`
        // row between `find_positions_for_memories` and
        // `count_by_thread_ids`. The `thread` row may still be alive,
        // but reporting `position` without a matching `thread_total`
        // would leave the client with an inconsistent `[N/M]` header.
        tracing::warn!(
            "representative-thread enrich: orphan thread detected (no thread_memory rows) — \
             memory_id={memory_id}, rep_thread_id={rep_tid} (concurrent detach?)"
        );
        return RepresentativeThreadInfo::UNSET;
    };
    RepresentativeThreadInfo {
        position: Some(pos),
        thread_total: Some(total),
        thread_id: Some(ThreadId { value: rep_tid }),
        thread_owner_user_id: Some(UserId {
            value: summary.user_id,
        }),
        thread_description: summary.description.clone(),
    }
}

/// Compute the per-attempt fetch_limit for the FTS approximate-mode
/// pipeline. The output is clamped so that:
/// 1. it never exceeds `fts_max_total_ids` (operator-set absolute upper
///    bound — protecting LanceDB against runaway over-fetch);
/// 2. it asks for at least `limit + 1` rows when that lower bound fits
///    under (1), so the pass is a real over-fetch and not a re-run of
///    the strict path;
/// 3. when `limit + 1 > fts_max_total_ids` the absolute upper bound
///    wins (safety > completeness — the trailing warn flags the
///    shortfall to the caller).
///
/// Pure function so it can be unit-tested without touching LanceDB.
pub(crate) fn approximate_fetch_limit(
    limit: usize,
    fts_overfetch_k: usize,
    attempt: usize,
    fts_max_total_ids: usize,
) -> usize {
    let k0 = fts_overfetch_k.max(1);
    let raw_fetch = limit.saturating_mul(k0).saturating_mul(attempt);
    let lower_bound = (limit + 1).min(fts_max_total_ids);
    raw_fetch.min(fts_max_total_ids).max(lower_bound)
}

/// Free-function form of `MemoryVectorAppImpl::build_search_filter` so
/// that ceiling logic can be unit-tested without spinning up a real
/// MemoryVectorAppImpl (which would require LanceDB + RDB).
pub(crate) fn build_search_filter_with_cfg(
    cfg: &ThreadFilterConfig,
    base: Option<&SafeFilter>,
    thread_filter_ids: Option<Vec<i64>>,
    kind: SearchKind,
) -> Result<FilterPlan> {
    match thread_filter_ids {
        // No thread_filter at all — pass through untouched.
        None => Ok(FilterPlan::Strict(base.cloned())),
        Some(ids) if ids.is_empty() => Ok(FilterPlan::ShortCircuit),
        Some(ids) => {
            // The route-agnostic cap on `ids.len()` is enforced upstream
            // in `resolve_memory_ids_from_thread_filter` against
            // `fts_max_total_ids`, so by the time we get here both ANN
            // and FTS paths see ≤ `fts_max_total_ids` memory_ids. The
            // ceilings below add the FTS-specific layer: `MAX_INLINE_IDS`
            // separates "fits inline" from "needs approximate-mode opt-in",
            // and the redundant `MAX_TOTAL_IDS` re-check is kept as a
            // belt-and-braces guard in case a future caller bypasses
            // `resolve_memory_ids_from_thread_filter`.
            if kind.uses_fts() {
                // MAX_TOTAL_IDS is the absolute upper bound — even
                // approximate mode must not exceed it (runaway guard).
                if ids.len() > cfg.fts_max_total_ids {
                    return Err(LlmMemoryError::FailedPrecondition(format!(
                        "thread_filter resolved to {} memory_ids, exceeding the absolute upper bound \
                         MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS = {}.",
                        ids.len(),
                        cfg.fts_max_total_ids,
                    ))
                    .into());
                }
                if ids.len() > cfg.fts_max_inline_ids {
                    if !cfg.fts_approximate_mode {
                        return Err(LlmMemoryError::FailedPrecondition(format!(
                            "thread_filter resolved to {} memory_ids, exceeding \
                             MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS = {}. Add labels / channel filters \
                             to narrow the thread set, or set MEMORY_THREAD_FILTER_FTS_APPROXIMATE_MODE=true \
                             to opt into the search-side over-fetch fallback.",
                            ids.len(),
                            cfg.fts_max_inline_ids,
                        ))
                        .into());
                    }
                    // Approximate mode: caller takes the two-phase
                    // over-fetch + INTERSECT path. Don't build an IN
                    // filter — that would defeat the whole point.
                    return Ok(FilterPlan::Approximate {
                        base: base.cloned(),
                        allowed: ids,
                    });
                }
            }
            if kind.uses_ann() && ids.len() > cfg.prefilter_threshold {
                // ANN performance hint: the IN list is large enough that
                // LanceDB's prefilter planner may slow down. Flipping
                // to postfilter() is a separate PR.
                tracing::warn!(
                    "thread_filter IN list size ({}) exceeds prefilter threshold ({}); ANN performance may degrade",
                    ids.len(),
                    cfg.prefilter_threshold,
                );
            }
            let in_filter = SafeFilter::in_i64_list("memory_id", &ids)?;
            let combined = match base.cloned() {
                Some(b) => b.and(in_filter),
                None => in_filter,
            };
            Ok(FilterPlan::Strict(Some(combined)))
        }
    }
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

/// Result for GetSurroundingMemories
#[derive(Debug)]
pub struct SurroundingMemoriesResult {
    pub target: Memory,
    pub before: Vec<Memory>,
    pub after: Vec<Memory>,
    pub has_more_before: bool,
    pub has_more_after: bool,
}

#[cfg(test)]
mod test {
    use super::*;
    use infra::infra::UseIdGenerator;
    use infra::infra::memory::rdb::MemoryRepository;
    use infra::infra::memory_vector::config::{DistanceType, VectorDBConfig};
    use infra::infra::memory_vector::repository::{
        AggregationStrategy, HybridOptions, HybridStrategy,
    };
    use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
    use protobuf::llm_memory::data::{MediaObjectId, MemoryData, UserId};
    use rand::Rng;

    fn random_embedding(dim: usize) -> Vec<f32> {
        let mut rng = rand::thread_rng();
        (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect()
    }

    fn similar_embedding(base: &[f32], noise: f32) -> Vec<f32> {
        let mut rng = rand::thread_rng();
        base.iter()
            .map(|&v| v + rng.gen_range(-noise..noise))
            .collect()
    }

    struct TestDb {
        path: String,
    }

    impl TestDb {
        fn config(dim: usize) -> (VectorDBConfig, Self) {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("/tmp/test_app_lancedb_{}", ts);
            let config = VectorDBConfig {
                uri: path.clone(),
                table_name: "test_memories".to_string(),
                vector_size: dim,
                distance_type: DistanceType::Cosine,
                optimize: infra::infra::memory_vector::config::OptimizeConfig {
                    prune_on_startup: false,
                    ..Default::default()
                },
                fts: infra::infra::memory_vector::config::FtsConfig::default(),
                vector_index: infra::infra::memory_vector::config::VectorIndexConfig::default(),
            };
            (config, Self { path })
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Create a memory in RDB and return its ID
    async fn create_test_memory(
        repo: &MemoryRepositoryImpl,
        pool: &'static infra_utils::infra::rdb::RdbPool,
        content: &str,
        user_id: i64,
    ) -> anyhow::Result<i64> {
        // RoleUser + ContentType::Text matches the only combination
        // accepted by `EmbeddingJobDispatcher::is_embeddable`. Earlier
        // fixtures passed role=0 (RoleUnspecified) which the dispatcher
        // now rejects as a proto-default sentinel, silently turning
        // every redispatch test into a no-op.
        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: user_id }),
            content: content.to_string(),
            content_type: protobuf::llm_memory::data::ContentType::Text as i32,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleUser as i32,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = pool.begin().await?;
        let id = repo.create(&mut *tx, &data).await?;
        tx.commit().await?;
        Ok(id.value)
    }

    /// Create a memory whose `media_object_id` points at a freshly
    /// inserted `kind=IMAGE`, `storage_backend=url` media_object (an
    /// embeddable image: `url` is neither `unresolvable` nor `inline`).
    /// Returns `(memory_id, media_object_id)`. Used by the
    /// `with_media_resolver` redispatch tests to prove the Media axis is
    /// only reachable when the resolver is wired.
    async fn create_test_memory_with_image(
        memory_repo: &MemoryRepositoryImpl,
        media_repo: &MediaObjectRepositoryImpl,
        pool: &'static infra_utils::infra::rdb::RdbPool,
        content: &str,
        user_id: i64,
    ) -> anyhow::Result<(i64, i64)> {
        let media_id = media_repo.id_generator().generate_id()?;
        let mut tx = pool.begin().await?;
        media_repo
            .insert_url_tx(
                &mut *tx,
                media_id,
                protobuf::llm_memory::data::ContentType::Image as i32,
                "image/png",
                None,
                None,
                None,
                "https://example.invalid/test-image.png",
                None,
            )
            .await?;
        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: user_id }),
            content: content.to_string(),
            content_type: protobuf::llm_memory::data::ContentType::Text as i32,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleUser as i32,
            external_id: None,
            media_object_id: Some(MediaObjectId { value: media_id }),
            thread_ids: Vec::new(),
        };
        let id = memory_repo.create(&mut *tx, &data).await?;
        tx.commit().await?;
        Ok((id.value, media_id))
    }

    async fn setup_app(
        dim: usize,
    ) -> anyhow::Result<(
        MemoryVectorAppImpl,
        &'static infra_utils::infra::rdb::RdbPool,
        TestDb,
    )> {
        let pool = if cfg!(feature = "postgres") {
            let pool = setup_test_rdb_from("../infra/sql/postgres").await;
            sqlx::query("TRUNCATE TABLE memory, thread, thread_memory, thread_label CASCADE;")
                .execute(pool)
                .await
                .unwrap();
            pool
        } else {
            let pool = setup_test_rdb_from("../infra/sql/sqlite").await;
            for tbl in ["thread_memory", "thread_label", "thread", "memory"] {
                sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {tbl}")))
                    .execute(pool)
                    .await
                    .unwrap();
            }
            pool
        };

        let id_gen = infra::test_helper::shared_id_generator();
        let memory_repo = MemoryRepositoryImpl::new(id_gen, pool);
        let thread_memory_repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_repo =
            ThreadRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let thread_label_repo = ThreadLabelRepositoryImpl::new(pool);
        let (vector_config, db) = TestDb::config(dim);
        let vector_repo = MemoryVectorRepositoryImpl::new(vector_config).await?;
        let app = MemoryVectorAppImpl::new(
            memory_repo,
            vector_repo,
            thread_memory_repo,
            thread_repo,
            thread_label_repo,
            None,
        );
        Ok((app, pool, db))
    }

    /// Like `setup_app` but with the media subsystem wired (`with_media`),
    /// so search results carry the cacheable half of `Memory.media`. The
    /// inline backend keeps the test self-contained (no minio/network).
    async fn setup_app_with_media(
        dim: usize,
    ) -> anyhow::Result<(
        MemoryVectorAppImpl,
        &'static infra_utils::infra::rdb::RdbPool,
        TestDb,
    )> {
        use infra::infra::media_storage::StorageBackend;
        let (app, pool, db) = setup_app(dim).await?;
        let media_repo =
            MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let storage = Arc::new(StorageBackend::Inline(
            infra::infra::media_storage::inline::InlineMediaStorage::new(),
        ));
        let media_app = Arc::new(crate::app::media::MediaAppImpl::new(
            MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool),
            storage,
            infra::test_helper::shared_id_generator(),
            "memories/".to_string(),
            900,
            20_971_520,
        ));
        let app = app.with_media(media_repo, media_app);
        Ok((app, pool, db))
    }

    /// Insert a memory whose `media_object_id` points at an
    /// `unresolvable` media_object (no body, gc_state=3, storage_uri
    /// NULL). Returns `(memory_id, media_object_id)`. Mirrors the
    /// migration-produced placeholder rows.
    async fn create_test_memory_with_unresolvable(
        memory_repo: &MemoryRepositoryImpl,
        media_repo: &MediaObjectRepositoryImpl,
        pool: &'static infra_utils::infra::rdb::RdbPool,
        content: &str,
        user_id: i64,
    ) -> anyhow::Result<(i64, i64)> {
        let media_id = media_repo.id_generator().generate_id()?;
        let mut tx = pool.begin().await?;
        media_repo
            .insert_unresolvable_tx(
                &mut *tx,
                media_id,
                protobuf::llm_memory::data::ContentType::Image as i32,
                "image/png",
                Some(123),
                &format!("sha_unresolvable_{media_id}"),
                None,
                None,
                None,
            )
            .await?;
        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: user_id }),
            content: content.to_string(),
            content_type: protobuf::llm_memory::data::ContentType::Text as i32,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: MessageRole::RoleUser as i32,
            external_id: None,
            media_object_id: Some(MediaObjectId { value: media_id }),
            thread_ids: Vec::new(),
        };
        let id = memory_repo.create(&mut *tx, &data).await?;
        tx.commit().await?;
        Ok((id.value, media_id))
    }

    #[test]
    fn test_search_by_vector_with_rdb_hydration() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;
            let base_emb = random_embedding(dim);

            // Create memories in RDB and upsert embeddings
            let id1 =
                create_test_memory(&app.memory_repo, pool, "machine learning basics", 10).await?;
            let id2 =
                create_test_memory(&app.memory_repo, pool, "deep learning advanced", 10).await?;

            let emb1 = similar_embedding(&base_emb, 0.01);
            let emb2 = random_embedding(dim);
            app.upsert_embedding(id1, &emb1, Some("test-model")).await?;
            app.upsert_embedding(id2, &emb2, Some("test-model")).await?;

            // Search with similar vector
            let results = app
                .search_by_vector(&[base_emb], None, None, None, 10, true, None)
                .await?;

            assert!(!results.is_empty());
            // id1 should rank first (closest to base_emb)
            assert_eq!(
                results[0].memory.id.as_ref().unwrap().value,
                id1,
                "Closest embedding should rank first"
            );
            // Verify RDB hydration
            let data = results[0].memory.data.as_ref().unwrap();
            assert_eq!(data.content, "machine learning basics");
            assert_eq!(data.user_id.as_ref().unwrap().value, 10);
            assert!(results[0].score > 0.0);
            Ok(())
        })
    }

    #[test]
    fn test_search_by_vector_multi_vector_aggregation() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;

            // Use a single base and derive two query vectors with controlled perturbation
            let base = random_embedding(dim);
            let query1 = similar_embedding(&base, 0.05);
            let query2 = similar_embedding(&base, 0.05);

            // id1: very close to base → should appear in both query results
            let id1 =
                create_test_memory(&app.memory_repo, pool, "topic A and B combined", 10).await?;
            app.upsert_embedding(id1, &similar_embedding(&base, 0.01), Some("test"))
                .await?;

            // id2: moderately close to base
            let id2 = create_test_memory(&app.memory_repo, pool, "topic A only", 10).await?;
            app.upsert_embedding(id2, &similar_embedding(&base, 0.3), Some("test"))
                .await?;

            // id3: further from base
            let id3 = create_test_memory(&app.memory_repo, pool, "topic B only", 10).await?;
            app.upsert_embedding(id3, &similar_embedding(&base, 0.4), Some("test"))
                .await?;

            // Multi-vector search with Sum strategy
            let results = app
                .search_by_vector(
                    &[query1, query2],
                    None,
                    None,
                    None,
                    3,
                    true,
                    Some(AggregationStrategy::Sum),
                )
                .await?;

            assert_eq!(results.len(), 3);
            // id1 (closest to base) should rank first with Sum aggregation
            assert_eq!(
                results[0].memory.id.as_ref().unwrap().value,
                id1,
                "Record closest to both query vectors should rank first"
            );
            // All results should have RDB-hydrated data and non-negative scores
            for r in &results {
                assert!(r.memory.data.is_some());
                assert!(r.score >= 0.0);
            }
            Ok(())
        })
    }

    #[test]
    fn test_search_by_text_with_rdb_hydration() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;

            let id1 = create_test_memory(
                &app.memory_repo,
                pool,
                "neural network training optimization",
                10,
            )
            .await?;
            let id2 =
                create_test_memory(&app.memory_repo, pool, "cooking recipes guide", 10).await?;

            app.upsert_embedding(id1, &random_embedding(dim), Some("test"))
                .await?;
            app.upsert_embedding(id2, &random_embedding(dim), Some("test"))
                .await?;

            let results = app
                .search_by_text("neural network", None, None, None, 10, true)
                .await?;

            assert!(!results.is_empty());
            assert_eq!(results[0].memory.id.as_ref().unwrap().value, id1);
            assert_eq!(
                results[0].memory.data.as_ref().unwrap().content,
                "neural network training optimization"
            );
            Ok(())
        })
    }

    /// Spec §P5「FTS 経路」近似モード: when approximate mode is on and
    /// `thread_filter` resolves to more than `FTS_MAX_INLINE_IDS`
    /// memory_ids, `search_by_text` must (a) NOT pass a giant IN list
    /// to LanceDB and (b) intersect the over-fetched FTS hits with the
    /// resolved memory_id set. This drives the public API end-to-end
    /// through the FilterPlan::Approximate dispatch.
    #[test]
    fn test_search_by_text_approximate_mode_end_to_end() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (mut app, pool, _db) = setup_app(dim).await?;
            // Force the planner into the approximate path with a tiny
            // inline ceiling: 2 resolved memory_ids exceed the cap of 1
            // ⇒ FilterPlan::Approximate is emitted by build_search_filter.
            app.thread_filter_config.fts_max_inline_ids = 1;
            app.thread_filter_config.fts_approximate_mode = true;

            // Two threads with label "match", one with "other". Each
            // thread owns a single memory matching the FTS query, so
            // BM25 alone would return all three. The thread_filter must
            // narrow the result to the two "match" memories.
            insert_thread_for_resolve_test(pool, 9_500_001, 10, None, &["match"], 1, 1).await?;
            insert_thread_for_resolve_test(pool, 9_500_002, 10, None, &["match"], 2, 2).await?;
            insert_thread_for_resolve_test(pool, 9_500_003, 10, None, &["other"], 3, 3).await?;
            let id_in_1 =
                create_test_memory(&app.memory_repo, pool, "neural network training", 10).await?;
            let id_in_2 =
                create_test_memory(&app.memory_repo, pool, "neural network research", 10).await?;
            let id_out =
                create_test_memory(&app.memory_repo, pool, "neural network deployment", 10).await?;
            for id in [id_in_1, id_in_2, id_out] {
                app.upsert_embedding(id, &random_embedding(dim), Some("test"))
                    .await?;
            }
            // Attach via the production thread_memory junction so the
            // resolve pipeline finds them through find_memory_ids_by_thread_ids.
            app.thread_memory_repo
                .insert_or_ignore_auto_position_tx(pool, 9_500_001, id_in_1, 1)
                .await?;
            app.thread_memory_repo
                .insert_or_ignore_auto_position_tx(pool, 9_500_002, id_in_2, 1)
                .await?;
            app.thread_memory_repo
                .insert_or_ignore_auto_position_tx(pool, 9_500_003, id_out, 1)
                .await?;

            let tf = ThreadSearchFilter {
                labels: vec!["match".into()],
                ..Default::default()
            };
            let results = app
                .search_by_text("neural network", None, Some(&tf), None, 10, true)
                .await?;

            assert!(
                !results.is_empty(),
                "approximate path must surface allowed hits"
            );
            // Every returned memory must belong to a "match"-labelled
            // thread. id_out belongs to "other" and must be filtered.
            for r in &results {
                let mid = r.memory.id.as_ref().unwrap().value;
                assert!(
                    mid == id_in_1 || mid == id_in_2,
                    "thread_filter must hold under the approximate path; leaked id={mid}"
                );
            }
            Ok(())
        })
    }

    /// Hydration-aware retry: the previous version of
    /// `two_phase_fts_search` broke out of the retry loop on
    /// `filtered.len() >= limit` (intersect length, before
    /// `enrich_hits`). When LanceDB had stale rows (embedding present,
    /// RDB row missing) the hydration step dropped them and we
    /// returned a result shorter than `limit` without giving the
    /// retry budget a chance to widen the fetch. This test wires up
    /// the stale scenario end-to-end and asserts (a) the response
    /// only contains live rows and (b) the response is the maximum
    /// achievable count given the fixture (= live row count).
    #[test]
    fn test_search_by_text_approximate_mode_drops_stale_after_hydration() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (mut app, pool, _db) = setup_app(dim).await?;
            // Force the planner into the approximate path. K=1 keeps
            // the per-attempt over-fetch small and predictable.
            app.thread_filter_config.fts_max_inline_ids = 1;
            app.thread_filter_config.fts_approximate_mode = true;
            app.thread_filter_config.fts_overfetch_k = 1;

            // Two threads with label "match"; we attach 3 memory_ids
            // to them (2 live, 1 stale). thread_memory has no FK to
            // memory in this schema, so a stale memory_id slips into
            // `allowed` via find_memory_ids_by_thread_ids.
            insert_thread_for_resolve_test(pool, 9_600_001, 10, None, &["match"], 1, 1).await?;
            insert_thread_for_resolve_test(pool, 9_600_002, 10, None, &["match"], 2, 2).await?;
            let id_live_1 =
                create_test_memory(&app.memory_repo, pool, "neural network alpha", 10).await?;
            let id_live_2 =
                create_test_memory(&app.memory_repo, pool, "neural network beta", 10).await?;
            // Stale: embedding exists in LanceDB but no RDB row.
            let id_stale = 9_999_999_999_i64;
            for id in [id_live_1, id_live_2] {
                app.upsert_embedding(id, &random_embedding(dim), Some("test"))
                    .await?;
            }
            let stale_record = infra::infra::memory_vector::record::MemoryVectorRecord {
                memory_id: id_stale,
                // Legacy single-row mapping: one (memory_id, "text", 0) row.
                vector_kind: "text".to_string(),
                chunk_index: 0,
                begin_position: 0,
                end_position: 0,
                user_id: 10,
                content: "neural network ghost".to_string(),
                content_type: 0,
                role: 0,
                embedding: random_embedding(dim),
                embedding_model: Some("test".to_string()),
                metadata_json: None,
                created_at: 1000,
                updated_at: 1000,
                indexed_at: command_utils::util::datetime::now_millis(),
            };
            app.vector_repo.upsert(&stale_record).await?;

            app.thread_memory_repo
                .insert_or_ignore_auto_position_tx(pool, 9_600_001, id_live_1, 1)
                .await?;
            app.thread_memory_repo
                .insert_or_ignore_auto_position_tx(pool, 9_600_001, id_stale, 2)
                .await?;
            app.thread_memory_repo
                .insert_or_ignore_auto_position_tx(pool, 9_600_002, id_live_2, 1)
                .await?;

            let tf = ThreadSearchFilter {
                labels: vec!["match".into()],
                ..Default::default()
            };
            // Request limit = 3. The fixture only has 2 live rows; the
            // approximate path should still retry up to attempt 3 to
            // try to satisfy `limit`, then return the 2 hydrated rows.
            let results = app
                .search_by_text("neural network", None, Some(&tf), None, 3, true)
                .await?;

            // Stale must be dropped and the result must be limited to
            // live rows. Length should be 2 (all live rows).
            assert_eq!(
                results.len(),
                2,
                "approximate path must hydrate before truncating; stale row must be filtered"
            );
            for r in &results {
                let mid = r.memory.id.as_ref().unwrap().value;
                assert!(
                    mid == id_live_1 || mid == id_live_2,
                    "stale id leaked into hydrated results: {mid}"
                );
            }
            Ok(())
        })
    }

    #[test]
    fn test_hybrid_search_rrf_with_rdb() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;
            let base_emb = random_embedding(dim);

            // id1: vector similar + keyword match
            let id1 = create_test_memory(
                &app.memory_repo,
                pool,
                "machine learning model training",
                10,
            )
            .await?;
            app.upsert_embedding(id1, &similar_embedding(&base_emb, 0.01), Some("test"))
                .await?;

            // id2: vector similar only
            let id2 =
                create_test_memory(&app.memory_repo, pool, "unrelated cooking topic", 10).await?;
            app.upsert_embedding(id2, &similar_embedding(&base_emb, 0.02), Some("test"))
                .await?;

            let options = HybridOptions {
                strategy: HybridStrategy::Rrf,
                vector_weight: None,
                rrf_k: Some(60.0),
            };
            let results = app
                .hybrid_search(
                    &[base_emb],
                    "machine learning",
                    None,
                    None,
                    None,
                    2,
                    &options,
                    true,
                )
                .await?;

            assert!(!results.is_empty());
            assert_eq!(
                results[0].memory.id.as_ref().unwrap().value,
                id1,
                "Record matching both vector and text should rank first"
            );
            assert!(results[0].memory.data.is_some());
            Ok(())
        })
    }

    #[test]
    fn test_hybrid_search_vector_then_fts_with_rdb() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;
            let base_emb = random_embedding(dim);

            let id1 = create_test_memory(
                &app.memory_repo,
                pool,
                "deep learning optimization techniques",
                10,
            )
            .await?;
            app.upsert_embedding(id1, &similar_embedding(&base_emb, 0.01), Some("test"))
                .await?;

            let id2 = create_test_memory(
                &app.memory_repo,
                pool,
                "completely different gardening topic",
                10,
            )
            .await?;
            app.upsert_embedding(id2, &similar_embedding(&base_emb, 0.02), Some("test"))
                .await?;

            let options = HybridOptions {
                strategy: HybridStrategy::VectorThenFts,
                vector_weight: Some(0.7),
                rrf_k: None,
            };
            let results = app
                .hybrid_search(
                    &[base_emb],
                    "deep learning",
                    None,
                    None,
                    None,
                    2,
                    &options,
                    true,
                )
                .await?;

            assert!(!results.is_empty());
            assert_eq!(results[0].memory.id.as_ref().unwrap().value, id1);
            // Verify full RDB hydration
            let data = results[0].memory.data.as_ref().unwrap();
            assert!(!data.content.is_empty());
            Ok(())
        })
    }

    #[test]
    fn test_search_exclude_content() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;

            let id1 = create_test_memory(&app.memory_repo, pool, "some content here", 10).await?;
            app.upsert_embedding(id1, &random_embedding(dim), Some("test"))
                .await?;

            // include_content = false
            let results = app
                .search_by_vector(&[random_embedding(dim)], None, None, None, 10, false, None)
                .await?;

            assert!(!results.is_empty());
            // Content should be cleared
            let data = results[0].memory.data.as_ref().unwrap();
            assert!(
                data.content.is_empty(),
                "Content should be empty when include_content=false"
            );
            Ok(())
        })
    }

    #[test]
    fn test_enrich_hits_filters_stale_records() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;

            // Create 2 memories in RDB
            let id1 = create_test_memory(&app.memory_repo, pool, "valid memory one", 10).await?;
            let id2 = create_test_memory(&app.memory_repo, pool, "valid memory two", 10).await?;

            let base_emb = random_embedding(dim);
            app.upsert_embedding(id1, &similar_embedding(&base_emb, 0.01), Some("test"))
                .await?;
            app.upsert_embedding(id2, &similar_embedding(&base_emb, 0.02), Some("test"))
                .await?;

            // Insert a stale record directly into LanceDB (no corresponding RDB entry)
            let stale_id = 999_999_999;
            let stale_record = infra::infra::memory_vector::record::MemoryVectorRecord {
                memory_id: stale_id,
                // Legacy single-row mapping: one (memory_id, "text", 0) row.
                vector_kind: "text".to_string(),
                chunk_index: 0,
                begin_position: 0,
                end_position: 0,
                user_id: 10,
                content: "stale ghost record".to_string(),
                content_type: 0,
                role: 0,
                embedding: similar_embedding(&base_emb, 0.005),
                embedding_model: Some("test".to_string()),
                metadata_json: None,
                created_at: 1000,
                updated_at: 1000,
                indexed_at: command_utils::util::datetime::now_millis(),
            };
            app.vector_repo.upsert(&stale_record).await?;

            // Search: LanceDB returns 3 hits, but RDB only has 2
            let results = app
                .search_by_vector(&[base_emb], None, None, None, 10, true, None)
                .await?;

            // Stale record should be filtered out
            assert_eq!(
                results.len(),
                2,
                "Stale record should be filtered by enrich_hits"
            );
            let ids: Vec<i64> = results
                .iter()
                .map(|r| r.memory.id.as_ref().unwrap().value)
                .collect();
            assert!(ids.contains(&id1));
            assert!(ids.contains(&id2));
            assert!(!ids.contains(&stale_id));
            Ok(())
        })
    }

    #[test]
    fn test_upsert_and_search_roundtrip() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            let (app, pool, _db) = setup_app(dim).await?;

            let emb = random_embedding(dim);
            let id =
                create_test_memory(&app.memory_repo, pool, "roundtrip test content", 42).await?;
            app.upsert_embedding(id, &emb, Some("test-model")).await?;

            // Immediately search
            let results = app
                .search_by_vector(std::slice::from_ref(&emb), None, None, None, 1, true, None)
                .await?;

            assert_eq!(results.len(), 1);
            assert_eq!(results[0].memory.id.as_ref().unwrap().value, id);
            assert_eq!(
                results[0].memory.data.as_ref().unwrap().content,
                "roundtrip test content"
            );
            assert_eq!(
                results[0]
                    .memory
                    .data
                    .as_ref()
                    .unwrap()
                    .user_id
                    .as_ref()
                    .unwrap()
                    .value,
                42
            );
            Ok(())
        })
    }

    // ---- Phase 4 review: pivot membership check for get_surrounding_memories ----

    /// `resolve_single_thread_for_memory` is the Phase 4 wire-compat
    /// fallback for `GetSurroundingMemories`. It must:
    /// - return the unique thread_id when the memory is attached to
    ///   exactly one thread,
    /// - return InvalidArgument when the memory is shared across multiple
    ///   threads (ambiguous),
    /// - return NotFound when the memory is not attached to any thread.
    #[test]
    fn test_resolve_single_thread_for_memory_unique() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let thread_a = 9_700_001;
            let user_id = 120;
            let mem_id = create_test_memory(&app.memory_repo, pool, "only thread", user_id).await?;
            app.thread_memory_repo
                .insert_auto_position_tx(pool, thread_a, mem_id, 1000)
                .await?;

            let resolved = app.resolve_single_thread_for_memory(mem_id).await?;
            assert_eq!(resolved, thread_a);

            app.thread_memory_repo
                .delete_by_thread_tx(pool, thread_a)
                .await?;
            Ok(())
        })
    }

    #[test]
    fn test_resolve_single_thread_for_memory_ambiguous_rejected() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let thread_a = 9_700_010;
            let thread_b = 9_700_011;
            let user_id = 121;
            let shared_id =
                create_test_memory(&app.memory_repo, pool, "shared memory", user_id).await?;
            // Attach to two threads — e.g. a shared ROLE_SYSTEM prompt.
            app.thread_memory_repo
                .insert_auto_position_tx(pool, thread_a, shared_id, 1000)
                .await?;
            app.thread_memory_repo
                .insert_auto_position_tx(pool, thread_b, shared_id, 1000)
                .await?;

            let result = app.resolve_single_thread_for_memory(shared_id).await;
            assert!(result.is_err(), "ambiguous resolve must return Err");
            let err = format!("{:?}", result.unwrap_err());
            assert!(
                err.contains("attached to multiple threads"),
                "error must explain ambiguity: {err}"
            );

            // Cleanup
            app.thread_memory_repo
                .delete_by_thread_tx(pool, thread_a)
                .await?;
            app.thread_memory_repo
                .delete_by_thread_tx(pool, thread_b)
                .await?;
            Ok(())
        })
    }

    #[test]
    fn test_resolve_single_thread_for_memory_unattached_notfound() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let user_id = 122;
            // Create a memory but never attach it to any thread.
            let mem_id = create_test_memory(&app.memory_repo, pool, "unattached", user_id).await?;

            let result = app.resolve_single_thread_for_memory(mem_id).await;
            assert!(result.is_err(), "unattached resolve must return Err");
            let err = format!("{:?}", result.unwrap_err());
            assert!(
                err.contains("not attached to any thread"),
                "error must say unattached: {err}"
            );
            Ok(())
        })
    }

    /// A missing memory row must surface as `"memory not found"` rather
    /// than silently collapse into the `"not attached to any thread"`
    /// branch — the caller (an old client hitting the wire-compat shim)
    /// needs to tell apart "never created" from "exists but orphaned".
    #[test]
    fn test_resolve_single_thread_for_memory_missing_memory_notfound() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;
            // Snowflake-shaped id that will never be issued in this test run.
            let phantom_id: i64 = 9_999_999_999_999;

            let result = app.resolve_single_thread_for_memory(phantom_id).await;
            assert!(result.is_err(), "missing memory must return Err");
            let err = format!("{:?}", result.unwrap_err());
            assert!(
                err.contains("memory not found"),
                "error must say memory is missing: {err}"
            );
            assert!(
                !err.contains("not attached to any thread"),
                "missing memory must not be reported as unattached: {err}"
            );
            Ok(())
        })
    }

    /// `get_surrounding_memories` must refuse to return neighbours if the
    /// pivot memory does not belong to the requested thread. Previously
    /// `find_surrounding` would return empty before/after lists in this
    /// case and the RPC would still hand back a target row, which is
    /// silently wrong for the caller.
    #[test]
    fn test_get_surrounding_memories_rejects_wrong_thread() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let thread_a = 9_600_001;
            let thread_b = 9_600_002;
            let user_id = 97;

            // Create a memory and attach it ONLY to thread_a via the junction.
            let mem_id = create_test_memory(&app.memory_repo, pool, "in thread A", user_id).await?;
            app.thread_memory_repo
                .insert_auto_position_tx(pool, thread_a, mem_id, 1000)
                .await?;

            // Asking for surroundings under thread_a works (returns empty
            // neighbours because the thread has a single member, but the
            // call itself must not error).
            let ok = app.get_surrounding_memories(mem_id, thread_a, 3, 3).await?;
            assert_eq!(ok.target.id.as_ref().unwrap().value, mem_id);
            assert!(ok.before.is_empty());
            assert!(ok.after.is_empty());

            // Asking for surroundings under thread_b (where the memory is
            // NOT attached) must return NotFound instead of silently
            // handing back the target.
            let wrong = app.get_surrounding_memories(mem_id, thread_b, 3, 3).await;
            assert!(
                wrong.is_err(),
                "wrong-thread pivot must be rejected, got Ok"
            );
            let err_string = format!("{:?}", wrong.unwrap_err());
            assert!(
                err_string.contains("not attached"),
                "error must explain pivot is not attached: {err_string}"
            );

            // Cleanup
            app.thread_memory_repo
                .delete_by_thread_tx(pool, thread_a)
                .await?;
            Ok(())
        })
    }

    #[test]
    fn test_get_surrounding_memories_hydrates_thread_ids() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let thread_id = 9_600_011;
            let shared_thread_id = 9_600_012;
            let user_id = 98;

            let before_id = create_test_memory(&app.memory_repo, pool, "before", user_id).await?;
            let target_id = create_test_memory(&app.memory_repo, pool, "target", user_id).await?;
            let after_id = create_test_memory(&app.memory_repo, pool, "after", user_id).await?;

            attach_memory_to_thread(pool, thread_id, before_id, 0).await?;
            attach_memory_to_thread(pool, thread_id, target_id, 1).await?;
            attach_memory_to_thread(pool, thread_id, after_id, 2).await?;
            attach_memory_to_thread(pool, shared_thread_id, target_id, 0).await?;

            let result = app
                .get_surrounding_memories(target_id, thread_id, 1, 1)
                .await?;

            let before_thread_ids = result.before[0]
                .data
                .as_ref()
                .expect("before data")
                .thread_ids
                .iter()
                .map(|id| id.value)
                .collect::<Vec<_>>();
            let target_thread_ids = result
                .target
                .data
                .as_ref()
                .expect("target data")
                .thread_ids
                .iter()
                .map(|id| id.value)
                .collect::<Vec<_>>();
            let after_thread_ids = result.after[0]
                .data
                .as_ref()
                .expect("after data")
                .thread_ids
                .iter()
                .map(|id| id.value)
                .collect::<Vec<_>>();

            assert_eq!(before_thread_ids, vec![thread_id]);
            assert_eq!(target_thread_ids, vec![thread_id, shared_thread_id]);
            assert_eq!(after_thread_ids, vec![thread_id]);

            app.thread_memory_repo
                .delete_by_thread_tx(pool, thread_id)
                .await?;
            app.thread_memory_repo
                .delete_by_thread_tx(pool, shared_thread_id)
                .await?;
            Ok(())
        })
    }

    /// Review P2 #2: an empty `rows` argument with a non-empty
    /// `replace_kinds` is a valid "stale-delete only" request — the
    /// embedding runner can yield 0 chunks (short / empty caption) yet
    /// still has to drop the memory's existing rows of those kinds.
    /// `batch_upsert_embeddings_rows` must forward an empty record set to
    /// `replace_kinds_upsert`, which treats it as a delete-only; otherwise
    /// the old rows leak and keep producing stale search hits.
    #[test]
    fn test_batch_upsert_rows_empty_is_stale_delete() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let user_id = 15_240;
            let mid =
                create_test_memory(&app.memory_repo, pool, "stale-delete me", user_id).await?;

            // Seed two text rows for this memory via the rows path.
            let (s, f, errs) = app
                .batch_upsert_embeddings_rows(
                    mid,
                    Some("test-model"),
                    &["text".to_string()],
                    vec![
                        (
                            "text".to_string(),
                            0,
                            0,
                            5,
                            "chunk a".to_string(),
                            random_embedding(dim),
                        ),
                        (
                            "text".to_string(),
                            1,
                            5,
                            10,
                            "chunk b".to_string(),
                            random_embedding(dim),
                        ),
                    ],
                )
                .await?;
            assert_eq!((s, f, errs.len()), (2, 0, 0));
            assert_eq!(
                app.get_stats().await?.total_records,
                2,
                "two text rows must be present after the seed upsert"
            );

            // Empty rows + replace_kinds=["text"] → stale-delete only.
            // success_count=0 (nothing inserted) but the old rows are
            // gone, NOT a no-op that leaves them behind.
            let (s2, f2, errs2) = app
                .batch_upsert_embeddings_rows(
                    mid,
                    Some("test-model"),
                    &["text".to_string()],
                    Vec::new(),
                )
                .await?;
            assert_eq!(
                (s2, f2, errs2.len()),
                (0, 0, 0),
                "stale-delete reports success_count=0 (no inserts) and no errors"
            );
            assert_eq!(
                app.get_stats().await?.total_records,
                0,
                "empty rows with replace_kinds MUST delete the memory's \
                 existing text rows (not a no-op)"
            );
            Ok(())
        })
    }

    #[test]
    fn test_redispatch_embeddings_requires_dispatcher() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;
            let result = app.redispatch_embeddings(None, None, Some(10), &[]).await;
            assert!(result.is_err(), "missing dispatcher must fail");
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("dispatcher"),
                "error should mention dispatcher configuration: {err}"
            );
            Ok(())
        })
    }

    // --- redispatch_embeddings counter semantics -----------------------
    //
    // Tests below exercise the dispatched/skipped/failed accounting via
    // a stub `EmbeddingDispatch` impl. The real dispatcher requires a
    // live jobworkerp instance, so only integration tests (`#[ignore]`)
    // can cover the Init/Enqueue error paths end-to-end. The stub lets
    // us pin down the counter logic deterministically in CI.

    use infra::infra::memory_vector::dispatcher::{EmbeddingDispatchStatus, EmbeddingJobId};

    /// Stub dispatcher that returns pre-programmed responses keyed by
    /// memory_id. Missing keys fall back to `Ok(Some(stub_job_id))` so
    /// the test only has to list the interesting cases.
    struct StubDispatcher {
        /// Responses to return, indexed by call order so a single
        /// memory_id can be tested regardless of which ids the RDB
        /// assigned during `create_test_memory`.
        responses: std::sync::Mutex<Vec<StubResponse>>,
    }

    enum StubResponse {
        Ok(Option<EmbeddingJobId>),
        Err(DispatchError),
    }

    impl StubDispatcher {
        fn new(responses: Vec<StubResponse>) -> Self {
            Self {
                responses: std::sync::Mutex::new(responses),
            }
        }
    }

    #[async_trait::async_trait]
    impl EmbeddingDispatch for StubDispatcher {
        async fn dispatch(
            &self,
            _memory_id: i64,
            _content: &str,
        ) -> std::result::Result<Option<EmbeddingJobId>, DispatchError> {
            let mut guard = self.responses.lock().unwrap();
            // If the test under-specified responses we default to success
            // rather than panic — this keeps the test focused on the
            // cases it explicitly listed.
            let next = if guard.is_empty() {
                StubResponse::Ok(Some(EmbeddingJobId { value: 1 }))
            } else {
                guard.remove(0)
            };
            match next {
                StubResponse::Ok(j) => Ok(j),
                StubResponse::Err(e) => Err(e),
            }
        }
    }

    #[test]
    fn test_redispatch_embeddings_counts_failures() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let user_id = 15_200;
            // Three memories, all embeddable (content_type=Text, non-empty content).
            create_test_memory(&app.memory_repo, pool, "mem A", user_id).await?;
            create_test_memory(&app.memory_repo, pool, "mem B", user_id).await?;
            create_test_memory(&app.memory_repo, pool, "mem C", user_id).await?;

            // First Ok(Some) -> dispatched; then Err(Enqueue) -> failed;
            // then Ok(None) -> skipped. Enqueue (not Init) is used so the
            // loop does not abort mid-way.
            let stub = StubDispatcher::new(vec![
                StubResponse::Ok(Some(EmbeddingJobId { value: 111 })),
                StubResponse::Err(DispatchError::Enqueue(
                    EmbeddingDispatchStatus::unavailable("simulated enqueue failure"),
                )),
                StubResponse::Ok(None),
            ]);

            let (dispatched, skipped, failed, _duration) = app
                .redispatch_embeddings_with(&stub, Some(user_id), None, Some(10), &[])
                .await?;

            assert_eq!(dispatched, 1, "exactly one successful enqueue");
            assert_eq!(skipped, 1, "Ok(None) must be counted as skipped");
            assert_eq!(failed, 1, "Enqueue error must be counted as failed");
            Ok(())
        })
    }

    /// Regression guard for the production wiring bug (review P2 #3):
    /// `redispatch_embeddings` can only evaluate the Media dispatch axis
    /// when `with_media_resolver` is wired. Without it, the per-page
    /// `media_by_id` map is always empty, so `dispatch_kinds` never yields
    /// `Media` and an image-bearing memory is silently skipped under
    /// `kinds=[MEDIA]` — i.e. the image vector recovery API is dead.
    ///
    /// Same fixture (one image-bearing memory, mode=Multimodal,
    /// `kinds=[MEDIA]`), only the resolver differs:
    /// - no resolver  → `media_by_id` empty → Media not emitted → skipped
    /// - with resolver → kind=IMAGE resolved → Media emitted → the stub's
    ///   text-only `dispatch_target` default rejects it (Enqueue
    ///   unimplemented) → counted as failed (NOT skipped)
    ///
    /// The skipped→failed flip is the observable proof the Media axis
    /// became reachable; a stub cannot enqueue an image job so `failed`
    /// (not `dispatched`) is the expected terminal state here.
    #[test]
    fn test_redispatch_media_axis_requires_media_resolver() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            use infra::infra::embedding_dispatch::ImageSearchMode;

            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let user_id = 15_230;
            let media_repo =
                MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
            create_test_memory_with_image(
                &app.memory_repo,
                &media_repo,
                pool,
                "an image-bearing memory",
                user_id,
            )
            .await?;

            // Pass A: resolver NOT wired. mode=Multimodal is set so the
            // ONLY thing that can suppress Media is the missing resolver.
            let app_no_resolver = app.with_image_search_mode(ImageSearchMode::Multimodal);
            let stub = StubDispatcher::new(vec![]);
            let (dispatched, skipped, failed, _d) = app_no_resolver
                .redispatch_embeddings_with(
                    &stub,
                    Some(user_id),
                    None,
                    Some(10),
                    &[DispatchKind::Media],
                )
                .await?;
            assert_eq!(
                (dispatched, skipped, failed),
                (0, 1, 0),
                "without media resolver the image memory must be skipped \
                 (Media axis unreachable)"
            );

            // Pass B: same fixture, resolver wired. Media is now emitted;
            // the text-only stub cannot route it, so it lands in `failed`
            // — proving the axis became reachable.
            let media_repo2 =
                MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
            let app_with_resolver = app_no_resolver.with_media_resolver(media_repo2);
            let stub2 = StubDispatcher::new(vec![]);
            let (dispatched2, skipped2, failed2, _d2) = app_with_resolver
                .redispatch_embeddings_with(
                    &stub2,
                    Some(user_id),
                    None,
                    Some(10),
                    &[DispatchKind::Media],
                )
                .await?;
            assert_eq!(
                (dispatched2, skipped2, failed2),
                (0, 0, 1),
                "with media resolver the Media axis is reachable: the \
                 text-only stub rejects the image job as failed (not \
                 skipped)"
            );
            Ok(())
        })
    }

    #[test]
    fn test_redispatch_embeddings_breaks_on_init_error() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let user_id = 15_210;
            create_test_memory(&app.memory_repo, pool, "mem A", user_id).await?;
            create_test_memory(&app.memory_repo, pool, "mem B", user_id).await?;
            create_test_memory(&app.memory_repo, pool, "mem C", user_id).await?;

            // First call is Init-error; remaining entries must never be
            // consulted because Init failure is a global blocker. If the
            // loop mistakenly continued, the stub's default (Ok(Some))
            // would inflate `dispatched`.
            let stub = StubDispatcher::new(vec![StubResponse::Err(DispatchError::Init(
                anyhow::anyhow!("simulated init failure"),
            ))]);

            let (dispatched, skipped, failed, _duration) = app
                .redispatch_embeddings_with(&stub, Some(user_id), None, Some(10), &[])
                .await?;

            assert_eq!(dispatched, 0, "no successful enqueue on init failure");
            assert_eq!(skipped, 0, "skipped must stay at 0 — no pre-filter hits");
            assert_eq!(
                failed, 1,
                "init failure should be counted exactly once before break"
            );
            Ok(())
        })
    }

    /// Regression guard: a transient Init failure must NOT permanently
    /// take the dispatcher offline. The retry path being exercised is
    /// the `tokio::sync::OnceCell::get_or_try_init` loop inside
    /// `EmbeddingDispatcherCore` — when the cell holds Err, the next
    /// `dispatch()` call re-runs the init closure. We model that
    /// directly: the **same** stub dispatcher instance returns
    /// `Err(Init)` on its first dispatch and `Ok` on the second.
    /// `redispatch_embeddings_with` is invoked twice against the same
    /// stub, and the second pass must successfully dispatch.
    ///
    /// Previous form of this test used two distinct stubs (failing then
    /// fresh), which would still pass even if the production code
    /// regressed to dropping the dispatcher to `None` after first init
    /// error — the test has to share an instance to actually pin the
    /// retry contract.
    #[test]
    fn test_dispatcher_retries_after_transient_init_error() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;
            let user_id = 15_220;
            create_test_memory(&app.memory_repo, pool, "mem A", user_id).await?;
            create_test_memory(&app.memory_repo, pool, "mem B", user_id).await?;

            // Single stub instance shared across both passes. First
            // dispatch returns Init error; from the second dispatch
            // onward it succeeds. This mirrors a real dispatcher whose
            // `OnceCell` retried init successfully on the next call.
            let stub = TransientInitStub::new(1);

            // Pass 1: same stub instance → Init error → loop aborts
            // after the first row (existing fast-fail behaviour).
            let (d1, _s1, f1, _) = app
                .redispatch_embeddings_with(&stub, Some(user_id), None, Some(10), &[])
                .await?;
            assert_eq!(d1, 0, "first pass: init error blocks all dispatch");
            assert_eq!(f1, 1, "init error counted exactly once before break");

            // Pass 2: same stub instance, init has now "recovered".
            // The whole point of keeping the dispatcher `Some` after
            // init error is so this second pass can succeed on the same
            // app + dispatcher pair, not on a fresh one.
            let (d2, _s2, f2, _) = app
                .redispatch_embeddings_with(&stub, Some(user_id), None, Some(10), &[])
                .await?;
            assert_eq!(
                d2, 2,
                "second pass must succeed against the same dispatcher instance"
            );
            assert_eq!(f2, 0, "no failure expected on the recovering pass");
            Ok(())
        })
    }

    /// Stub dispatcher that returns `Err(DispatchError::Init)` for the
    /// first `init_fail_count` calls, then `Ok(Some(_))` thereafter.
    /// Models the retry-on-next-call behaviour of
    /// `tokio::sync::OnceCell::get_or_try_init` so we can pin the
    /// "transient init failure recovers on retry" contract without
    /// standing up a real jobworkerp connection.
    struct TransientInitStub {
        init_fail_count: usize,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl TransientInitStub {
        fn new(init_fail_count: usize) -> Self {
            Self {
                init_fail_count,
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl EmbeddingDispatch for TransientInitStub {
        async fn dispatch(
            &self,
            _memory_id: i64,
            _content: &str,
        ) -> std::result::Result<Option<EmbeddingJobId>, DispatchError> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n < self.init_fail_count {
                Err(DispatchError::Init(anyhow::anyhow!(
                    "simulated transient init failure (call #{})",
                    n + 1
                )))
            } else {
                Ok(Some(EmbeddingJobId {
                    value: 4_000 + n as i64,
                }))
            }
        }
    }

    // ===== P5: thread_filter resolve pipeline =====
    //
    // The pure-logic unit tests (`has_non_label_filters_recognises_each_field`,
    // `needs_other_route_skips_user_id_only_when_labels_set`) were moved to
    // `crate::app::thread_filter_resolver::tests` together with the helpers
    // themselves. The integration tests below still drive the resolver via
    // `MemoryVectorAppImpl::resolve_memory_ids_from_thread_filter` (the thin
    // wrapper) so the vector-side wiring stays covered end-to-end.

    fn cfg_for_test(
        fts_max_inline_ids: usize,
        fts_max_total_ids: usize,
        fts_approximate_mode: bool,
    ) -> ThreadFilterConfig {
        ThreadFilterConfig {
            intermediate_hard_limit: 1_000_000,
            max_thread_ids: 100_000,
            fts_max_inline_ids,
            fts_max_total_ids,
            fts_approximate_mode,
            prefilter_threshold: 200,
            fts_overfetch_k: 8,
            fts_count_hard_cap: 50_000,
            count_vector_hard_cap: 1_000,
            vector_dedup_overfetch_k: 4,
            vector_dedup_max_fetch: 10_000,
        }
    }

    /// `build_search_filter_with_cfg` is the route-aware ceiling logic.
    /// FTS / Hybrid must enforce `FTS_MAX_*`; pure `Vector` must NOT —
    /// otherwise an oversized thread_filter would knock out a perfectly
    /// valid ANN query, which is the regression flagged in the P1 review.
    #[test]
    fn build_search_filter_vector_ignores_fts_inline_ceiling() {
        let cfg = cfg_for_test(100, 50_000, false);
        // 5x over the FTS inline ceiling — Vector must still succeed
        // and produce a strict plan.
        let ids: Vec<i64> = (0..500).collect();
        match build_search_filter_with_cfg(&cfg, None, Some(ids), SearchKind::Vector) {
            Ok(FilterPlan::Strict(Some(_))) => {}
            Ok(other) => panic!("Vector must use Strict path with N>inline; got {:?}", other),
            Err(e) => panic!("Vector route must not be blocked by FTS_MAX_INLINE_IDS: {e}"),
        }
    }

    #[test]
    fn build_search_filter_fts_rejects_inline_ceiling_overflow_when_strict() {
        let cfg = cfg_for_test(100, 50_000, false);
        let ids: Vec<i64> = (0..500).collect();
        match build_search_filter_with_cfg(&cfg, None, Some(ids), SearchKind::Fts) {
            Ok(plan) => panic!("FTS over-limit must error in strict mode; got {:?}", plan),
            Err(e) => {
                let msg = e.to_string();
                assert!(
                    msg.contains("MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS"),
                    "FTS overflow must mention the responsible env var: {msg}"
                );
            }
        }
    }

    #[test]
    fn build_search_filter_hybrid_obeys_fts_ceiling_when_strict() {
        let cfg = cfg_for_test(100, 50_000, false);
        let ids: Vec<i64> = (0..500).collect();
        // Hybrid touches FTS, so the inline ceiling must apply when
        // approximate mode is off.
        match build_search_filter_with_cfg(&cfg, None, Some(ids), SearchKind::Hybrid) {
            Ok(plan) => panic!(
                "Hybrid over-limit must error in strict mode; got {:?}",
                plan
            ),
            Err(e) => assert!(
                e.to_string()
                    .contains("MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS")
            ),
        }
    }

    #[test]
    fn build_search_filter_fts_total_ceiling_rejects_even_in_approximate_mode() {
        // approximate is ON, but N exceeds the absolute upper bound.
        let cfg = cfg_for_test(100, 1_000, true);
        let ids: Vec<i64> = (0..1_500).collect();
        match build_search_filter_with_cfg(&cfg, None, Some(ids), SearchKind::Fts) {
            Ok(plan) => panic!(
                "approximate mode must NOT bypass MAX_TOTAL_IDS; got {:?}",
                plan
            ),
            Err(e) => assert!(
                e.to_string()
                    .contains("MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS"),
                "approximate mode must NOT bypass the absolute upper bound"
            ),
        }
    }

    /// Spec §P5「FTS 経路」近似モード: when enabled and N > MAX_INLINE_IDS,
    /// the planner must hand back the raw `allowed` set so the caller
    /// can run the two-phase pipeline. Strict-mode error path was
    /// covered above; this test pins the approximate fork.
    #[test]
    fn build_search_filter_fts_approximate_returns_allowed_set() {
        let cfg = cfg_for_test(100, 50_000, true);
        let ids: Vec<i64> = (0..500).collect();
        match build_search_filter_with_cfg(&cfg, None, Some(ids.clone()), SearchKind::Fts) {
            Ok(FilterPlan::Approximate {
                base: None,
                allowed,
            }) => assert_eq!(allowed, ids, "allowed must be the original resolved set"),
            other => panic!(
                "approximate mode + N>inline must yield Approximate plan; got {:?}",
                other
            ),
        }
    }

    #[test]
    fn build_search_filter_hybrid_approximate_returns_allowed_set() {
        let cfg = cfg_for_test(100, 50_000, true);
        let ids: Vec<i64> = (0..500).collect();
        match build_search_filter_with_cfg(&cfg, None, Some(ids), SearchKind::Hybrid) {
            Ok(FilterPlan::Approximate { .. }) => {}
            other => panic!(
                "Hybrid+approximate must take the two-phase path; got {:?}",
                other
            ),
        }
    }

    #[test]
    fn build_search_filter_empty_ids_short_circuits() {
        let cfg = cfg_for_test(100, 50_000, false);
        for kind in [SearchKind::Vector, SearchKind::Fts, SearchKind::Hybrid] {
            match build_search_filter_with_cfg(&cfg, None, Some(Vec::new()), kind) {
                Ok(FilterPlan::ShortCircuit) => {}
                other => panic!(
                    "empty thread_filter ids must short-circuit for {:?}; got {:?}",
                    kind, other
                ),
            }
        }
    }

    #[test]
    fn build_search_filter_no_thread_filter_passthrough() {
        let cfg = cfg_for_test(100, 50_000, false);
        for kind in [SearchKind::Vector, SearchKind::Fts, SearchKind::Hybrid] {
            match build_search_filter_with_cfg(&cfg, None, None, kind) {
                Ok(FilterPlan::Strict(None)) => {}
                other => panic!(
                    "no thread_filter must pass through as Strict(None) for {:?}; got {:?}",
                    kind, other
                ),
            }
        }
    }

    /// `approximate_fetch_limit` is the per-attempt clamp used by the
    /// two-phase pipeline. The two invariants that matter are
    /// (1) the absolute upper bound is never exceeded, and (2) we
    /// always ask for a real over-fetch (`limit + 1` or more) when the
    /// upper bound permits it.
    #[test]
    fn approximate_fetch_limit_clamps_to_total_cap() {
        // limit + 1 fits under the cap → fetch grows with attempt but
        // is capped at fts_max_total_ids.
        // limit=10, K=8, attempt=3 → raw=240, cap=100 → 100.
        assert_eq!(approximate_fetch_limit(10, 8, 3, 100), 100);
        // limit=10, K=8, attempt=1 → raw=80, cap=100 → 80.
        assert_eq!(approximate_fetch_limit(10, 8, 1, 100), 80);
    }

    #[test]
    fn approximate_fetch_limit_floors_at_limit_plus_one_when_room() {
        // raw fetch is below `limit + 1` (degenerate K) — the floor
        // kicks in so we still get a real over-fetch.
        // limit=10, K=0 → k0=1, attempt=1 → raw=10, lower_bound=11 → 11.
        assert_eq!(approximate_fetch_limit(10, 0, 1, 100), 11);
    }

    #[test]
    fn approximate_fetch_limit_safety_wins_when_cap_below_limit() {
        // Operator-set cap is below `limit + 1`. Safety > completeness:
        // the cap is binding and the caller will see a shortfall warn.
        // limit=100, K=8, attempt=3 → raw=2400, cap=50 → 50, NOT 101.
        assert_eq!(approximate_fetch_limit(100, 8, 3, 50), 50);
        // Edge: cap exactly equals limit — still bounded by cap.
        assert_eq!(approximate_fetch_limit(100, 8, 1, 100), 100);
    }

    #[test]
    fn approximate_fetch_limit_is_monotonic_under_cap() {
        // Below the cap, attempt N+1 must fetch >= attempt N.
        let a1 = approximate_fetch_limit(10, 8, 1, 10_000);
        let a2 = approximate_fetch_limit(10, 8, 2, 10_000);
        let a3 = approximate_fetch_limit(10, 8, 3, 10_000);
        assert!(
            a1 <= a2 && a2 <= a3,
            "fetch must grow per attempt: {a1}, {a2}, {a3}"
        );
    }

    // ===== Image memory Phase 4: N-row de-dup pure fns =====

    fn hit_with(
        memory_id: i64,
        score: f32,
        vector_kind: &str,
        chunk_index: i32,
    ) -> VectorSearchHit {
        VectorSearchHit {
            memory_id,
            score,
            distance: 1.0 - score,
            vector_kind: Some(vector_kind.to_string()),
            chunk_index: Some(chunk_index),
            begin_position: Some(chunk_index * 100),
            end_position: Some(chunk_index * 100 + 50),
            matched_content: Some(format!("{vector_kind}#{chunk_index}")),
        }
    }

    /// spec §6.1「検索 de-dup」: a memory with N chunk rows collapses to
    /// one hit, the score is the max across its rows, and the winning
    /// row's matched-* metadata is the one kept.
    #[test]
    fn dedup_keeps_one_per_memory_with_max_score_row() {
        let hits = vec![
            hit_with(1, 0.50, "text", 0),
            hit_with(2, 0.90, "image", 0),
            hit_with(1, 0.80, "text", 3), // higher score for memory 1
            hit_with(1, 0.30, "caption", 0),
            hit_with(2, 0.40, "text", 1),
        ];
        let out = dedup_vector_hits(hits);
        assert_eq!(out.len(), 2, "one entry per memory_id");

        let m1 = out.iter().find(|h| h.memory_id == 1).unwrap();
        assert_eq!(m1.score, 0.80, "max score across memory 1's rows");
        assert_eq!(m1.vector_kind.as_deref(), Some("text"));
        assert_eq!(m1.chunk_index, Some(3), "winning row's chunk is kept");
        assert_eq!(m1.matched_content.as_deref(), Some("text#3"));

        let m2 = out.iter().find(|h| h.memory_id == 2).unwrap();
        assert_eq!(m2.score, 0.90);
        assert_eq!(m2.vector_kind.as_deref(), Some("image"));
    }

    /// Output preserves the best-ranked memory's first-appearance order;
    /// a later equal/lower hit never displaces the winner.
    #[test]
    fn dedup_preserves_first_appearance_order_and_ignores_ties() {
        let hits = vec![
            hit_with(10, 0.70, "text", 0),
            hit_with(20, 0.60, "text", 0),
            hit_with(10, 0.70, "image", 0), // tie — must NOT replace
            hit_with(10, 0.65, "caption", 0),
        ];
        let out = dedup_vector_hits(hits);
        assert_eq!(
            out.iter().map(|h| h.memory_id).collect::<Vec<_>>(),
            vec![10, 20]
        );
        // The tie did not overwrite the original text/0 winner (`>` not `>=`).
        let m10 = &out[0];
        assert_eq!(m10.vector_kind.as_deref(), Some("text"));
    }

    #[test]
    fn dedup_empty_is_empty() {
        assert!(dedup_vector_hits(Vec::new()).is_empty());
    }

    /// design 1/3 §2.6.4 / 3/3 §13.3 score_source boundary: the value is
    /// decided by the query's ORIGIN, not the matched row's kind, except
    /// for the server-embedded origins which DO follow the row kind.
    #[test]
    fn score_source_is_decided_by_query_origin_not_hit_kind() {
        use protobuf::llm_memory::data::ScoreSource;

        // Generic origins ignore the hit kind entirely.
        for kind in [Some("text"), Some("image"), Some("caption"), None] {
            assert_eq!(
                QueryOrigin::ExternalVector.score_source(kind),
                ScoreSource::ScoreVector as i32,
                "SearchByVector stays SCORE_VECTOR for any hit kind"
            );
            assert_eq!(
                QueryOrigin::Hybrid.score_source(kind),
                ScoreSource::ScoreHybrid as i32,
                "HybridSearch stays SCORE_HYBRID"
            );
            assert_eq!(
                QueryOrigin::Bm25Text.score_source(kind),
                ScoreSource::ScoreText as i32,
            );
        }

        // Server-embedded origins map to the kind-specific SCORE_*_EMBED.
        assert_eq!(
            QueryOrigin::ServerText.score_source(Some("text")),
            ScoreSource::ScoreTextEmbed as i32
        );
        assert_eq!(
            QueryOrigin::ServerText.score_source(Some("image")),
            ScoreSource::ScoreImageEmbed as i32
        );
        assert_eq!(
            QueryOrigin::ServerImage.score_source(Some("caption")),
            ScoreSource::ScoreCaptionEmbed as i32
        );
        // Unknown / missing kind on a server-embedded query defaults to
        // text-embed (the query was produced in the embed space).
        assert_eq!(
            QueryOrigin::ServerImage.score_source(None),
            ScoreSource::ScoreTextEmbed as i32
        );
    }

    /// Helper: insert a thread row directly so the test controls every
    /// attribute (created_at / updated_at / channel / user_id) without
    /// the `fill_timestamps` auto-fill in `create`. Reuses the production
    /// `INSERT_SQL` constant so the placeholder syntax stays in sync
    /// with the active backend (`?` for SQLite, `$N` for PostgreSQL).
    #[allow(clippy::too_many_arguments)]
    async fn insert_thread_for_resolve_test(
        pool: &'static infra_utils::infra::rdb::RdbPool,
        id: i64,
        user_id: i64,
        channel: Option<&str>,
        labels: &[&str],
        created_at: i64,
        updated_at: i64,
    ) -> anyhow::Result<()> {
        let label_repo = ThreadLabelRepositoryImpl::new(pool);
        sqlx::query::<infra_utils::infra::rdb::Rdb>(infra::infra::thread::rdb::INSERT_SQL)
            .bind(id)
            .bind(None::<i64>)
            .bind(user_id)
            .bind(None::<String>)
            .bind(channel.map(|s| s.to_string()))
            .bind(None::<Vec<u8>>)
            .bind(None::<i32>)
            .bind(created_at)
            .bind(updated_at)
            .bind(None::<String>) // metadata
            .execute(pool)
            .await?;
        if !labels.is_empty() {
            let mut tx = pool.begin().await?;
            for label in labels {
                label_repo
                    .add_labels_tx(&mut *tx, id, label, created_at)
                    .await?;
            }
            tx.commit().await?;
        }
        Ok(())
    }

    /// Attach an existing memory to a thread at a fresh position. Used
    /// to seed the junction table before exercising the resolve pipeline.
    async fn attach_memory_to_thread(
        pool: &'static infra_utils::infra::rdb::RdbPool,
        thread_id: i64,
        memory_id: i64,
        position: i32,
    ) -> anyhow::Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        repo.insert_tx(pool, thread_id, memory_id, position, 1_000)
            .await
    }

    /// Empty filter must short-circuit to `Ok(None)` — neither route
    /// fires, no SQL is sent against the label / non-label paths.
    #[test]
    fn resolve_returns_none_for_empty_filter() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let (app, _pool, _db) = setup_app(8).await?;
            let result = app
                .resolve_memory_ids_from_thread_filter(&ThreadSearchFilter::default(), None)
                .await?;
            assert!(result.is_none());
            Ok(())
        })
    }

    /// labels-only filter resolves through the label repo and unions
    /// memory ids across matching threads. Shared memories must appear
    /// once thanks to `find_memory_ids_by_thread_ids` DISTINCT.
    #[test]
    fn resolve_labels_only_dedups_shared_memory() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let (app, pool, _db) = setup_app(8).await?;
            // Two threads both labeled "rust", a third labeled "go".
            insert_thread_for_resolve_test(pool, 9_300_001, 1, None, &["rust"], 1, 1).await?;
            insert_thread_for_resolve_test(pool, 9_300_002, 1, None, &["rust"], 2, 2).await?;
            insert_thread_for_resolve_test(pool, 9_300_003, 1, None, &["go"], 3, 3).await?;

            // Memory shared between the two rust threads.
            let shared = create_test_memory(&app.memory_repo, pool, "shared body", 1).await?;
            let only_rust2 = create_test_memory(&app.memory_repo, pool, "only-rust2", 1).await?;
            let only_go = create_test_memory(&app.memory_repo, pool, "only-go", 1).await?;
            attach_memory_to_thread(pool, 9_300_001, shared, 0).await?;
            attach_memory_to_thread(pool, 9_300_002, shared, 0).await?;
            attach_memory_to_thread(pool, 9_300_002, only_rust2, 1).await?;
            attach_memory_to_thread(pool, 9_300_003, only_go, 0).await?;

            let filter = ThreadSearchFilter {
                labels: vec!["rust".into()],
                ..Default::default()
            };
            let mut result = app
                .resolve_memory_ids_from_thread_filter(&filter, None)
                .await?
                .expect("labels-only filter must resolve to Some(_)");
            result.sort();
            let mut expected = vec![shared, only_rust2];
            expected.sort();
            assert_eq!(result, expected);
            Ok(())
        })
    }

    /// labels + non-label intersection: a thread that satisfies labels
    /// but not the time-range bound must drop out of the resolved set.
    #[test]
    fn resolve_intersects_labels_and_non_label_routes() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let (app, pool, _db) = setup_app(8).await?;
            insert_thread_for_resolve_test(pool, 9_310_001, 1, None, &["rust"], 100, 200).await?;
            insert_thread_for_resolve_test(pool, 9_310_002, 1, None, &["rust"], 100, 50).await?;
            insert_thread_for_resolve_test(pool, 9_310_003, 1, None, &["go"], 100, 200).await?;
            let m_a = create_test_memory(&app.memory_repo, pool, "a", 1).await?;
            let m_b = create_test_memory(&app.memory_repo, pool, "b", 1).await?;
            let m_c = create_test_memory(&app.memory_repo, pool, "c", 1).await?;
            attach_memory_to_thread(pool, 9_310_001, m_a, 0).await?;
            attach_memory_to_thread(pool, 9_310_002, m_b, 0).await?;
            attach_memory_to_thread(pool, 9_310_003, m_c, 0).await?;

            // labels = "rust" (matches threads 1 and 2);
            // updated_after = 100 (matches threads 1 and 3).
            // Intersection = thread 1 only → memory m_a only.
            let filter = ThreadSearchFilter {
                labels: vec!["rust".into()],
                updated_after: Some(100),
                ..Default::default()
            };
            let result = app
                .resolve_memory_ids_from_thread_filter(&filter, None)
                .await?
                .expect("intersection must resolve to Some(_)");
            assert_eq!(result, vec![m_a]);
            Ok(())
        })
    }

    /// A filter that no thread matches must resolve to `Some(empty)` —
    /// distinct from `None` so the caller short-circuits the search to
    /// an empty result instead of falling back to "no thread filter".
    #[test]
    fn resolve_returns_some_empty_when_no_thread_matches() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let (app, pool, _db) = setup_app(8).await?;
            insert_thread_for_resolve_test(pool, 9_320_001, 1, None, &["rust"], 100, 100).await?;

            let filter = ThreadSearchFilter {
                labels: vec!["go".into()],
                ..Default::default()
            };
            let result = app
                .resolve_memory_ids_from_thread_filter(&filter, None)
                .await?;
            assert_eq!(
                result,
                Some(Vec::new()),
                "no-match filter must distinguish itself from no-filter"
            );
            Ok(())
        })
    }

    /// MAX_THREAD_IDS overflow must surface as `FailedPrecondition`.
    /// We squeeze the cap down via env so the test can fit in 3 threads.
    #[test]
    fn resolve_rejects_overflow_with_failed_precondition() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            // SAFETY: setting env in a single-threaded test runner
            // (`--test-threads=1` is mandatory in this crate per CLAUDE.md).
            unsafe {
                std::env::set_var("MEMORY_THREAD_FILTER_MAX_THREAD_IDS", "2");
            }
            let (app, pool, _db) = setup_app(8).await?;
            for (i, id) in [9_330_001_i64, 9_330_002, 9_330_003].iter().enumerate() {
                insert_thread_for_resolve_test(pool, *id, 1, None, &["bulk"], 100, 100 + i as i64)
                    .await?;
            }
            let filter = ThreadSearchFilter {
                labels: vec!["bulk".into()],
                ..Default::default()
            };
            let err = app
                .resolve_memory_ids_from_thread_filter(&filter, None)
                .await
                .expect_err("expected FailedPrecondition for resolved set > 2");
            let msg = format!("{:?}", err);
            assert!(
                msg.contains("FailedPrecondition") && msg.contains("3"),
                "unexpected error: {msg}"
            );
            unsafe {
                std::env::remove_var("MEMORY_THREAD_FILTER_MAX_THREAD_IDS");
            }
            Ok(())
        })
    }

    /// memory_id-level overflow: a small number of threads (well under
    /// `MAX_THREAD_IDS`) can still fan out to a huge memory_id set if
    /// each thread is heavy. The cap shipped with the resolve junction
    /// lookup must reject before the full set is materialized.
    #[test]
    fn resolve_rejects_memory_id_overflow_with_failed_precondition() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            // SAFETY: --test-threads=1.
            unsafe {
                std::env::set_var("MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS", "3");
            }
            let (app, pool, _db) = setup_app(8).await?;
            // Two threads, but together they hold 5 distinct memories
            // (3 + 2). With `fts_max_total_ids = 3` the resolve must
            // fail because the merged junction set is 5.
            insert_thread_for_resolve_test(pool, 9_340_001, 1, None, &["heavy"], 100, 100).await?;
            insert_thread_for_resolve_test(pool, 9_340_002, 1, None, &["heavy"], 200, 200).await?;
            for (i, mid) in [9_340_101_i64, 9_340_102, 9_340_103].iter().enumerate() {
                let memory_id =
                    create_test_memory(&app.memory_repo, pool, &format!("a{i}"), 1).await?;
                attach_memory_to_thread(pool, 9_340_001, memory_id, i as i32).await?;
                let _ = mid;
            }
            for (i, mid) in [9_340_201_i64, 9_340_202].iter().enumerate() {
                let memory_id =
                    create_test_memory(&app.memory_repo, pool, &format!("b{i}"), 1).await?;
                attach_memory_to_thread(pool, 9_340_002, memory_id, i as i32).await?;
                let _ = mid;
            }

            let filter = ThreadSearchFilter {
                labels: vec!["heavy".into()],
                ..Default::default()
            };
            let result = app
                .resolve_memory_ids_from_thread_filter(&filter, None)
                .await;
            unsafe {
                std::env::remove_var("MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS");
            }
            let err = result.expect_err("expected FailedPrecondition for memory_id overflow");
            let msg = format!("{:?}", err);
            assert!(
                msg.contains("FailedPrecondition")
                    && msg.contains("MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS"),
                "unexpected error: {msg}"
            );
            Ok(())
        })
    }

    // ===== P1 (improve-search): enrich_hits =====

    /// Insert a thread with an explicit `description` (the resolve-test
    /// helper above hard-codes `None`). Used by the P1 tests that need
    /// to exercise the `thread_description = None` vs `Some(_)` paths.
    async fn insert_thread_with_description(
        pool: &'static infra_utils::infra::rdb::RdbPool,
        id: i64,
        user_id: i64,
        description: Option<&str>,
    ) -> anyhow::Result<()> {
        sqlx::query::<infra_utils::infra::rdb::Rdb>(infra::infra::thread::rdb::INSERT_SQL)
            .bind(id)
            .bind(None::<i64>)
            .bind(user_id)
            .bind(description.map(|s| s.to_string()))
            .bind(None::<String>)
            .bind(None::<Vec<u8>>)
            .bind(None::<i32>)
            .bind(1_000_i64)
            .bind(1_000_i64)
            .bind(None::<String>) // metadata
            .execute(pool)
            .await?;
        Ok(())
    }

    /// Helper to upsert an embedding for a memory (the search path needs
    /// LanceDB to have a row to return). Reuses `app.upsert_embedding`.
    async fn upsert_dummy_embedding(
        app: &MemoryVectorAppImpl,
        memory_id: i64,
        dim: usize,
    ) -> anyhow::Result<()> {
        let emb = random_embedding(dim);
        app.upsert_embedding(memory_id, &emb, Some("test-model"))
            .await
    }

    /// Build a hit list manually so we can drive `enrich_hits` directly
    /// without hitting LanceDB. Tests the population logic in isolation.
    fn make_hits(ids: &[i64]) -> Vec<VectorSearchHit> {
        ids.iter()
            .map(|&id| VectorSearchHit {
                memory_id: id,
                score: 1.0,
                distance: 0.0,
                ..Default::default()
            })
            .collect()
    }

    /// Spec test #1: every hit gets the five P1 fields populated; no N+1.
    #[test]
    fn p1_enrich_populates_five_fields() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_400_001, 42, Some("hello world")).await?;
            let m1 = create_test_memory(&app.memory_repo, pool, "alpha", 7).await?;
            let m2 = create_test_memory(&app.memory_repo, pool, "beta", 7).await?;
            attach_memory_to_thread(pool, 9_400_001, m1, 0).await?;
            attach_memory_to_thread(pool, 9_400_001, m2, 1).await?;

            let results = app
                .enrich_hits(
                    make_hits(&[m1, m2]),
                    true,
                    None,
                    QueryOrigin::ExternalVector,
                )
                .await?;
            assert_eq!(results.len(), 2);
            for r in &results {
                assert_eq!(r.thread_id.as_ref().unwrap().value, 9_400_001);
                assert_eq!(r.thread_total, Some(2));
                assert_eq!(r.thread_owner_user_id.as_ref().unwrap().value, 42);
                assert_eq!(r.thread_description.as_deref(), Some("hello world"));
                let data = r.memory.data.as_ref().expect("memory data");
                assert_eq!(
                    data.thread_ids
                        .iter()
                        .map(|id| id.value)
                        .collect::<Vec<_>>(),
                    vec![9_400_001]
                );
            }
            // Positions match insertion order.
            let pos_for = |id: i64| {
                results
                    .iter()
                    .find(|r| r.memory.id.as_ref().unwrap().value == id)
                    .and_then(|r| r.position)
                    .unwrap()
            };
            assert_eq!(pos_for(m1), 0);
            assert_eq!(pos_for(m2), 1);
            Ok(())
        })
    }

    /// Spec test #2: ROLE_SYSTEM memories never get the five P1 fields,
    /// even when they are attached to a thread (the protection runs
    /// before the SQLs).
    #[test]
    fn p1_role_system_protection_unsets_all_five() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_410_001, 1, Some("with sys")).await?;
            // Create a ROLE_SYSTEM memory directly so MemoryData.role = 3.
            let id_gen = infra::test_helper::shared_id_generator();
            let memory_repo = MemoryRepositoryImpl::new(id_gen, pool);
            let sys_data = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: 1 }),
                content: "system prompt".to_string(),
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
            let mut tx = pool.begin().await?;
            let sys_id = memory_repo.create(&mut *tx, &sys_data).await?.value;
            tx.commit().await?;
            attach_memory_to_thread(pool, 9_410_001, sys_id, 0).await?;

            // Also a regular memory in the same thread to confirm only
            // the ROLE_SYSTEM hit gets the five fields stripped.
            let user_id = create_test_memory(&app.memory_repo, pool, "user msg", 1).await?;
            attach_memory_to_thread(pool, 9_410_001, user_id, 1).await?;

            let results = app
                .enrich_hits(
                    make_hits(&[sys_id, user_id]),
                    true,
                    None,
                    QueryOrigin::ExternalVector,
                )
                .await?;
            assert_eq!(results.len(), 2);
            let sys = results
                .iter()
                .find(|r| r.memory.id.as_ref().unwrap().value == sys_id)
                .unwrap();
            assert!(sys.position.is_none());
            assert!(sys.thread_total.is_none());
            assert!(sys.thread_id.is_none());
            assert!(sys.thread_owner_user_id.is_none());
            assert!(sys.thread_description.is_none());
            // The non-system memory keeps its fields populated — the
            // protection is per-hit, not per-batch.
            let usr = results
                .iter()
                .find(|r| r.memory.id.as_ref().unwrap().value == user_id)
                .unwrap();
            assert!(usr.thread_id.is_some());
            Ok(())
        })
    }

    /// Spec test #3: a memory that has no thread_memory row returns the
    /// five fields unset.
    #[test]
    fn p1_no_thread_membership_unsets_all_five() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            let orphan = create_test_memory(&app.memory_repo, pool, "no thread", 1).await?;
            // Intentionally do NOT call attach_memory_to_thread.

            let results = app
                .enrich_hits(
                    make_hits(&[orphan]),
                    true,
                    None,
                    QueryOrigin::ExternalVector,
                )
                .await?;
            assert_eq!(results.len(), 1);
            assert!(results[0].position.is_none());
            assert!(results[0].thread_id.is_none());
            assert!(results[0].thread_owner_user_id.is_none());
            assert!(results[0].thread_description.is_none());
            assert!(results[0].thread_total.is_none());
            Ok(())
        })
    }

    /// Spec test #4: orphan thread (thread_memory row exists but
    /// `thread.id` row was deleted). All five fields must be unset —
    /// never leave `thread_id` set with the rest empty.
    #[test]
    fn p1_orphan_thread_row_deleted_unsets_all_five() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            let tid = 9_420_001_i64;
            insert_thread_with_description(pool, tid, 1, Some("about-to-vanish")).await?;
            let mid = create_test_memory(&app.memory_repo, pool, "body", 1).await?;
            attach_memory_to_thread(pool, tid, mid, 0).await?;

            // Bypass app cascade and delete the thread row directly so
            // junction is left dangling.
            #[cfg(feature = "postgres")]
            let del_sql = "DELETE FROM thread WHERE id = $1";
            #[cfg(not(feature = "postgres"))]
            let del_sql = "DELETE FROM thread WHERE id = ?";
            sqlx::query::<infra_utils::infra::rdb::Rdb>(del_sql)
                .bind(tid)
                .execute(pool)
                .await?;

            let results = app
                .enrich_hits(make_hits(&[mid]), true, None, QueryOrigin::ExternalVector)
                .await?;
            assert_eq!(results.len(), 1);
            assert!(
                results[0].thread_id.is_none(),
                "orphan thread must not leak thread_id"
            );
            assert!(results[0].position.is_none());
            assert!(results[0].thread_total.is_none());
            assert!(results[0].thread_owner_user_id.is_none());
            assert!(results[0].thread_description.is_none());
            Ok(())
        })
    }

    /// Race regression: `find_positions_for_memories` snapshotted a
    /// (memory_id, rep_thread_id, position) row, but by the time
    /// `count_by_thread_ids` runs the last `thread_memory` row for that
    /// thread has been detached. `totals.get(rep_tid)` is `None` while
    /// `summaries.get(rep_tid)` is still `Some(_)`. The all-or-none
    /// orphan rule must clear the entire representative-thread block —
    /// surfacing `position` without `thread_total` would render `[N/M]`
    /// with the M missing on the client. See spec §P1 "orphan thread
    /// の取り扱い".
    #[test]
    fn representative_thread_concurrent_detach_unsets_all_five() {
        let memory_id = 9_440_001_i64;
        let rep_tid = 9_440_002_i64;
        let positions_map: std::collections::HashMap<i64, (i64, i32)> =
            std::iter::once((memory_id, (rep_tid, 3))).collect();
        let mut summaries = std::collections::HashMap::new();
        summaries.insert(
            rep_tid,
            infra::infra::thread::rdb::ThreadSummary {
                user_id: 7,
                description: Some("still alive".to_string()),
            },
        );
        // Empty totals = `count_by_thread_ids` returned no row for
        // rep_tid (last attachment removed mid-flight).
        let totals: std::collections::HashMap<i64, i32> = std::collections::HashMap::new();

        let info = super::resolve_representative_thread_info(
            memory_id,
            &positions_map,
            &summaries,
            &totals,
        );
        assert!(info.position.is_none(), "position must drop");
        assert!(info.thread_total.is_none(), "thread_total must drop");
        assert!(info.thread_id.is_none(), "thread_id must drop");
        assert!(info.thread_owner_user_id.is_none());
        assert!(info.thread_description.is_none());
    }

    /// Spec test #4.5: thread row exists but `description` column is
    /// NULL → only `thread_description` is unset; the other four stay
    /// populated so the UI keeps jump enabled.
    #[test]
    fn p1_thread_description_null_only_description_unset() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_430_001, 9, None).await?;
            let mid = create_test_memory(&app.memory_repo, pool, "body", 1).await?;
            attach_memory_to_thread(pool, 9_430_001, mid, 5).await?;

            let results = app
                .enrich_hits(make_hits(&[mid]), true, None, QueryOrigin::ExternalVector)
                .await?;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].thread_id.as_ref().unwrap().value, 9_430_001);
            assert_eq!(results[0].thread_owner_user_id.as_ref().unwrap().value, 9);
            assert_eq!(results[0].position, Some(5));
            assert_eq!(results[0].thread_total, Some(1));
            assert!(
                results[0].thread_description.is_none(),
                "NULL description must surface as unset",
            );
            Ok(())
        })
    }

    /// Spec test #5: empty hits short-circuits without firing extra SQL
    /// (we can't observe SQL count from here, but the early return is
    /// the structural assert).
    #[test]
    fn p1_empty_hits_short_circuits() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;
            let results = app
                .enrich_hits(Vec::new(), true, None, QueryOrigin::ExternalVector)
                .await?;
            assert!(results.is_empty());
            Ok(())
        })
    }

    /// Spec test #6: cross-user search — memory's owner differs from
    /// thread's owner. `thread_owner_user_id` must reflect the THREAD
    /// owner, not the memory owner (so the chat-jump URL routes to the
    /// right user's session).
    #[test]
    fn p1_cross_user_owner_resolution() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            // Thread owned by user 11.
            insert_thread_with_description(pool, 9_440_001, 11, Some("Bob's thread")).await?;
            // Memory owned by user 22 (different user — could be a shared
            // ROLE_USER memory contributed to Bob's thread).
            let mid = create_test_memory(&app.memory_repo, pool, "shared body", 22).await?;
            attach_memory_to_thread(pool, 9_440_001, mid, 0).await?;

            // Drive a hit and inspect the resolved owner.
            upsert_dummy_embedding(&app, mid, dim).await?;
            let results = app
                .enrich_hits(make_hits(&[mid]), true, None, QueryOrigin::ExternalVector)
                .await?;
            assert_eq!(results.len(), 1);
            assert_eq!(
                results[0].thread_owner_user_id.as_ref().unwrap().value,
                11,
                "owner must match the THREAD, not the memory owner",
            );
            assert_eq!(
                results[0].thread_description.as_deref(),
                Some("Bob's thread")
            );
            Ok(())
        })
    }

    /// Representative-thread selection: when a memory is attached to
    /// multiple threads, the smallest `thread_id` wins (snowflake = oldest
    /// thread). Confirms the JOIN/MIN selection is honoured by enrich_hits.
    #[test]
    fn p1_representative_thread_is_smallest_id() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            let small = 9_450_001_i64;
            let large = 9_450_002_i64;
            insert_thread_with_description(pool, small, 1, Some("older")).await?;
            insert_thread_with_description(pool, large, 2, Some("newer")).await?;

            let mid = create_test_memory(&app.memory_repo, pool, "shared", 1).await?;
            // Attach to LARGE first, position 0; then to SMALL at position 7.
            // The representative must still be SMALL (older snowflake).
            attach_memory_to_thread(pool, large, mid, 0).await?;
            attach_memory_to_thread(pool, small, mid, 7).await?;

            let results = app
                .enrich_hits(make_hits(&[mid]), true, None, QueryOrigin::ExternalVector)
                .await?;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].thread_id.as_ref().unwrap().value, small);
            assert_eq!(results[0].position, Some(7));
            assert_eq!(results[0].thread_owner_user_id.as_ref().unwrap().value, 1);
            assert_eq!(results[0].thread_description.as_deref(), Some("older"));
            Ok(())
        })
    }

    // ===== highlights pipeline (P3) =====

    /// Spec test (P3 #1): when `query_text` matches the hit's content,
    /// `enrich_hits` attaches a single `HighlightField` with
    /// `source = CONTENT` and at least one range. Locks in that the
    /// query_text hand-off through `enrich_hits` is wired all the way
    /// to the per-item proto field.
    #[test]
    fn p3_enrich_attaches_highlights_when_query_text_set() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_410_001, 1, Some("desc")).await?;
            let mid = create_test_memory(
                &app.memory_repo,
                pool,
                "search highlights work end to end",
                1,
            )
            .await?;
            attach_memory_to_thread(pool, 9_410_001, mid, 0).await?;

            let results = app
                .enrich_hits(
                    make_hits(&[mid]),
                    true,
                    Some("highlights"),
                    QueryOrigin::ExternalVector,
                )
                .await?;
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].highlights.len(), 1);
            let hf = &results[0].highlights[0];
            assert_eq!(
                hf.source,
                protobuf::llm_memory::data::HighlightSource::Content as i32,
                "highlights for memory results must carry source = CONTENT"
            );
            assert!(
                !hf.ranges.is_empty(),
                "ranges must be non-empty for an actual content match"
            );
            Ok(())
        })
    }

    /// Spec test (P3 #2): vector-only path passes `query_text = None`
    /// and the proto field stays empty. This is the contract that
    /// guarantees a vector-only search never produces dangling
    /// highlight ranges (which would mislead the UI).
    #[test]
    fn p3_enrich_returns_no_highlights_when_query_text_none() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_410_002, 1, Some("desc")).await?;
            let mid =
                create_test_memory(&app.memory_repo, pool, "highlights everywhere", 1).await?;
            attach_memory_to_thread(pool, 9_410_002, mid, 0).await?;

            let results = app
                .enrich_hits(make_hits(&[mid]), true, None, QueryOrigin::ExternalVector)
                .await?;
            assert_eq!(results.len(), 1);
            assert!(
                results[0].highlights.is_empty(),
                "search_by_vector path (query_text = None) must return empty highlights"
            );
            Ok(())
        })
    }

    /// Spec test (P3 #3): with `include_content = false` the server
    /// strips the content body, so returning offsets into a stripped
    /// payload would be misleading. enrich_hits must short-circuit and
    /// return an empty highlights vec, even when `query_text` is set.
    #[test]
    fn p3_enrich_returns_no_highlights_when_include_content_false() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_410_003, 1, Some("desc")).await?;
            let mid =
                create_test_memory(&app.memory_repo, pool, "rust testing pipeline", 1).await?;
            attach_memory_to_thread(pool, 9_410_003, mid, 0).await?;

            let results = app
                .enrich_hits(
                    make_hits(&[mid]),
                    false,
                    Some("rust"),
                    QueryOrigin::ExternalVector,
                )
                .await?;
            assert_eq!(results.len(), 1);
            assert!(
                results[0].highlights.is_empty(),
                "include_content=false must yield empty highlights regardless of query_text"
            );
            // Sanity: the content was stripped, otherwise the assertion above
            // would not have caught the case we care about.
            assert_eq!(
                results[0]
                    .memory
                    .data
                    .as_ref()
                    .map(|d| d.content.as_str())
                    .unwrap_or(""),
                ""
            );
            Ok(())
        })
    }

    // ===== P2 (improve-search): CountSearchMatches =====

    /// Helper: feed memories with a fixed user_id and content into the
    /// vector index so FTS / FILTER_ONLY counts have something to count.
    async fn count_setup_corpus(
        app: &MemoryVectorAppImpl,
        pool: &'static infra_utils::infra::rdb::RdbPool,
        dim: usize,
        items: &[(i64, &str)],
    ) -> anyhow::Result<Vec<i64>> {
        let mut ids = Vec::new();
        for (user, content) in items {
            let id = create_test_memory(&app.memory_repo, pool, content, *user).await?;
            upsert_dummy_embedding(app, id, dim).await?;
            ids.push(id);
        }
        Ok(ids)
    }

    /// Spec test #1: FILTER_ONLY exact count. role / user filtering.
    #[test]
    fn p2_count_filter_only_exact_with_user_filter() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            count_setup_corpus(
                &app,
                pool,
                dim,
                &[(10, "alpha"), (10, "bravo"), (20, "charlie")],
            )
            .await?;

            let filter = protobuf::llm_memory::data::MemorySearchFilter {
                user_id: Some(10),
                ..Default::default()
            };
            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::FilterOnly,
                    filter: Some(&filter),
                    query_text: None,
                    query_vectors: &[],
                    aggregation: None,
                    hybrid_options: None,
                })
                .await?;
            assert_eq!(out.total, 2);
            assert!(!out.is_truncated);
            assert_eq!(out.mode, CountMode::FilterOnly);
            Ok(())
        })
    }

    /// Spec test #2: FILTER_ONLY without filter returns the global total.
    /// (We can't observe the INFO log from the test without extra setup,
    /// but the structural behaviour — filter omission != error — is
    /// what callers actually rely on.)
    #[test]
    fn p2_count_filter_only_no_filter_global_total() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            count_setup_corpus(&app, pool, dim, &[(1, "x"), (2, "y"), (3, "z")]).await?;

            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::FilterOnly,
                    filter: None,
                    query_text: None,
                    query_vectors: &[],
                    aggregation: None,
                    hybrid_options: None,
                })
                .await?;
            assert_eq!(out.total, 3);
            assert!(!out.is_truncated);
            Ok(())
        })
    }

    /// Spec test #3: TEXT exact count.
    #[test]
    fn p2_count_text_exact() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            count_setup_corpus(
                &app,
                pool,
                dim,
                &[
                    (1, "the quick brown fox"),
                    (1, "another fox in town"),
                    (1, "completely unrelated"),
                ],
            )
            .await?;

            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Text,
                    filter: None,
                    query_text: Some("fox"),
                    query_vectors: &[],
                    aggregation: None,
                    hybrid_options: None,
                })
                .await?;
            assert_eq!(out.total, 2);
            assert!(!out.is_truncated);
            Ok(())
        })
    }

    /// Spec test #3.1: TEXT hard_cap exceeded → is_truncated=true.
    /// `--test-threads=1` is mandatory for the env-var dance below; the
    /// project enforces this at the CI level.
    #[test]
    fn p2_count_text_hard_cap_truncated() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            // SAFETY: env var manipulation is single-threaded under
            // `--test-threads=1`. The cap is read by
            // `ThreadFilterConfig::from_env()` inside `setup_app`, so it
            // must be set BEFORE the setup call.
            unsafe {
                std::env::set_var("MEMORY_FTS_COUNT_HARD_CAP", "2");
            }
            let (app, pool, _db) = setup_app(dim).await?;

            // Five hits.
            count_setup_corpus(
                &app,
                pool,
                dim,
                &[
                    (1, "fox alpha"),
                    (1, "fox bravo"),
                    (1, "fox charlie"),
                    (1, "fox delta"),
                    (1, "fox echo"),
                ],
            )
            .await?;

            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Text,
                    filter: None,
                    query_text: Some("fox"),
                    query_vectors: &[],
                    aggregation: None,
                    hybrid_options: None,
                })
                .await;
            unsafe {
                std::env::remove_var("MEMORY_FTS_COUNT_HARD_CAP");
            }
            let out = out?;
            assert_eq!(out.total, 2);
            assert!(out.is_truncated, "stream past hard_cap must truncate");
            Ok(())
        })
    }

    // Phase 5-2 (P2): VECTOR / HYBRID are now implemented; the
    // `p2_count_vector_hybrid_unimplemented` regression test was retired
    // here. New coverage lives in `p2_count_vector_*` and
    // `p2_count_hybrid_*` below.

    /// Spec test #7: TEXT with empty / unset query rejected.
    #[test]
    fn p2_count_text_empty_query_rejected() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;

            for q in [Some(""), None] {
                let err = app
                    .count_search_matches(CountSearchInput {
                        mode: CountMode::Text,
                        filter: None,
                        query_text: q,
                        query_vectors: &[],
                        aggregation: None,
                        hybrid_options: None,
                    })
                    .await
                    .expect_err("empty / unset query_text must be invalid_argument");
                let msg = format!("{:?}", err);
                assert!(
                    msg.contains("InvalidArgument"),
                    "expected InvalidArgument, got: {msg}"
                );
            }
            Ok(())
        })
    }

    /// Spec test #4: TEXT + filter (role / user) combined.
    #[test]
    fn p2_count_text_with_filter() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            count_setup_corpus(
                &app,
                pool,
                dim,
                &[(10, "fox alpha"), (20, "fox bravo"), (10, "no match")],
            )
            .await?;

            let filter = protobuf::llm_memory::data::MemorySearchFilter {
                user_id: Some(10),
                ..Default::default()
            };
            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Text,
                    filter: Some(&filter),
                    query_text: Some("fox"),
                    query_vectors: &[],
                    aggregation: None,
                    hybrid_options: None,
                })
                .await?;
            assert_eq!(out.total, 1);
            Ok(())
        })
    }

    /// Spec test #13.1: TEXT + thread_filter resolving above
    /// `fts_max_inline_ids` is rejected (BM25 chunking inconsistent).
    #[test]
    fn p2_count_text_thread_filter_inline_ceiling_rejected() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            // SAFETY: --test-threads=1 — pre-set the inline ceiling so a
            // tiny resolved set exceeds it deterministically. The env
            // value is read by `ThreadFilterConfig::from_env()` which
            // runs inside `setup_app()`, so we set it BEFORE the setup
            // call.
            unsafe {
                std::env::set_var("MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS", "1");
            }
            let (app, pool, _db) = setup_app(dim).await?;

            // Build two threads, each with one memory, both labeled
            // "rust" → resolved set len = 2 > 1.
            insert_thread_for_resolve_test(pool, 9_500_001, 1, None, &["rust"], 100, 100).await?;
            insert_thread_for_resolve_test(pool, 9_500_002, 1, None, &["rust"], 200, 200).await?;
            let m1 = create_test_memory(&app.memory_repo, pool, "fox alpha", 1).await?;
            let m2 = create_test_memory(&app.memory_repo, pool, "fox bravo", 1).await?;
            attach_memory_to_thread(pool, 9_500_001, m1, 0).await?;
            attach_memory_to_thread(pool, 9_500_002, m2, 0).await?;

            let filter = protobuf::llm_memory::data::MemorySearchFilter {
                thread_filter: Some(ThreadSearchFilter {
                    labels: vec!["rust".to_string()],
                    ..Default::default()
                }),
                ..Default::default()
            };
            let result = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Text,
                    filter: Some(&filter),
                    query_text: Some("fox"),
                    query_vectors: &[],
                    aggregation: None,
                    hybrid_options: None,
                })
                .await;
            unsafe {
                std::env::remove_var("MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS");
            }
            let err = result
                .expect_err("TEXT count above fts_max_inline_ids must be failed_precondition");
            let msg = format!("{:?}", err);
            assert!(
                msg.contains("FailedPrecondition") && msg.contains("FTS_MAX_INLINE_IDS"),
                "expected FailedPrecondition + FTS_MAX_INLINE_IDS, got: {msg}"
            );
            Ok(())
        })
    }

    // ===== P2 (Phase 5-2): VECTOR / HYBRID count =====

    /// Insert memories whose embeddings are clustered around `base` so the
    /// VECTOR count path has deterministic neighbours to count.
    async fn count_setup_corpus_with_emb(
        app: &MemoryVectorAppImpl,
        pool: &'static infra_utils::infra::rdb::RdbPool,
        base: &[f32],
        items: &[(i64, &str)],
    ) -> anyhow::Result<Vec<i64>> {
        let mut ids = Vec::new();
        for (user, content) in items {
            let id = create_test_memory(&app.memory_repo, pool, content, *user).await?;
            let emb = similar_embedding(base, 0.01);
            app.upsert_embedding(id, &emb, Some("test-model")).await?;
            ids.push(id);
        }
        Ok(ids)
    }

    #[test]
    fn p2_count_vector_single_exact() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 16;
            let (app, pool, _db) = setup_app(dim).await?;
            let base = random_embedding(dim);
            count_setup_corpus_with_emb(
                &app,
                pool,
                &base,
                &[(1, "a"), (1, "b"), (1, "c"), (1, "d"), (1, "e")],
            )
            .await?;

            let qv = vec![base.clone()];
            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Vector,
                    filter: None,
                    query_text: None,
                    query_vectors: &qv,
                    aggregation: None,
                    hybrid_options: None,
                })
                .await?;
            assert_eq!(out.total, 5);
            assert!(!out.is_truncated);
            assert_eq!(out.mode, CountMode::Vector);
            Ok(())
        })
    }

    #[test]
    fn p2_count_vector_single_hard_cap_truncated() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 16;
            // SAFETY: --test-threads=1 — env mutation is single-threaded.
            unsafe {
                std::env::set_var("MEMORY_COUNT_VECTOR_HARD_CAP", "2");
            }
            let (app, pool, _db) = setup_app(dim).await?;
            let base = random_embedding(dim);
            count_setup_corpus_with_emb(
                &app,
                pool,
                &base,
                &[(1, "a"), (1, "b"), (1, "c"), (1, "d"), (1, "e")],
            )
            .await?;

            let qv = vec![base.clone()];
            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Vector,
                    filter: None,
                    query_text: None,
                    query_vectors: &qv,
                    aggregation: None,
                    hybrid_options: None,
                })
                .await;
            unsafe {
                std::env::remove_var("MEMORY_COUNT_VECTOR_HARD_CAP");
            }
            let out = out?;
            assert_eq!(out.total, 2);
            assert!(out.is_truncated);
            Ok(())
        })
    }

    #[test]
    fn p2_count_vector_multi_union_truncated() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 16;
            unsafe {
                std::env::set_var("MEMORY_COUNT_VECTOR_HARD_CAP", "2");
            }
            let (app, pool, _db) = setup_app(dim).await?;
            let base = random_embedding(dim);
            count_setup_corpus_with_emb(
                &app,
                pool,
                &base,
                &[(1, "a"), (1, "b"), (1, "c"), (1, "d"), (1, "e"), (1, "f")],
            )
            .await?;

            // Two query vectors targeting the same cluster — the union
            // still exceeds the cap so `is_truncated` must surface.
            let qv = vec![base.clone(), similar_embedding(&base, 0.005)];
            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Vector,
                    filter: None,
                    query_text: None,
                    query_vectors: &qv,
                    aggregation: None,
                    hybrid_options: None,
                })
                .await;
            unsafe {
                std::env::remove_var("MEMORY_COUNT_VECTOR_HARD_CAP");
            }
            let out = out?;
            assert_eq!(out.total, 2);
            assert!(out.is_truncated);
            Ok(())
        })
    }

    #[test]
    fn p2_count_vector_empty_query_vectors_rejected() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;
            let err = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Vector,
                    filter: None,
                    query_text: None,
                    query_vectors: &[],
                    aggregation: None,
                    hybrid_options: None,
                })
                .await
                .expect_err("VECTOR count with empty query_vectors must be invalid_argument");
            let msg = format!("{:?}", err);
            assert!(
                msg.contains("InvalidArgument"),
                "expected InvalidArgument, got: {msg}"
            );
            Ok(())
        })
    }

    #[test]
    fn p2_count_hybrid_rrf_truncation_propagates() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            unsafe {
                std::env::set_var("MEMORY_COUNT_VECTOR_HARD_CAP", "2");
            }
            let (app, pool, _db) = setup_app(dim).await?;
            let base = random_embedding(dim);
            // Five rows hit both vector and FTS branches → vector branch
            // truncates at cap=2.
            count_setup_corpus_with_emb(
                &app,
                pool,
                &base,
                &[
                    (1, "fox neural network alpha"),
                    (1, "fox neural network beta"),
                    (1, "fox neural network charlie"),
                    (1, "fox neural network delta"),
                    (1, "fox neural network echo"),
                ],
            )
            .await?;

            let qv = vec![base.clone()];
            let opts = HybridOptions {
                strategy: HybridStrategy::Rrf,
                vector_weight: None,
                rrf_k: Some(60.0),
            };
            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Hybrid,
                    filter: None,
                    query_text: Some("fox"),
                    query_vectors: &qv,
                    aggregation: None,
                    hybrid_options: Some(&opts),
                })
                .await;
            unsafe {
                std::env::remove_var("MEMORY_COUNT_VECTOR_HARD_CAP");
            }
            let out = out?;
            assert!(out.is_truncated);
            assert_eq!(out.mode, CountMode::Hybrid);
            Ok(())
        })
    }

    #[test]
    fn p2_count_hybrid_two_phase_uses_primary_truncation() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 32;
            unsafe {
                std::env::set_var("MEMORY_COUNT_VECTOR_HARD_CAP", "2");
            }
            let (app, pool, _db) = setup_app(dim).await?;
            let base = random_embedding(dim);
            count_setup_corpus_with_emb(
                &app,
                pool,
                &base,
                &[
                    (1, "fox a"),
                    (1, "fox b"),
                    (1, "fox c"),
                    (1, "fox d"),
                    (1, "fox e"),
                ],
            )
            .await?;

            let qv = vec![base.clone()];
            let opts = HybridOptions {
                strategy: HybridStrategy::VectorThenFts,
                vector_weight: Some(0.7),
                rrf_k: None,
            };
            let out = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Hybrid,
                    filter: None,
                    query_text: Some("fox"),
                    query_vectors: &qv,
                    aggregation: None,
                    hybrid_options: Some(&opts),
                })
                .await;
            unsafe {
                std::env::remove_var("MEMORY_COUNT_VECTOR_HARD_CAP");
            }
            let out = out?;
            assert_eq!(out.total, 2);
            assert!(out.is_truncated);
            Ok(())
        })
    }

    #[test]
    fn p2_count_hybrid_empty_query_text_rejected() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;
            let qv = vec![random_embedding(dim)];
            let err = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Hybrid,
                    filter: None,
                    query_text: Some(""),
                    query_vectors: &qv,
                    aggregation: None,
                    hybrid_options: None,
                })
                .await
                .expect_err("HYBRID count with empty query_text must be invalid_argument");
            let msg = format!("{:?}", err);
            assert!(
                msg.contains("InvalidArgument"),
                "expected InvalidArgument, got: {msg}"
            );
            Ok(())
        })
    }

    #[test]
    fn p2_count_hybrid_multi_vector_rejected() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;
            let qv = vec![random_embedding(dim), random_embedding(dim)];
            let err = app
                .count_search_matches(CountSearchInput {
                    mode: CountMode::Hybrid,
                    filter: None,
                    query_text: Some("fox"),
                    query_vectors: &qv,
                    aggregation: None,
                    hybrid_options: None,
                })
                .await
                .expect_err("HYBRID count with multi query_vectors must be invalid_argument");
            let msg = format!("{:?}", err);
            assert!(
                msg.contains("InvalidArgument"),
                "expected InvalidArgument, got: {msg}"
            );
            Ok(())
        })
    }

    // ===== compute_memory_highlights: content-type contract =====
    //
    // memory_vector.proto §SearchByText/HybridSearch documents that
    // `highlights` MUST be empty for non-text content
    // (`memory_vector.proto:104`). The dispatcher's `is_embeddable`
    // gate prevents auto-embedding for ContentType ∈ {TOOL, URL,
    // IMAGE, AUDIO, VIDEO, UNSPECIFIED}, but two paths still seed
    // non-text rows into LanceDB and surface them through search:
    //   1. Pre-existing rows from before the embeddable filter was
    //      tightened (Tool/URL records embedded by older deployments).
    //   2. Manual `UpsertEmbedding` RPC calls that bypass the
    //      dispatcher entirely.
    //
    // `MemoryVectorRecord::from_memory_data` copies `data.content`
    // verbatim regardless of `content_type`, so when one of those
    // legacy/manual rows wins a BM25 hit, the body reaches
    // `compute_memory_highlights`. Without the explicit guard below,
    // the wrapper happily produced highlight ranges into a payload
    // the proto contract says clients should not see, and a
    // highlight-aware UI would mark up arbitrary tool output / URL
    // strings as if they were search-matched prose. These tests pin
    // the contract for every non-text variant individually so any
    // future ContentType addition that should also stay un-highlighted
    // is forced to add itself here.

    fn highlight_test_memory(content: &str, content_type: i32) -> Memory {
        Memory {
            id: Some(MemoryId { value: 1 }),
            data: Some(MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: 1 }),
                content: content.to_string(),
                content_type,
                params: None,
                metadata: None,
                created_at: 0,
                updated_at: 0,
                role: MessageRole::RoleUser as i32,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            }),
            media: None,
        }
    }

    fn simple_fts_cfg() -> infra::infra::memory_vector::config::FtsConfig {
        use infra::infra::memory_vector::config::{FtsConfig, FtsTokenizerKind};
        FtsConfig::apply_preset(FtsTokenizerKind::Simple)
    }

    /// Sanity check: TEXT content still produces highlights so the
    /// suppression doesn't accidentally short-circuit the happy path.
    /// This is the regression guard for the wrapper itself, not the
    /// underlying `compute_highlights` (those are exercised in
    /// `infra::memory_vector::highlight::tests`).
    #[test]
    fn compute_memory_highlights_emits_for_text_content() {
        use protobuf::llm_memory::data::ContentType;
        let memory = highlight_test_memory("hello world", ContentType::Text as i32);
        let cfg = simple_fts_cfg();
        let fields = compute_memory_highlights(&memory, Some(("hello", &cfg)));
        assert_eq!(
            fields.len(),
            1,
            "TEXT content must produce one HighlightField"
        );
        assert!(
            !fields[0].ranges.is_empty(),
            "TEXT content with a matching query must have at least one range"
        );
    }

    /// `Tool` rows can leak into the index from the
    /// `UpsertEmbedding` direct path; the wrapper must drop them per
    /// `memory_vector.proto:104`.
    #[test]
    fn compute_memory_highlights_suppresses_tool_content() {
        use protobuf::llm_memory::data::ContentType;
        let memory = highlight_test_memory("hello world", ContentType::Tool as i32);
        let cfg = simple_fts_cfg();
        let fields = compute_memory_highlights(&memory, Some(("hello", &cfg)));
        assert!(
            fields.is_empty(),
            "TOOL content must not be highlighted (proto contract), got {fields:?}"
        );
    }

    /// `Url` content is excluded from auto-embedding (see
    /// `dispatcher::is_embeddable`), but legacy rows / direct upserts
    /// can still reach the index. The proto contract is the same as
    /// for tool content: stay un-highlighted.
    #[test]
    fn compute_memory_highlights_suppresses_url_content() {
        use protobuf::llm_memory::data::ContentType;
        let memory = highlight_test_memory("https://example.com/hello", ContentType::Url as i32);
        let cfg = simple_fts_cfg();
        let fields = compute_memory_highlights(&memory, Some(("hello", &cfg)));
        assert!(
            fields.is_empty(),
            "URL content must not be highlighted, got {fields:?}"
        );
    }

    /// Binary-ish content types — Image / Audio / Video — never have
    /// meaningful textual matches, but a row reaching this code path
    /// with a non-empty `content` (perhaps a caption string the
    /// importer mistakenly stored) must still produce no highlights.
    #[test]
    fn compute_memory_highlights_suppresses_binary_content_types() {
        use protobuf::llm_memory::data::ContentType;
        let cfg = simple_fts_cfg();
        for ct in [ContentType::Image, ContentType::Audio, ContentType::Video] {
            let memory = highlight_test_memory("hello caption", ct as i32);
            let fields = compute_memory_highlights(&memory, Some(("hello", &cfg)));
            assert!(
                fields.is_empty(),
                "ContentType={ct:?} must not be highlighted, got {fields:?}"
            );
        }
    }

    /// Out-of-range `content_type` values (negative, past the last
    /// proto variant) must fail closed: `ContentType::try_from`
    /// rejects them, and the gate must treat the row as non-text.
    /// Otherwise a malformed import or a future enum variant added
    /// only on the client side would produce highlights against the
    /// proto contract.
    #[test]
    fn compute_memory_highlights_suppresses_out_of_range_content_type() {
        let cfg = simple_fts_cfg();
        for ct in [-1_i32, 99_i32, i32::MAX] {
            let memory = highlight_test_memory("hello world", ct);
            let fields = compute_memory_highlights(&memory, Some(("hello", &cfg)));
            assert!(
                fields.is_empty(),
                "out-of-range content_type={ct} must produce no highlights, got {fields:?}"
            );
        }
    }

    /// Memory with `data: None` is a degenerate case (the RDB
    /// schema enforces NOT NULL on the underlying columns, but the
    /// proto wraps `data` in `Option`), and the wrapper already
    /// returned empty for it. Lock that in so the new content-type
    /// gate doesn't accidentally regress the None branch.
    #[test]
    fn compute_memory_highlights_handles_missing_data() {
        let memory = Memory {
            id: Some(MemoryId { value: 1 }),
            data: None,
            media: None,
        };
        let cfg = simple_fts_cfg();
        let fields = compute_memory_highlights(&memory, Some(("hello", &cfg)));
        assert!(fields.is_empty());
    }

    async fn load_memory(repo: &MemoryRepositoryImpl, id: i64) -> anyhow::Result<Memory> {
        let m = repo
            .find(&MemoryId { value: id }, false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("memory {id} not found"))?;
        Ok(m)
    }

    /// Happy path: every non-system memory in the batch gets the five
    /// fields populated, mirroring the search-hit `enrich_hits` test.
    #[test]
    fn enrich_memories_with_thread_info_populates_five_fields() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_460_001, 42, Some("hello world")).await?;
            let m1 = create_test_memory(&app.memory_repo, pool, "alpha", 7).await?;
            let m2 = create_test_memory(&app.memory_repo, pool, "beta", 7).await?;
            attach_memory_to_thread(pool, 9_460_001, m1, 0).await?;
            attach_memory_to_thread(pool, 9_460_001, m2, 1).await?;

            let memories = vec![
                load_memory(&app.memory_repo, m1).await?,
                load_memory(&app.memory_repo, m2).await?,
            ];
            let results = super::enrich_memories_with_thread_info(
                &app.thread_repo,
                &app.thread_memory_repo,
                memories,
            )
            .await?;
            assert_eq!(results.len(), 2);
            for (_, info) in &results {
                assert_eq!(info.thread_id.as_ref().unwrap().value, 9_460_001);
                assert_eq!(info.thread_total, Some(2));
                assert_eq!(info.thread_owner_user_id.as_ref().unwrap().value, 42);
                assert_eq!(info.thread_description.as_deref(), Some("hello world"));
            }
            Ok(())
        })
    }

    /// ROLE_SYSTEM rows must come back with all five fields unset even
    /// when they have a thread attachment, because the proto contract
    /// forbids picking a representative thread for cross-user system
    /// memories.
    #[test]
    fn enrich_memories_with_thread_info_skips_role_system() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_461_001, 1, Some("with sys")).await?;
            // Insert a ROLE_SYSTEM memory directly (the helper hard-codes RoleUser).
            let sys_data = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: 99 }),
                content: "system body".to_string(),
                content_type: protobuf::llm_memory::data::ContentType::Text as i32,
                params: None,
                metadata: None,
                created_at: 0,
                updated_at: 0,
                role: MessageRole::RoleSystem as i32,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = pool.begin().await?;
            let sys_id = app.memory_repo.create(&mut *tx, &sys_data).await?.value;
            tx.commit().await?;
            let user_id_mem = create_test_memory(&app.memory_repo, pool, "user body", 1).await?;
            attach_memory_to_thread(pool, 9_461_001, sys_id, 0).await?;
            attach_memory_to_thread(pool, 9_461_001, user_id_mem, 1).await?;

            let memories = vec![
                load_memory(&app.memory_repo, sys_id).await?,
                load_memory(&app.memory_repo, user_id_mem).await?,
            ];
            let results = super::enrich_memories_with_thread_info(
                &app.thread_repo,
                &app.thread_memory_repo,
                memories,
            )
            .await?;
            assert_eq!(results.len(), 2);
            let (sys_mem, sys_info) = &results[0];
            assert_eq!(sys_mem.id.as_ref().unwrap().value, sys_id);
            assert!(
                sys_info.thread_id.is_none(),
                "ROLE_SYSTEM must not leak thread_id"
            );
            assert!(sys_info.position.is_none());
            assert!(sys_info.thread_total.is_none());
            assert!(sys_info.thread_owner_user_id.is_none());
            assert!(sys_info.thread_description.is_none());

            let (_, user_info) = &results[1];
            assert_eq!(user_info.thread_id.as_ref().unwrap().value, 9_461_001);
            // thread_total counts both rows even though the system one is unset on output
            assert_eq!(user_info.thread_total, Some(2));
            Ok(())
        })
    }

    /// Memory with no thread attachment → all five fields unset.
    #[test]
    fn enrich_memories_with_thread_info_handles_orphan_no_attachment() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            let m = create_test_memory(&app.memory_repo, pool, "lonely", 1).await?;
            // Intentionally do NOT call attach_memory_to_thread.

            let memories = vec![load_memory(&app.memory_repo, m).await?];
            let results = super::enrich_memories_with_thread_info(
                &app.thread_repo,
                &app.thread_memory_repo,
                memories,
            )
            .await?;
            assert_eq!(results.len(), 1);
            let (_, info) = &results[0];
            assert!(info.thread_id.is_none());
            assert!(info.position.is_none());
            assert!(info.thread_total.is_none());
            assert!(info.thread_owner_user_id.is_none());
            assert!(info.thread_description.is_none());
            Ok(())
        })
    }

    /// Memory attached to multiple threads → representative thread is
    /// the one with the smallest id (`MIN(thread_id)` per
    /// `find_positions_for_memories`).
    #[test]
    fn enrich_memories_with_thread_info_picks_min_thread_id() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            // Two threads; the smaller id should win.
            insert_thread_with_description(pool, 9_462_001, 1, Some("smaller")).await?;
            insert_thread_with_description(pool, 9_462_002, 1, Some("larger")).await?;
            let m = create_test_memory(&app.memory_repo, pool, "shared", 1).await?;
            attach_memory_to_thread(pool, 9_462_001, m, 5).await?;
            attach_memory_to_thread(pool, 9_462_002, m, 0).await?;

            let memories = vec![load_memory(&app.memory_repo, m).await?];
            let results = super::enrich_memories_with_thread_info(
                &app.thread_repo,
                &app.thread_memory_repo,
                memories,
            )
            .await?;
            let (_, info) = &results[0];
            assert_eq!(info.thread_id.as_ref().unwrap().value, 9_462_001);
            assert_eq!(info.position, Some(5));
            // thread_total counts only the representative thread's attachments (one).
            assert_eq!(info.thread_total, Some(1));
            assert_eq!(info.thread_description.as_deref(), Some("smaller"));
            Ok(())
        })
    }

    /// `thread.description = NULL` → only `thread_description` is
    /// unset; the other four stay populated so the client keeps jump UI
    /// enabled.
    #[test]
    fn enrich_memories_with_thread_info_null_description_only_description_unset()
    -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, pool, _db) = setup_app(dim).await?;

            insert_thread_with_description(pool, 9_463_001, 9, None).await?;
            let m = create_test_memory(&app.memory_repo, pool, "body", 1).await?;
            attach_memory_to_thread(pool, 9_463_001, m, 5).await?;

            let memories = vec![load_memory(&app.memory_repo, m).await?];
            let results = super::enrich_memories_with_thread_info(
                &app.thread_repo,
                &app.thread_memory_repo,
                memories,
            )
            .await?;
            let (_, info) = &results[0];
            assert_eq!(info.thread_id.as_ref().unwrap().value, 9_463_001);
            assert_eq!(info.position, Some(5));
            assert_eq!(info.thread_total, Some(1));
            assert_eq!(info.thread_owner_user_id.as_ref().unwrap().value, 9);
            assert!(info.thread_description.is_none());
            Ok(())
        })
    }

    /// Empty input must short-circuit without firing any SQL.
    /// We cannot directly observe the SQL count here, but we check that
    /// the call returns an empty Vec without error against a DB state
    /// where the helper SQLs would otherwise return rows (proxy: no
    /// panic, no error, length zero).
    #[test]
    fn enrich_memories_with_thread_info_empty_input_short_circuits() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 8;
            let (app, _pool, _db) = setup_app(dim).await?;

            let results = super::enrich_memories_with_thread_info(
                &app.thread_repo,
                &app.thread_memory_repo,
                Vec::new(),
            )
            .await?;
            assert!(results.is_empty());
            Ok(())
        })
    }

    /// An embeddable image memory hit through search carries the
    /// cacheable half of `Memory.media` (metadata + !unresolved), with
    /// `presigned_url` left for the gRPC layer. Mirrors the Find path.
    #[test]
    fn search_by_text_hydrates_media_for_image_memory() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 16;
            let (app, pool, _db) = setup_app_with_media(dim).await?;
            let media_repo =
                MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
            let (mid, _media_id) = create_test_memory_with_image(
                &app.memory_repo,
                &media_repo,
                pool,
                "a screenshot of the deployment dashboard",
                42,
            )
            .await?;
            app.upsert_embedding(mid, &random_embedding(dim), Some("m"))
                .await?;

            let results = app
                .search_by_text("deployment dashboard", None, None, None, 10, true)
                .await?;
            let hit = results
                .iter()
                .find(|r| r.memory.id.as_ref().map(|i| i.value) == Some(mid))
                .expect("image memory should be a hit");
            let media = hit
                .memory
                .media
                .as_ref()
                .expect("media must be hydrated for an image memory");
            assert!(!media.unresolved, "url backend image is resolvable");
            assert!(
                media.presigned_url.is_none(),
                "presign is the gRPC layer's job"
            );
            let meta = media.metadata.as_ref().expect("metadata present");
            assert_eq!(meta.media_type, "image/png");
            Ok(())
        })
    }

    /// No media subsystem wired (env-less / non-media deployment): search
    /// hits leave `media = None`, exactly as before this change.
    #[test]
    fn search_no_media_subsystem_leaves_media_none() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 16;
            let (app, pool, _db) = setup_app(dim).await?;
            let media_repo =
                MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
            let (mid, _media_id) = create_test_memory_with_image(
                &app.memory_repo,
                &media_repo,
                pool,
                "an image memory without media wiring",
                43,
            )
            .await?;
            app.upsert_embedding(mid, &random_embedding(dim), Some("m"))
                .await?;

            let results = app
                .search_by_text("image memory", None, None, None, 10, true)
                .await?;
            let hit = results
                .iter()
                .find(|r| r.memory.id.as_ref().map(|i| i.value) == Some(mid))
                .expect("memory should still be a hit");
            assert!(
                hit.memory.media.is_none(),
                "no media subsystem => media stays None (backward compatible)"
            );
            Ok(())
        })
    }

    /// An unresolvable media (migration placeholder: no body, gc_state=3)
    /// hydrates with `unresolved=true` and no presign — a normal
    /// recovering state, not an error / panic.
    #[test]
    fn search_unresolvable_media_sets_unresolved_no_panic() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 16;
            let (app, pool, _db) = setup_app_with_media(dim).await?;
            let media_repo =
                MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
            let (mid, _media_id) = create_test_memory_with_unresolvable(
                &app.memory_repo,
                &media_repo,
                pool,
                "an elided image awaiting recovery",
                44,
            )
            .await?;
            app.upsert_embedding(mid, &random_embedding(dim), Some("m"))
                .await?;

            let results = app
                .search_by_text("elided image", None, None, None, 10, true)
                .await?;
            let hit = results
                .iter()
                .find(|r| r.memory.id.as_ref().map(|i| i.value) == Some(mid))
                .expect("memory should be a hit");
            let media = hit
                .memory
                .media
                .as_ref()
                .expect("metadata still hydrated for unresolvable");
            assert!(media.unresolved, "unresolvable => unresolved=true");
            assert!(media.presigned_url.is_none());
            let meta = media.metadata.as_ref().expect("metadata present");
            assert!(meta.sha256.is_some(), "sha256 survives for recovery");
            Ok(())
        })
    }

    /// A dangling media_object_id (row deleted out from under the memory)
    /// leaves `media = None` — the memory still renders; the NotFound is
    /// swallowed, not propagated.
    #[test]
    fn search_dangling_media_id_returns_memory_without_media() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let dim = 16;
            let (app, pool, _db) = setup_app_with_media(dim).await?;
            let media_repo =
                MediaObjectRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
            let (mid, media_id) = create_test_memory_with_image(
                &app.memory_repo,
                &media_repo,
                pool,
                "a memory whose media row will vanish",
                45,
            )
            .await?;
            app.upsert_embedding(mid, &random_embedding(dim), Some("m"))
                .await?;
            // Delete the media_object row, leaving memory.media_object_id
            // dangling.
            let mut tx = pool.begin().await?;
            media_repo
                .delete_if_unreferenced_tx(&mut *tx, media_id)
                .await?;
            tx.commit().await?;

            let results = app
                .search_by_text("media row will vanish", None, None, None, 10, true)
                .await?;
            let hit = results
                .iter()
                .find(|r| r.memory.id.as_ref().map(|i| i.value) == Some(mid))
                .expect("memory should still be a hit");
            assert!(
                hit.memory.media.is_none(),
                "dangling media_object_id => media None (NotFound swallowed)"
            );
            Ok(())
        })
    }
}
