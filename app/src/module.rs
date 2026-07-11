use crate::app::memory::MemoryAppImpl;
use crate::app::memory_rating::MemoryRatingAppImpl;
use crate::app::reflection::ReflectionAppImpl;
use crate::app::thread::ThreadAppImpl;

use infra::infra::module::RepositoryModule;
use protobuf::llm_memory::data::{Memory, MemoryRating, Thread};
use std::sync::Arc;

/// Outcome of pre-init for an embedding dispatcher. Encodes the rule
/// "transient init failure must not silently disable the dispatcher"
/// in a value the caller can inspect rather than as a control-flow
/// branch buried in `new_by_env`. Tests can assert directly on this
/// outcome to pin down the regression that motivated this PR.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DispatcherInitOutcome {
    /// `ensure_initialized` succeeded; dispatcher is ready and `Some`.
    Ready,
    /// `ensure_initialized` returned `Err`. The dispatcher MUST still
    /// be retained as `Some` so the lazy `OnceCell` retry path inside
    /// the dispatcher core can recover on the next `dispatch()` call.
    /// This is the regression class — previous code dropped to `None`,
    /// permanently disabling auto-embedding after a flaky jobworkerp
    /// startup.
    InitDeferred,
}

/// Decide what to do with a freshly constructed dispatcher: log
/// outcome, then declare it Ready or InitDeferred. The function never
/// drops the dispatcher; the caller wraps `Some(dispatcher)` regardless
/// of outcome. Extracted from `AppModule::new_by_env` so the
/// "init-error keeps Some" invariant is checkable in isolation without
/// standing up a real jobworkerp / OnceCell.
pub(crate) async fn classify_dispatcher_init<F>(label: &str, init: F) -> DispatcherInitOutcome
where
    F: std::future::Future<Output = anyhow::Result<()>>,
{
    match init.await {
        Ok(()) => {
            tracing::info!("{label} initialized");
            DispatcherInitOutcome::Ready
        }
        Err(e) => {
            tracing::warn!("{label} init deferred (will retry on first dispatch): {e:#}");
            DispatcherInitOutcome::InitDeferred
        }
    }
}

fn auto_embedding_config_startup_error(
    error: anyhow::Error,
) -> infra::infra::startup_error::StartupError {
    infra::infra::startup_error::StartupError::ConfigLoadFailed {
        component: "auto-embedding dispatcher".into(),
        message: format!("{error:#}"),
    }
}

pub struct AppModule {
    pub memory_app: MemoryAppImpl,
    pub memory_rating_app: MemoryRatingAppImpl,
    pub media_app: Arc<crate::app::media::MediaAppImpl>,
    pub thread_app: ThreadAppImpl,
    pub reflection_app: ReflectionAppImpl,
    pub memory_vector_app: Option<crate::app::memory_vector::MemoryVectorAppImpl>,
    pub thread_vector_app: Option<Arc<crate::app::thread_vector::ThreadVectorAppImpl>>,
}

impl AppModule {
    pub async fn new_by_env(mut repositories: RepositoryModule) -> Self {
        let mc_config = envy::prefixed("MEMORY_CACHE_")
            .from_env::<memory_utils::cache::stretto::MemoryCacheConfig>()
            .unwrap_or_else(|e| {
                infra::infra::startup_error::StartupError::ConfigLoadFailed {
                    component: "MemoryCacheConfig (MEMORY_CACHE_*)".into(),
                    message: format!("{e:#}"),
                }
                .fatal()
            });
        let memory_cache =
            memory_utils::cache::stretto::new_memory_cache::<Arc<String>, Memory>(&mc_config);
        // MemoryApp and ThreadApp must share the same thread cache so that
        // `delete_memory` can invalidate defaults cleared through direct SQL.
        let thread_cache =
            memory_utils::cache::stretto::new_memory_cache::<Arc<String>, Thread>(&mc_config);
        // Build a separate MemoryRepositoryImpl for ThreadAppImpl (cascade delete needs it).
        let memory_repository_for_thread = repositories.create_memory_repository();
        // Build separate MemoryRatingRepositoryImpl instances for cascade delete in MemoryApp and ThreadApp.
        let memory_rating_repository_for_cascade = repositories.create_memory_rating_repository();
        let rating_repository_for_thread = repositories.create_memory_rating_repository();
        // ThreadMemoryRepositoryImpl for MemoryApp (cascade delete of junction rows).
        let thread_memory_for_memory = repositories.create_thread_memory_repository();
        // ThreadRepositoryImpl for MemoryApp (clear stale default_system_memory_id on delete).
        let thread_repository_for_memory = repositories.create_thread_repository();

        // Snapshot the shared id generator before any `repositories.*`
        // field is moved (MediaApp needs it but is built after
        // `repositories.media_object_repository` is moved out).
        let media_id_generator = repositories.id_generator();
        // Extract vector repo before repositories fields are moved.
        // create_memory_repository() borrows self immutably, so call it first.
        let vector_memory_repo = repositories.create_memory_repository();
        // MemoryVectorApp uses ThreadMemoryRepository only for pivot-membership
        // validation in `get_surrounding_memories`; a dedicated instance keeps
        // its dependency story independent from ThreadApp's own junction repo.
        let vector_thread_memory_repo = repositories.create_thread_memory_repository();
        // Initialize auto-embedding dispatcher if enabled.
        //
        // The callback env validation (`require_grpc_callback_env`) is
        // intentionally NOT run here. Constructing an `AppModule` is a
        // dependency of *every* memories binary — including
        // `memories-import --skip-embeddings` and `--dry-run`, which
        // never enqueue an embedding job and have no need for the
        // callback host/port. Pushing the validation up to whoever
        // actually needs the dispatcher (grpc-admin always, importer
        // only when embeddings will be redispatched) keeps the failure
        // mode aligned with the failure surface and avoids a regression
        // where import-only runs die on missing env that they would
        // never use.
        let auto_embedding_enabled = std::env::var("MEMORY_AUTO_EMBEDDING_ENABLED")
            .unwrap_or_default()
            .eq_ignore_ascii_case("true");
        let embedding_dispatcher: Option<
            Arc<infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher>,
        > = if auto_embedding_enabled {
            match infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher::from_env() {
                Ok(d) => {
                    // Eager init surfaces YAML / jobworkerp / schema errors
                    // at startup as a warning. The dispatcher is retained
                    // as `Some` regardless of outcome — `classify_dispatcher_init`
                    // never drops the dispatcher — so the lazy `OnceCell`
                    // retry path can recover on the next `dispatch()` call.
                    // Dropping to `None` here would permanently disable
                    // auto-embedding after a transient jobworkerp startup
                    // delay (regression fix).
                    let _ = classify_dispatcher_init(
                        "Auto-embedding dispatcher",
                        d.ensure_initialized(),
                    )
                    .await;
                    Some(Arc::new(d))
                }
                Err(e) => {
                    // Config error (env / YAML path) is not transient —
                    // the dispatcher cannot even be constructed, so
                    // `from_env` returning Err means the operator must
                    // fix configuration before serving writes.
                    auto_embedding_config_startup_error(e).fatal()
                }
            }
        } else {
            None
        };

        // Initialize thread description embedding dispatcher (shares config with memory embedding)
        let thread_embedding_dispatcher: Option<
            Arc<infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher>,
        > = if auto_embedding_enabled {
            match infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher::from_env()
            {
                Ok(d) => {
                    // Same retry rationale as `embedding_dispatcher` above.
                    let _ = classify_dispatcher_init(
                        "Thread auto-embedding dispatcher",
                        d.ensure_initialized(),
                    )
                    .await;
                    Some(Arc::new(d))
                }
                Err(e) => auto_embedding_config_startup_error(e).fatal(),
            }
        } else {
            None
        };

        // P5 (improve-search Phase 5-1): MemoryVectorAppImpl now resolves
        // `MemorySearchFilter.thread_filter` server-side via the thread /
        // thread_label repositories, so dedicated instances are constructed
        // here and handed to the constructor. The instances are independent
        // from those owned by ThreadVectorAppImpl below — same pattern as
        // `vector_thread_memory_repo` above — so the vector-search
        // dependency story stays separate from the thread-side surface.
        let memory_vector_thread_repo = repositories.create_thread_repository();
        let memory_vector_thread_label_repo = repositories.create_thread_label_repository();
        // Dedicated media_object repo so `redispatch_embeddings` can resolve
        // each memory's linked media (kind / storage_backend) and drive the
        // Media dispatch axis. Created here (before `media_object_repository`
        // is moved into MediaApp below) following the same "create before
        // move" discipline as `vector_memory_repo`. Without this wiring
        // `redispatch_embeddings` stays text-only and skips every
        // image-bearing memory under `kinds=[MEDIA]`.
        let memory_vector_media_repo = repositories.create_media_object_repository();
        // Second media_object sibling: feeds the search-result media
        // enrich (cacheable half of Memory.media), independent of the
        // redispatch resolver above. Created before media_object_repository
        // is moved into MediaApp (same create-before-move discipline). The
        // MemoryVectorApp itself is assembled AFTER `media_app_arc` exists
        // (it needs the finalizer); the LanceDB repo is taken here and
        // carried in an Option so the take()/map happens before the move.
        let memory_vector_enrich_media_repo = repositories.create_media_object_repository();
        let memory_vector_repo_opt = repositories.memory_vector_repository.take();

        // Create thread_vector_app before fields are moved
        let thread_vector_app = repositories
            .thread_vector_repository
            .take()
            .map(|vector_repo| {
                crate::app::thread_vector::ThreadVectorAppImpl::new(
                    repositories.create_thread_repository(),
                    repositories.create_thread_label_repository(),
                    vector_repo,
                    thread_embedding_dispatcher.clone(),
                )
            });

        // Wrap in Arc so it can be shared between ThreadAppImpl and AppModule
        let thread_vector_app_arc: Option<Arc<crate::app::thread_vector::ThreadVectorAppImpl>> =
            thread_vector_app.map(Arc::new);

        // Reflection dispatchers — gated separately from MEMORY_AUTO_*.
        // The reflection workflow YAML lands in Phase F; until then,
        // `MEMORY_REFLECTION_DISPATCH_ENABLED` defaults to false so
        // reflection memories sit at status=PENDING and get picked up
        // later via RedispatchReflectionEmbeddings (commit 4).
        let reflection_dispatch_enabled = std::env::var("MEMORY_REFLECTION_DISPATCH_ENABLED")
            .unwrap_or_default()
            .eq_ignore_ascii_case("true");
        let reflection_summary_dispatcher: Option<
            Arc<infra::infra::reflection_summary_dispatch::ReflectionSummaryDispatcher>,
        > = if reflection_dispatch_enabled {
            match infra::infra::reflection_summary_dispatch::ReflectionSummaryDispatcher::from_env()
            {
                Ok(d) => {
                    let _ = classify_dispatcher_init(
                        "Reflection summary dispatcher",
                        d.ensure_initialized(),
                    )
                    .await;
                    Some(Arc::new(d))
                }
                Err(e) => {
                    tracing::warn!("Reflection summary dispatcher disabled: config error: {e}");
                    None
                }
            }
        } else {
            None
        };
        let reflection_intent_dispatcher: Option<
            Arc<infra::infra::reflection_intent_dispatch::ReflectionIntentDispatcher>,
        > = if reflection_dispatch_enabled {
            match infra::infra::reflection_intent_dispatch::ReflectionIntentDispatcher::from_env() {
                Ok(d) => {
                    let _ = classify_dispatcher_init(
                        "Reflection intent dispatcher",
                        d.ensure_initialized(),
                    )
                    .await;
                    Some(Arc::new(d))
                }
                Err(e) => {
                    tracing::warn!("Reflection intent dispatcher disabled: config error: {e}");
                    None
                }
            }
        } else {
            None
        };

        // Shared jobworkerp client used by Generate (F-G1) and
        // FindSimilarByIntentText (F-S8). Constructed whenever
        // `JOBWORKERP_ADDR` is set — Generate's kill switch
        // (`MEMORY_REFLECTION_DISPATCH_ENABLED`) is checked separately
        // inside `generate::generate`, so flipping it off must NOT take
        // F-S8 search down with it: the search RPC only needs the
        // embedding worker, which is independent of whether reflection
        // workflow dispatch is allowed. A 30-second connect timeout
        // matches `EmbeddingDispatcherCore::get_or_init` so startup
        // failure modes stay consistent across dispatchers.
        //
        // `JOBWORKERP_ADDR` absence is the recognised "no jobworkerp
        // here" signal — `new_by_env` would panic on an unset value, so
        // we pre-check and degrade to the `Unimplemented` path that
        // both call sites already handle.
        let reflection_jobworkerp_client: Option<
            Arc<jobworkerp_client::client::wrapper::JobworkerpClientWrapper>,
        > = if std::env::var("JOBWORKERP_ADDR").is_err() {
            tracing::info!(
                "JOBWORKERP_ADDR is not set; Generate / FindSimilarByIntentText will return \
                 Unimplemented."
            );
            None
        } else {
            match jobworkerp_client::client::wrapper::JobworkerpClientWrapper::new_by_env(Some(30))
                .await
            {
                Ok(c) => Some(Arc::new(c)),
                Err(e) => {
                    tracing::warn!(
                        "Reflection jobworkerp client unavailable; Generate / \
                         FindSimilarByIntentText will return Unimplemented: {e}"
                    );
                    None
                }
            }
        };

        let reflection_pool = repositories.pool();
        let reflection_intent_vector_repo = repositories.reflection_intent_vector_repository.take();
        let reflection_app = ReflectionAppImpl::new(
            reflection_pool,
            repositories.create_memory_repository(),
            repositories.create_thread_repository(),
            repositories.create_thread_memory_repository(),
            repositories.create_thread_label_repository(),
            repositories.create_reflection_aggregate_thread_repository(),
            repositories.create_reflection_index_repository(),
            repositories.create_reflection_failure_mode_repository(),
            repositories.create_reflection_tool_repository(),
            repositories.create_reflection_tool_outcome_repository(),
            repositories.create_reflection_fact_repository(),
            repositories.create_reflection_applied_target_repository(),
            repositories.create_reflection_few_shot_usage_repository(),
            repositories.create_reflection_stats_repository(),
            repositories.create_reflection_dictionary_repository(),
            repositories.create_reflection_signature_norm_repository(),
            reflection_intent_vector_repo,
            embedding_dispatcher
                .clone()
                .map(|d| d as Arc<dyn infra::infra::memory_vector::dispatcher::EmbeddingDispatch>),
            reflection_summary_dispatcher,
            reflection_intent_dispatcher,
            reflection_jobworkerp_client,
            // Share the same Stretto handle MemoryAppImpl will own
            // below; cloning here is cheap (Arc-counted) and lets the
            // reflection cascade invalidate `memory_id:<id>` entries
            // before the 30s TTL would otherwise expose stale rows
            // to a same-process `MemoryApp::find_memory`.
            Some(memory_cache.clone()),
        )
        .await;

        // Build the MediaApp first (as an Arc) so MemoryAppImpl can share
        // the very same instance for post-commit `finish_delete` and the
        // gRPC layer can share it too (no second storage backend). Create
        // every `repositories.create_*()` sibling MemoryApp needs BEFORE
        // `repositories.media_object_repository` is moved into MediaApp —
        // a `create_*` immutable borrow is impossible once any field is
        // partially moved (same pattern as the thread_label sibling).
        let memory_media_object_repo = repositories.create_media_object_repository();
        // Sibling for ThreadApp's batch path: AddMemoriesBatch (used by
        // agent-chat-import) must bump media_object.ref_count in the
        // memory-insert tx, else imported images stay orphaned
        // (ref_count=0, gc_state=1) and the deferred GC deletes them.
        // Same "create before move" discipline.
        let thread_media_object_repo = repositories.create_media_object_repository();
        let thread_label_for_memory = repositories.create_thread_label_repository();
        let media_app_arc: Arc<crate::app::media::MediaAppImpl> = {
            use infra::infra::media_storage::{MediaConfig, StorageBackend};
            let mcfg = MediaConfig::from_env();
            // Fail-fast on a misconfigured backend: a broken media
            // backend should surface at startup, not on the first
            // Upload (consistent with the project's lazy-init panic
            // convention for other subsystems).
            let storage = Arc::new(StorageBackend::from_env(&mcfg).unwrap_or_else(|e| {
                infra::infra::startup_error::StartupError::EnvVarInvalid {
                    name: "MEDIA_STORAGE_BACKEND".into(),
                    message: format!("{e:#}"),
                }
                .fatal()
            }));
            Arc::new(crate::app::media::MediaAppImpl::new(
                repositories.media_object_repository,
                storage,
                media_id_generator,
                mcfg.s3_prefix,
                mcfg.presign_ttl_sec,
                mcfg.upload_max_bytes,
            ))
        };

        // Assembled here (not at the take() site above) because
        // `with_media` needs the `media_app_arc` finalizer that only
        // exists now. `with_media_resolver` (redispatch axis) and
        // `with_media` (search-result enrich) wire two independent
        // media_object siblings created before the move.
        let memory_vector_app = memory_vector_repo_opt.map(|vector_repo| {
            crate::app::memory_vector::MemoryVectorAppImpl::new(
                vector_memory_repo,
                vector_repo,
                vector_thread_memory_repo,
                memory_vector_thread_repo,
                memory_vector_thread_label_repo,
                embedding_dispatcher.clone(),
            )
            .with_media_resolver(memory_vector_media_repo)
            .with_media(memory_vector_enrich_media_repo, media_app_arc.clone())
        });

        AppModule {
            memory_app: {
                // `thread_label_for_memory` (P8) and
                // `memory_media_object_repo` were created above, before any
                // `repositories` field was partially moved into MediaApp.
                MemoryAppImpl::new(
                    repositories.memory_repository,
                    memory_rating_repository_for_cascade,
                    thread_repository_for_memory,
                    thread_memory_for_memory,
                    thread_label_for_memory,
                    thread_cache.clone(),
                    memory_cache.clone(),
                    embedding_dispatcher.clone(),
                )
                // Wire the media subsystem so create/update/delete keep
                // media_object.ref_count consistent in the memory tx.
                .with_media(memory_media_object_repo, media_app_arc.clone())
            },
            memory_rating_app: MemoryRatingAppImpl::new(
                repositories.memory_rating_repository,
                memory_utils::cache::stretto::new_memory_cache::<Arc<String>, MemoryRating>(
                    &mc_config,
                ),
            ),
            media_app: media_app_arc.clone(),
            thread_app: ThreadAppImpl::new(
                repositories.thread_repository,
                repositories.thread_memory_repository,
                repositories.thread_label_repository,
                memory_repository_for_thread,
                rating_repository_for_thread,
                thread_cache,
                memory_cache,
                embedding_dispatcher,
                thread_embedding_dispatcher,
                thread_vector_app_arc.clone(),
            )
            // Keep media_object.ref_count consistent across the thread
            // path: bump on batch import, decrement + finalize on
            // thread delete (image memory feature).
            .with_media(thread_media_object_repo, media_app_arc.clone()),
            reflection_app,
            memory_vector_app,
            thread_vector_app: thread_vector_app_arc,
        }
    }
}

#[cfg(test)]
mod dispatcher_init_outcome_tests {
    //! Pin the regression fix: `AppModule::new_by_env` must NOT drop a
    //! dispatcher to `None` when its eager init returns `Err`. The check
    //! lives on `classify_dispatcher_init` because the helper carries
    //! the only branch that distinguishes Ready vs InitDeferred — every
    //! caller in `new_by_env` wraps `Some(Arc::new(d))` directly after,
    //! so as long as the helper never says "drop me", the invariant
    //! holds. Refactoring `new_by_env` to once again drop the dispatcher
    //! on init error would force this test (and the `InitDeferred`
    //! variant) to be deleted — making the regression visible at code
    //! review time, not at runtime.

    use super::*;

    #[tokio::test]
    async fn ok_init_classifies_as_ready() {
        let outcome =
            classify_dispatcher_init("test-dispatcher", async { Ok::<(), anyhow::Error>(()) })
                .await;
        assert_eq!(outcome, DispatcherInitOutcome::Ready);
    }

    #[tokio::test]
    async fn err_init_classifies_as_init_deferred_not_drop() {
        let outcome = classify_dispatcher_init("test-dispatcher", async {
            Err::<(), anyhow::Error>(anyhow::anyhow!("simulated transient init failure"))
        })
        .await;
        // The outcome enum has no "Drop" variant by construction. If a
        // future refactor added one and routed Err there, this assertion
        // would force a conscious decision rather than silently changing
        // the on-Err behaviour.
        assert_eq!(outcome, DispatcherInitOutcome::InitDeferred);
    }

    #[test]
    fn configuration_errors_are_promoted_to_fatal_startup_errors() {
        let error = auto_embedding_config_startup_error(anyhow::anyhow!("invalid prefix"));
        assert!(matches!(
            error,
            infra::infra::startup_error::StartupError::ConfigLoadFailed { .. }
        ));
    }
}
