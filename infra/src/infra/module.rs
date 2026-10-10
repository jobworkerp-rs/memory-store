use super::{
    IdGeneratorWrapper, media_object::rdb::MediaObjectRepositoryImpl,
    memory::rdb::MemoryRepositoryImpl, memory_rating::rdb::MemoryRatingRepositoryImpl,
    reflection::aggregate_thread::ThreadAggregateKeyRepositoryImpl,
    reflection::applied_target::ReflectionAppliedTargetRepositoryImpl,
    reflection::dictionary::FailureModeDictionaryRepositoryImpl,
    reflection::fact::ReflectionFactRepositoryImpl,
    reflection::failure_mode::ReflectionFailureModeRepositoryImpl,
    reflection::few_shot_usage::ReflectionFewShotUsageRepositoryImpl,
    reflection::rdb::ThreadReflectionIndexRepositoryImpl,
    reflection::signature_norm::FailureSignatureIndicatorNormRepositoryImpl,
    reflection::stats::ReflectionStatsRepositoryImpl,
    reflection::tool::ReflectionToolRepositoryImpl,
    reflection::tool_outcome::ReflectionToolOutcomeRepositoryImpl, startup_error::StartupError,
    thread::rdb::ThreadRepositoryImpl, thread_group::audit::ThreadGroupAuditRepositoryImpl,
    thread_group::candidate::ThreadGroupCandidateAssociationRepositoryImpl,
    thread_group::canonical_key::ThreadCanonicalKeyRepositoryImpl,
    thread_group::collection::ManualCollectionRepositoryImpl,
    thread_group::deletion_marker::ThreadDeletionMarkerRepositoryImpl,
    thread_group::group::ThreadGroupRepositoryImpl,
    thread_group::lock::ThreadGroupLockRepositoryImpl,
    thread_group::member::ThreadGroupMemberRepositoryImpl,
    thread_group::observation::ThreadObservationRepositoryImpl,
    thread_group::operator_decision::OperatorDecisionRepositoryImpl,
    thread_group::outbox::ThreadGroupEventOutboxRepositoryImpl,
    thread_group::relation::ThreadRelationRepositoryImpl,
    thread_group::source_identity::SourceThreadIdentityRepositoryImpl,
    thread_label::rdb::ThreadLabelRepositoryImpl, thread_memory::rdb::ThreadMemoryRepositoryImpl,
};
use infra_utils::infra::rdb::RdbPool;
use std::sync::Arc;

/// Inspect an `anyhow::Error` produced by a LanceDB-init call and route
/// to the right `StartupError::fatal()` branch. Used by every vector
/// repository bootstrap below — the structured `LancedbSchemaMismatch`
/// is constructed inside `verify_table_schema_or_fail` and wrapped in
/// `anyhow::Error`, so we downcast here; everything else is reported as
/// `LancedbInitFailed { uri }` (kept distinct from `StartupError::Other`
/// so agent-app can still attribute the failure to the LanceDB path
/// even when the underlying cause is non-schema, e.g. permission /
/// disk full).
fn fatal_lancedb_init_error(uri: &str, e: anyhow::Error) -> ! {
    e.downcast::<StartupError>()
        .unwrap_or_else(|other| StartupError::LancedbInitFailed {
            uri: uri.to_string(),
            message: format!("{other:#}"),
        })
        .fatal()
}

/// Open one embedding index per LanceDB directory and attach it to the
/// vector repositories in that directory.
async fn attach_embedding_indexes(
    memory: Option<super::memory_vector::repository::MemoryVectorRepositoryImpl>,
    thread: Option<super::thread_vector::repository::ThreadVectorRepositoryImpl>,
    intent: Option<super::reflection_intent_vector::repository::ReflectionIntentVectorRepository>,
) -> (
    Option<super::memory_vector::repository::MemoryVectorRepositoryImpl>,
    Option<super::thread_vector::repository::ThreadVectorRepositoryImpl>,
    Option<super::reflection_intent_vector::repository::ReflectionIntentVectorRepository>,
) {
    use super::embedding_index::EmbeddingIndex;
    let mut opened: std::collections::HashMap<String, EmbeddingIndex> =
        std::collections::HashMap::new();
    let mut index_for = async |uri: &str| -> EmbeddingIndex {
        if let Some(index) = opened.get(uri) {
            return index.clone();
        }
        let index = EmbeddingIndex::open(uri)
            .await
            .unwrap_or_else(|e| fatal_lancedb_init_error(uri, e));
        opened.insert(uri.to_string(), index.clone());
        index
    };
    let memory = match memory {
        Some(r) => {
            let index = index_for(r.uri()).await;
            Some(r.with_embedding_index(index))
        }
        None => None,
    };
    let thread = match thread {
        Some(r) => {
            let index = index_for(r.uri()).await;
            Some(r.with_embedding_index(index))
        }
        None => None,
    };
    let intent = match intent {
        Some(r) => {
            let index = index_for(r.uri()).await;
            Some(r.with_embedding_index(index))
        }
        None => None,
    };
    (memory, thread, intent)
}

/// Resolve the configured embedding space and run the startup decision
/// over the enabled vector tables. Any failure stops startup.
#[allow(clippy::too_many_arguments)]
async fn bootstrap_embedding_space(
    pool: &'static RdbPool,
    id_generator: &IdGeneratorWrapper,
    memory: Option<&super::memory_vector::repository::MemoryVectorRepositoryImpl>,
    thread: Option<&super::thread_vector::repository::ThreadVectorRepositoryImpl>,
    intent: Option<&super::reflection_intent_vector::repository::ReflectionIntentVectorRepository>,
    vector_size: usize,
    distance: super::memory_vector::config::DistanceType,
) -> super::embedding_space::bootstrap::SpaceState {
    use super::embedding_space::bootstrap::{VectorTable, run};
    use super::embedding_space::rdb_targets::{RdbTargetSources, rdb_has_embedding_target};

    if let Err(e) = super::embedding_space::workers::validate_base_name(
        super::embedding_dispatch::MM_EMBEDDING_WORKER_ENV,
        &super::embedding_dispatch::mm_embedding_worker_base(),
    ) {
        e.fatal();
    }
    let workers_yaml = super::memory_vector::dispatcher::workers_yaml_path_from_env();
    let current = super::embedding_space::SpaceComponents::resolve(
        &workers_yaml,
        u32::try_from(vector_size).unwrap_or(u32::MAX),
        distance.as_str(),
    )
    .unwrap_or_else(|e| {
        StartupError::ConfigLoadFailed {
            component: "embedding space (MEMORY_WORKERS_YAML)".into(),
            message: format!("{e:#}"),
        }
        .fatal()
    });

    let chunking =
        super::embedding_space::chunking_fingerprint(&workers_yaml).unwrap_or_else(|e| {
            StartupError::ConfigLoadFailed {
                component: "embedding chunking (MEMORY_WORKERS_YAML)".into(),
                message: format!("{e:#}"),
            }
            .fatal()
        });

    let mut tables = Vec::new();
    if let Some(r) = memory {
        tables.push(VectorTable {
            label: "memory",
            table: r.table_handle(),
        });
    }
    if let Some(r) = thread {
        tables.push(VectorTable {
            label: "thread",
            table: r.table_handle(),
        });
    }
    if let Some(r) = intent {
        tables.push(VectorTable {
            label: "reflection_intent",
            table: r.table_handle(),
        });
    }

    verify_storage_set(pool, &tables, memory, thread, intent).await;

    let memory_repo = MemoryRepositoryImpl::new(id_generator.clone(), pool);
    let media_repo = MediaObjectRepositoryImpl::new(id_generator.clone(), pool);
    let thread_repo = ThreadRepositoryImpl::new(id_generator.clone(), pool);
    let sources = RdbTargetSources {
        memories: (memory.is_some() || intent.is_some()).then_some((&memory_repo, &media_repo)),
        threads: thread.is_some().then_some(&thread_repo),
        image_search_mode: super::embedding_dispatch::ImageSearchMode::from_env(),
    };
    let state = run(&tables, &current, &chunking, || {
        rdb_has_embedding_target(&sources)
    })
    .await
    .unwrap_or_else(|e| StartupError::fatal_anyhow("embedding_space", e));
    tracing::info!(
        space_id = %current.space_id(),
        rebuilding_attempt = ?state.rebuilding_attempt,
        "embedding space verified"
    );
    super::embedding_space::workers::set_current_space(Some(current.space_id()));
    super::embedding_space::token::set_rebuilding_attempt(state.rebuilding_attempt.clone());
    super::embedding_space::token::set_legacy_accept(state.accepts_legacy_writes());
    state
}

/// Check that the RDB, the LanceDB directories, and the embedding state
/// directory belong together, recording identifiers on first use.
async fn verify_storage_set(
    pool: &'static RdbPool,
    tables: &[super::embedding_space::bootstrap::VectorTable],
    memory: Option<&super::memory_vector::repository::MemoryVectorRepositoryImpl>,
    thread: Option<&super::thread_vector::repository::ThreadVectorRepositoryImpl>,
    intent: Option<&super::reflection_intent_vector::repository::ReflectionIntentVectorRepository>,
) {
    use super::embedding_space::storage;
    let rdb_id = storage::read_rdb_id(pool).await.unwrap_or_else(|e| {
        StartupError::Other {
            component: "embedding storage identity".into(),
            message: format!("{e:#}"),
        }
        .fatal()
    });
    let uri_of = |label: &str| -> &str {
        match label {
            "memory" => memory.map(|r| r.uri()),
            "thread" => thread.map(|r| r.uri()),
            _ => intent.map(|r| r.uri()),
        }
        .expect("a table is only listed when its repository exists")
    };
    let stores: Vec<storage::StoreTable> = tables
        .iter()
        .map(|t| storage::StoreTable {
            table: t,
            uri: uri_of(t.label),
        })
        .collect();
    storage::verify_and_record(rdb_id, &storage::state_dir_from_env(), &stores)
        .await
        .unwrap_or_else(|e| StartupError::fatal_anyhow("embedding storage identity", e));
}

// module for DI
pub struct RepositoryModule {
    pub memory_repository: MemoryRepositoryImpl,
    pub memory_rating_repository: MemoryRatingRepositoryImpl,
    pub media_object_repository: MediaObjectRepositoryImpl,
    pub thread_repository: ThreadRepositoryImpl,
    pub thread_memory_repository: ThreadMemoryRepositoryImpl,
    pub thread_label_repository: ThreadLabelRepositoryImpl,

    // ThreadGroup RDB repositories (schema 20260920000001, design
    // section 5). All RDB-only; no LanceDB involvement.
    pub thread_group_repository: ThreadGroupRepositoryImpl,
    pub thread_group_member_repository: ThreadGroupMemberRepositoryImpl,
    pub thread_relation_repository: ThreadRelationRepositoryImpl,
    pub thread_observation_repository: ThreadObservationRepositoryImpl,
    pub thread_group_candidate_association_repository:
        ThreadGroupCandidateAssociationRepositoryImpl,
    pub operator_decision_repository: OperatorDecisionRepositoryImpl,
    pub source_thread_identity_repository: SourceThreadIdentityRepositoryImpl,
    pub thread_canonical_key_repository: ThreadCanonicalKeyRepositoryImpl,
    pub thread_deletion_marker_repository: ThreadDeletionMarkerRepositoryImpl,
    pub thread_group_lock_repository: ThreadGroupLockRepositoryImpl,
    pub manual_collection_repository: ManualCollectionRepositoryImpl,
    pub thread_group_event_outbox_repository: ThreadGroupEventOutboxRepositoryImpl,
    pub thread_group_audit_repository: ThreadGroupAuditRepositoryImpl,

    // Reflection RDB repositories. Search/aggregate/CRUD do not
    // depend on LanceDB.
    pub reflection_index_repository: ThreadReflectionIndexRepositoryImpl,
    pub reflection_failure_mode_repository: ReflectionFailureModeRepositoryImpl,
    pub reflection_tool_repository: ReflectionToolRepositoryImpl,
    pub reflection_tool_outcome_repository: ReflectionToolOutcomeRepositoryImpl,
    pub reflection_fact_repository: ReflectionFactRepositoryImpl,
    pub reflection_applied_target_repository: ReflectionAppliedTargetRepositoryImpl,
    pub reflection_few_shot_usage_repository: ReflectionFewShotUsageRepositoryImpl,
    pub reflection_stats_repository: ReflectionStatsRepositoryImpl,
    pub reflection_dictionary_repository: FailureModeDictionaryRepositoryImpl,
    pub reflection_signature_norm_repository: FailureSignatureIndicatorNormRepositoryImpl,
    pub reflection_aggregate_thread_repository: ThreadAggregateKeyRepositoryImpl,

    pub memory_vector_repository:
        Option<super::memory_vector::repository::MemoryVectorRepositoryImpl>,
    pub thread_vector_repository:
        Option<super::thread_vector::repository::ThreadVectorRepositoryImpl>,
    pub search_index_maintenance_executor:
        Arc<dyn super::search_index_maintenance::MaintenanceExecutor>,
    /// Reflection intent-vector store. `None` until both
    /// `MEMORY_VECTOR_ENABLED=true` and reflection-vector knobs are
    /// satisfied — the app layer falls back to RDB-only behaviour
    /// when intent search is unavailable.
    pub reflection_intent_vector_repository:
        Option<super::reflection_intent_vector::repository::ReflectionIntentVectorRepository>,
    /// Embedding space the vector tables were verified against at
    /// startup. `None` when no vector store is enabled.
    pub embedding_space: Option<super::embedding_space::bootstrap::SpaceState>,
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl RepositoryModule {
    pub async fn new_by_env() -> Self {
        let id_generator = IdGeneratorWrapper::new();
        let pool = super::resource::setup_rdb_by_env().await;
        // Announce this writer before reading any migration state, so a
        // migration command either sees it or this process waits for the
        // command to finish.
        super::embedding_space::writer_lock::hold_shared(pool)
            .await
            .unwrap_or_else(|e| StartupError::fatal_anyhow("embedding writer lock", e));
        // Dimension and distance of the embedding space, taken from the
        // first enabled vector store (memory, then reflection, then
        // thread), all of which default to the `MEMORY_*` values.
        let mut space_geometry: Option<(usize, super::memory_vector::config::DistanceType)> = None;

        let memory_vector_repository = {
            if super::embedding_space::vector_store_enabled("MEMORY_VECTOR_ENABLED") {
                let config = super::memory_vector::config::VectorDBConfig::from_env()
                    .unwrap_or_else(|e| {
                        StartupError::ConfigLoadFailed {
                            component: "VectorDBConfig (MEMORY_VECTOR_SIZE)".into(),
                            message: format!("{e:#}"),
                        }
                        .fatal()
                    });
                let uri = config.uri.clone();
                space_geometry.get_or_insert((config.vector_size, config.distance_type));
                Some(
                    super::memory_vector::repository::MemoryVectorRepositoryImpl::new(config)
                        .await
                        .unwrap_or_else(|e| fatal_lancedb_init_error(&uri, e)),
                )
            } else {
                None
            }
        };

        // Reflection intent-vector store: opt-in alongside
        // memory_vector. Required only for the F-S3 / F-S8 search
        // paths; the rest of the reflection app layer works
        // RDB-only, so we tolerate `None` and let `ReflectionApp`
        // surface a clear error when an intent query arrives without
        // a configured store.
        let reflection_intent_vector_repository = {
            if super::embedding_space::vector_store_enabled("MEMORY_VECTOR_ENABLED")
                && super::embedding_space::vector_store_enabled("REFLECTION_INTENT_VECTOR_ENABLED")
            {
                let config =
                    super::reflection_intent_vector::config::ReflectionIntentVectorConfig::from_env(
                    )
                    .unwrap_or_else(|e| {
                        StartupError::ConfigLoadFailed {
                            component: "ReflectionIntentVectorConfig".into(),
                            message: format!("{e:#}"),
                        }
                        .fatal()
                    });
                let uri = config.uri.clone();
                space_geometry.get_or_insert((config.vector_size, config.distance_type));
                Some(
                    super::reflection_intent_vector::repository::ReflectionIntentVectorRepository::open(
                        config,
                    )
                    .await
                    .unwrap_or_else(|e| fatal_lancedb_init_error(&uri, e)),
                )
            } else {
                None
            }
        };

        let thread_vector_repository = {
            if super::embedding_space::vector_store_enabled("THREAD_VECTOR_ENABLED") {
                let config = super::thread_vector::config::ThreadVectorDBConfig::from_env()
                    .unwrap_or_else(|e| {
                        StartupError::ConfigLoadFailed {
                            component: "ThreadVectorDBConfig".into(),
                            message: format!("{e:#}"),
                        }
                        .fatal()
                    });
                let uri = config.uri.clone();
                space_geometry.get_or_insert((config.vector_size, config.distance_type));
                Some(
                    super::thread_vector::repository::ThreadVectorRepositoryImpl::new(config)
                        .await
                        .unwrap_or_else(|e| fatal_lancedb_init_error(&uri, e)),
                )
            } else {
                None
            }
        };
        let (
            memory_vector_repository,
            thread_vector_repository,
            reflection_intent_vector_repository,
        ) = attach_embedding_indexes(
            memory_vector_repository,
            thread_vector_repository,
            reflection_intent_vector_repository,
        )
        .await;
        let embedding_space = match space_geometry {
            None => None,
            Some((vector_size, distance)) => Some(
                bootstrap_embedding_space(
                    pool,
                    &id_generator,
                    memory_vector_repository.as_ref(),
                    thread_vector_repository.as_ref(),
                    reflection_intent_vector_repository.as_ref(),
                    vector_size,
                    distance,
                )
                .await,
            ),
        };
        let search_index_maintenance_executor = Arc::new(
            super::search_index_maintenance::repository_executor::RepositoryMaintenanceExecutor::new(
                memory_vector_repository.clone(),
                thread_vector_repository.clone(),
            ),
        );

        RepositoryModule {
            memory_repository: MemoryRepositoryImpl::new(id_generator.clone(), pool),
            memory_rating_repository: MemoryRatingRepositoryImpl::new(id_generator.clone(), pool),
            media_object_repository: MediaObjectRepositoryImpl::new(id_generator.clone(), pool),
            thread_repository: ThreadRepositoryImpl::new(id_generator.clone(), pool),
            thread_memory_repository: ThreadMemoryRepositoryImpl::new(pool),
            thread_label_repository: ThreadLabelRepositoryImpl::new(pool),

            thread_group_repository: ThreadGroupRepositoryImpl::new(id_generator.clone(), pool),
            thread_group_member_repository: ThreadGroupMemberRepositoryImpl::new(pool),
            thread_relation_repository: ThreadRelationRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            thread_observation_repository: ThreadObservationRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            thread_group_candidate_association_repository:
                ThreadGroupCandidateAssociationRepositoryImpl::new(id_generator.clone(), pool),
            operator_decision_repository: OperatorDecisionRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            source_thread_identity_repository: SourceThreadIdentityRepositoryImpl::new(pool),
            thread_canonical_key_repository: ThreadCanonicalKeyRepositoryImpl::new(pool),
            thread_deletion_marker_repository: ThreadDeletionMarkerRepositoryImpl::new(pool),
            thread_group_lock_repository: ThreadGroupLockRepositoryImpl::new(pool),
            manual_collection_repository: ManualCollectionRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            thread_group_event_outbox_repository: ThreadGroupEventOutboxRepositoryImpl::new(pool),
            thread_group_audit_repository: ThreadGroupAuditRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),

            reflection_index_repository: ThreadReflectionIndexRepositoryImpl::new(pool),
            reflection_failure_mode_repository: ReflectionFailureModeRepositoryImpl::new(pool),
            reflection_tool_repository: ReflectionToolRepositoryImpl::new(pool),
            reflection_tool_outcome_repository: ReflectionToolOutcomeRepositoryImpl::new(pool),
            reflection_fact_repository: ReflectionFactRepositoryImpl::new(pool),
            reflection_applied_target_repository: ReflectionAppliedTargetRepositoryImpl::new(pool),
            reflection_few_shot_usage_repository: ReflectionFewShotUsageRepositoryImpl::new(pool),
            reflection_stats_repository: ReflectionStatsRepositoryImpl::new(pool),
            reflection_dictionary_repository: FailureModeDictionaryRepositoryImpl::new(pool),
            reflection_signature_norm_repository: FailureSignatureIndicatorNormRepositoryImpl::new(
                pool,
            ),
            reflection_aggregate_thread_repository: ThreadAggregateKeyRepositoryImpl::new(pool),

            memory_vector_repository,
            thread_vector_repository,
            search_index_maintenance_executor,
            reflection_intent_vector_repository,
            embedding_space,
            pool,
            id_generator,
        }
    }

    pub fn create_memory_repository(&self) -> MemoryRepositoryImpl {
        MemoryRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_memory_rating_repository(&self) -> MemoryRatingRepositoryImpl {
        MemoryRatingRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_media_object_repository(&self) -> MediaObjectRepositoryImpl {
        MediaObjectRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    /// Shared snowflake generator. Exposed so the app layer can hand the
    /// same generator to `MediaApp` (it generates both upload ids and
    /// media_object ids outside any single repository).
    pub fn id_generator(&self) -> IdGeneratorWrapper {
        self.id_generator.clone()
    }

    pub fn create_thread_repository(&self) -> ThreadRepositoryImpl {
        ThreadRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_thread_memory_repository(&self) -> ThreadMemoryRepositoryImpl {
        ThreadMemoryRepositoryImpl::new(self.pool)
    }

    pub fn create_thread_label_repository(&self) -> ThreadLabelRepositoryImpl {
        ThreadLabelRepositoryImpl::new(self.pool)
    }

    pub fn create_thread_group_repository(&self) -> ThreadGroupRepositoryImpl {
        ThreadGroupRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_thread_group_member_repository(&self) -> ThreadGroupMemberRepositoryImpl {
        ThreadGroupMemberRepositoryImpl::new(self.pool)
    }

    pub fn create_thread_relation_repository(&self) -> ThreadRelationRepositoryImpl {
        ThreadRelationRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_thread_observation_repository(&self) -> ThreadObservationRepositoryImpl {
        ThreadObservationRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_thread_group_candidate_association_repository(
        &self,
    ) -> ThreadGroupCandidateAssociationRepositoryImpl {
        ThreadGroupCandidateAssociationRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_operator_decision_repository(&self) -> OperatorDecisionRepositoryImpl {
        OperatorDecisionRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_source_thread_identity_repository(&self) -> SourceThreadIdentityRepositoryImpl {
        SourceThreadIdentityRepositoryImpl::new(self.pool)
    }

    pub fn create_thread_canonical_key_repository(&self) -> ThreadCanonicalKeyRepositoryImpl {
        ThreadCanonicalKeyRepositoryImpl::new(self.pool)
    }

    pub fn create_thread_deletion_marker_repository(&self) -> ThreadDeletionMarkerRepositoryImpl {
        ThreadDeletionMarkerRepositoryImpl::new(self.pool)
    }

    pub fn create_thread_group_lock_repository(&self) -> ThreadGroupLockRepositoryImpl {
        ThreadGroupLockRepositoryImpl::new(self.pool)
    }

    pub fn create_manual_collection_repository(&self) -> ManualCollectionRepositoryImpl {
        ManualCollectionRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_thread_group_event_outbox_repository(
        &self,
    ) -> ThreadGroupEventOutboxRepositoryImpl {
        ThreadGroupEventOutboxRepositoryImpl::new(self.pool)
    }

    pub fn create_thread_group_audit_repository(&self) -> ThreadGroupAuditRepositoryImpl {
        ThreadGroupAuditRepositoryImpl::new(self.id_generator.clone(), self.pool)
    }

    pub fn create_reflection_index_repository(&self) -> ThreadReflectionIndexRepositoryImpl {
        ThreadReflectionIndexRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_failure_mode_repository(&self) -> ReflectionFailureModeRepositoryImpl {
        ReflectionFailureModeRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_tool_repository(&self) -> ReflectionToolRepositoryImpl {
        ReflectionToolRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_tool_outcome_repository(&self) -> ReflectionToolOutcomeRepositoryImpl {
        ReflectionToolOutcomeRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_fact_repository(&self) -> ReflectionFactRepositoryImpl {
        ReflectionFactRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_applied_target_repository(
        &self,
    ) -> ReflectionAppliedTargetRepositoryImpl {
        ReflectionAppliedTargetRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_few_shot_usage_repository(
        &self,
    ) -> ReflectionFewShotUsageRepositoryImpl {
        ReflectionFewShotUsageRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_stats_repository(&self) -> ReflectionStatsRepositoryImpl {
        ReflectionStatsRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_dictionary_repository(&self) -> FailureModeDictionaryRepositoryImpl {
        FailureModeDictionaryRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_signature_norm_repository(
        &self,
    ) -> FailureSignatureIndicatorNormRepositoryImpl {
        FailureSignatureIndicatorNormRepositoryImpl::new(self.pool)
    }

    pub fn create_reflection_aggregate_thread_repository(
        &self,
    ) -> ThreadAggregateKeyRepositoryImpl {
        ThreadAggregateKeyRepositoryImpl::new(self.pool)
    }

    /// Expose the shared pool handle for app-layer modules that need
    /// to drive their own transactions (e.g. `ReflectionAppImpl`'s
    /// 3-phase commit). The pool is `&'static` so this is a cheap
    /// reference handout, not a clone.
    pub fn pool(&self) -> &'static RdbPool {
        self.pool
    }
}

/// Initializes only the relational database pool for RDB-only tools.
///
/// Migration CLIs must not open LanceDB because they can run before the
/// replacement vector schema exists.
pub async fn rdb_pool_by_env() -> anyhow::Result<RdbPool> {
    super::resource::new_rdb_pool_by_env().await
}
