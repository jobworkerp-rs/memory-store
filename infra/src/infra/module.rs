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
    thread::rdb::ThreadRepositoryImpl, thread_label::rdb::ThreadLabelRepositoryImpl,
    thread_memory::rdb::ThreadMemoryRepositoryImpl,
};
use infra_utils::infra::rdb::RdbPool;

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

// module for DI
pub struct RepositoryModule {
    pub memory_repository: MemoryRepositoryImpl,
    pub memory_rating_repository: MemoryRatingRepositoryImpl,
    pub media_object_repository: MediaObjectRepositoryImpl,
    pub thread_repository: ThreadRepositoryImpl,
    pub thread_memory_repository: ThreadMemoryRepositoryImpl,
    pub thread_label_repository: ThreadLabelRepositoryImpl,

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
    /// Reflection intent-vector store. `None` until both
    /// `MEMORY_VECTOR_ENABLED=true` and reflection-vector knobs are
    /// satisfied — the app layer falls back to RDB-only behaviour
    /// when intent search is unavailable.
    pub reflection_intent_vector_repository:
        Option<super::reflection_intent_vector::repository::ReflectionIntentVectorRepository>,
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl RepositoryModule {
    pub async fn new_by_env() -> Self {
        let id_generator = IdGeneratorWrapper::new();
        let pool = super::resource::setup_rdb_by_env().await;

        let memory_vector_repository = {
            if std::env::var("MEMORY_VECTOR_ENABLED").unwrap_or_default() == "true" {
                let config = super::memory_vector::config::VectorDBConfig::from_env()
                    .unwrap_or_else(|e| {
                        StartupError::ConfigLoadFailed {
                            component: "VectorDBConfig (MEMORY_VECTOR_SIZE)".into(),
                            message: format!("{e:#}"),
                        }
                        .fatal()
                    });
                let uri = config.uri.clone();
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
            if std::env::var("MEMORY_VECTOR_ENABLED").unwrap_or_default() == "true"
                && std::env::var("REFLECTION_INTENT_VECTOR_ENABLED").unwrap_or_default() == "true"
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

        RepositoryModule {
            memory_repository: MemoryRepositoryImpl::new(id_generator.clone(), pool),
            memory_rating_repository: MemoryRatingRepositoryImpl::new(id_generator.clone(), pool),
            media_object_repository: MediaObjectRepositoryImpl::new(id_generator.clone(), pool),
            thread_repository: ThreadRepositoryImpl::new(id_generator.clone(), pool),
            thread_memory_repository: ThreadMemoryRepositoryImpl::new(pool),
            thread_label_repository: ThreadLabelRepositoryImpl::new(pool),

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
            thread_vector_repository: {
                if std::env::var("THREAD_VECTOR_ENABLED").unwrap_or_default() == "true" {
                    let config = super::thread_vector::config::ThreadVectorDBConfig::from_env()
                        .unwrap_or_else(|e| {
                            StartupError::ConfigLoadFailed {
                                component: "ThreadVectorDBConfig".into(),
                                message: format!("{e:#}"),
                            }
                            .fatal()
                        });
                    let uri = config.uri.clone();
                    Some(
                        super::thread_vector::repository::ThreadVectorRepositoryImpl::new(config)
                            .await
                            .unwrap_or_else(|e| fatal_lancedb_init_error(&uri, e)),
                    )
                } else {
                    None
                }
            },
            reflection_intent_vector_repository,
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
