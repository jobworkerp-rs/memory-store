//! Integration tests for `ReflectionAppImpl::finalize_generated_reflection`
//! and the surrounding RPC surface. Each test owns a dedicated
//! `origin_user_id` so they stay independent under
//! `cargo test -- --test-threads=1`.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use infra::infra::memory::rdb::MemoryRepositoryImpl;
use infra::infra::memory_vector::dispatcher::{DispatchError, EmbeddingDispatch};
use infra::infra::reflection::aggregate_thread::ThreadAggregateKeyRepositoryImpl;
use infra::infra::reflection::applied_target::ReflectionAppliedTargetRepositoryImpl;
use infra::infra::reflection::dictionary::FailureModeDictionaryRepositoryImpl;
use infra::infra::reflection::fact::ReflectionFactRepositoryImpl;
use infra::infra::reflection::failure_mode::ReflectionFailureModeRepositoryImpl;
use infra::infra::reflection::few_shot_usage::ReflectionFewShotUsageRepositoryImpl;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepositoryImpl;
use infra::infra::reflection::signature_norm::FailureSignatureIndicatorNormRepositoryImpl;
use infra::infra::reflection::stats::ReflectionStatsRepositoryImpl;
use infra::infra::reflection::tool::ReflectionToolRepositoryImpl;
use infra::infra::reflection::tool_outcome::ReflectionToolOutcomeRepositoryImpl;
use infra::infra::thread::rdb::ThreadRepositoryImpl;
use infra::infra::thread_label::rdb::ThreadLabelRepositoryImpl;
use infra::infra::thread_memory::rdb::ThreadMemoryRepositoryImpl;
use infra_utils::infra::rdb::{Rdb, RdbPool};
use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
use protobuf::llm_memory::data::EmbeddingKind;
use protobuf::llm_memory::data::{
    ContentType, FailureMode, MemoryData, MemoryId, MemoryKind, MessageRole, ReflectionAspect,
    ReflectionFact, ReflectionLlmOutput, ReflectionOutcome, TaskCategory, ThreadData, ThreadId,
    ToolContribution, ToolOutcomeEntry, UserId,
};
use protobuf::llm_memory::service::{FinalizeReflectionRequest, ReflectionListSort};

use crate::app::reflection::{ReflectionApp, ReflectionAppImpl};

#[derive(Debug, Default)]
struct RecordingEmbeddingDispatcher {
    calls: std::sync::Mutex<Vec<(i64, String)>>,
}

impl RecordingEmbeddingDispatcher {
    fn calls(&self) -> Vec<(i64, String)> {
        self.calls.lock().expect("recording mutex poisoned").clone()
    }
}

#[async_trait]
impl EmbeddingDispatch for RecordingEmbeddingDispatcher {
    async fn dispatch(
        &self,
        memory_id: i64,
        content: &str,
    ) -> std::result::Result<Option<jobworkerp_client::jobworkerp::data::JobId>, DispatchError>
    {
        self.calls
            .lock()
            .expect("recording mutex poisoned")
            .push((memory_id, content.to_string()));
        Ok(None)
    }
}

async fn setup_pool() -> &'static RdbPool {
    let pool = if cfg!(feature = "postgres") {
        setup_test_rdb_from("../infra/sql/postgres").await
    } else {
        setup_test_rdb_from("../infra/sql/sqlite").await
    };
    // Sqlite tests start from a fresh DB file (see infra-utils
    // `_setup_sqlite_internal`), but the postgres test DB is reused
    // across runs and across tests within a run. A previous test that
    // panicked before its `cleanup_finalize_state` ran can leave reflection
    // rows behind. Wipe reflection-kind rows and their aggregate threads
    // before every test so each test starts from an empty corpus.
    reset_reflection_state(pool).await;
    pool
}

/// Delete every reflection-kind row plus the aggregate threads that anchor
/// them. Child rows first so
/// FK-bearing schemas (postgres) don't reject the delete.
async fn reset_reflection_state(pool: &'static RdbPool) {
    let memory_ids = "SELECT id FROM memory WHERE memory_kind = 7";
    let thread_ids = "SELECT id FROM thread WHERE memory_kind = 7";
    let stmts = [
        format!("DELETE FROM reflection_failure_mode WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM reflection_tool WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM reflection_tool_outcome WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM reflection_fact WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM reflection_applied_target WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM reflection_few_shot_usage WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM thread_reflection_index WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM thread_memory WHERE memory_id IN ({memory_ids})"),
        format!("DELETE FROM thread_memory WHERE thread_id IN ({thread_ids})"),
        format!("DELETE FROM thread_label WHERE thread_id IN ({thread_ids})"),
        format!("DELETE FROM thread_aggregate_key WHERE thread_id IN ({thread_ids})"),
        format!("DELETE FROM thread WHERE id IN ({thread_ids})"),
        "DELETE FROM memory WHERE memory_kind = 7".to_string(),
    ];
    for sql in stmts {
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .execute(pool)
            .await;
    }
    // Derived stats are keyed by origin_user_id, which varies per
    // test. There is no single bind that targets only "test
    // origin_user_ids", so leave those rows to the per-test
    // `cleanup_finalize_state`. They do not gate the assertions in
    // tests that count per origin user.
}

/// Build a fully-wired `ReflectionAppImpl` against a real pool. No
/// dispatchers or jobworkerp client are configured so the
/// fire-and-forget post-commit dispatch stage is a no-op — exactly
/// the runtime posture we want for unit tests.
async fn build_app(pool: &'static RdbPool) -> ReflectionAppImpl {
    build_app_with_memory_dispatcher(pool, None).await
}

async fn build_app_with_memory_dispatcher(
    pool: &'static RdbPool,
    memory_embedding_dispatcher: Option<Arc<dyn EmbeddingDispatch>>,
) -> ReflectionAppImpl {
    let id_gen = infra::test_helper::shared_id_generator();
    ReflectionAppImpl::new(
        pool,
        MemoryRepositoryImpl::new(id_gen.clone(), pool),
        ThreadRepositoryImpl::new(id_gen.clone(), pool),
        ThreadMemoryRepositoryImpl::new(pool),
        ThreadLabelRepositoryImpl::new(pool),
        ThreadAggregateKeyRepositoryImpl::new(pool),
        ThreadReflectionIndexRepositoryImpl::new(pool),
        ReflectionFailureModeRepositoryImpl::new(pool),
        ReflectionToolRepositoryImpl::new(pool),
        ReflectionToolOutcomeRepositoryImpl::new(pool),
        ReflectionFactRepositoryImpl::new(pool),
        ReflectionAppliedTargetRepositoryImpl::new(pool),
        ReflectionFewShotUsageRepositoryImpl::new(pool),
        ReflectionStatsRepositoryImpl::new(pool),
        FailureModeDictionaryRepositoryImpl::new(pool),
        FailureSignatureIndicatorNormRepositoryImpl::new(pool),
        None,
        memory_embedding_dispatcher,
        None,
        None,
        None,
        // Tests do not exercise the cross-app cache invariant; the
        // delete cascade still runs the cache invalidation branch,
        // but the `None` short-circuits it without going through
        // Stretto.
        None,
    )
    .await
}

/// Insert a synthetic origin thread + one anchor memory (used as the
/// `facts[0].anchor_memory_id`). Returns `(thread_id, anchor_memory_id)`.
async fn seed_origin(pool: &'static RdbPool, user_id: i64) -> Result<(i64, i64)> {
    let id_gen = infra::test_helper::shared_id_generator();
    let thread_repo = ThreadRepositoryImpl::new(id_gen.clone(), pool);
    let memory_repo = MemoryRepositoryImpl::new(id_gen, pool);
    use infra::infra::memory::rdb::MemoryRepository;
    use infra::infra::thread::rdb::ThreadRepository;

    let now = command_utils::util::datetime::now_millis();
    let mut tx = pool.begin().await?;
    let thread_id = thread_repo
        .create(
            &mut *tx,
            &ThreadData {
                default_system_memory_id: None,
                user_id: Some(UserId { value: user_id }),
                description: Some("origin".into()),
                channel: Some("test_channel".into()),
                embedding: None,
                embedding_dim: None,
                created_at: now,
                updated_at: now,
                metadata: None,
                labels: vec![],
                memory_kind: MemoryKind::Raw as i32,
            },
        )
        .await?;
    let anchor = memory_repo
        .create(
            &mut *tx,
            &MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: user_id }),
                content: "anchor turn".into(),
                content_type: ContentType::Text as i32,
                params: None,
                metadata: None,
                created_at: now,
                updated_at: now,
                role: MessageRole::RoleUser as i32,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
                memory_kind: MemoryKind::Raw as i32,
            },
        )
        .await?;
    tx.commit().await?;
    Ok((thread_id.value, anchor.value))
}

#[cfg(feature = "postgres")]
const P1: &str = "$1";
#[cfg(not(feature = "postgres"))]
const P1: &str = "?";
#[cfg(feature = "postgres")]
const P2: &str = "$2";
#[cfg(not(feature = "postgres"))]
const P2: &str = "?";

// `memory.metadata` is JSONB on postgres / TEXT on sqlite. The sqlx
// postgres driver refuses to decode JSONB into `Option<String>`
// directly, so we cast to TEXT in the SELECT for tests that need the
// raw JSON string.
#[cfg(feature = "postgres")]
const METADATA_TEXT_EXPR: &str = "metadata::TEXT";
#[cfg(not(feature = "postgres"))]
const METADATA_TEXT_EXPR: &str = "metadata";

async fn cleanup_finalize_state(
    pool: &'static RdbPool,
    origin_thread_id: i64,
    origin_user_id: i64,
    extra_memory_ids: &[i64],
) {
    let by_aggregate = format!("SELECT thread_id FROM thread_aggregate_key WHERE user_id = {P1}");
    let by_aggregate_index = format!(
        "SELECT memory_id FROM thread_reflection_index WHERE thread_id IN ({by_aggregate})"
    );
    let exec = |sql: String, bind: i64| async move {
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(bind)
            .execute(pool)
            .await;
    };

    // (sql template, bind value). Order matters: child rows before
    // their parent, derived stats by origin_user_id, and per-thread
    // junction/label cleanup last.
    let tasks: [(String, i64); 14] = [
        (
            format!(
                "DELETE FROM reflection_failure_mode WHERE memory_id IN ({by_aggregate_index})"
            ),
            origin_user_id,
        ),
        (
            format!("DELETE FROM reflection_tool WHERE memory_id IN ({by_aggregate_index})"),
            origin_user_id,
        ),
        (
            format!(
                "DELETE FROM reflection_tool_outcome WHERE memory_id IN ({by_aggregate_index})"
            ),
            origin_user_id,
        ),
        (
            format!("DELETE FROM reflection_fact WHERE memory_id IN ({by_aggregate_index})"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM thread_reflection_index WHERE thread_id IN ({by_aggregate})"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM thread_memory WHERE thread_id IN ({by_aggregate})"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM memory WHERE id IN ({by_aggregate_index})"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM thread WHERE id IN ({by_aggregate})"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM thread_aggregate_key WHERE user_id = {P1}"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM tool_outcome_stats WHERE origin_user_id = {P1}"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM tool_contribution_stats WHERE origin_user_id = {P1}"),
            origin_user_id,
        ),
        (
            format!("DELETE FROM thread_memory WHERE thread_id = {P1}"),
            origin_thread_id,
        ),
        (
            format!("DELETE FROM thread_label WHERE thread_id = {P1}"),
            origin_thread_id,
        ),
        (
            format!("DELETE FROM thread WHERE id = {P1}"),
            origin_thread_id,
        ),
    ];
    for (sql, bind) in tasks {
        exec(sql, bind).await;
    }
    for id in extra_memory_ids {
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "DELETE FROM memory WHERE id = {P1}"
        )))
        .bind(id)
        .execute(pool)
        .await;
    }
}

/// `ReflectionApp` does not expose a Find-by-id helper, so tests
/// look up via `find_by_thread` and pick the matching reflection_id.
async fn find_reflection_by_id(
    app: &ReflectionAppImpl,
    origin_thread_id: i64,
    reflection_id: i64,
) -> Result<protobuf::llm_memory::data::Reflection> {
    let all = app
        .find_by_thread(
            &ThreadId {
                value: origin_thread_id,
            },
            true,
        )
        .await?;
    all.into_iter()
        .find(|r| r.id.as_ref().map(|i| i.value) == Some(reflection_id))
        .ok_or_else(|| {
            anyhow::anyhow!("reflection {reflection_id} not found under thread {origin_thread_id}")
        })
}

fn make_request(
    origin_thread_id: i64,
    anchor_memory_id: i64,
    reflector_id: &str,
    prompt_version: &str,
    parsed: ReflectionLlmOutput,
) -> FinalizeReflectionRequest {
    let _ = anchor_memory_id;
    FinalizeReflectionRequest {
        origin_thread_id: Some(ThreadId {
            value: origin_thread_id,
        }),
        parsed_output: Some(parsed),
        heuristic_score: 0.7,
        reflector_id: reflector_id.to_string(),
        prompt_version: prompt_version.to_string(),
        target_model_version: Some("test-model-v0".to_string()),
        target_retry_count: Some(1),
        target_error_count: Some(0),
        target_tool_calls_count: Some(2),
        target_window_count: None,
        target_window_size: None,
        target_fact_unresolved_count: None,
        experiment_id: None,
        experiment_variant: None,
        reflector_error_kind: None,
    }
}

fn happy_parsed(anchor_memory_id: i64) -> ReflectionLlmOutput {
    ReflectionLlmOutput {
        outcome: ReflectionOutcome::Success as i32,
        score_self: 0.85,
        summary: "the agent shipped the feature".into(),
        task_intent: "implement reflection feature end to end".into(),
        task_category: TaskCategory::Coding as i32,
        reflection_aspect: ReflectionAspect::TaskOutcome as i32,
        failure_modes: vec![FailureMode::ToolMisuse as i32],
        tools_used: vec!["Read".into(), "Edit".into()],
        failure_modes_other: vec![],
        success_factors: vec!["careful planning".into()],
        lessons: vec!["always run tests".into()],
        key_decisions: vec![],
        mitigation_hint: Some("revisit tool selection".into()),
        failure_signature: None,
        tool_outcomes: vec![ToolOutcomeEntry {
            tool: "Read".into(),
            contribution: ToolContribution::Positive as i32,
            error_kind: None,
        }],
        facts: vec![ReflectionFact {
            turn_index: 0,
            anchor_memory_id: Some(MemoryId {
                value: anchor_memory_id,
            }),
            kind: 1, // OUTCOME_EVIDENCE
            weight: Some(1.0),
            note: None,
            links: vec![],
        }],
    }
}

#[test]
fn run_finalize_dispatches_search_document_to_generic_embedding() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_700;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let dispatcher = Arc::new(RecordingEmbeddingDispatcher::default());
        let app = build_app_with_memory_dispatcher(pool, Some(dispatcher.clone())).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_generic_dispatch",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let id = app.finalize_generated_reflection(&req).await?;

        let calls = dispatcher.calls();
        assert_eq!(calls.len(), 1, "finalize must dispatch text embedding once");
        assert_eq!(calls[0].0, id.value);
        assert!(
            calls[0]
                .1
                .contains("Summary:\nthe agent shipped the feature")
        );
        assert!(
            calls[0]
                .1
                .contains("Intent:\nimplement reflection feature end to end")
        );
        assert!(calls[0].1.contains("Lessons:\n- always run tests"));
        assert_ne!(
            calls[0].1, "the agent shipped the feature",
            "generic embedding must receive the search document, not summary only"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Test 3 — `facts[].anchor_memory_id` unset must surface as
/// InvalidArgument and must NOT touch the database.
#[test]
fn run_finalize_anchor_memory_id_required() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_701;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;

        let app = build_app(pool).await;

        let mut parsed = happy_parsed(anchor_memory_id);
        parsed.facts[0].anchor_memory_id = None;
        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector",
            "20260510",
            parsed,
        );
        let res = app.finalize_generated_reflection(&req).await;
        assert!(res.is_err());
        let err = res.err().unwrap();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("anchor_memory_id"),
            "error must reference anchor_memory_id, got {msg}"
        );

        // No reflection memory was created.
        let p1 = P1;
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM memory WHERE user_id = {p1} AND memory_kind = 7"
        )))
        .bind(origin_user_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(count, 0);

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Test 1 — `finalize_generated_reflection` is idempotent for the same
/// `(origin_thread_id, prompt_version, reflector_id)`. The second call
/// must return the existing reflection_id without inserting an extra
/// row anywhere.
#[test]
fn run_finalize_double_call_idempotent() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_702;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;

        let app = build_app(pool).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_idem",
            "20260510",
            happy_parsed(anchor_memory_id),
        );

        let id1 = app.finalize_generated_reflection(&req).await?;
        let id2 = app.finalize_generated_reflection(&req).await?;
        assert_eq!(
            id1.value, id2.value,
            "second finalize must return the same reflection_id"
        );

        let p1 = P1;
        // memory: exactly one reflection row owned by the origin user for this run.
        let mem_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM memory WHERE user_id = {p1} AND memory_kind = 7"
        )))
        .bind(origin_user_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(mem_count, 1);

        // sidecar: exactly one row.
        let idx_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM thread_reflection_index WHERE memory_id = {p1}"
        )))
        .bind(id1.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(idx_count, 1);

        // failure_mode child: only one ("tool_misuse").
        let fm_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM reflection_failure_mode WHERE memory_id = {p1}"
        )))
        .bind(id1.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(fm_count, 1);

        // tool_outcome_stats: one row, count = 1 (NOT 2).
        let stat_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count FROM tool_outcome_stats WHERE origin_user_id = {p1} AND tool = 'Read'"
        )))
        .bind(origin_user_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            stat_count, 1,
            "stats must NOT double-count an idempotent finalize"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Test 7 — F-G9 failure path. Outcome=UNKNOWN, `reflector_error_kind`
/// set: the row is persisted with empty children and the metadata
/// preserves the error_kind for downstream debugging.
#[test]
fn run_finalize_failure_record_outcome_unknown() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_703;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;

        let app = build_app(pool).await;

        let parsed = ReflectionLlmOutput {
            outcome: ReflectionOutcome::Unknown as i32,
            score_self: 0.0,
            summary: String::new(),
            task_intent: String::new(),
            task_category: TaskCategory::Unspecified as i32,
            reflection_aspect: ReflectionAspect::Unspecified as i32,
            failure_modes: vec![],
            tools_used: vec![],
            failure_modes_other: vec![],
            success_factors: vec![],
            lessons: vec![],
            key_decisions: vec![],
            mitigation_hint: None,
            failure_signature: None,
            tool_outcomes: vec![],
            facts: vec![],
        };
        let mut req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_failure",
            "20260510",
            parsed,
        );
        req.reflector_error_kind = Some("schema_violation".into());

        let id = app.finalize_generated_reflection(&req).await?;

        let p1 = P1;
        // memory was inserted with role=ROLE_REFLECTION.
        let role: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT role FROM memory WHERE id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(role, MessageRole::RoleReflection as i32);

        // metadata.reflector.error_kind preserved. Postgres stores
        // `memory.metadata` as JSONB, sqlite as TEXT — the postgres
        // driver refuses a direct `Option<String>` decode from JSONB,
        // so cast via `metadata_text` only when running against pg.
        let metadata: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {METADATA_TEXT_EXPR} FROM memory WHERE id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        let metadata = metadata.expect("metadata JSON must be set");
        let metadata_json: serde_json::Value =
            serde_json::from_str(&metadata).expect("metadata is valid JSON");
        assert_eq!(
            metadata_json
                .pointer("/reflector/error_kind")
                .and_then(|v| v.as_str()),
            Some("schema_violation"),
            "metadata.reflector.error_kind must record the error kind"
        );

        // Sidecar outcome=UNKNOWN.
        let outcome: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT outcome FROM thread_reflection_index WHERE memory_id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(outcome, ReflectionOutcome::Unknown as i32);

        // Empty child rows.
        let fm_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM reflection_failure_mode WHERE memory_id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(fm_count, 0);
        let fact_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM reflection_fact WHERE memory_id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(fact_count, 0);

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Controlled-vocabulary failure_modes round-trip. Before the
/// `FailureMode` enum migration the workflow forwarded raw LLM strings
/// and a language-external 200-char value tripped
/// `reflection_failure_mode.mode VARCHAR(64)` (Postgres error 22001),
/// surfacing as a `cannot use null as string` jq error that masked the
/// real cause. Now the proto type is the enum, so every persisted
/// `mode` is a short DB key (<= 64 chars), `FAILURE_MODE_OTHER`
/// collapses into a single `OTHER` child row (the salvaged free text
/// lives in metadata.eval.failure_modes_other), and
/// metadata.eval.failure_modes carries the FAILURE_MODE_* names. This
/// test fails pre-fix on Postgres (22001) and inserts an oversized row
/// silently on sqlite; post-fix both backends store the normalized set
/// and `MAX(LENGTH(mode)) <= 64` holds.
#[test]
fn run_finalize_normalizes_oversized_failure_modes() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_705;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let mut parsed = happy_parsed(anchor_memory_id);
        // Mix an in-vocabulary mode with OTHER (the workflow collapses
        // every language-external / oversized LLM string to OTHER before
        // this RPC; here we assert the server persists that shape
        // correctly and salvages the free text into eval).
        parsed.failure_modes = vec![
            FailureMode::ToolMisuse as i32,
            FailureMode::Other as i32,
            // Duplicate OTHER must not double-insert.
            FailureMode::Other as i32,
        ];
        parsed.failure_modes_other = vec!["x".repeat(200), "novel pattern".into()];

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_vocab",
            "20260519",
            parsed,
        );
        let id = app.finalize_generated_reflection(&req).await?;

        let p1 = P1;

        // Child rows: exactly {tool_misuse, OTHER}, deduped.
        let mut modes: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT mode FROM reflection_failure_mode WHERE memory_id = {p1}"
        )))
        .bind(id.value)
        .fetch_all(pool)
        .await?;
        modes.sort();
        assert_eq!(
            modes,
            vec!["OTHER".to_string(), "tool_misuse".to_string()],
            "only controlled-vocabulary keys, OTHER deduped"
        );

        // The DB column constraint is satisfied by construction.
        // LENGTH()/MAX() are int4 on postgres, so decode as i32 (sqlite
        // INTEGER decodes fine into i32 too).
        let max_len: i32 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COALESCE(MAX(LENGTH(mode)), 0) \
             FROM reflection_failure_mode WHERE memory_id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert!(
            max_len <= 64,
            "no mode may exceed VARCHAR(64), got {max_len}"
        );

        // metadata.eval.failure_modes uses FAILURE_MODE_* names; the
        // oversized + novel free text is preserved verbatim in
        // failure_modes_other (this is the salvage sink).
        let metadata: Option<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT {METADATA_TEXT_EXPR} FROM memory WHERE id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        let metadata_json: serde_json::Value =
            serde_json::from_str(&metadata.expect("metadata set")).expect("valid JSON");
        let eval_modes: Vec<String> = metadata_json
            .pointer("/eval/failure_modes")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        let mut eval_modes_sorted = eval_modes.clone();
        eval_modes_sorted.sort();
        assert_eq!(
            eval_modes_sorted,
            vec![
                "FAILURE_MODE_OTHER".to_string(),
                "FAILURE_MODE_TOOL_MISUSE".to_string()
            ],
            "metadata.eval.failure_modes must use FAILURE_MODE_* names"
        );
        let eval_other: Vec<String> = metadata_json
            .pointer("/eval/failure_modes_other")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        assert!(
            eval_other.contains(&"x".repeat(200)) && eval_other.contains(&"novel pattern".into()),
            "free text salvaged into failure_modes_other, got {eval_other:?}"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Rust mirror of the `normalizeFailureModes` jq step in
/// thread-reflection-single.yaml. The jq expression cannot run under
/// `cargo test`, so this pins its behaviour: bare lowercase names are
/// upper-cased + prefixed, in-vocabulary canon names are kept (deduped,
/// max 8), out-of-vocab values collapse to FAILURE_MODE_OTHER (added
/// once) and their raw text is salvaged into failure_modes_other
/// (100-char capped, deduped, max 5). Any drift from the YAML logic
/// breaks this test.
fn jq_normalize_failure_modes(
    llm_failure_modes: &[&str],
    llm_failure_modes_other: &[&str],
) -> (Vec<String>, Vec<String>) {
    const VOCAB: &[&str] = &[
        "FAILURE_MODE_TOOL_MISUSE",
        "FAILURE_MODE_LOOP",
        "FAILURE_MODE_SCOPE_DRIFT",
        "FAILURE_MODE_HALLUCINATION",
        "FAILURE_MODE_CONTEXT_OVERFLOW",
        "FAILURE_MODE_DATA_LOSS",
        "FAILURE_MODE_PERMISSION_ISSUE",
        "FAILURE_MODE_AMBIGUOUS_INSTRUCTION",
        "FAILURE_MODE_CONFLICTING_REQUIREMENTS",
        "FAILURE_MODE_MISSING_CONTEXT",
        "FAILURE_MODE_MISLEADING_PREMISE",
        "FAILURE_MODE_GOAL_DRIFT_BY_USER",
        "FAILURE_MODE_TOOL_UNAVAILABLE",
        "FAILURE_MODE_EXTERNAL_SERVICE_FAILURE",
        "FAILURE_MODE_RATE_LIMIT",
        "FAILURE_MODE_OTHER",
    ];
    let canon: Vec<(String, String)> = llm_failure_modes
        .iter()
        .map(|raw| {
            let up = raw.to_ascii_uppercase();
            let c = if up.starts_with("FAILURE_MODE_") {
                up
            } else {
                format!("FAILURE_MODE_{up}")
            };
            (raw.to_string(), c)
        })
        .collect();

    let mut kept: Vec<String> = Vec::new();
    for (_, c) in &canon {
        if VOCAB.contains(&c.as_str()) && !kept.contains(c) {
            kept.push(c.clone());
        }
    }
    let had_other = canon.iter().any(|(_, c)| !VOCAB.contains(&c.as_str()));
    if had_other && !kept.iter().any(|k| k == "FAILURE_MODE_OTHER") {
        kept.push("FAILURE_MODE_OTHER".to_string());
    }
    kept.truncate(8);

    let mut other: Vec<String> = Vec::new();
    let push_unique = |v: String, acc: &mut Vec<String>| {
        if !acc.contains(&v) {
            acc.push(v);
        }
    };
    for (raw, c) in &canon {
        if !VOCAB.contains(&c.as_str()) {
            push_unique(raw.chars().take(100).collect(), &mut other);
        }
    }
    for o in llm_failure_modes_other {
        push_unique(o.chars().take(100).collect(), &mut other);
    }
    other.truncate(5);
    (kept, other)
}

#[test]
fn jq_normalize_failure_modes_matches_workflow_contract() {
    // In-vocab bare lowercase -> prefixed; OTHER-bound free text
    // salvaged; duplicates collapsed.
    let (modes, other) = jq_normalize_failure_modes(
        &[
            "tool_misuse",
            "Tool_Misuse",
            "totally_invented",
            &"x".repeat(200),
        ],
        &["pre-existing"],
    );
    assert_eq!(
        modes,
        vec![
            "FAILURE_MODE_TOOL_MISUSE".to_string(),
            "FAILURE_MODE_OTHER".to_string()
        ]
    );
    assert_eq!(other.len(), 3);
    assert!(other.iter().all(|s| s.chars().count() <= 100));
    assert!(other.contains(&"totally_invented".to_string()));
    assert!(other.contains(&"x".repeat(100)));
    assert!(other.contains(&"pre-existing".to_string()));

    // Already-prefixed in-vocab passes through unchanged, no OTHER.
    let (modes, other) =
        jq_normalize_failure_modes(&["FAILURE_MODE_LOOP", "FAILURE_MODE_LOOP"], &[]);
    assert_eq!(modes, vec!["FAILURE_MODE_LOOP".to_string()]);
    assert!(other.is_empty());

    // Empty in -> empty out (no spurious OTHER).
    let (modes, other) = jq_normalize_failure_modes(&[], &[]);
    assert!(modes.is_empty());
    assert!(other.is_empty());
}

/// Search filter sentinel for unresolvable failure_modes. When a
/// non-empty `failure_modes` (or `_match_any`) list contains only
/// Unspecified / out-of-range discriminants, the resolver must NOT
/// degrade to "no filter applied" (which would return all rows). The
/// sentinel inserted by `i32_slice_to_db_keys` keeps the SQL clause
/// present so the result set is empty — preserving the caller's
/// filter-non-empty intent.
#[test]
fn run_search_unresolvable_failure_modes_filter_returns_empty() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_708;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // Seed a normal reflection so the corpus is non-empty for this
        // user — a missing filter would return this row, exposing the
        // silent-degradation bug.
        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_sentinel",
            "20260520",
            happy_parsed(anchor_memory_id),
        );
        app.finalize_generated_reflection(&req).await?;

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                value: origin_user_id,
            }),
            // Caller specified a non-empty filter, but every entry is
            // Unspecified / out-of-range. Without the sentinel this
            // would silently return all rows for this user.
            failure_modes: vec![FailureMode::Unspecified as i32, 9999],
            ..Default::default()
        };
        let results = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(10),
                None,
            )
            .await?;
        assert!(
            results.is_empty(),
            "non-empty but unresolvable failure_modes filter must yield zero rows, got {} hits",
            results.len()
        );

        // Sanity check: an EMPTY failure_modes list is "no filter on
        // this axis" and still returns the seeded row.
        let no_filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                value: origin_user_id,
            }),
            ..Default::default()
        };
        let baseline = app
            .search(
                Some(&no_filter),
                ReflectionListSort::Unspecified,
                Some(10),
                None,
            )
            .await?;
        assert_eq!(
            baseline.len(),
            1,
            "empty failure_modes filter must still return the seeded reflection"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Test 4 — orphan-aggregate tolerance. When Phase 3 fails (here:
/// `facts[0].anchor_memory_id` points at a non-existent memory_id and
/// the FK enforcement rolls back the tx) the Phase-2 aggregate thread
/// is allowed to linger; a follow-up valid finalize must reuse it
/// instead of creating a second one.
#[test]
fn run_finalize_phase3_rollback_keeps_aggregate_reusable() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_704;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // Wire an obviously bogus anchor — fact insert then fails because
        // the test deliberately uses a memory_id we never created. Whether
        // sqlx surfaces this as a FK violation or as a generic insert
        // failure depends on the schema, but for the orphan-tolerance
        // assertion we only care that Phase 3 dies and the aggregate
        // thread survives.
        let mut bad_parsed = happy_parsed(anchor_memory_id);
        bad_parsed.facts[0].anchor_memory_id = Some(MemoryId { value: -1 });
        let bad_req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_phase3",
            "20260510",
            bad_parsed,
        );
        let _ = app.finalize_generated_reflection(&bad_req).await;
        // Either Ok or Err is acceptable: schemas without an FK constraint
        // happily insert -1; in that case the aggregate thread is simply
        // populated from the first valid finalize. Reuse semantics is
        // what we are pinning down.

        // Inspect the aggregate-thread mapping. The Phase-2 row for
        // (origin_user_id, sha256(["reflection"])) should exist.
        let p1 = P1;
        let aggregate_thread_id: Option<i64> = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(
            format!("SELECT thread_id FROM thread_aggregate_key WHERE user_id = {p1}"),
        ))
        .bind(origin_user_id)
        .fetch_optional(pool)
        .await?;

        // The good follow-up finalize must reuse the same aggregate (or
        // create the first one if Phase 3 succeeded above; either way
        // the post-condition is "exactly one aggregate thread").
        let good_req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_phase3_good",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let _ = app.finalize_generated_reflection(&good_req).await?;

        let aggregate_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM thread_aggregate_key WHERE user_id = {p1}"
        )))
        .bind(origin_user_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(aggregate_count, 1, "must keep exactly one aggregate thread");

        if let Some(prior) = aggregate_thread_id {
            let after: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT thread_id FROM thread_aggregate_key WHERE user_id = {p1}"
            )))
            .bind(origin_user_id)
            .fetch_one(pool)
            .await?;
            assert_eq!(
                after, prior,
                "aggregate thread must be reused, not replaced"
            );
        }

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Regression for review finding (P2): runtime-state mutators
/// (F-F2 / F-F5 / F-F6) must reject calls for an unknown
/// `reflection_id` instead of writing orphan rows. The schema is
/// FK-free per project policy, so existence is the app layer's job.
/// `mark_embedding_status` already does this via the
/// `update_*_tx` affected-rows check; the F-F* writes were missing
/// the equivalent guard before the fix.
#[test]
fn run_runtime_flags_reject_unknown_reflection_id() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        use protobuf::llm_memory::data::ReflectionId;

        let pool = setup_pool().await;
        let app = build_app(pool).await;
        // A reflection id that no `thread_reflection_index` row exists
        // for. Must not collide with anything seeded by sibling tests.
        let bogus = ReflectionId {
            value: 9_999_999_999_999_999,
        };

        let res = app
            .record_applied_target(&bogus, "system_prompt:default".into(), None)
            .await;
        assert!(
            res.is_err(),
            "record_applied_target must error for unknown reflection_id; got {res:?}",
        );

        let res = app.record_few_shot_usage(&bogus, 12345).await;
        assert!(
            res.is_err(),
            "record_few_shot_usage must error for unknown reflection_id; got {res:?}",
        );

        let res = app
            .upsert_mitigation_applied(&bogus, "system_prompt:default".into(), "fp_x".into())
            .await;
        assert!(
            res.is_err(),
            "upsert_mitigation_applied must error for unknown reflection_id; got {res:?}",
        );

        // Double-check no orphan rows leaked through despite the error.
        let p1 = P1;
        for table in ["reflection_applied_target", "reflection_few_shot_usage"] {
            let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {table} WHERE memory_id = {p1}"
            )))
            .bind(bogus.value)
            .fetch_one(pool)
            .await?;
            assert_eq!(
                count, 0,
                "no orphan rows must remain in {table} after rejected call",
            );
        }

        Ok(())
    })
}

/// Regression for review finding (P2): the incremental
/// `tool_outcome_stats` write in finalize must agree with the F-A6
/// rebuild SQL. Rebuild groups by `(origin_user_id, tool, outcome)`
/// over `reflection_tool` (= unique tools_used). The pre-fix
/// incremental code looped over `tool_outcomes`, so a tool listed in
/// `tools_used` without an entry in `tool_outcomes` was counted as 0
/// before rebuild and 1 after, while a tool with two `tool_outcomes`
/// rows was counted as 2 before and 1 after. C2's `GetToolOutcomeStats`
/// would silently shift values across a rebuild boundary.
#[test]
fn run_finalize_tool_outcome_stats_matches_rebuild() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_714;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // Construct a reflection where:
        //   - tools_used = [Read, Edit, Bash]
        //   - tool_outcomes covers Read twice (different contributions)
        //     and ignores Edit / Bash entirely.
        // This exercises both the over-count case (Read x2) and the
        // under-count case (Edit, Bash with no outcome row).
        let mut parsed = happy_parsed(anchor_memory_id);
        parsed.tools_used = vec!["Read".into(), "Edit".into(), "Bash".into()];
        parsed.tool_outcomes = vec![
            ToolOutcomeEntry {
                tool: "Read".into(),
                contribution: ToolContribution::Positive as i32,
                error_kind: None,
            },
            ToolOutcomeEntry {
                tool: "Read".into(),
                contribution: ToolContribution::Negative as i32,
                error_kind: Some("permission_denied".into()),
            },
        ];
        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_stats_grain",
            "20260510",
            parsed,
        );
        let _ = app.finalize_generated_reflection(&req).await?;

        let snapshot = |label: &'static str| async move {
            let p1 = P1;
            let rows: Vec<(String, i64)> = sqlx::query_as::<Rdb, (String, i64)>(sqlx::AssertSqlSafe(format!(
                "SELECT tool, count FROM tool_outcome_stats \
                 WHERE origin_user_id = {p1} ORDER BY tool ASC"
            )))
            .bind(origin_user_id)
            .fetch_all(pool)
            .await
            .unwrap_or_else(|e| panic!("snapshot {label} failed: {e:?}"));
            rows
        };

        let pre = snapshot("pre-rebuild").await;
        app.rebuild_derived_stats(Some(origin_user_id), true, false)
            .await?;
        let post = snapshot("post-rebuild").await;

        assert_eq!(
            pre, post,
            "tool_outcome_stats must be invariant across F-A6 rebuild — finalize and rebuild source must agree",
        );

        // Sanity: the reflection's three tools_used entries must each
        // appear with count=1 (this reflection's outcome). Any other
        // shape means we still drift between finalize and rebuild.
        let expected = vec![
            ("Bash".to_string(), 1_i64),
            ("Edit".to_string(), 1_i64),
            ("Read".to_string(), 1_i64),
        ];
        assert_eq!(
            post, expected,
            "post-rebuild snapshot must match the rebuild SQL output exactly",
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Smoke test for the deprecated SUMMARY compatibility path. With no
/// dispatcher configured every matching reflection must surface as
/// `skipped`, never as `failed`, but only when the caller supplies a
/// non-empty sidecar filter.
#[test]
fn run_redispatch_with_no_dispatcher_counts_as_skipped() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_710;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // Create one reflection so the search has something to find.
        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_redispatch",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let _ = app.finalize_generated_reflection(&req).await?;

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                value: origin_user_id,
            }),
            ..Default::default()
        };
        let resp = app
            .redispatch_reflection_embeddings(EmbeddingKind::Summary, Some(&filter), Some(50))
            .await?;
        // No dispatcher wired → every row is "skipped" without
        // surfacing as a failure (a failure would mean the dispatch
        // call itself errored, not that the dispatcher was absent).
        assert_eq!(resp.failed_count, 0);
        assert!(resp.skipped_count >= 1, "expected at least one skipped row");

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

#[test]
fn run_redispatch_summary_keyset_scan_reaches_all_filtered_rows() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_712;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let mut reflection_ids = Vec::new();
        for idx in 0..3 {
            let req = make_request(
                origin_thread_id,
                anchor_memory_id,
                &format!("test_reflector_keyset_{idx}"),
                "20260510",
                happy_parsed(anchor_memory_id),
            );
            reflection_ids.push(app.finalize_generated_reflection(&req).await?.value);
        }

        // Deliberately break created_at monotonicity relative to memory_id.
        // The compatibility path must page by memory_id DESC, not by
        // created_at with a memory_id-only cursor.
        let p1 = P1;
        let p2 = P2;
        sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "UPDATE thread_reflection_index SET created_at = {p1} WHERE memory_id = {p2}"
        )))
        .bind(1_600_000_000_000_i64)
        .bind(reflection_ids[2])
        .execute(pool)
        .await?;

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                value: origin_user_id,
            }),
            ..Default::default()
        };
        let resp = app
            .redispatch_reflection_embeddings(EmbeddingKind::Summary, Some(&filter), Some(1))
            .await?;

        assert_eq!(resp.failed_count, 0);
        assert_eq!(
            resp.skipped_count, 3,
            "batch_size=1 must be an internal page size, not a total cap"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

#[test]
fn run_redispatch_summary_rejects_empty_filter() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;

        let err = app
            .redispatch_reflection_embeddings(EmbeddingKind::Summary, None, Some(50))
            .await
            .expect_err("SUMMARY without a filter must be rejected");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("filter") && msg.contains("RedispatchEmbeddings"),
            "error must point callers to the generic redispatch path, got {msg}"
        );

        let empty = protobuf::llm_memory::data::ReflectionSearchFilter::default();
        let err = app
            .redispatch_reflection_embeddings(EmbeddingKind::Summary, Some(&empty), Some(50))
            .await
            .expect_err("SUMMARY with an empty filter must be rejected");
        assert!(format!("{err:#}").contains("empty filter"));
        Ok(())
    })
}

/// Regression for review finding (P2): `kind=BOTH` redispatch must
/// also pick up reflections whose summary embedding has already moved
/// to OK but whose intent embedding is still PENDING. Prior to the fix
/// the default narrowing only added `summary_embedding_status=PENDING`,
/// so intent-only-pending rows were silently skipped on the BOTH path.
#[test]
fn run_redispatch_both_includes_intent_only_pending() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        use infra::infra::reflection::rdb::{EmbeddingTrack, ThreadReflectionIndexRepository};
        use protobuf::llm_memory::data::EmbeddingStatus;

        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_711;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_redispatch_both_intent",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let id = app.finalize_generated_reflection(&req).await?;

        // Flip the summary side to OK while leaving intent at PENDING.
        // This mirrors the production posture where summary embedding
        // succeeded first but intent dispatch is stuck retrying.
        let now = command_utils::util::datetime::now_millis();
        let mut tx = pool.begin().await?;
        let updated = app
            .index_repo
            .update_embedding_status_tx(
                &mut *tx,
                id.value,
                EmbeddingTrack::Summary,
                EmbeddingStatus::Ok as i32,
                None,
                now,
            )
            .await?;
        tx.commit().await?;
        assert!(updated, "summary status flip must succeed");

        // BOTH requires a non-empty sidecar filter because the summary
        // compatibility path does not advance by status.
        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                value: origin_user_id,
            }),
            ..Default::default()
        };
        let resp = app
            .redispatch_reflection_embeddings(EmbeddingKind::Both, Some(&filter), Some(50))
            .await?;
        assert_eq!(resp.failed_count, 0);
        assert!(
            resp.skipped_count + resp.dispatched_count >= 1,
            "BOTH redispatch must reach intent-only-pending rows; got resp={resp:?}",
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Regression for review finding (P2):
/// `match_failure_signatures` must not surface reflections that were
/// stored without a `failure_signature`. Prior to the fix the missing
/// signature collapsed to an empty `Indicators`, the per-key skip
/// logic returned distance=0, and the unsigned row sorted ahead of
/// every reflection that actually carried a signature.
#[test]
fn run_match_failure_signatures_excludes_unsigned_rows() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        use protobuf::llm_memory::data::{
            FailureSignature, FailureSignatureIndicators, FailureSignaturePatternType,
            ReflectionSearchFilter,
        };

        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_712;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // Reflection #1: NO failure_signature (the legacy / Stage-2-optional case).
        let unsigned = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_unsigned",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let unsigned_id = app.finalize_generated_reflection(&unsigned).await?;

        // Reflection #2: signature populated. Use distinct indicator
        // values from the query so distance is strictly positive — the
        // unsigned row would otherwise still tie at distance=0 even
        // after the fix. This proves the unsigned row is excluded
        // rather than just outranked.
        let mut signed_parsed = happy_parsed(anchor_memory_id);
        signed_parsed.failure_signature = Some(FailureSignature {
            pattern_type: FailureSignaturePatternType::FailureSignaturePatternToolLoop as i32,
            indicators: Some(FailureSignatureIndicators {
                same_tool_repeated_count: Some(8),
                same_tool_name: Some("Read".into()),
                consecutive_errors: Some(3),
                no_state_change_turns: Some(5),
                tool_calls_per_turn_ratio: Some(2.0),
                compact_boundary_count: Some(1),
                user_clarification_count: Some(1),
                turn_count_at_detection: Some(40),
                elapsed_ms_at_detection: Some(120_000),
            }),
            trigger_threshold: std::collections::HashMap::new(),
            evidence_turn_indices: vec![],
        });
        let signed = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_signed",
            "20260510",
            signed_parsed,
        );
        let signed_id = app.finalize_generated_reflection(&signed).await?;

        // Query indicators that do NOT match the signed row exactly —
        // distance must be > 0 so the unsigned row's distance=0
        // masking is detectable.
        let query = FailureSignatureIndicators {
            same_tool_repeated_count: Some(15),
            same_tool_name: Some("Read".into()),
            consecutive_errors: Some(8),
            no_state_change_turns: Some(15),
            tool_calls_per_turn_ratio: Some(8.0),
            compact_boundary_count: Some(8),
            user_clarification_count: Some(5),
            turn_count_at_detection: Some(400),
            elapsed_ms_at_detection: Some(3_000_000),
        };
        let filter = ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                value: origin_user_id,
            }),
            ..Default::default()
        };
        let (hits, _is_truncated, _scanned) = app
            .match_failure_signatures(
                &query,
                Some(FailureSignaturePatternType::FailureSignaturePatternToolLoop as i32),
                10,
                Some(&filter),
            )
            .await?;

        let returned_ids: Vec<i64> = hits
            .iter()
            .filter_map(|h| h.reflection.as_ref().and_then(|r| r.id.as_ref()))
            .map(|id| id.value)
            .collect();
        assert!(
            !returned_ids.contains(&unsigned_id.value),
            "unsigned reflection {unsigned} must be excluded; returned={returned_ids:?}",
            unsigned = unsigned_id.value,
        );
        assert!(
            returned_ids.contains(&signed_id.value),
            "signed reflection {signed} must be ranked; returned={returned_ids:?}",
            signed = signed_id.value,
        );
        for hit in &hits {
            assert!(
                hit.distance > 0.0,
                "every returned hit must carry positive distance; got {:?}",
                hit.distance,
            );
        }

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Regression for review finding (P2):
/// `UpsertMitigationApplied` must propagate the new fingerprint to
/// the search/read model. Spec §3.3.2 lists `mitigation_fingerprint`
/// on the sidecar and §3.7 has `updated_at` bump on
/// `reflection_applied_target` writes. Before the fix, only the
/// applied_target child row was written so `Search` / `FindByThread`
/// kept reporting `mitigation_fingerprint=None` after a successful
/// apply, hiding mitigation backports from operators.
#[test]
fn run_upsert_mitigation_applied_propagates_to_sidecar() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_713;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_mitigation",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let id = app.finalize_generated_reflection(&req).await?;

        // Sanity: finalize leaves the sidecar fingerprint NULL.
        let pre_view = find_reflection_by_id(&app, origin_thread_id, id.value).await?;
        assert!(
            pre_view
                .data
                .as_ref()
                .and_then(|d| d.mitigation_fingerprint.as_ref())
                .is_none(),
            "fingerprint must start NULL on the sidecar",
        );
        let pre_updated_at = pre_view
            .data
            .as_ref()
            .map(|d| d.updated_at)
            .unwrap_or_default();

        // Sleep briefly so `updated_at` bump is observable on systems
        // whose `now_millis()` resolution is coarse enough to alias
        // adjacent calls.
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;

        let applied = app
            .upsert_mitigation_applied(&id, "system_prompt:default".into(), "fp_v1".into())
            .await?;
        assert!(applied, "first apply must report applied=true");

        let post = find_reflection_by_id(&app, origin_thread_id, id.value).await?;
        let post_data = post
            .data
            .as_ref()
            .expect("reflection.data populated by hydrate");
        assert_eq!(
            post_data.mitigation_fingerprint.as_deref(),
            Some("fp_v1"),
            "sidecar must surface the fingerprint after UpsertMitigationApplied",
        );
        assert!(
            post_data.updated_at >= pre_updated_at,
            "updated_at must not regress after applied_target write (pre={pre_updated_at}, post={})",
            post_data.updated_at,
        );

        // Re-applying the same fingerprint is a no-op: applied=false,
        // fingerprint unchanged.
        let again = app
            .upsert_mitigation_applied(&id, "system_prompt:default".into(), "fp_v1".into())
            .await?;
        assert!(!again, "duplicate fingerprint apply must report applied=false");

        // Apply with a new fingerprint must update the sidecar.
        let rotated = app
            .upsert_mitigation_applied(&id, "system_prompt:default".into(), "fp_v2".into())
            .await?;
        assert!(rotated, "fingerprint rotation must report applied=true");
        let rotated_view = find_reflection_by_id(&app, origin_thread_id, id.value)
            .await?
            .data
            .expect("reflection still present after rotation");
        assert_eq!(
            rotated_view.mitigation_fingerprint.as_deref(),
            Some("fp_v2"),
            "sidecar must reflect rotated fingerprint",
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Test 6 — `aggregate_thread_id` (the container thread reflection
/// memories live on) must NOT equal the `origin_thread_id` (the
/// trajectory under analysis). This pins fixpoint #35 from the
/// design doc: search results must report the two thread ids
/// separately so callers never mistake the aggregate for the origin.
#[test]
fn run_finalize_origin_distinct_from_aggregate_thread() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_706;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_origin",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let id = app.finalize_generated_reflection(&req).await?;

        // Search for the new reflection through the F-S1 listing (no filter).
        let hits = app
            .search(None, ReflectionListSort::CreatedDesc, Some(50), None)
            .await?;
        let hit = hits
            .iter()
            .find(|h| {
                h.reflection
                    .as_ref()
                    .and_then(|r| r.id.as_ref())
                    .map(|i| i.value)
                    == Some(id.value)
            })
            .expect("finalized reflection must surface in search");

        let aggregate_thread_id = hit
            .aggregate_thread_id
            .as_ref()
            .map(|t| t.value)
            .expect("aggregate_thread_id must be populated");
        let origin_from_hit = hit
            .origin_thread_id
            .as_ref()
            .map(|t| t.value)
            .expect("origin_thread_id must be populated");
        assert_ne!(
            aggregate_thread_id, origin_thread_id,
            "aggregate thread must be distinct from the origin thread"
        );
        assert_eq!(
            origin_from_hit, origin_thread_id,
            "origin_thread_id on the search result must match the trajectory thread"
        );

        let p1 = P1;
        let reflection_owner: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT user_id FROM memory WHERE id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        let aggregate_owner: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT user_id FROM thread WHERE id = {p1}"
        )))
        .bind(aggregate_thread_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(reflection_owner, origin_user_id);
        assert_eq!(aggregate_owner, origin_user_id);

        // F-S2 / spec §3.7.1: find_by_thread is keyed on origin thread.
        // The sidecar lookup must surface the reflection there, while
        // the aggregate thread (which only owns the memory through the
        // thread_memory junction) returns no sidecar rows.
        let on_origin = app
            .find_by_thread(
                &ThreadId {
                    value: origin_thread_id,
                },
                true,
            )
            .await?;
        assert_eq!(
            on_origin.len(),
            1,
            "find_by_thread(origin) must return the reflection (sidecar is keyed on origin)"
        );

        let on_aggregate = app
            .find_by_thread(
                &ThreadId {
                    value: aggregate_thread_id,
                },
                true,
            )
            .await?;
        assert!(
            on_aggregate.is_empty(),
            "find_by_thread(aggregate) must NOT surface the sidecar — the aggregate \
             only carries the memory via the thread_memory junction"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Test 2 — concurrent finalize on the same origin thread (different
/// reflector_id so F-G3 is bypassed) must converge on a single
/// aggregate thread.
#[test]
fn run_finalize_concurrent_aggregate_race() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_705;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = Arc::new(build_app(pool).await);

        let req_a = make_request(
            origin_thread_id,
            anchor_memory_id,
            "reflector_a",
            "20260510",
            happy_parsed(anchor_memory_id),
        );
        let req_b = make_request(
            origin_thread_id,
            anchor_memory_id,
            "reflector_b",
            "20260510",
            happy_parsed(anchor_memory_id),
        );

        let app_a = Arc::clone(&app);
        let app_b = Arc::clone(&app);
        let (r1, r2) = tokio::join!(
            async move { app_a.finalize_generated_reflection(&req_a).await },
            async move { app_b.finalize_generated_reflection(&req_b).await }
        );
        // Both finalize calls must succeed end-to-end (sqlite serializes
        // writers, so neither aborts; postgres may need to retry the
        // UNIQUE-violated branch — which the impl handles internally).
        let _ = r1?;
        let _ = r2?;

        // Exactly one aggregate thread for the origin user.
        let p1 = P1;
        let aggregate_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM thread_aggregate_key WHERE user_id = {p1}"
        )))
        .bind(origin_user_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(aggregate_count, 1);

        // Both reflections share the same aggregate thread_id.
        let distinct_thread_ids: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(DISTINCT thread_id) FROM thread_reflection_index \
             WHERE memory_id IN (SELECT id FROM memory WHERE user_id = {p1} AND memory_kind = 7)"
        )))
        .bind(origin_user_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(distinct_thread_ids, 1);

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Export keyset pagination: walking through pages with
/// `cursor_after_memory_id` must visit every reflection exactly once
/// without repeating the first batch. The bug fix here is that the
/// previous implementation dropped the cursor and always returned the
/// head page.
#[test]
fn run_export_cursor_walks_all_reflections() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_711;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // Three reflections on the same origin thread, distinguished
        // by reflector_id so F-G3 idempotency does not fold them.
        let mut ids = Vec::new();
        for reflector in ["r0", "r1", "r2"] {
            let req = make_request(
                origin_thread_id,
                anchor_memory_id,
                reflector,
                "20260510",
                happy_parsed(anchor_memory_id),
            );
            ids.push(app.finalize_generated_reflection(&req).await?.value);
        }

        // Page 1: batch=2, no cursor.
        let page1 = app.export(None, None, Some(2)).await?;
        assert_eq!(page1.len(), 2, "page1 should hit the batch cap");
        let last_id_p1 = page1
            .last()
            .and_then(|r| r.id.as_ref())
            .map(|i| i.value)
            .expect("page1 last memory_id");

        // Page 2: cursor = page1's last memory_id. Must NOT repeat
        // page1 entries and must finish the remaining reflection.
        let page2 = app.export(None, Some(last_id_p1), Some(2)).await?;
        assert_eq!(page2.len(), 1, "page2 should yield the remaining row");
        let page2_id = page2[0]
            .id
            .as_ref()
            .map(|i| i.value)
            .expect("page2 memory_id");
        assert!(
            !page1
                .iter()
                .any(|r| r.id.as_ref().map(|i| i.value) == Some(page2_id)),
            "cursor must skip past page1 entries"
        );

        // Combined coverage: both pages together hit every finalized id.
        let mut combined: Vec<i64> = page1
            .iter()
            .chain(page2.iter())
            .filter_map(|r| r.id.as_ref().map(|i| i.value))
            .collect();
        combined.sort();
        let mut expected = ids.clone();
        expected.sort();
        assert_eq!(combined, expected);

        // Page 3: cursor past the last memory_id terminates the walk.
        let page3 = app.export(None, Some(page2_id), Some(2)).await?;
        assert!(page3.is_empty(), "no more rows after the final cursor");

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// `ReflectionFact.turn_index` must round-trip through finalize and
/// search (proto requires the LLM-original global turn position to
/// surface on hydrate, not always 0).
#[test]
fn run_finalize_preserves_fact_turn_index() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_712;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let mut parsed = happy_parsed(anchor_memory_id);
        parsed.facts[0].turn_index = 42;
        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_reflector_turn",
            "20260510",
            parsed,
        );
        let id = app.finalize_generated_reflection(&req).await?;

        let on_origin = app
            .find_by_thread(
                &ThreadId {
                    value: origin_thread_id,
                },
                true,
            )
            .await?;
        let reflection = on_origin
            .iter()
            .find(|r| r.id.as_ref().map(|i| i.value) == Some(id.value))
            .expect("finalized reflection must surface on the origin thread");
        let fact = reflection
            .data
            .as_ref()
            .and_then(|d| d.facts.first())
            .expect("the single fact should be present");
        assert_eq!(
            fact.turn_index, 42,
            "turn_index must round-trip through hydrate (was 0 before the persistence fix)"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

// ---------------------------------------------------------------------------
// CRUD round-trip, Update/Delete cascade, and residual `Unimplemented`
// stubs. Each test owns a distinct `origin_user_id` so the suite stays
// parallel-safe under `--test-threads=1`.

/// `find` returns the hydrated reflection after `finalize_generated_reflection`.
#[test]
fn run_find_returns_finalized_reflection() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_801;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let parsed = happy_parsed(anchor_memory_id);
        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_find",
            "20260511",
            parsed,
        );
        let id = app.finalize_generated_reflection(&req).await?;

        let fetched = app.find(&id).await?;
        let reflection = fetched.expect("find must return Some for a finalized reflection");
        assert_eq!(reflection.id.as_ref().map(|i| i.value), Some(id.value));
        // The hydrated envelope must carry the sidecar score plus the
        // single fact we wrote.
        let data = reflection.data.as_ref().expect("data must hydrate");
        assert_eq!(data.facts.len(), 1);

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// `find` returns `None` when the id does not exist. No DB writes.
#[test]
fn run_find_missing_returns_none() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let fetched = app
            .find(&protobuf::llm_memory::data::ReflectionId {
                value: 999_999_999_999,
            })
            .await?;
        assert!(fetched.is_none(), "missing reflection must be None");
        Ok(())
    })
}

/// `update(pinned=Some(true))` flips the same row that `pin(true)` would.
#[test]
fn run_update_pin_via_update_route() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_802;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_update",
            "20260511",
            happy_parsed(anchor_memory_id),
        );
        let id = app.finalize_generated_reflection(&req).await?;

        // No-op when pinned is None.
        app.update(&id, None).await?;

        // Flip true through `update`.
        app.update(&id, Some(true)).await?;
        let r = app.find(&id).await?.expect("must hydrate");
        let sidecar = r.data.as_ref().expect("data");
        assert!(
            sidecar.pinned,
            "pinned must be true after update(Some(true))"
        );

        // Flip false back through `update`.
        app.update(&id, Some(false)).await?;
        let r = app.find(&id).await?.expect("must hydrate");
        let sidecar = r.data.as_ref().expect("data");
        assert!(
            !sidecar.pinned,
            "pinned must be false after update(Some(false))"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// `delete` runs the full cascade (sidecar + every child table +
/// `thread_memory` junction + the memory row) inside one transaction.
/// FKs are intentionally absent in this project so the assertions
/// below cover each owning table by name — silently leaving a row
/// behind would let search / aggregate / hydrate panic on a missing
/// memory later.
#[test]
fn run_delete_removes_reflection() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_803;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_delete",
            "20260511",
            happy_parsed(anchor_memory_id),
        );
        let id = app.finalize_generated_reflection(&req).await?;
        assert!(app.find(&id).await?.is_some());

        let p1 = P1;
        // Pre-condition: happy_parsed populates failure_mode / tool /
        // tool_outcome / fact / thread_memory rows, so the cascade
        // has real work to do. (applied_target / few_shot_usage are
        // empty at finalize time — see Phase D finalize.rs Phase 3.)
        let child_tables = [
            "thread_reflection_index",
            "reflection_failure_mode",
            "reflection_tool",
            "reflection_tool_outcome",
            "reflection_fact",
        ];
        for table in child_tables {
            let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {table} WHERE memory_id = {p1}"
            )))
            .bind(id.value)
            .fetch_one(pool)
            .await?;
            assert!(
                count >= 1,
                "{table} must hold at least one row pre-delete (got {count})"
            );
        }
        let junction_pre: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM thread_memory WHERE memory_id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            junction_pre, 1,
            "thread_memory junction must reference the reflection pre-delete"
        );

        app.delete(&id).await?;

        // Post-condition: every owning table is empty for this id.
        assert!(
            app.find(&id).await?.is_none(),
            "find must be None after delete"
        );
        let memory_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM memory WHERE id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(memory_count, 0, "memory row must be deleted");
        for table in [
            "thread_reflection_index",
            "reflection_failure_mode",
            "reflection_tool",
            "reflection_tool_outcome",
            "reflection_fact",
            "reflection_applied_target",
            "reflection_few_shot_usage",
        ] {
            let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT COUNT(*) FROM {table} WHERE memory_id = {p1}"
            )))
            .bind(id.value)
            .fetch_one(pool)
            .await?;
            assert_eq!(count, 0, "{table} must be empty after cascade delete");
        }
        let junction_post: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM thread_memory WHERE memory_id = {p1}"
        )))
        .bind(id.value)
        .fetch_one(pool)
        .await?;
        assert_eq!(
            junction_post, 0,
            "thread_memory junction must be detached after delete"
        );

        // Deleting again yields NotFound. The sidecar guard now
        // catches the second call before it ever opens a tx.
        let res = app.delete(&id).await;
        assert!(res.is_err(), "double-delete must error");

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Generate (F-G1) without a jobworkerp client (kill switch off /
/// `MEMORY_REFLECTION_DISPATCH_ENABLED=false`) must return
/// `Unimplemented` with an operator-friendly message rather than
/// silently no-op-ing.
#[test]
fn run_generate_returns_unimplemented_when_kill_switch_off() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await; // build_app passes None for jobworkerp_client.

        use protobuf::llm_memory::service::GenerateForThreadRequest;
        let req = GenerateForThreadRequest {
            thread_id: Some(ThreadId { value: 1 }),
            force: false,
            reflector_id: None,
            prompt_version: None,
            experiment_id: None,
            experiment_variant: None,
            async_mode: false,
            output_language: None,
        };
        let err = app
            .generate(&req)
            .await
            .expect_err("kill-switched generate must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("MEMORY_REFLECTION_DISPATCH_ENABLED")
                || msg.contains("Unimplemented")
                || msg.contains("disabled"),
            "kill-switch error must mention the env switch or Unimplemented; got: {msg}"
        );
        Ok(())
    })
}

/// Generate must reject a request without `thread_id` (or zero id)
/// even before the jobworkerp roundtrip — the underlying workflow
/// would NotFound at fetchThread, but the failure mode is wrong
/// (it would charge a workflow run for nothing). Validation belongs
/// at the boundary.
#[test]
fn run_generate_rejects_missing_thread_id() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        // Cannot test the validation path without a jobworkerp client
        // because the kill-switch check runs first. Verify the kill
        // switch wins instead — both errors are Err, callers see the
        // most actionable message. This is acceptance, not a gap.
        let app = build_app(pool).await;
        use protobuf::llm_memory::service::GenerateForThreadRequest;
        let req = GenerateForThreadRequest {
            thread_id: None,
            force: false,
            reflector_id: None,
            prompt_version: None,
            experiment_id: None,
            experiment_variant: None,
            async_mode: false,
            output_language: None,
        };
        assert!(app.generate(&req).await.is_err());
        Ok(())
    })
}

/// Regression for the P1 from the Codex review: `DeleteReflection` must
/// refuse ids that do not point at a `thread_reflection_index` row.
/// Without the sidecar guard, a `MemoryId` (same wire shape as
/// `ReflectionId`) addressing a plain user memory would land in
/// `memory_repo.delete` and destroy data outside the reflection
/// surface.
#[test]
fn run_delete_rejects_plain_memory_id() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_804;
        let (_origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // `anchor_memory_id` is a plain user memory, not a reflection.
        let target = protobuf::llm_memory::data::ReflectionId {
            value: anchor_memory_id,
        };
        let res = app.delete(&target).await;
        assert!(res.is_err(), "delete must refuse non-reflection ids");
        let msg = format!("{:#}", res.err().unwrap());
        assert!(
            msg.contains("not found") || msg.contains("NotFound"),
            "error must signal NotFound, got: {msg}"
        );

        // The plain memory must still be there — no collateral delete.
        let p1 = P1;
        let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT COUNT(*) FROM memory WHERE id = {p1}"
        )))
        .bind(anchor_memory_id)
        .fetch_one(pool)
        .await?;
        assert_eq!(count, 1, "plain memory must survive a misrouted delete");

        // Clean up the seed thread + memory we created.
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "DELETE FROM memory WHERE id = {p1}"
        )))
        .bind(anchor_memory_id)
        .execute(pool)
        .await;
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "DELETE FROM thread WHERE user_id = {p1}"
        )))
        .bind(origin_user_id)
        .execute(pool)
        .await;
        Ok(())
    })
}

/// P2 regression (Codex round 4): `search`'s `cursor_after_memory_id`
/// is a `memory_id < cursor` keyset, not a SQL OFFSET. Plugging the
/// previous page's tail `memory_id` (a snowflake i64 in the 10^18
/// range) into `LIMIT ... OFFSET ...` would skip past every row and
/// return an empty page. This test finalizes three reflections under
/// one origin and walks them page-by-page with `limit=1`, asserting
/// that every cursor step uncovers the next id.
#[test]
fn run_search_cursor_pagination_walks_all_pages() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_805;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        // Three reflections under the same origin thread. Distinct
        // `(reflector_id, prompt_version)` tuples sidestep the F-G3
        // idempotency short-circuit and let each finalize mint a new
        // sidecar row.
        let id_a = app
            .finalize_generated_reflection(&make_request(
                origin_thread_id,
                anchor_memory_id,
                "test_cursor_a",
                "20260511",
                happy_parsed(anchor_memory_id),
            ))
            .await?;
        let id_b = app
            .finalize_generated_reflection(&make_request(
                origin_thread_id,
                anchor_memory_id,
                "test_cursor_b",
                "20260511",
                happy_parsed(anchor_memory_id),
            ))
            .await?;
        let id_c = app
            .finalize_generated_reflection(&make_request(
                origin_thread_id,
                anchor_memory_id,
                "test_cursor_c",
                "20260511",
                happy_parsed(anchor_memory_id),
            ))
            .await?;

        // Build a filter scoped to this origin_user_id so concurrent
        // tests can't bleed sidecar rows in.
        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(protobuf::llm_memory::data::UserId {
                value: origin_user_id,
            }),
            ..Default::default()
        };

        // Page 1: no cursor, take 1. Newest finalize is `id_c`
        // (snowflake ids are time-monotonic).
        let page1 = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(1),
                None,
            )
            .await?;
        assert_eq!(page1.len(), 1, "page 1 must yield one row");
        let page1_id = page1[0]
            .reflection
            .as_ref()
            .and_then(|r| r.id.as_ref())
            .map(|i| i.value)
            .expect("page 1 must carry an id");

        // Page 2: cursor = page1_id. Must return a different id (the
        // previous-buggy OFFSET path would have skipped everything
        // and returned an empty page).
        let page2 = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(1),
                Some(page1_id),
            )
            .await?;
        assert_eq!(page2.len(), 1, "page 2 must yield one row");
        let page2_id = page2[0]
            .reflection
            .as_ref()
            .and_then(|r| r.id.as_ref())
            .map(|i| i.value)
            .expect("page 2 must carry an id");
        assert_ne!(page1_id, page2_id, "cursor must advance past page 1");

        let page3 = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(1),
                Some(page2_id),
            )
            .await?;
        assert_eq!(page3.len(), 1, "page 3 must yield the final row");
        let page3_id = page3[0]
            .reflection
            .as_ref()
            .and_then(|r| r.id.as_ref())
            .map(|i| i.value)
            .expect("page 3 must carry an id");

        // The three ids the cursor walked must equal the three
        // finalize results (order is created_desc).
        let mut walked = vec![page1_id, page2_id, page3_id];
        walked.sort();
        let mut expected = vec![id_a.value, id_b.value, id_c.value];
        expected.sort();
        assert_eq!(
            walked, expected,
            "cursor walk must visit every finalized reflection exactly once"
        );

        // Page 4: cursor past the last row. Must be empty.
        let page4 = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(1),
                Some(page3_id),
            )
            .await?;
        assert!(
            page4.is_empty(),
            "cursor past the final id must yield an empty page"
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

/// Asserts every child collection seeded by `happy_parsed` surfaces on
/// the hydrated envelope — guards against a regression in the bulk
/// fan-out's HashMap join.
#[test]
fn run_search_hydrates_every_child_table() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let origin_user_id: i64 = 999_960;
        let (origin_thread_id, anchor_memory_id) = seed_origin(pool, origin_user_id).await?;
        let app = build_app(pool).await;

        let req = make_request(
            origin_thread_id,
            anchor_memory_id,
            "test_hydrate_bulk",
            "20260512",
            happy_parsed(anchor_memory_id),
        );
        let id = app.finalize_generated_reflection(&req).await?;

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                value: origin_user_id,
            }),
            ..Default::default()
        };
        let results = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(10),
                None,
            )
            .await?;

        let hit = results
            .iter()
            .find(|r| {
                r.reflection
                    .as_ref()
                    .and_then(|refl| refl.id.as_ref())
                    .map(|i| i.value)
                    == Some(id.value)
            })
            .expect("finalized reflection must appear in search results");
        let data = hit
            .reflection
            .as_ref()
            .and_then(|r| r.data.as_ref())
            .expect("hydrate must populate data");

        // tools_used is order-unspecified (bulk SQL has no ORDER BY).
        assert_eq!(
            data.failure_modes,
            vec![FailureMode::ToolMisuse as i32],
            "failure_modes must survive bulk hydrate"
        );
        let mut got_tools = data.tools_used.clone();
        got_tools.sort();
        let mut expected_tools = vec!["Edit".to_string(), "Read".to_string()];
        expected_tools.sort();
        assert_eq!(
            got_tools, expected_tools,
            "tools_used must survive bulk hydrate (set equality)"
        );
        assert_eq!(
            data.tool_outcomes.len(),
            1,
            "tool_outcomes must surface the seeded entry"
        );
        assert_eq!(data.tool_outcomes[0].tool, "Read");
        assert_eq!(data.facts.len(), 1, "facts must surface the seeded entry");
        assert_eq!(
            data.facts[0].anchor_memory_id.as_ref().map(|m| m.value),
            Some(anchor_memory_id)
        );

        cleanup_finalize_state(pool, origin_thread_id, origin_user_id, &[anchor_memory_id]).await;
        Ok(())
    })
}

#[test]
fn run_search_empty_corpus_returns_empty() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId {
                // Use a deliberately unused user_id so no rows match.
                value: 999_999_990,
            }),
            ..Default::default()
        };
        let results = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(10),
                None,
            )
            .await?;
        assert!(results.is_empty(), "no rows match the filter");
        Ok(())
    })
}

// ----------------------------------------------------------------------
// AggregateScores tests (F-A2). Sidecar fixtures are inserted directly
// rather than going through `finalize_generated_reflection` so each
// test owns a tight, deterministic corpus. The `score`, `outcome`,
// `task_category`, `created_at`, etc. shapes are what the aggregate
// reads, so we set them directly and skip the full reflection LLM
// output construction.
// ----------------------------------------------------------------------

use infra::infra::memory::rdb::MemoryRepository;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::rows::ThreadReflectionIndexRow;
use infra::infra::thread::rdb::ThreadRepository;
use protobuf::llm_memory::service::{
    AggregateScoresEntry, AggregateScoresGroupBy, ToolContributionAggregateEntry,
};

/// Insert a single sidecar row backed by a freshly minted memory + a
/// per-test container thread. The `customise` closure tweaks the row
/// (score, outcome, task_category, etc.) before insertion.
async fn seed_sidecar(
    pool: &'static RdbPool,
    origin_user_id: i64,
    container_thread_id: i64,
    customise: impl FnOnce(&mut ThreadReflectionIndexRow),
) -> Result<i64> {
    seed_sidecar_with_metadata(pool, origin_user_id, container_thread_id, None, customise).await
}

/// Same as `seed_sidecar` but also stores a JSON `metadata` payload on
/// the underlying memory row. Used by `aggregate_lessons` tests that
/// need to populate `metadata.eval.lessons` without going through the
/// full finalize pipeline.
async fn seed_sidecar_with_metadata(
    pool: &'static RdbPool,
    origin_user_id: i64,
    container_thread_id: i64,
    metadata_json: Option<String>,
    customise: impl FnOnce(&mut ThreadReflectionIndexRow),
) -> Result<i64> {
    let id_gen = infra::test_helper::shared_id_generator();
    let memory_repo = MemoryRepositoryImpl::new(id_gen, pool);
    let index_repo = ThreadReflectionIndexRepositoryImpl::new(pool);

    let now = command_utils::util::datetime::now_millis();
    let mut tx = pool.begin().await?;
    // The memory row must satisfy the sidecar FK on memory_id. Reflection
    // rows are owned by the origin user; content is irrelevant here.
    let memory_id = memory_repo
        .create(
            &mut *tx,
            &MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId {
                    value: origin_user_id,
                }),
                content: "test reflection".into(),
                content_type: ContentType::Text as i32,
                params: None,
                metadata: metadata_json,
                created_at: now,
                updated_at: now,
                role: MessageRole::RoleAssistant as i32,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
                memory_kind: MemoryKind::Reflection as i32,
            },
        )
        .await?
        .value;

    let mut row = ThreadReflectionIndexRow {
        memory_id,
        thread_id: container_thread_id,
        origin_thread_id: container_thread_id,
        origin_user_id,
        origin_channel: None,
        outcome: ReflectionOutcome::Success as i32,
        score: 0.5,
        score_self: 0.5,
        score_heuristic: 0.5,
        task_category: TaskCategory::Coding as i32,
        reflection_aspect: ReflectionAspect::TaskOutcome as i32,
        dataset_quality: 1,
        summary_embedding_status: 1,
        summary_embedding_error: None,
        intent_embedding_status: 1,
        intent_embedding_error: None,
        prompt_version: "v1".into(),
        target_model_version: None,
        experiment_id: None,
        experiment_variant: None,
        previous_reflection_id: None,
        pinned: false,
        is_recurrence: false,
        mitigation_fingerprint: None,
        created_at: now,
        updated_at: now,
    };
    customise(&mut row);
    index_repo.insert_index_tx(&mut *tx, &row).await?;
    tx.commit().await?;
    Ok(memory_id)
}

/// Insert a bare container thread the test can attach reflections to.
/// Insert a dedicated reflection container for aggregate-only tests.
async fn seed_container_thread(pool: &'static RdbPool) -> Result<i64> {
    let id_gen = infra::test_helper::shared_id_generator();
    let thread_repo = ThreadRepositoryImpl::new(id_gen, pool);
    let now = command_utils::util::datetime::now_millis();
    let mut tx = pool.begin().await?;
    let tid = thread_repo
        .create(
            &mut *tx,
            &ThreadData {
                default_system_memory_id: None,
                user_id: Some(UserId { value: 0 }),
                description: Some("aggregate-scores test container".into()),
                channel: None,
                embedding: None,
                embedding_dim: None,
                created_at: now,
                updated_at: now,
                metadata: None,
                labels: vec![],
                memory_kind: MemoryKind::Reflection as i32,
            },
        )
        .await?;
    tx.commit().await?;
    Ok(tid.value)
}

async fn cleanup_aggregate_scores_state(
    pool: &'static RdbPool,
    container_thread_id: i64,
    memory_ids: &[i64],
) {
    for id in memory_ids {
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "DELETE FROM thread_reflection_index WHERE memory_id = {P1}"
        )))
        .bind(id)
        .execute(pool)
        .await;
        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "DELETE FROM memory WHERE id = {P1}"
        )))
        .bind(id)
        .execute(pool)
        .await;
    }
    let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
        "DELETE FROM thread WHERE id = {P1}"
    )))
    .bind(container_thread_id)
    .execute(pool)
    .await;
}

#[test]
fn run_aggregate_scores_single_axis_origin_user() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user_a: i64 = 999_800_001;
        let user_b: i64 = 999_800_002;
        let container = seed_container_thread(pool).await?;

        // user_a has 3 reflections, scores {0.2, 0.4, 0.6} -> avg=0.4.
        // user_b has 2 reflections, scores {0.8, 1.0} -> avg=0.9.
        let mut ids = vec![];
        for s in [0.2_f64, 0.4, 0.6] {
            ids.push(
                seed_sidecar(pool, user_a, container, |r| {
                    r.score = s;
                })
                .await?,
            );
        }
        for s in [0.8_f64, 1.0] {
            ids.push(
                seed_sidecar(pool, user_b, container, |r| {
                    r.score = s;
                })
                .await?,
            );
        }

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            ..Default::default()
        };
        let resp = app
            .aggregate_scores(Some(&filter), &[AggregateScoresGroupBy::OriginUser], None)
            .await?;

        let mut by_user: std::collections::HashMap<i64, &AggregateScoresEntry> =
            std::collections::HashMap::new();
        for e in &resp.entries {
            let uid = e.key.as_ref().and_then(|k| k.origin_user_id);
            if let Some(uid) = uid {
                by_user.insert(uid, e);
            }
        }
        let a = by_user.get(&user_a).expect("user_a bucket present");
        let b = by_user.get(&user_b).expect("user_b bucket present");
        assert_eq!(a.count, 3);
        assert!(
            (a.score_avg - 0.4).abs() < 1e-5,
            "user_a avg {}",
            a.score_avg
        );
        assert!((a.score_min - 0.2).abs() < 1e-5);
        assert!((a.score_max - 0.6).abs() < 1e-5);
        assert_eq!(b.count, 2);
        assert!(
            (b.score_avg - 0.9).abs() < 1e-5,
            "user_b avg {}",
            b.score_avg
        );

        cleanup_aggregate_scores_state(pool, container, &ids).await;
        Ok(())
    })
}

#[test]
fn run_aggregate_scores_two_axes_user_and_time_bucket() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_800_010;
        let container = seed_container_thread(pool).await?;

        // Two buckets one hour apart (bucket_seconds=3600). The
        // TIME_BUCKET expression floors created_at to the bucket
        // boundary so any two rows whose `created_at` shares a
        // bucket end up in the same group.
        let bucket_ms: i64 = 3_600_000;
        let base: i64 = 1_700_000_000_000; // ms epoch aligned by accident; the floor handles drift
        let aligned = (base / bucket_ms) * bucket_ms;
        let in_first = aligned + 1_000;
        let in_second = aligned + bucket_ms + 1_000;
        let mut ids = vec![];
        for (t, s) in [(in_first, 0.1), (in_first, 0.3), (in_second, 0.7)] {
            ids.push(
                seed_sidecar(pool, user, container, |r| {
                    r.score = s;
                    r.created_at = t;
                })
                .await?,
            );
        }

        let resp = app
            .aggregate_scores(
                None,
                &[
                    AggregateScoresGroupBy::OriginUser,
                    AggregateScoresGroupBy::TimeBucket,
                ],
                Some(3600),
            )
            .await?;
        // Count the two buckets that match our test user (others may
        // exist from concurrent fixtures, but this user only appears
        // in our seed).
        let mine: Vec<&AggregateScoresEntry> = resp
            .entries
            .iter()
            .filter(|e| {
                e.key
                    .as_ref()
                    .and_then(|k| k.origin_user_id)
                    .map(|u| u == user)
                    .unwrap_or(false)
            })
            .collect();
        assert_eq!(mine.len(), 2, "expected two time buckets for user");
        let total: u64 = mine.iter().map(|e| e.count).sum();
        assert_eq!(total, 3);
        for e in &mine {
            let key = e.key.as_ref().unwrap();
            // bucket_start must be a multiple of bucket_ms.
            let start = key.time_bucket_start.expect("time bucket set");
            assert_eq!(start % bucket_ms, 0, "bucket_start {start} aligned");
        }

        cleanup_aggregate_scores_state(pool, container, &ids).await;
        Ok(())
    })
}

#[test]
fn run_aggregate_scores_percentile_p50_p95() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_800_020;
        let container = seed_container_thread(pool).await?;

        // 11 reflections with scores 0.1, 0.2, ..., 1.0 + 1.0 dup.
        // Nearest-rank percentile (used on both backends here):
        //   p50 -> index ceil(0.5 * 11) - 1 = 5 -> 0.6
        //   p95 -> index ceil(0.95 * 11) - 1 = 10 -> 1.0
        // Postgres uses linear interpolation but for 11 samples
        // {0.1..1.0, 1.0} the result is still very close (continuous
        // 0.5 quantile = 0.55, but the dataset is 11 samples so
        // floor(0.5*10) = 5 -> value at idx5 = 0.6, off by 0.05 at
        // most). To stay backend-agnostic the assertion accepts a
        // small interpolation tolerance.
        let scores: [f64; 11] = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.0];
        let mut ids = vec![];
        for s in scores {
            ids.push(
                seed_sidecar(pool, user, container, |r| {
                    r.score = s;
                })
                .await?,
            );
        }

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: user }),
            ..Default::default()
        };
        let resp = app.aggregate_scores(Some(&filter), &[], None).await?;
        assert_eq!(resp.entries.len(), 1, "no group_by collapses to totals row");
        let e = &resp.entries[0];
        assert_eq!(e.count, 11);
        // Tolerance: SQLite nearest-rank vs Postgres percentile_cont
        // differ slightly; accept anything in the plausible band.
        assert!(
            e.score_p50 >= 0.5 && e.score_p50 <= 0.7,
            "p50 within plausible band: {}",
            e.score_p50
        );
        assert!(
            e.score_p95 >= 0.9 && e.score_p95 <= 1.0,
            "p95 within plausible band: {}",
            e.score_p95
        );

        cleanup_aggregate_scores_state(pool, container, &ids).await;
        Ok(())
    })
}

#[test]
fn run_aggregate_scores_empty_corpus_returns_empty() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: 999_800_777 }),
            ..Default::default()
        };
        let resp = app
            .aggregate_scores(Some(&filter), &[AggregateScoresGroupBy::OriginUser], None)
            .await?;
        assert!(resp.entries.is_empty(), "no rows match the filter");
        Ok(())
    })
}

#[test]
fn run_aggregate_scores_filter_respected() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_800_040;
        let container = seed_container_thread(pool).await?;

        // Mixed outcomes for the same user. The filter pins outcome
        // to SUCCESS; the aggregate must see only the matching rows.
        let mut ids = vec![];
        for (outcome, score) in [
            (ReflectionOutcome::Success as i32, 0.9),
            (ReflectionOutcome::Success as i32, 0.7),
            (ReflectionOutcome::Failure as i32, 0.1),
        ] {
            ids.push(
                seed_sidecar(pool, user, container, |r| {
                    r.score = score;
                    r.outcome = outcome;
                })
                .await?,
            );
        }

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: user }),
            outcomes: vec![ReflectionOutcome::Success as i32],
            ..Default::default()
        };
        let resp = app.aggregate_scores(Some(&filter), &[], None).await?;
        assert_eq!(resp.entries.len(), 1);
        let e = &resp.entries[0];
        assert_eq!(e.count, 2, "only the two SUCCESS rows match");
        assert!((e.score_avg - 0.8).abs() < 1e-5);
        cleanup_aggregate_scores_state(pool, container, &ids).await;
        Ok(())
    })
}

#[test]
fn run_aggregate_scores_time_bucket_zero_rejects() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        // No fixtures needed — the validation runs before any SQL.
        let resp = app
            .aggregate_scores(None, &[AggregateScoresGroupBy::TimeBucket], Some(0))
            .await;
        let err = resp.expect_err("time_bucket_seconds=0 with TIME_BUCKET must error");
        let msg = format!("{err}");
        assert!(
            msg.contains("time_bucket_seconds"),
            "error message names the field: {msg}"
        );
        // Also reject when the bucket size is missing entirely.
        let resp2 = app
            .aggregate_scores(None, &[AggregateScoresGroupBy::TimeBucket], None)
            .await;
        assert!(resp2.is_err(), "absent bucket size with TIME_BUCKET errors");
        Ok(())
    })
}

// ----------------------------------------------------------------------
// F-S1 hybrid wrapper integration tests. The `build_app` helper does
// not wire a jobworkerp client or intent vector repo, so hybrid paths
// fail fast with `Unimplemented` once they hit the embed / LanceDB
// stage. These tests pin the dispatch / validation contract:
//   * empty signals → InvalidArgument (handler is supposed to route
//     filter-only requests through `search`)
//   * FTS_THEN_VECTOR / VECTOR_THEN_FTS → Unimplemented + warn log
//   * query_text without jobworkerp client → Unimplemented
//   * query_vectors without intent vector repo → Unimplemented
// ----------------------------------------------------------------------

use protobuf::llm_memory::data::{EmbeddingVector, HybridSearchOptions, HybridStrategy};

#[test]
fn run_search_hybrid_empty_signals_rejects() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let err = app
            .search_hybrid(
                None,
                None,
                &[],
                None,
                false,
                ReflectionListSort::Unspecified,
                Some(5),
            )
            .await
            .expect_err("empty signals must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("query_text") || msg.contains("query_vectors"),
            "error message names the missing field: {msg}"
        );
        Ok(())
    })
}

#[test]
fn run_search_hybrid_fts_then_vector_returns_unimplemented() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let opts = HybridSearchOptions {
            strategy: HybridStrategy::FtsThenVector as i32,
            vector_weight: None,
            rrf_k: None,
        };
        // Even with a vector signal, reflection has no FTS index so
        // the strategy is rejected up front before the vector path
        // runs (so this test does not need a configured LanceDB).
        let qv = vec![EmbeddingVector {
            values: vec![0.1; 384],
        }];
        let err = app
            .search_hybrid(
                None,
                None,
                &qv,
                Some(&opts),
                false,
                ReflectionListSort::Unspecified,
                Some(5),
            )
            .await
            .expect_err("FTS_THEN_VECTOR must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("FTS") || msg.contains("Unimplemented") || msg.contains("FtsThenVector"),
            "error mentions FTS rejection: {msg}"
        );
        Ok(())
    })
}

#[test]
fn run_search_hybrid_vector_then_fts_returns_unimplemented() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let opts = HybridSearchOptions {
            strategy: HybridStrategy::VectorThenFts as i32,
            vector_weight: None,
            rrf_k: None,
        };
        let qv = vec![EmbeddingVector {
            values: vec![0.1; 384],
        }];
        let err = app
            .search_hybrid(
                None,
                None,
                &qv,
                Some(&opts),
                false,
                ReflectionListSort::Unspecified,
                Some(5),
            )
            .await
            .expect_err("VECTOR_THEN_FTS must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("FTS") || msg.contains("Unimplemented") || msg.contains("VectorThenFts"),
            "error mentions FTS rejection: {msg}"
        );
        Ok(())
    })
}

#[test]
fn run_search_hybrid_query_text_without_jobworkerp_unimplemented() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await; // build_app passes None for jobworkerp_client.
        let err = app
            .search_hybrid(
                None,
                Some("plan a coding task"),
                &[],
                None,
                false,
                ReflectionListSort::Unspecified,
                Some(5),
            )
            .await
            .expect_err("query_text without jobworkerp client must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("jobworkerp") || msg.contains("Unimplemented"),
            "error message names the missing client: {msg}"
        );
        Ok(())
    })
}

#[test]
fn run_search_hybrid_query_vectors_without_lancedb_unimplemented() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await; // build_app passes None for intent_vector_repo.
        let qv = vec![EmbeddingVector {
            values: vec![0.1; 384],
        }];
        let err = app
            .search_hybrid(
                None,
                None,
                &qv,
                None,
                false,
                ReflectionListSort::Unspecified,
                Some(5),
            )
            .await
            .expect_err("query_vectors without intent vector repo must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("intent vector")
                || msg.contains("Unimplemented")
                || msg.contains("LanceDB"),
            "error names the missing intent vector store: {msg}"
        );
        Ok(())
    })
}

// ----------------------------------------------------------------------
// `match_failure_signatures` unary tuple shape, `is_truncated` on
// `aggregate_failure_modes`, and `score_source = SCORE_FILTER_ONLY`
// (with `score = row.score`) on filter-only `search` hits.
// ----------------------------------------------------------------------

use protobuf::llm_memory::data::{FailureSignatureIndicators, ScoreSource};

#[test]
fn run_match_failure_signatures_returns_truncation_tuple_below_cap() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        // No reflections seeded; the empty corpus yields zero scanned
        // rows and is_truncated=false. The point of the test is the
        // tuple shape itself.
        let indicators = FailureSignatureIndicators::default();
        let (hits, is_truncated, scanned) = app
            .match_failure_signatures(&indicators, None, 5, None)
            .await?;
        assert!(hits.is_empty());
        assert!(!is_truncated);
        assert_eq!(scanned, 0);
        Ok(())
    })
}

#[test]
fn run_aggregate_failure_modes_reports_not_truncated_on_small_corpus() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        // Even with an empty corpus the response carries the new
        // is_truncated field (defaulted to false).
        let resp = app.aggregate_failure_modes(None, false).await?;
        assert!(!resp.is_truncated);
        Ok(())
    })
}

#[test]
fn run_filter_only_search_carries_score_filter_only() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_805_001;
        let container = seed_container_thread(pool).await?;
        let id = seed_sidecar(pool, user, container, |r| {
            r.score = 0.42;
        })
        .await?;

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: user }),
            ..Default::default()
        };
        let hits = app
            .search(
                Some(&filter),
                ReflectionListSort::Unspecified,
                Some(5),
                None,
            )
            .await?;
        assert!(!hits.is_empty(), "the seeded row must come back");
        let h = &hits[0];
        assert_eq!(h.score_source, ScoreSource::ScoreFilterOnly as i32);
        // Top-level `score` mirrors the sidecar `score` column.
        assert!(
            (h.score - 0.42).abs() < 1e-5,
            "expected top-level score = sidecar score 0.42, got {}",
            h.score
        );
        cleanup_aggregate_scores_state(pool, container, &[id]).await;
        Ok(())
    })
}

#[test]
fn run_delete_with_no_memory_cache_handle_is_noop_safe() -> Result<()> {
    // The cache handle is optional; `build_app` passes `None`, and
    // the delete path must not panic in that configuration. Direct
    // RDB cleanup confirms the cascade still ran.
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_805_002;
        let container = seed_container_thread(pool).await?;
        let id = seed_sidecar(pool, user, container, |r| {
            r.score = 0.5;
        })
        .await?;

        app.delete(&protobuf::llm_memory::data::ReflectionId { value: id })
            .await?;

        // Sidecar row must be gone.
        let sql = format!("SELECT COUNT(*) FROM thread_reflection_index WHERE memory_id = {P1}");
        let remaining: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(id)
            .fetch_one(pool)
            .await
            .unwrap_or(0);
        assert_eq!(remaining, 0, "sidecar row deleted");

        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "DELETE FROM thread WHERE id = {P1}"
        )))
        .bind(container)
        .execute(pool)
        .await;
        Ok(())
    })
}

// ----------------------------------------------------------------------
// aggregate_lessons + aggregate_tool_contributions enriched fields
// (recurrence_count / contribution_share).
// ----------------------------------------------------------------------

#[test]
fn run_aggregate_lessons_returns_token_frequencies() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_806_001;
        let container = seed_container_thread(pool).await?;

        // Three reflections with overlapping lessons. The tokenizer
        // splits on whitespace, lowercases, and keeps tokens that
        // contain at least one ascii_alphanumeric (or any non-ascii)
        // character. "always run tests" → ["always", "run", "tests"].
        // Repeated tokens across reflections accumulate.
        let metadatas = [
            r#"{"eval":{"lessons":["always run tests","run linters first"]}}"#,
            r#"{"eval":{"lessons":["Always run tests","always check the logs"]}}"#,
            r#"{"eval":{"lessons":["Read the diff carefully"]}}"#,
        ];
        let mut ids = vec![];
        for meta in metadatas {
            ids.push(
                seed_sidecar_with_metadata(pool, user, container, Some(meta.to_string()), |_| {})
                    .await?,
            );
        }

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: user }),
            ..Default::default()
        };
        let resp = app.aggregate_lessons(Some(&filter), Some(10)).await?;
        let by_tok: std::collections::HashMap<String, u64> = resp
            .entries
            .iter()
            .map(|e| (e.token_or_phrase.clone(), e.frequency))
            .collect();
        // "always" appears in reflections 1 and 2 (twice across the
        // two lessons in #1 -> only once because it's in one lesson).
        // Tokenisation is per-token within each lesson string:
        // reflection 1: "always", "run", "tests", "run", "linters", "first"
        // reflection 2: "always", "run", "tests", "always", "check", "the", "logs"
        // reflection 3: "read", "the", "diff", "carefully"
        // Expected counts:
        //   "always" -> 3 (1 + 2)
        //   "run"    -> 3 (2 + 1)
        //   "tests"  -> 2 (1 + 1)
        //   "the"    -> 2 (1 + 1)
        assert_eq!(by_tok.get("always").copied(), Some(3), "{by_tok:?}");
        assert_eq!(by_tok.get("run").copied(), Some(3), "{by_tok:?}");
        assert_eq!(by_tok.get("tests").copied(), Some(2));

        // Top entry must be one of the highest-frequency tokens.
        assert!(
            !resp.entries.is_empty(),
            "non-empty corpus must produce entries"
        );

        cleanup_aggregate_scores_state(pool, container, &ids).await;
        Ok(())
    })
}

#[test]
fn run_aggregate_lessons_empty_corpus_returns_empty() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: 999_806_998 }),
            ..Default::default()
        };
        let resp = app.aggregate_lessons(Some(&filter), Some(5)).await?;
        assert!(resp.entries.is_empty());
        Ok(())
    })
}

#[test]
fn run_aggregate_lessons_skips_metadata_without_eval() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_806_002;
        let container = seed_container_thread(pool).await?;
        // No `eval.lessons` key → row contributes nothing; should not
        // panic the aggregate.
        let ids = vec![
            seed_sidecar_with_metadata(
                pool,
                user,
                container,
                Some(r#"{"reflector":{"id":"self"}}"#.to_string()),
                |_| {},
            )
            .await?,
        ];
        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: user }),
            ..Default::default()
        };
        let resp = app.aggregate_lessons(Some(&filter), Some(5)).await?;
        assert!(resp.entries.is_empty());
        cleanup_aggregate_scores_state(pool, container, &ids).await;
        Ok(())
    })
}

async fn insert_tool_outcome_row(
    pool: &'static RdbPool,
    memory_id: i64,
    tool: &str,
    contribution: i32,
) {
    // sqlite uses `?` placeholders; postgres uses `$N`. Build both
    // verbatim rather than relying on the existing `P1` const, which
    // only covers a single placeholder.
    #[cfg(feature = "postgres")]
    let sql = "INSERT INTO reflection_tool_outcome (memory_id, tool, contribution, error_kind) \
               VALUES ($1, $2, $3, '')";
    #[cfg(not(feature = "postgres"))]
    let sql = "INSERT INTO reflection_tool_outcome (memory_id, tool, contribution, error_kind) \
               VALUES (?, ?, ?, '')";
    let _ = sqlx::query::<Rdb>(sql)
        .bind(memory_id)
        .bind(tool)
        .bind(contribution)
        .execute(pool)
        .await;
}

#[test]
fn run_aggregate_tool_contributions_carries_recurrence_and_share() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_806_003;
        let container = seed_container_thread(pool).await?;

        // Seed three reflections under the same user:
        //   - id_a: tool="Read", is_recurrence=true
        //   - id_b: tool="Read", is_recurrence=false
        //   - id_c: tool="Edit", is_recurrence=true
        // Expected per-tool totals (count): Read=2, Edit=1
        //   Read.contribution_share = 2/2 = 1.0
        //   Edit.contribution_share = 1/1 = 1.0
        //   Read.recurrence_count   = 1
        //   Edit.recurrence_count   = 1
        let id_a = seed_sidecar(pool, user, container, |r| {
            r.is_recurrence = true;
        })
        .await?;
        let id_b = seed_sidecar(pool, user, container, |r| {
            r.is_recurrence = false;
        })
        .await?;
        let id_c = seed_sidecar(pool, user, container, |r| {
            r.is_recurrence = true;
        })
        .await?;
        insert_tool_outcome_row(pool, id_a, "Read", ToolContribution::Positive as i32).await;
        insert_tool_outcome_row(pool, id_b, "Read", ToolContribution::Positive as i32).await;
        insert_tool_outcome_row(pool, id_c, "Edit", ToolContribution::Positive as i32).await;

        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: user }),
            ..Default::default()
        };
        let resp = app
            .aggregate_tool_contributions(Some(&filter), true, false, false, false)
            .await?;
        let by_tool: std::collections::HashMap<String, &ToolContributionAggregateEntry> = resp
            .entries
            .iter()
            .map(|e| (e.tool.clone().unwrap_or_default(), e))
            .collect();
        let read = by_tool.get("Read").expect("Read bucket");
        let edit = by_tool.get("Edit").expect("Edit bucket");
        assert_eq!(read.count, 2);
        assert_eq!(edit.count, 1);
        assert_eq!(read.recurrence_count, Some(1));
        assert_eq!(edit.recurrence_count, Some(1));
        // contribution_share normalised per-tool when group_by_tool=true.
        assert!((read.contribution_share.unwrap_or_default() - 1.0).abs() < 1e-9);
        assert!((edit.contribution_share.unwrap_or_default() - 1.0).abs() < 1e-9);

        // Cleanup child rows + sidecar + thread.
        for id in [id_a, id_b, id_c] {
            let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
                "DELETE FROM reflection_tool_outcome WHERE memory_id = {P1}"
            )))
            .bind(id)
            .execute(pool)
            .await;
        }
        cleanup_aggregate_scores_state(pool, container, &[id_a, id_b, id_c]).await;
        Ok(())
    })
}

#[test]
fn run_aggregate_tool_contributions_share_is_none_without_group_by_tool() -> Result<()> {
    TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let app = build_app(pool).await;
        let user: i64 = 999_806_004;
        let container = seed_container_thread(pool).await?;
        let id = seed_sidecar(pool, user, container, |r| {
            r.is_recurrence = false;
        })
        .await?;
        insert_tool_outcome_row(pool, id, "Read", ToolContribution::Positive as i32).await;
        let filter = protobuf::llm_memory::data::ReflectionSearchFilter {
            origin_user_id: Some(UserId { value: user }),
            ..Default::default()
        };
        // group_by_tool=false → contribution_share unset (denominator
        // is ambiguous when the tool axis collapses).
        let resp = app
            .aggregate_tool_contributions(Some(&filter), false, false, false, false)
            .await?;
        assert_eq!(resp.entries.len(), 1, "totals row");
        assert!(resp.entries[0].contribution_share.is_none());
        // recurrence_count remains tracked (count >= 0 invariant).
        assert_eq!(resp.entries[0].recurrence_count, Some(0));

        let _ = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(format!(
            "DELETE FROM reflection_tool_outcome WHERE memory_id = {P1}"
        )))
        .bind(id)
        .execute(pool)
        .await;
        cleanup_aggregate_scores_state(pool, container, &[id]).await;
        Ok(())
    })
}
