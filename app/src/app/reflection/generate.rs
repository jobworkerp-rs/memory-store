//! `Generate` RPC (F-G1): kick off the thread-reflection workflow
//! via jobworkerp.
//!
//! Two modes:
//!   - `async_mode = false` (default): synchronously enqueue and wait
//!     for the workflow to finish. Returns the resulting `reflection_id`
//!     parsed out of the workflow output.
//!   - `async_mode = true`: enqueue and return immediately. Returns
//!     the jobworkerp `JobId` as a string so callers can poll on
//!     their own schedule (used by `thread-reflection-batch.yaml`).
//!
//! Both paths share an idempotent duplicate-detection guard that
//! mirrors the `checkExistsActiveReflection` step in the workflow
//! YAML: the `(thread_id, prompt_version, reflector_id)` tuple maps
//! 1:1 to a single `memory.external_id` enforced UNIQUE by
//! `FinalizeReflection` (see `finalize.rs` Phase 3). Once a row
//! exists for the tuple no LLM run can produce a new one — Phase 3
//! observes the conflict and returns the existing id — so this
//! short-circuit saves the LLM round-trip without changing the
//! observable contract. `force=true` is accepted but logged as a
//! no-op for the same reason; regenerating the reflection requires
//! bumping `prompt_version`.
//!
//! Kill switch: when `MEMORY_REFLECTION_DISPATCH_ENABLED` is unset
//! or `false`, this function returns `Unimplemented` regardless of
//! whether the jobworkerp client is wired up. The client is shared
//! with `FindSimilarByIntentText` (F-S8); coupling the kill switch
//! to client construction would also disable the search RPC, so the
//! gating is read here per-call instead.

use anyhow::Result;
use infra::error::LlmMemoryError;
use protobuf::llm_memory::data::ReflectionId;
use protobuf::llm_memory::service::{GenerateForThreadRequest, GenerateForThreadResponse};

use crate::app::reflection::ReflectionAppImpl;

const WORKER_NAME_PREFIX: &str = "memories-thread-reflection-single";

/// Default reflector model env override. When the operator has not
/// pinned a model, the workflow `reflector_model` input falls back to
/// this value. Kept here (not in the proto) because the model name is
/// deployment-specific runtime configuration, not a stable API
/// contract.
const REFLECTOR_MODEL_ENV: &str = "MEMORY_REFLECTION_REFLECTOR_MODEL";
const REFLECTOR_BASE_URL_ENV: &str = "MEMORY_REFLECTION_REFLECTOR_BASE_URL";

/// Default `prompt_version` when the proto field is omitted.
///
/// MUST be deploy-time stable: the value participates in F-G3
/// idempotency via `external_id_for(thread_id, prompt_version,
/// reflector_id)`, so a date-derived default would mint a fresh
/// external_id every day for the same thread/reflector tuple, defeating
/// the gate. Operators who need to bump the prompt revision should
/// pass an explicit `prompt_version` from the client; bumping this
/// constant is a deploy-coordinated event.
const DEFAULT_PROMPT_VERSION: &str = "default-v1";

/// Gating env for `Generate`. Read at every call instead of being
/// baked into client construction so flipping it does not also
/// disable `FindSimilarByIntentText`, which shares the same
/// jobworkerp client.
const DISPATCH_ENABLED_ENV: &str = "MEMORY_REFLECTION_DISPATCH_ENABLED";

/// Default output language env for reflection. Deliberately separate
/// from a memories-wide `MEMORY_DEFAULT_LANGUAGE`: reflection is meant
/// to be split out of memories later, so its language default must not
/// depend on a memories-scoped setting.
const DEFAULT_LANGUAGE_ENV: &str = "REFLECTION_DEFAULT_LANGUAGE";

/// Hard fallback language when neither the request nor the env pins one.
const FALLBACK_LANGUAGE: &str = "ja";

/// Supported output languages. A request asking for anything else is a
/// client error: silently falling back would emit a reflection in an
/// unexpected language whose prompt file does not exist.
const SUPPORTED_LANGUAGES: [&str; 2] = ["ja", "en"];

/// Resolve the reflection output language: request `output_language` >
/// `REFLECTION_DEFAULT_LANGUAGE` > `ja`. The resolved value is
/// validated against `SUPPORTED_LANGUAGES`; an unsupported explicit
/// request is rejected rather than coerced, so a typo surfaces instead
/// of silently producing the wrong language.
fn resolve_language(req_language: Option<&str>) -> Result<String, LlmMemoryError> {
    let candidate = req_language
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| {
            std::env::var(DEFAULT_LANGUAGE_ENV)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|s| !s.is_empty())
        })
        .unwrap_or_else(|| FALLBACK_LANGUAGE.to_string());
    if !SUPPORTED_LANGUAGES.contains(&candidate.as_str()) {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "unsupported reflection output_language `{candidate}`; supported: {}",
            SUPPORTED_LANGUAGES.join(", ")
        )));
    }
    Ok(candidate)
}

fn worker_name_for_language(lang: &str) -> Result<String, LlmMemoryError> {
    if !SUPPORTED_LANGUAGES.contains(&lang) {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "unsupported reflection output_language `{lang}`; supported: {}",
            SUPPORTED_LANGUAGES.join(", ")
        )));
    }
    Ok(format!("{WORKER_NAME_PREFIX}-{lang}"))
}

/// Pure gating helper extracted so the kill-switch contract can be
/// tested without standing up an `AppModule`. Trims and lowercases
/// the value so common operator typos (`"True"`, `" true "`) match
/// the documented sentinel.
fn dispatch_enabled_from_value(raw: Option<&str>) -> bool {
    matches!(raw, Some(v) if v.trim().eq_ignore_ascii_case("true"))
}

/// Process-wide kill-switch read. Centralised so the env var name
/// lives in one place; callers in tests should prefer
/// `dispatch_enabled_from_value` instead of mutating the process
/// environment.
fn dispatch_enabled() -> bool {
    dispatch_enabled_from_value(std::env::var(DISPATCH_ENABLED_ENV).ok().as_deref())
}

pub async fn generate(
    app: &ReflectionAppImpl,
    req: &GenerateForThreadRequest,
) -> Result<GenerateForThreadResponse> {
    use crate::app::reflection::build_memory_data::external_id_for;
    use infra::infra::memory::rdb::MemoryRepository;
    use infra::infra::thread::rdb::ThreadRepository;
    use protobuf::llm_memory::data::ThreadId as ProtoThreadId;
    use protobuf::llm_memory::service::generate_for_thread_response::Result as ResponseResult;

    // 1. Kill switch — read MEMORY_REFLECTION_DISPATCH_ENABLED at the
    //    call site so flipping it does NOT also disable F-S8
    //    (FindSimilarByIntentText), which shares this jobworkerp
    //    client. The client itself is wired whenever JOBWORKERP_ADDR
    //    is set; missing client surfaces with its own message so the
    //    operator can distinguish "client not configured" from
    //    "dispatch disabled".
    if !dispatch_enabled() {
        return Err(LlmMemoryError::Unimplemented(
            "Generate is disabled. Set MEMORY_REFLECTION_DISPATCH_ENABLED=true and ensure the \
             `memories-thread-reflection-single-ja/en` workflow workers are registered with jobworkerp."
                .into(),
        )
        .into());
    }
    let Some(client) = app.jobworkerp_client.as_ref() else {
        return Err(LlmMemoryError::Unimplemented(
            "Generate requires the jobworkerp client. Set JOBWORKERP_ADDR and ensure the \
             `memories-thread-reflection-single-ja/en` workflow workers are registered."
                .into(),
        )
        .into());
    };

    // 2. Validate the thread_id — the proto wraps it in an Option but
    //    a missing thread is an InvalidArgument, not a server error.
    let thread_id = req
        .thread_id
        .as_ref()
        .ok_or_else(|| LlmMemoryError::InvalidArgument("thread_id is required".into()))?
        .value;
    if thread_id == 0 {
        return Err(LlmMemoryError::InvalidArgument(
            "thread_id must be a non-zero snowflake".into(),
        )
        .into());
    }

    // 3. Normalise the workflow inputs that have well-known defaults
    //    (proto declares them `optional`). The workflow YAML repeats
    //    these defaults for direct grpcurl callers; we centralise them
    //    here so duplicate-detection sees the canonical value.
    let prompt_version = req
        .prompt_version
        .clone()
        .unwrap_or_else(|| DEFAULT_PROMPT_VERSION.to_string());
    let reflector_id = req
        .reflector_id
        .clone()
        .unwrap_or_else(|| "self".to_string());

    // 4. Resolve the origin thread before doing anything else. Both
    //    branches below (existing reflection short-circuit and
    //    workflow dispatch) need a live thread; rejecting a deleted
    //    thread up front keeps the two paths symmetric and avoids
    //    paying the workflow round-trip just to surface a NotFound.
    //    The owner `user_id` is forwarded into the workflow input so
    //    the persisted reflection / future user-scoped queries stay
    //    consistent with the origin thread; hard-coding it used to
    //    misattribute every reflection to user 1.
    let origin_thread = app
        .thread_repo
        .find(&ProtoThreadId { value: thread_id })
        .await?
        .ok_or_else(|| {
            LlmMemoryError::NotFound(format!(
                "thread {thread_id} not found; cannot generate reflection"
            ))
        })?;
    let origin_user_id = origin_thread
        .data
        .as_ref()
        .and_then(|d| d.user_id.as_ref())
        .map(|u| u.value)
        .ok_or_else(|| {
            LlmMemoryError::OtherError(format!(
                "thread {thread_id} is missing data.user_id; cannot generate reflection"
            ))
        })?;

    // 5. Idempotent duplicate detection. external_id is UNIQUE on
    //    `(thread_id, prompt_version, reflector_id)` (see finalize.rs
    //    Phase 3); once a row exists for the tuple FinalizeReflection
    //    cannot mint a new one — even with `force=true` Phase 3 returns
    //    the same id. Short-circuit here to save the LLM round-trip.
    //    Regenerating requires bumping `prompt_version`.
    let external_id = external_id_for(thread_id, &prompt_version, &reflector_id);
    if let Some(existing) = app.memory_repo.find_by_external_id(&external_id).await? {
        if req.force {
            tracing::warn!(
                target = "reflection.generate",
                thread_id,
                prompt_version = %prompt_version,
                reflector_id = %reflector_id,
                "Generate called with force=true on a tuple that already has a reflection; \
                 returning the existing id because external_id is UNIQUE. Bump prompt_version \
                 to regenerate.",
            );
        }
        let existing_id = existing.id.as_ref().ok_or_else(|| {
            LlmMemoryError::OtherError(format!(
                "memory with external_id={external_id} returned without an id"
            ))
        })?;
        return Ok(GenerateForThreadResponse {
            result: Some(ResponseResult::ReflectionId(ReflectionId {
                value: existing_id.value,
            })),
        });
    }

    // 6. Build the workflow input. Keep the JSON keys in lock-step
    //    with `thread-reflection-single.yaml::input.schema.document`
    //    — a mismatch is silently coerced to defaults inside the
    //    workflow, which produces hard-to-debug nulls downstream.
    //
    //    `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` validation is shared
    //    with the auto-embedding path via `require_grpc_callback_env`,
    //    which also rejects `0.0.0.0` / `::` wildcards, whitespace, port
    //    0, and the deprecated `MEMORY_GRPC_ADDR`. Re-implementing it
    //    inline used to let those slip into the workflow input.
    infra::infra::require_grpc_callback_env().map_err(|e| {
        LlmMemoryError::InvalidArgument(format!(
            "memories gRPC callback config invalid for reflection workflow: {e:#}"
        ))
    })?;
    let memories_grpc_host = std::env::var("MEMORY_GRPC_HOST")
        .expect("MEMORY_GRPC_HOST validated by require_grpc_callback_env");
    let memories_grpc_port: i64 = std::env::var("MEMORY_GRPC_PORT")
        .expect("MEMORY_GRPC_PORT validated by require_grpc_callback_env")
        .parse()
        .expect("MEMORY_GRPC_PORT validated by require_grpc_callback_env");
    let reflector_model = require_env(
        REFLECTOR_MODEL_ENV,
        "The reflection workflow needs a default reflector model name (e.g. qwen3.6:27b).",
    )?;
    let reflector_base_url = require_env(
        REFLECTOR_BASE_URL_ENV,
        "The reflection workflow needs the LLM endpoint base URL (Ollama / vLLM compatible).",
    )?;

    // Resolve the output language and select the matching lang-worker.
    // Prompt bodies are pinned in the worker settings, not supplied by
    // request-time workflow_context, so direct callers cannot override them.
    let output_language = resolve_language(req.output_language.as_deref())?;
    let worker_name = worker_name_for_language(&output_language)?;

    let mut args = serde_json::json!({
        "user_id": origin_user_id,
        "thread_id": thread_id,
        "memories_grpc_host": memories_grpc_host,
        "memories_grpc_port": memories_grpc_port,
        "reflector_model": reflector_model,
        "reflector_base_url": reflector_base_url,
        "prompt_version": prompt_version,
        "reflector_id": reflector_id,
        "output_language": output_language,
        "force": req.force,
    });
    // Forward optional experiment-tracking fields when present.
    if let Some(eid) = &req.experiment_id {
        args["experiment_id"] = serde_json::Value::String(eid.clone());
    }
    if let Some(variant) = &req.experiment_variant {
        args["experiment_variant"] = serde_json::Value::String(variant.clone());
    }

    // 6. Dispatch. async_mode hands `external_id` to jobworkerp as
    //    `uniq_key` for in-queue dedupe; sync mode does not yet plumb
    //    `uniq_key` through `execute_worker_by_name`, so concurrent
    //    sync calls for the same tuple can still both run the LLM
    //    before the external_id UNIQUE collapses one at write time.
    if req.async_mode {
        enqueue_async(client, &worker_name, args, &external_id).await
    } else {
        enqueue_sync(client, &worker_name, args).await
    }
}

async fn enqueue_sync(
    client: &std::sync::Arc<jobworkerp_client::client::wrapper::JobworkerpClientWrapper>,
    worker_name: &str,
    args: serde_json::Value,
) -> Result<GenerateForThreadResponse> {
    use protobuf::llm_memory::service::generate_for_thread_response::Result as ResponseResult;

    // Lang-worker prompt context is fixed in the worker settings. Runtime
    // job args only carry client-visible workflow input.
    let job_args = serde_json::json!({ "input": args.to_string() });
    let output = client
        .execute_worker_by_name(worker_name, job_args, Some("run"))
        .await
        .map_err(|e| {
            LlmMemoryError::OtherError(format!("thread-reflection-single workflow failed: {e:#}"))
        })?;

    // The workflow `output.as` block emits `{reflection_id, completed,
    // outcome, ...}`. proto3 int64 round-trips through JSON as a
    // string by default (the workflow exports `$reflection_id` straight
    // from `FinalizeReflection`'s ReflectionId.value), so accept both
    // shapes — see `parse_reflection_id` for the full contract. A
    // missing `reflection_id` means the workflow skipped (existing
    // reflection, not enough turns, etc.); surface that as an explicit
    // error rather than a silent zero id.
    let reflection_id = parse_reflection_id(output.get("reflection_id")).ok_or_else(|| {
        let skipped = output
            .get("skipped")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let skip_reason = output
            .get("skip_reason")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown reason");
        if skipped {
            LlmMemoryError::NotFound(format!(
                "reflection workflow skipped without producing a reflection: {skip_reason}"
            ))
        } else {
            LlmMemoryError::OtherError(format!(
                "reflection workflow output did not contain reflection_id: {output}"
            ))
        }
    })?;

    Ok(GenerateForThreadResponse {
        result: Some(ResponseResult::ReflectionId(ReflectionId {
            value: reflection_id,
        })),
    })
}

/// Build the protobuf-encoded `JobRequest` for the reflection workflow.
///
/// `uniq_key = external_id` is the load-bearing piece: jobworkerp uses
/// it to dedupe in-queue jobs, so concurrent Generate calls for the
/// same `(thread_id, prompt_version, reflector_id)` tuple collapse on
/// the queue instead of both running the reflector LLM and discovering
/// the conflict only at FinalizeReflection.
async fn build_workflow_job_request(
    client: &std::sync::Arc<jobworkerp_client::client::wrapper::JobworkerpClientWrapper>,
    worker_name: &str,
    args: serde_json::Value,
    uniq_key: &str,
) -> Result<jobworkerp_client::jobworkerp::service::JobRequest> {
    use jobworkerp_client::client::helper::UseJobworkerpClientHelper;
    use jobworkerp_client::jobworkerp::service::{JobRequest, job_request};
    use jobworkerp_client::proto::JobworkerpProto;
    use std::collections::HashMap;
    use std::sync::Arc;

    // worker and runner lookups have no data dependency; the batch
    // workflow fans out N concurrent Generates so paying the gRPC RTT
    // for both in series is wasteful.
    let metadata = Arc::new(HashMap::new());
    let (worker_lookup, runner_lookup) = tokio::try_join!(
        client.find_worker_by_name(None, metadata.clone(), worker_name),
        client.find_runner_or_error(None, metadata.clone(), "WORKFLOW"),
    )
    .map_err(|e| LlmMemoryError::OtherError(format!("jobworkerp metadata lookup failed: {e:#}")))?;
    let (worker_id, _worker_data) = worker_lookup.ok_or_else(|| {
        LlmMemoryError::Unimplemented(format!(
            "jobworkerp worker `{worker_name}` is not registered. Run \
             `memories-import upsert-generation-workers --feature reflection` first."
        ))
    })?;
    let (_, wf_rdata) = runner_lookup;

    // WORKFLOW runner takes a JSON-encoded `input` string; prompt context
    // comes from the language-specific worker settings.
    let job_args_json = serde_json::json!({ "input": args.to_string() });
    let args_descriptor = JobworkerpProto::parse_job_args_schema_descriptor(&wf_rdata, Some("run"))
        .map_err(|e| {
            LlmMemoryError::OtherError(format!("WORKFLOW args schema parse failed: {e:#}"))
        })?;
    let args_bytes = match args_descriptor {
        Some(desc) => JobworkerpProto::json_value_to_message(desc, &job_args_json, true, true)
            .map_err(|e| {
                LlmMemoryError::OtherError(format!("WORKFLOW args encode failed: {e:#}"))
            })?,
        None => job_args_json.to_string().into_bytes(),
    };

    Ok(JobRequest {
        args: args_bytes,
        timeout: Some(3_600_000),
        worker: Some(job_request::Worker::WorkerId(worker_id)),
        priority: Some(jobworkerp_client::jobworkerp::data::Priority::Medium as i32),
        using: Some("run".to_string()),
        uniq_key: Some(uniq_key.to_string()),
        ..Default::default()
    })
}

async fn enqueue_async(
    client: &std::sync::Arc<jobworkerp_client::client::wrapper::JobworkerpClientWrapper>,
    worker_name: &str,
    args: serde_json::Value,
    uniq_key: &str,
) -> Result<GenerateForThreadResponse> {
    use jobworkerp_client::client::UseJobworkerpClient;
    use protobuf::llm_memory::service::generate_for_thread_response::Result as ResponseResult;

    let job_request = build_workflow_job_request(client, worker_name, args, uniq_key).await?;
    let response = client
        .jobworkerp_client()
        .job_client()
        .await
        .enqueue(tonic::Request::new(job_request))
        .await
        .map_err(|e| LlmMemoryError::OtherError(format!("enqueue failed: {e}")))?;
    let job_id = response.into_inner().id.ok_or_else(|| {
        LlmMemoryError::OtherError("jobworkerp returned an EnqueueResponse without a JobId".into())
    })?;

    Ok(GenerateForThreadResponse {
        result: Some(ResponseResult::JobId(job_id.value.to_string())),
    })
}

/// Read a required env var, mapping missing values to a structured
/// `InvalidArgument` that includes a caller-supplied hint. Extracted
/// so the reflector model / base URL knobs share the same operator-
/// facing failure message shape; `MEMORY_GRPC_HOST` / `_PORT` use the
/// dedicated `require_grpc_callback_env` validator instead because it
/// rejects more than just absence (wildcards, whitespace, port 0).
fn require_env(name: &str, hint: &str) -> Result<String> {
    std::env::var(name)
        .map_err(|_| LlmMemoryError::InvalidArgument(format!("{name} is not set. {hint}")).into())
}

/// Pull `reflection_id` out of the workflow output, accepting either
/// JSON-number form (raw int64) or JSON-string form (proto3 default
/// for int64). Returns `None` when the field is missing, null, or
/// fails to parse — callers translate that into the skipped /
/// not-found / malformed-output branches respectively.
///
/// proto3 int64 fields are encoded as JSON strings by both prost-json
/// and the canonical proto JSON mapping; the workflow simply forwards
/// `$reflection_id` from a GRPC FinalizeReflection response, so the
/// string form is the common case in practice. The number branch is
/// kept for forward-compat in case a future workflow rewrites the
/// id through `tonumber` before exporting.
fn parse_reflection_id(v: Option<&serde_json::Value>) -> Option<i64> {
    let v = v?;
    if let Some(n) = v.as_i64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse::<i64>().ok())
}

#[cfg(test)]
mod parse_reflection_id_tests {
    //! proto3 int64 round-trips as a JSON string by default. The
    //! single-thread reflection workflow forwards
    //! `FinalizeReflection`'s `ReflectionId.value` straight into its
    //! output, so the string branch is the common case — pinning both
    //! shapes here prevents a regression that would silently send
    //! every successful Generate through the "missing reflection_id"
    //! error path.

    use serde_json::json;

    use super::parse_reflection_id;

    #[test]
    fn parses_string_form() {
        let v = json!("12345");
        assert_eq!(parse_reflection_id(Some(&v)), Some(12345));
    }

    #[test]
    fn parses_number_form() {
        let v = json!(67890);
        assert_eq!(parse_reflection_id(Some(&v)), Some(67890));
    }

    #[test]
    fn parses_large_int64_string() {
        // Beyond f64 precision (>2^53) — the JSON-number branch would
        // round-trip lossily, but the proto-mandated string form is
        // exact.
        let v = json!("9007199254740993");
        assert_eq!(parse_reflection_id(Some(&v)), Some(9_007_199_254_740_993));
    }

    #[test]
    fn missing_or_null_returns_none() {
        assert_eq!(parse_reflection_id(None), None);
        let v = json!(null);
        assert_eq!(parse_reflection_id(Some(&v)), None);
    }

    #[test]
    fn non_numeric_string_returns_none() {
        let v = json!("not-a-number");
        assert_eq!(parse_reflection_id(Some(&v)), None);
    }
}

#[cfg(test)]
mod dispatch_enabled_tests {
    //! `MEMORY_REFLECTION_DISPATCH_ENABLED` is the Generate-only kill
    //! switch. Pin the parsing so a future refactor cannot quietly
    //! couple it back to FindSimilarByIntentText (which used to share
    //! the same client gate and would lose its search RPC alongside).

    use super::dispatch_enabled_from_value;

    #[test]
    fn unset_is_disabled() {
        assert!(!dispatch_enabled_from_value(None));
    }

    #[test]
    fn empty_or_false_is_disabled() {
        assert!(!dispatch_enabled_from_value(Some("")));
        assert!(!dispatch_enabled_from_value(Some("false")));
        assert!(!dispatch_enabled_from_value(Some("0")));
        assert!(!dispatch_enabled_from_value(Some("no")));
    }

    #[test]
    fn true_variants_enable() {
        assert!(dispatch_enabled_from_value(Some("true")));
        assert!(dispatch_enabled_from_value(Some("TRUE")));
        assert!(dispatch_enabled_from_value(Some("True")));
        // Operator typos with whitespace are forgiven so an env file
        // accidentally containing trailing spaces still works.
        assert!(dispatch_enabled_from_value(Some(" true ")));
    }
}

#[cfg(test)]
mod language_prompt_tests {
    use super::{resolve_language, worker_name_for_language};

    #[test]
    fn resolve_language_accepts_supported_explicit_values() {
        assert_eq!(resolve_language(Some("ja")).unwrap(), "ja");
        assert_eq!(resolve_language(Some(" en ")).unwrap(), "en");
    }

    #[test]
    fn resolve_language_rejects_path_like_unsupported_value() {
        let err = resolve_language(Some("../en")).unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported reflection output_language")
        );
    }

    #[test]
    fn worker_name_for_language_uses_language_suffix() {
        assert_eq!(
            worker_name_for_language("ja").unwrap(),
            "memories-thread-reflection-single-ja"
        );
        assert_eq!(
            worker_name_for_language("en").unwrap(),
            "memories-thread-reflection-single-en"
        );
    }

    #[test]
    fn worker_name_for_language_rejects_unsupported_value() {
        let err = worker_name_for_language("../en").unwrap_err();
        assert!(
            err.to_string()
                .contains("unsupported reflection output_language")
        );
    }
}
