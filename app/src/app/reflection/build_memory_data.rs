//! Build the immutable backing `MemoryData` and `ThreadReflectionIndexRow`
//! for a finalized reflection.
//!
//! Spec §3.5 / §3.6 / §3.7: the reflection memory is owned by
//! `REFLECTION_USER_ID`, content carries the LLM-generated `summary`,
//! and `external_id` follows
//! `reflection:<thread>:<prompt_version>:<reflector_id>` so the
//! existing `memory.external_id` UNIQUE index covers F-G3 idempotency
//! without a new migration.
//!
//! `metadata` is composed of four prefix groups (target.* / reflector.* /
//! experiment.* / eval.*) and serialized as a single JSON object so
//! ad-hoc filtering keeps working through the existing
//! `MemoryRepository`.

use anyhow::Result;
use infra::error::LlmMemoryError;
use protobuf::llm_memory::data::{
    ContentType, MemoryData, MessageRole, ReflectionLlmOutput, UserId,
};
use protobuf::llm_memory::service::FinalizeReflectionRequest;
use serde_json::{Map, Value, json};

use crate::app::REFLECTION_USER_ID;

pub struct ReflectionMemoryParts {
    pub memory_data: MemoryData,
    pub external_id: String,
    pub heuristic_score: f32,
    pub score_self: f32,
    pub score_resolved: f32,
    pub task_intent: String,
}

pub fn external_id_for(thread_id: i64, prompt_version: &str, reflector_id: &str) -> String {
    format!("reflection:{thread_id}:{prompt_version}:{reflector_id}")
}

/// Strict score-source resolution per spec §9.1 fixpoint #4. Defaults
/// to `score_self`; operators can flip to `score_heuristic` via the
/// `MEMORY_REFLECTION_SCORE_SOURCE` env. Anything else falls back to
/// `score_self` with a `tracing::warn!` so misconfiguration surfaces in
/// logs rather than silently changing rankings.
fn resolved_score(score_self: f32, heuristic_score: f32) -> f32 {
    match std::env::var("MEMORY_REFLECTION_SCORE_SOURCE")
        .as_deref()
        .map(|s| s.trim().to_ascii_lowercase())
    {
        Ok(ref s) if s == "score_heuristic" => heuristic_score,
        Ok(ref s) if s == "score_self" => score_self,
        Ok(other) => {
            tracing::warn!(
                "Unknown MEMORY_REFLECTION_SCORE_SOURCE={other:?}; falling back to score_self",
            );
            score_self
        }
        Err(_) => score_self,
    }
}

pub fn build_parts(
    req: &FinalizeReflectionRequest,
    parsed: &ReflectionLlmOutput,
    origin_thread_id: i64,
    now_millis: i64,
) -> Result<ReflectionMemoryParts> {
    let external_id = external_id_for(origin_thread_id, &req.prompt_version, &req.reflector_id);
    let metadata_json = build_metadata_json(req, parsed)?;

    let memory_data = MemoryData {
        // No parent_ids: a reflection memory references the original
        // turns through `reflection_fact.fact_memory_id`, not via the
        // conversation-history edge consumed by other roles.
        parent_ids: Vec::new(),
        user_id: Some(UserId {
            value: REFLECTION_USER_ID,
        }),
        content: parsed.summary.clone(),
        content_type: ContentType::Text as i32,
        params: None,
        metadata: Some(metadata_json),
        created_at: now_millis,
        updated_at: now_millis,
        role: MessageRole::RoleReflection as i32,
        external_id: Some(external_id.clone()),
        // Reflection memories are text-only; no media body.
        media_object_id: None,
        thread_ids: Vec::new(),
    };

    let score_self = parsed.score_self;
    let resolved = resolved_score(score_self, req.heuristic_score);

    Ok(ReflectionMemoryParts {
        memory_data,
        external_id,
        heuristic_score: req.heuristic_score,
        score_self,
        score_resolved: resolved,
        task_intent: parsed.task_intent.clone(),
    })
}

fn build_metadata_json(
    req: &FinalizeReflectionRequest,
    parsed: &ReflectionLlmOutput,
) -> Result<String> {
    // ---- target.* (heuristic-score derivation, model version) ----
    let mut target = Map::new();
    if let Some(v) = &req.target_model_version {
        target.insert("model_version".into(), Value::String(v.clone()));
    }
    if let Some(v) = req.target_retry_count {
        target.insert("retry_count".into(), json!(v));
    }
    if let Some(v) = req.target_error_count {
        target.insert("error_count".into(), json!(v));
    }
    if let Some(v) = req.target_tool_calls_count {
        target.insert("tool_calls_count".into(), json!(v));
    }
    if let Some(v) = req.target_window_count {
        target.insert("window_count".into(), json!(v));
    }
    if let Some(v) = req.target_window_size {
        target.insert("window_size".into(), json!(v));
    }
    if let Some(v) = req.target_fact_unresolved_count {
        target.insert("fact_unresolved_count".into(), json!(v));
    }

    // ---- reflector.* (LLM identity, error_kind on F-G9 path) ----
    let mut reflector = Map::new();
    reflector.insert("id".into(), Value::String(req.reflector_id.clone()));
    reflector.insert(
        "prompt_version".into(),
        Value::String(req.prompt_version.clone()),
    );
    if let Some(v) = &req.reflector_error_kind {
        reflector.insert("error_kind".into(), Value::String(v.clone()));
    }

    // ---- experiment.* ----
    let mut experiment = Map::new();
    if let Some(v) = &req.experiment_id {
        experiment.insert("id".into(), Value::String(v.clone()));
    }
    if let Some(v) = &req.experiment_variant {
        experiment.insert("variant".into(), Value::String(v.clone()));
    }

    // ---- eval.* (reflection LLM output structure preserved verbatim
    // under metadata.eval so downstream consumers can inspect the
    // raw shape without joining child tables). ----
    let eval_value = build_eval_view(parsed);

    let mut root = Map::new();
    root.insert("target".into(), Value::Object(target));
    root.insert("reflector".into(), Value::Object(reflector));
    root.insert("experiment".into(), Value::Object(experiment));
    root.insert("eval".into(), eval_value);

    serde_json::to_string(&Value::Object(root))
        .map_err(|e| LlmMemoryError::OtherError(format!("metadata serialize: {e}")).into())
}

/// Compact view of the LLM output preserved under `metadata.eval`.
/// Hand-built `serde_json::Value` (rather than `#[derive(Serialize)]`)
/// keeps the app crate free of a direct `serde` dependency — only
/// `serde_json` is in scope here.
fn build_eval_view(p: &ReflectionLlmOutput) -> Value {
    use protobuf::llm_memory::data::{
        FailureMode, ReflectionAspect, ReflectionOutcome, TaskCategory,
    };

    let outcome = ReflectionOutcome::try_from(p.outcome)
        .map(|v| v.as_str_name())
        .unwrap_or("REFLECTION_OUTCOME_UNSPECIFIED");
    // Denormalized debug/offline-eval snapshot of the
    // reflection_failure_mode child rows. Dedup mirrors the
    // (memory_id, mode) PRIMARY KEY so the snapshot stays consistent
    // with the authoritative child table.
    let mut seen = std::collections::HashSet::new();
    let failure_modes: Vec<&'static str> = p
        .failure_modes
        .iter()
        .filter_map(|v| FailureMode::try_from(*v).ok())
        .filter(|m| *m != FailureMode::Unspecified)
        .map(|m| m.as_str_name())
        .filter(|name| seen.insert(*name))
        .collect();
    let task_category = TaskCategory::try_from(p.task_category)
        .map(|v| v.as_str_name())
        .unwrap_or("TASK_CATEGORY_UNSPECIFIED");
    let reflection_aspect = ReflectionAspect::try_from(p.reflection_aspect)
        .map(|v| v.as_str_name())
        .unwrap_or("REFLECTION_ASPECT_UNSPECIFIED");

    let failure_signature = p
        .failure_signature
        .as_ref()
        .map(build_failure_signature_view)
        .unwrap_or(Value::Null);

    json!({
        "outcome": outcome,
        "score_self": p.score_self,
        "summary": p.summary,
        "task_intent": p.task_intent,
        "task_category": task_category,
        "reflection_aspect": reflection_aspect,
        "failure_modes": failure_modes,
        "failure_modes_other": p.failure_modes_other,
        "success_factors": p.success_factors,
        "lessons": p.lessons,
        "key_decisions": p.key_decisions,
        "tools_used": p.tools_used,
        "mitigation_hint": p.mitigation_hint,
        "failure_signature": failure_signature,
    })
}

fn build_failure_signature_view(fs: &protobuf::llm_memory::data::FailureSignature) -> Value {
    use protobuf::llm_memory::data::FailureSignaturePatternType;
    let pattern_type = FailureSignaturePatternType::try_from(fs.pattern_type)
        .map(|v| v.as_str_name())
        .unwrap_or("FAILURE_SIGNATURE_PATTERN_UNSPECIFIED");
    let indicators = fs
        .indicators
        .as_ref()
        .map(|i| {
            json!({
                "same_tool_repeated_count": i.same_tool_repeated_count,
                "same_tool_name": i.same_tool_name,
                "consecutive_errors": i.consecutive_errors,
                "no_state_change_turns": i.no_state_change_turns,
                "tool_calls_per_turn_ratio": i.tool_calls_per_turn_ratio,
                "compact_boundary_count": i.compact_boundary_count,
                "user_clarification_count": i.user_clarification_count,
                "turn_count_at_detection": i.turn_count_at_detection,
                "elapsed_ms_at_detection": i.elapsed_ms_at_detection,
            })
        })
        .unwrap_or(Value::Null);
    json!({
        "pattern_type": pattern_type,
        "indicators": indicators,
        "trigger_threshold": fs.trigger_threshold,
        "evidence_turn_indices": fs.evidence_turn_indices,
    })
}
