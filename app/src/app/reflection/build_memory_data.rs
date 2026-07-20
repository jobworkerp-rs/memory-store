//! Build the immutable backing `MemoryData` and `ThreadReflectionIndexRow`
//! for a finalized reflection.
//!
//! Spec §3.5 / §3.6 / §3.7: the reflection memory is owned by its
//! origin user, content carries the search document, and
//! `external_id` follows
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
use infra::infra::reflection::failure_mode_convert;
use protobuf::llm_memory::data::{
    ContentType, FailureMode, MemoryData, MemoryKind, MessageRole, ReflectionLlmOutput, UserId,
};
use protobuf::llm_memory::service::FinalizeReflectionRequest;
use serde_json::{Map, Value, json};

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
    origin_user_id: i64,
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
            value: origin_user_id,
        }),
        content: build_reflection_search_document(parsed),
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
        memory_kind: MemoryKind::Reflection as i32,
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

pub(crate) fn build_reflection_search_document(parsed: &ReflectionLlmOutput) -> String {
    let mut failure_modes: Vec<String> = parsed
        .failure_modes
        .iter()
        .filter_map(|mode| failure_mode_convert::db_name_from_i32(*mode).map(str::to_string))
        .collect();
    failure_modes.extend(
        parsed
            .failure_modes_other
            .iter()
            .map(|mode| mode.trim())
            .filter(|mode| !mode.is_empty())
            .map(str::to_string),
    );
    build_reflection_search_document_from_parts(
        &parsed.summary,
        &parsed.task_intent,
        &parsed.lessons,
        &parsed.key_decisions,
        &parsed.success_factors,
        &failure_modes,
        parsed.mitigation_hint.as_deref(),
    )
}

pub fn build_reflection_search_document_from_metadata(metadata: &str) -> Result<String> {
    let root: Value = serde_json::from_str(metadata)
        .map_err(|e| LlmMemoryError::OtherError(format!("reflection metadata parse: {e}")))?;
    let eval = root
        .get("eval")
        .ok_or_else(|| LlmMemoryError::OtherError("reflection metadata missing eval".into()))?;

    let summary = json_string(eval, "summary");
    let task_intent = json_string(eval, "task_intent");
    let lessons = json_string_array(eval, "lessons");
    let key_decisions = json_string_array(eval, "key_decisions");
    let success_factors = json_string_array(eval, "success_factors");
    let mut failure_modes =
        metadata_failure_modes_to_db_keys(json_string_array(eval, "failure_modes"));
    failure_modes.extend(json_string_array(eval, "failure_modes_other"));
    let mitigation_hint = json_string(eval, "mitigation_hint");

    Ok(build_reflection_search_document_from_parts(
        &summary,
        &task_intent,
        &lessons,
        &key_decisions,
        &success_factors,
        &failure_modes,
        (!mitigation_hint.is_empty()).then_some(mitigation_hint.as_str()),
    ))
}

fn metadata_failure_modes_to_db_keys(modes: Vec<String>) -> Vec<String> {
    modes
        .into_iter()
        .filter_map(|mode| {
            let mode = mode.trim();
            if mode.is_empty() {
                return None;
            }
            if let Some(parsed) = FailureMode::from_str_name(mode) {
                return failure_mode_from_metadata_enum(parsed);
            }
            if let Some(parsed) = failure_mode_convert::from_db_name(mode) {
                return failure_mode_from_metadata_enum(parsed);
            }
            Some(mode.to_string())
        })
        .collect()
}

fn failure_mode_from_metadata_enum(mode: FailureMode) -> Option<String> {
    match mode {
        FailureMode::Unspecified => None,
        _ => Some(failure_mode_convert::to_db_name(mode).to_string()),
    }
}

fn build_reflection_search_document_from_parts(
    summary: &str,
    task_intent: &str,
    lessons: &[String],
    key_decisions: &[String],
    success_factors: &[String],
    failure_modes: &[String],
    mitigation_hint: Option<&str>,
) -> String {
    let mut sections = Vec::new();
    push_scalar_section(&mut sections, "Summary", summary);
    push_scalar_section(&mut sections, "Intent", task_intent);
    push_list_section(&mut sections, "Lessons", lessons);
    push_list_section(&mut sections, "Key decisions", key_decisions);
    push_list_section(&mut sections, "Success factors", success_factors);
    push_list_section(&mut sections, "Failure modes", failure_modes);

    if let Some(hint) = mitigation_hint {
        push_scalar_section(&mut sections, "Mitigation", hint);
    }
    sections.join("\n\n")
}

fn json_string(parent: &Value, key: &str) -> String {
    parent
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn json_string_array(parent: &Value, key: &str) -> Vec<String> {
    parent
        .get(key)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn push_scalar_section(sections: &mut Vec<String>, title: &str, value: &str) {
    let value = value.trim();
    if !value.is_empty() {
        sections.push(format!("{title}:\n{value}"));
    }
}

fn push_list_section(sections: &mut Vec<String>, title: &str, values: &[String]) {
    let lines: Vec<String> = values
        .iter()
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(|v| format!("- {v}"))
        .collect();
    if !lines.is_empty() {
        sections.push(format!("{title}:\n{}", lines.join("\n")));
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::llm_memory::data::{
        FailureMode, ReflectionAspect, ReflectionOutcome, TaskCategory, ToolOutcomeEntry,
    };

    fn parsed() -> ReflectionLlmOutput {
        ReflectionLlmOutput {
            outcome: ReflectionOutcome::Success as i32,
            score_self: 0.9,
            summary: "Shipped the cache fix.".into(),
            task_intent: "Reduce stale reads after reflection deletes.".into(),
            task_category: TaskCategory::Coding as i32,
            reflection_aspect: ReflectionAspect::TaskOutcome as i32,
            failure_modes: vec![],
            tools_used: vec!["Read".into()],
            failure_modes_other: vec!["stale cache".into()],
            success_factors: vec!["Added focused regression tests.".into()],
            lessons: vec!["Invalidate shared cache before returning.".into()],
            key_decisions: vec!["Use the existing cache key helper.".into()],
            mitigation_hint: Some("Check cross-app cache owners.".into()),
            failure_signature: None,
            tool_outcomes: vec![ToolOutcomeEntry {
                tool: "Read".into(),
                contribution: 1,
                error_kind: None,
            }],
            facts: vec![],
        }
    }

    #[test]
    fn search_document_includes_non_empty_sections_as_plain_text() {
        let doc = build_reflection_search_document(&parsed());

        assert!(doc.contains("Summary:\nShipped the cache fix."));
        assert!(doc.contains("Intent:\nReduce stale reads"));
        assert!(doc.contains("Lessons:\n- Invalidate shared cache"));
        assert!(doc.contains("Key decisions:\n- Use the existing cache key helper."));
        assert!(doc.contains("Success factors:\n- Added focused regression tests."));
        assert!(doc.contains("Failure modes:\n- stale cache"));
        assert!(doc.contains("Mitigation:\nCheck cross-app cache owners."));
        assert!(!doc.contains("\"summary\""));
        assert!(!doc.contains("task_intent"));
    }

    #[test]
    fn search_document_omits_empty_sections() {
        let mut p = parsed();
        p.task_intent.clear();
        p.lessons.clear();
        p.key_decisions.clear();
        p.success_factors.clear();
        p.failure_modes_other.clear();
        p.mitigation_hint = None;

        let doc = build_reflection_search_document(&p);

        assert_eq!(doc, "Summary:\nShipped the cache fix.");
    }

    #[test]
    fn search_document_drops_unresolvable_failure_modes() {
        let mut p = parsed();
        p.failure_modes = vec![FailureMode::Unspecified as i32, 9999];
        p.failure_modes_other.clear();

        let doc = build_reflection_search_document(&p);

        assert!(!doc.contains("__failure_mode_sentinel_no_match__"));
        assert!(!doc.contains("Failure modes:"));
    }

    #[test]
    fn search_document_from_metadata_converts_failure_modes_like_finalize() {
        let mut p = parsed();
        p.failure_modes = vec![FailureMode::ToolMisuse as i32];
        p.failure_modes_other = vec!["custom mode".into()];
        let finalize_doc = build_reflection_search_document(&p);
        let metadata = json!({
            "eval": {
                "summary": p.summary,
                "task_intent": p.task_intent,
                "lessons": p.lessons,
                "key_decisions": p.key_decisions,
                "success_factors": p.success_factors,
                "failure_modes": ["FAILURE_MODE_TOOL_MISUSE"],
                "failure_modes_other": p.failure_modes_other,
                "mitigation_hint": p.mitigation_hint,
            }
        })
        .to_string();

        let backfill_doc = build_reflection_search_document_from_metadata(&metadata).unwrap();

        assert_eq!(backfill_doc, finalize_doc);
        assert!(backfill_doc.contains("Failure modes:\n- tool_misuse\n- custom mode"));
        assert!(!backfill_doc.contains("FAILURE_MODE_TOOL_MISUSE"));
    }
}
