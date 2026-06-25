use anyhow::{Context, Result, anyhow};
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Decide whether the post-import personality extraction workflow
/// should be skipped. Same two-condition gate as `summarize::skip_reason`:
///
/// 1. The import reported one or more session-level errors. A
///    half-imported thread leaves a stale `updated_at` window that
///    would otherwise pull incomplete data into the extraction;
///    personality extraction is expensive enough that swallowing
///    partial-failure state silently is worse than asking the operator
///    to rerun.
/// 2. Nothing was actually imported (every session was Skipped or all
///    `memories_imported == 0`). Without `--since`, `merge_template`
///    does not inject `updated_after_ms`, so a "0-imported" run would
///    dispatch a workflow whose effective scope is the *entire user
///    history*. That is both expensive and semantically wrong — the
///    import did nothing this run, so the extraction should not run
///    either.
pub(crate) fn skip_reason(import_errors: usize, memories_imported: usize) -> Option<String> {
    if import_errors > 0 {
        return Some(format!(
            "import reported {import_errors} error(s); rerun after resolving them"
        ));
    }
    if memories_imported == 0 {
        return Some(
            "no memories were imported this run (all sessions skipped or empty); \
             skipping to avoid extracting against the entire user history"
                .to_string(),
        );
    }
    None
}

/// Apply import-derived overrides on top of the user-supplied template.
/// The template MUST be a JSON object — this is enforced at startup so
/// the workflow input shape is correct before any import work begins.
///
/// `updated_after_ms` is forwarded as an absolute lower bound for
/// `updated_at` so the extraction window aligns exactly with the
/// imported set. Same rationale as the summary path: a relative
/// `updated_within_hours` would re-anchor the window at workflow
/// execution time and drift past the import boundary on dispatch
/// delay.
fn merge_template(
    mut template: Value,
    user_id: i64,
    updated_after_ms: Option<i64>,
    output_language: &str,
) -> Result<Value> {
    let obj = template
        .as_object_mut()
        .ok_or_else(|| anyhow!("--extract-personality-after-* must be a JSON object"))?;
    obj.insert("user_id".into(), json!(user_id));
    obj.insert("output_language".into(), json!(output_language));
    if let Some(ms) = updated_after_ms {
        obj.insert("updated_after_ms".into(), json!(ms));
    }
    Ok(template)
}

/// Validate the JSON the user provided for the workflow before any
/// import work runs.
pub(crate) fn parse_template(raw: &str) -> Result<Value> {
    let val: Value =
        serde_json::from_str(raw).context("parse --extract-personality-after-* as JSON")?;
    if !val.is_object() {
        return Err(anyhow!(
            "--extract-personality-after-* must be a JSON object (got {})",
            value_kind(&val)
        ));
    }
    Ok(val)
}

fn value_kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Dispatch `thread-personality-batch.yaml` on jobworkerp using the
/// merged workflow input. Treats workflow execution failure as a
/// warning at the call site — import data is already persisted, so a
/// failed extraction must not undo a successful import.
///
/// `timeout_sec` is forwarded into `JobworkerpClientWrapper::new_by_env`
/// because `execute_workflow` reuses the client's `request_timeout` as
/// the job's timeout (jobworkerp's default 1200s is far below what
/// batch personality extraction typically needs).
pub(crate) async fn run_personality_after(
    template: Value,
    workflow_path: &Path,
    channel: Option<&str>,
    user_id: i64,
    since_millis: Option<i64>,
    output_language: &str,
    timeout_sec: u32,
) -> Result<Value> {
    let addr = std::env::var("JOBWORKERP_ADDR")
        .map_err(|_| anyhow!("JOBWORKERP_ADDR is required for --extract-personality-after-*"))?;
    if !addr.starts_with("http://") && !addr.starts_with("https://") {
        return Err(anyhow!(
            "JOBWORKERP_ADDR must include a URI scheme (e.g. http://{addr}); got `{addr}`"
        ));
    }

    let raw = workflow_path.to_string_lossy();
    let workflow_url = if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.into_owned()
    } else {
        let abs_path = workflow_path
            .canonicalize()
            .with_context(|| format!("resolve workflow path {}", workflow_path.display()))?;
        abs_path.to_string_lossy().into_owned()
    };

    // Prompt bodies are baked into the personality language workers at
    // registration, so no workflow_context is injected here.
    let input = merge_template(template, user_id, since_millis, output_language)?;
    let body = serde_json::to_string(&input)?;

    let client = JobworkerpClientWrapper::new_by_env(Some(timeout_sec)).await?;
    let result = client
        .execute_workflow(
            None,
            Arc::new(HashMap::new()),
            &workflow_url,
            &body,
            channel,
        )
        .await?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- merge_template ----

    #[test]
    fn merge_overrides_user_id() {
        let tmpl = json!({
            "user_id": 999,
            "memories_grpc_host": "localhost",
        });
        let out = merge_template(tmpl, 1, None, "ja").unwrap();
        assert_eq!(out["user_id"], json!(1));
        assert_eq!(out["output_language"], json!("ja"));
        assert_eq!(out["memories_grpc_host"], json!("localhost"));
    }

    #[test]
    fn merge_keeps_existing_updated_after_ms_when_none() {
        let tmpl = json!({
            "updated_after_ms": 1_700_000_000_000_i64,
            "custom_field": "/x.yaml",
        });
        let out = merge_template(tmpl, 1, None, "ja").unwrap();
        assert_eq!(out["updated_after_ms"], json!(1_700_000_000_000_i64));
        assert_eq!(out["custom_field"], json!("/x.yaml"));
    }

    #[test]
    fn merge_inserts_updated_after_ms_when_some() {
        let tmpl = json!({});
        let out = merge_template(tmpl, 1, Some(1_700_000_000_000), "ja").unwrap();
        assert_eq!(out["updated_after_ms"], json!(1_700_000_000_000_i64));
        assert_eq!(out["user_id"], json!(1));
    }

    #[test]
    fn merge_overrides_updated_after_ms_when_some() {
        let tmpl = json!({"updated_after_ms": 100});
        let out = merge_template(tmpl, 1, Some(2_000_000_000_000), "ja").unwrap();
        assert_eq!(out["updated_after_ms"], json!(2_000_000_000_000_i64));
    }

    #[test]
    fn merge_overrides_output_language() {
        let tmpl = json!({"output_language": "ja"});
        let out = merge_template(tmpl, 1, None, "en").unwrap();
        assert_eq!(out["output_language"], json!("en"));
    }

    #[test]
    fn merge_does_not_emit_updated_within_hours() {
        let tmpl = json!({});
        let out = merge_template(tmpl, 1, Some(1_700_000_000_000), "ja").unwrap();
        assert!(out.get("updated_within_hours").is_none());
    }

    #[test]
    fn merge_preserves_unrelated_keys() {
        let tmpl = json!({
            "memories_grpc_host": "h",
            "memories_grpc_port": 9100,
            "custom_field": "/x.yaml",
            "labels_filter": ["a", "b"],
            "personality_user_id": 200000,
            "summary_user_id": 100000,
        });
        let out = merge_template(tmpl, 1, Some(1_700_000_000_000), "ja").unwrap();
        assert_eq!(out["memories_grpc_host"], json!("h"));
        assert_eq!(out["memories_grpc_port"], json!(9100));
        assert_eq!(out["custom_field"], json!("/x.yaml"));
        assert_eq!(out["labels_filter"], json!(["a", "b"]));
        assert_eq!(out["personality_user_id"], json!(200000));
        assert_eq!(out["summary_user_id"], json!(100000));
        assert_eq!(out["user_id"], json!(1));
        assert_eq!(out["updated_after_ms"], json!(1_700_000_000_000_i64));
    }

    #[test]
    fn thread_personality_workflow_filters_non_conversation_scaffolding() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("workers/personality/thread-personality-single.yaml");
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));

        assert!(
            body.contains("visible_messages:"),
            "thread-personality-single must distinguish raw fetched messages from visible conversation messages"
        );
        for marker in [
            "# AGENTS.md instructions for ",
            "# CLAUDE.md instructions for ",
            "<environment_context>",
            "<permissions instructions>",
            "<turn_aborted>",
        ] {
            assert!(
                body.contains(marker),
                "personality extraction must filter Codex-injected user scaffolding marker {marker:?}"
            );
        }
        assert!(
            body.contains("payload_type == \"agent_message\""),
            "personality extraction must filter Codex assistant event shadows"
        );
        assert!(
            body.contains("($visible_messages | length)"),
            "min-message gating must count only visible conversation messages"
        );
        assert!(
            body.contains("$visible_messages | map(select(.data.role == \"ROLE_USER\"))"),
            "min-user gating must count only visible user messages"
        );
    }

    #[test]
    fn personality_workflows_use_language_workers_for_batch_fanout() {
        let single = include_str!("../workers/personality/thread-personality-single.yaml");
        let batch = include_str!("../workflows/personality/thread-personality-batch.yaml");
        let merge = include_str!("../workers/personality/user-personality-merge.yaml");
        let agent_summary = include_str!("../workflows/agent-chat-summary/agent-chat-summary.yaml");

        for yaml in [single, merge] {
            assert!(yaml.contains("prompt_context_missing"));
            assert!(!yaml.contains("system_prompt: |\n"));
        }
        assert!(single.contains("${ $thread_personality_system_prompt }"));
        assert!(merge.contains("${ $user_personality_merge_system_prompt }"));
        assert!(single.contains("{{ thread_personality_user_tail }}"));
        assert!(merge.contains("{{ user_personality_merge_user_tail }}"));
        assert!(single.contains("thread_personality_user_tail is missing"));
        assert!(merge.contains("user_personality_merge_user_tail is missing"));
        assert!(batch.contains("output_language: { type: string, enum: [\"ja\", \"en\"]"));
        assert!(batch.contains("merge_enabled:"));
        assert!(batch.contains("type: boolean"));
        assert!(batch.contains("default: false"));
        assert!(batch.contains(
            "workerName: '${ \"memories-thread-personality-single-\" + $workflow.input.output_language }'"
        ));
        assert!(batch.contains(
            "workerName: '${ \"memories-user-personality-merge-\" + $workflow.input.output_language }'"
        ));
        assert!(!batch.contains("workflow_url: \"${$workflow.input.single_workflow_path}\""));
        assert!(!batch.contains("workflow_url: \"${ $workflow.input.merge_workflow_path }\""));
        assert!(!batch.contains("workflow_context:"));
        assert!(agent_summary.contains("merge_enabled: $personality_merge_enabled"));
        // The personality batch is a named-worker fan-out now, so
        // agent-chat-summary must NOT relay prompt context to it.
        assert!(!agent_summary.contains("thread_personality_system_prompt"));
        assert!(!agent_summary.contains("thread_personality_user_tail"));
        assert!(!agent_summary.contains("user_personality_merge_system_prompt"));
        assert!(!agent_summary.contains("user_personality_merge_user_tail"));
    }

    #[test]
    fn merge_rejects_non_object_array() {
        let err = merge_template(json!([1, 2, 3]), 1, None, "ja").unwrap_err();
        assert!(err.to_string().contains("JSON object"));
    }

    #[test]
    fn merge_rejects_non_object_string() {
        let err = merge_template(json!("nope"), 1, None, "ja").unwrap_err();
        assert!(err.to_string().contains("JSON object"));
    }

    #[test]
    fn merge_rejects_non_object_number() {
        let err = merge_template(json!(42), 1, None, "ja").unwrap_err();
        assert!(err.to_string().contains("JSON object"));
    }

    #[test]
    fn merge_rejects_non_object_null() {
        let err = merge_template(Value::Null, 1, None, "ja").unwrap_err();
        assert!(err.to_string().contains("JSON object"));
    }

    // ---- parse_template ----

    #[test]
    fn parse_accepts_object() {
        let v = parse_template(r#"{"user_id": 1, "memories_grpc_host": "h"}"#).unwrap();
        assert_eq!(v["user_id"], json!(1));
    }

    #[test]
    fn parse_rejects_invalid_json() {
        let err = parse_template("{not json}").unwrap_err();
        assert!(
            err.to_string()
                .contains("parse --extract-personality-after-*")
        );
    }

    #[test]
    fn parse_rejects_array() {
        let err = parse_template("[]").unwrap_err();
        assert!(err.to_string().contains("array"));
    }

    #[test]
    fn parse_rejects_string() {
        let err = parse_template(r#""hello""#).unwrap_err();
        assert!(err.to_string().contains("string"));
    }

    // ---- skip_reason ----

    #[test]
    fn skip_reason_passes_when_imported_and_no_errors() {
        assert!(skip_reason(0, 5).is_none());
    }

    #[test]
    fn skip_reason_blocks_on_errors_even_with_imports() {
        let r = skip_reason(1, 5).expect("must skip on errors");
        assert!(r.contains("error"), "reason: {r}");
    }

    #[test]
    fn skip_reason_blocks_when_zero_imported() {
        let r = skip_reason(0, 0).expect("must skip on zero imports");
        assert!(
            r.contains("no memories") || r.contains("entire user history"),
            "reason should explain the zero-import skip: {r}"
        );
    }

    #[test]
    fn skip_reason_prefers_error_message_over_zero_count() {
        let r = skip_reason(2, 0).expect("must skip");
        assert!(r.contains("error"), "reason: {r}");
        assert!(!r.contains("entire user history"), "reason: {r}");
    }
}
