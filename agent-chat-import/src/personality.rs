use crate::common::workflow_input::merge_thread_batch_template;
use anyhow::{Context, Result, anyhow};
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Decide whether the post-import personality extraction workflow
/// should be skipped. Same two-condition gate as `summarize::skip_reason`:
///
/// 1. The import reported one or more session-level errors. A
///    half-imported thread leaves a stale latest-message window that
///    would otherwise pull incomplete data into the extraction;
///    personality extraction is expensive enough that swallowing
///    partial-failure state silently is worse than asking the operator
///    to rerun.
/// 2. Nothing was actually imported (every session was Skipped or all
///    `memories_imported == 0`). Without `--since`, `merge_template`
///    does not inject `last_message_after_ms`, so a "0-imported" run would
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

fn merge_template(
    template: Value,
    user_id: i64,
    last_message_after_ms: Option<i64>,
    output_language: &str,
) -> Result<Value> {
    merge_thread_batch_template(
        template,
        "--extract-personality-after-*",
        &["personality_user_id", "summary_user_id"],
        user_id,
        last_message_after_ms,
        output_language,
    )
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
    fn merge_rejects_legacy_personality_owners() {
        let tmpl = json!({
            "memories_grpc_host": "h",
            "memories_grpc_port": 9100,
            "personality_user_id": 200000,
            "summary_user_id": 100000,
        });
        let error = merge_template(tmpl, 1, Some(1_700_000_000_000), "ja").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("personality_user_id is no longer supported")
        );
    }

    #[test]
    fn personality_workflows_scope_by_kind_without_legacy_owners() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for relative in [
            "workers/personality/thread-personality-single.yaml",
            "workers/personality/user-personality-merge.yaml",
        ] {
            let path = root.join(relative);
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            assert!(!body.contains("personality_user_id: { type"));
            assert!(!body.contains("personality_user_id:\n          type"));
            assert!(body.contains("memory_kind: \"MEMORY_KIND_PERSONALITY\""));
        }
        let batch = std::fs::read_to_string(
            root.join("workflows/personality/thread-personality-batch.yaml"),
        )
        .unwrap();
        assert!(batch.contains("memory_kinds: [\"MEMORY_KIND_RAW\"]"));

        let merge =
            std::fs::read_to_string(root.join("workers/personality/user-personality-merge.yaml"))
                .unwrap();
        assert!(merge.contains(
            "thread_filter: {\n                  user_id: $effective_personality_user_id,\n                  memory_kinds: [\"MEMORY_KIND_PERSONALITY\"]"
        ));
        // Count and list must share one row scope so the derived
        // no-signal count cannot go negative.
        assert_eq!(
            merge
                .matches(
                    "user_id: { value: $effective_personality_user_id },\n                memory_kinds: [\"MEMORY_KIND_PERSONALITY\"],\n                external_id_prefix: \"personality:\""
                )
                .count(),
            2
        );
        assert!(
            merge.contains("method: \"/llm_memory.service.MemoryService/FindListByCondition\"")
        );
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
            "# AGENTS.md instructions",
            "# CLAUDE.md instructions",
            "contains(\"<INSTRUCTIONS>\")",
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
