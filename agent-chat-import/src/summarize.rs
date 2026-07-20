use crate::common::workflow_input::reject_removed_fields;
use anyhow::{Context, Result, anyhow};
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Decide whether the post-import summary workflow should be skipped.
///
/// Two pre-conditions block dispatch:
///
/// 1. The import reported one or more session-level errors. A
///    half-imported thread leaves a stale `updated_at` window that
///    would otherwise pull incomplete data into the summary; LLM
///    summarization is expensive enough that swallowing partial-failure
///    state silently is worse than asking the operator to rerun.
/// 2. Nothing was actually imported (every session was Skipped or all
///    `memories_imported == 0`). Without `--since`, `merge_template`
///    does not inject `updated_after_ms` (see `merge_template` below),
///    so a "0-imported" run would dispatch a workflow whose effective
///    scope is the *entire user history*. That is both expensive and
///    semantically wrong — the import did nothing this run, so the
///    summary should not run either.
///
/// Returns `Some(reason)` when dispatch must be skipped, with a string
/// suitable for logging to stderr. Returns `None` when dispatch should
/// proceed.
pub(crate) fn skip_reason(import_errors: usize, memories_imported: usize) -> Option<String> {
    if import_errors > 0 {
        return Some(format!(
            "import reported {import_errors} error(s); rerun after resolving them"
        ));
    }
    if memories_imported == 0 {
        return Some(
            "no memories were imported this run (all sessions skipped or empty); \
             skipping to avoid summarizing the entire user history"
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
/// `updated_at` so the summary window aligns exactly with the imported
/// set. We deliberately do *not* convert it to `updated_within_hours`:
/// that path re-anchors the window at workflow execution time, which
/// drifts past the import boundary whenever there is dispatch / queue
/// delay between the CLI and the workflow worker.
fn merge_template(
    mut template: Value,
    user_id: i64,
    updated_after_ms: Option<i64>,
    output_language: &str,
) -> Result<Value> {
    let obj = template
        .as_object_mut()
        .ok_or_else(|| anyhow!("--summarize-after-* must be a JSON object"))?;
    reject_removed_fields(obj, &["summary_user_id"])?;
    obj.insert("user_id".into(), json!(user_id));
    obj.insert("output_language".into(), json!(output_language));
    if let Some(ms) = updated_after_ms {
        obj.insert("updated_after_ms".into(), json!(ms));
    }
    Ok(template)
}

/// Validate input the user provided for the workflow before any import
/// work runs, so a typo'd JSON file fails fast instead of after a long
/// import.
pub(crate) fn parse_template(raw: &str) -> Result<Value> {
    let val: Value = serde_json::from_str(raw).context("parse --summarize-after-* as JSON")?;
    if !val.is_object() {
        return Err(anyhow!(
            "--summarize-after-* must be a JSON object (got {})",
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

/// Dispatch `thread-summary-batch.yaml` on jobworkerp using the merged
/// workflow input. Treats workflow execution failure as a warning at
/// the call site — import data is already persisted, so a failed
/// summary should not undo a successful import.
///
/// `timeout_sec` is forwarded into `JobworkerpClientWrapper::new_by_env`
/// because `execute_workflow` reuses the client's `request_timeout` as
/// the job's timeout (jobworkerp's default 1200s is far below what
/// batch summarization typically needs).
pub(crate) async fn run_summarize_after(
    template: Value,
    workflow_path: &Path,
    channel: Option<&str>,
    user_id: i64,
    since_millis: Option<i64>,
    output_language: &str,
    timeout_sec: u32,
) -> Result<Value> {
    // Surface a friendly error before paying the gRPC connection cost
    // (`new_by_env` panics on missing env, which is the wrong UX here).
    // The value must include the URI scheme (http:// or https://); the
    // tonic endpoint parser rejects bare host:port silently, so we
    // catch the common typo up front instead of letting it surface as
    // a confusing connection error.
    let addr = std::env::var("JOBWORKERP_ADDR")
        .map_err(|_| anyhow!("JOBWORKERP_ADDR is required for --summarize-after-*"))?;
    if !addr.starts_with("http://") && !addr.starts_with("https://") {
        return Err(anyhow!(
            "JOBWORKERP_ADDR must include a URI scheme (e.g. http://{addr}); got `{addr}`"
        ));
    }

    // Accept either a local filesystem path (which we canonicalize so the
    // jobworkerp WORKFLOW runner can `fs::read` it inside its container —
    // assuming a shared volume) or an http(s):// URL passed straight through.
    // The URL form is the production path: jobworkerp pods cannot see the
    // operator's local FS, so a publicly fetchable URL is the practical way
    // to ship a workflow definition into the cluster.
    let raw = workflow_path.to_string_lossy();
    let workflow_url = if raw.starts_with("http://") || raw.starts_with("https://") {
        raw.into_owned()
    } else {
        let abs_path = workflow_path
            .canonicalize()
            .with_context(|| format!("resolve workflow path {}", workflow_path.display()))?;
        abs_path.to_string_lossy().into_owned()
    };

    // Forward `--since` as an absolute `updated_after_ms` so the summary
    // window matches the imported set verbatim regardless of how long the
    // workflow waits in the jobworkerp queue. Prompt bodies are baked into
    // the language workers at registration, so no workflow_context is
    // injected here.
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
    fn merge_overrides_output_language() {
        let tmpl = json!({
            "output_language": "ja",
        });
        let out = merge_template(tmpl, 1, None, "en").unwrap();
        assert_eq!(out["output_language"], json!("en"));
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
    fn merge_does_not_emit_updated_within_hours() {
        // The CLI no longer derives `updated_within_hours` from `--since`;
        // it forwards the absolute boundary instead. Make sure we don't
        // accidentally re-introduce the relative path.
        let tmpl = json!({});
        let out = merge_template(tmpl, 1, Some(1_700_000_000_000), "ja").unwrap();
        assert!(out.get("updated_within_hours").is_none());
    }

    #[test]
    fn merge_rejects_legacy_summary_owner() {
        let tmpl = json!({
            "memories_grpc_host": "h",
            "memories_grpc_port": 9010,
            "summary_user_id": 100000,
        });
        let error = merge_template(tmpl, 1, Some(1_700_000_000_000), "ja").unwrap_err();
        assert!(
            error
                .to_string()
                .contains("summary_user_id is no longer supported")
        );
    }

    #[test]
    fn thread_summary_workflows_scope_by_kind_and_reject_legacy_owner() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for relative in [
            "workers/thread-summary/thread-summary-single.yaml",
            "workflows/thread-summary/thread-summary-batch.yaml",
        ] {
            let path = root.join(relative);
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            assert!(body.contains("summary_user_id is no longer supported"));
            assert!(
                body.contains("memory_kinds: [\"MEMORY_KIND_RAW\"]")
                    || body.contains("memory_kinds: [\"MEMORY_KIND_THREAD_SUMMARY\"]")
            );
        }
        let single =
            std::fs::read_to_string(root.join("workers/thread-summary/thread-summary-single.yaml"))
                .unwrap();
        assert!(single.contains("memory_kind: \"MEMORY_KIND_THREAD_SUMMARY\""));
    }

    #[test]
    fn production_import_and_summary_guides_do_not_send_legacy_owner() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("workspace root");
        for relative in [
            "production-import/run-import.sh",
            "agent-chat-import/README.md",
            "agent-chat-import/README_ja.md",
            "agent-chat-import/workflows/thread-summary/README.md",
            "agent-chat-import/workflows/thread-summary/README_ja.md",
        ] {
            let path = root.join(relative);
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            assert!(
                !body.contains("summary_user_id"),
                "{relative} must not send the removed workflow field"
            );
        }
        let script = std::fs::read_to_string(root.join("production-import/run-import.sh")).unwrap();
        assert!(script.contains("<<'PY' \"$USER_ID\""));
    }

    #[test]
    fn summary_orchestration_uses_user_owned_kind_scoped_aggregates() {
        let agent_summary = include_str!("../workflows/agent-chat-summary/agent-chat-summary.yaml");
        let pipeline = include_str!("../workflows/agent-chat-pipeline/agent-chat-pipeline.yaml");
        let summary_script = include_str!("../workflows/agent-chat-summary/run-summary.sh");
        let pipeline_script = include_str!("../workflows/agent-chat-pipeline/run-pipeline.sh");

        for workflow in [agent_summary, pipeline] {
            assert!(
                !workflow.contains("summary_user_id"),
                "orchestration must not forward the removed summary owner"
            );
            assert!(
                !workflow.contains("personality_user_id"),
                "orchestration must not forward the removed personality owner"
            );
        }
        for script in [summary_script, pipeline_script] {
            assert!(!script.contains("SUMMARY_USER_ID"));
            assert!(!script.contains("PERSONALITY_USER_ID"));
        }

        for (worker, input_kind, output_kind, external_id_prefix) in [
            (
                include_str!("../workers/daily-work-summary/daily-work-summary-single.yaml"),
                "THREAD_SUMMARY",
                "DAILY_SUMMARY",
                "daily",
            ),
            (
                include_str!("../workers/weekly-work-summary/weekly-work-summary-single.yaml"),
                "DAILY_SUMMARY",
                "WEEKLY_SUMMARY",
                "weekly",
            ),
            (
                include_str!("../workers/monthly-work-summary/monthly-work-summary-single.yaml"),
                "WEEKLY_SUMMARY",
                "MONTHLY_SUMMARY",
                "monthly",
            ),
        ] {
            assert!(!worker.contains("source_user_id"));
            assert!(worker.contains(&format!("MEMORY_KIND_{input_kind}")));
            assert!(worker.contains(&format!("memory_kind: \"MEMORY_KIND_{output_kind}\"")));
            assert!(
                worker.contains(&format!(
                    "thread_filter: {{\n                  user_id: $effective_user_id,\n                  memory_kinds: [\"MEMORY_KIND_{input_kind}\"]"
                )),
                "{input_kind} must be a ThreadSearchFilter condition"
            );
            assert!(
                worker.contains(&format!(
                    "\"{external_id_prefix}:\" + ($effective_user_id | tostring)"
                )),
                "{external_id_prefix} aggregation external IDs must be scoped by user_id"
            );
            assert!(
                !worker.contains(&format!("legacy_{external_id_prefix}_external_id")),
                "{external_id_prefix} aggregation must require the migrated external ID format"
            );
        }
    }

    #[test]
    fn aggregate_workflows_require_user_id() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for relative in [
            "workflows/daily-work-summary/daily-work-summary-batch.yaml",
            "workflows/weekly-work-summary/weekly-work-summary-batch.yaml",
            "workflows/monthly-work-summary/monthly-work-summary-batch.yaml",
            "workers/daily-work-summary/daily-work-summary-single.yaml",
            "workers/weekly-work-summary/weekly-work-summary-single.yaml",
            "workers/monthly-work-summary/monthly-work-summary-single.yaml",
        ] {
            let path = root.join(relative);
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            assert!(
                body.contains("required:\n        - user_id"),
                "{relative} must require user_id"
            );
        }
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
        assert!(err.to_string().contains("parse --summarize-after-*"));
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

    #[test]
    fn summary_workflows_declare_language_and_call_language_workers() {
        let summary_single = include_str!("../workers/thread-summary/thread-summary-single.yaml");
        let summary_batch = include_str!("../workflows/thread-summary/thread-summary-batch.yaml");
        let daily_single =
            include_str!("../workers/daily-work-summary/daily-work-summary-single.yaml");
        let daily_batch =
            include_str!("../workflows/daily-work-summary/daily-work-summary-batch.yaml");
        let daily_script = include_str!("../workflows/daily-work-summary/run-daily-summary.sh");
        let agent_summary = include_str!("../workflows/agent-chat-summary/agent-chat-summary.yaml");

        for yaml in [summary_single, daily_single] {
            assert!(yaml.contains("output_language: { type: string, enum: [\"ja\", \"en\"]"));
            assert!(yaml.contains("prompt_context_missing"));
            assert!(!yaml.contains("system_prompt: |\n"));
        }
        assert!(summary_single.contains("${ $summary_system_prompt }"));
        assert!(daily_single.contains("${ $daily_work_summary_system_prompt }"));
        assert!(summary_single.contains("{{ summary_user_tail }}"));
        assert!(daily_single.contains("{{ daily_work_summary_user_tail }}"));
        assert!(!summary_single.contains("system prompt で指定された出力言語"));
        assert!(!daily_single.contains("system prompt で指定された出力言語"));
        assert!(summary_single.contains("## Thread info"));
        assert!(daily_single.contains("## Target date"));

        assert!(summary_batch.contains(
            "workerName: '${ \"memories-thread-summary-single-\" + $workflow.input.output_language }'"
        ));
        assert!(summary_batch.contains("output_language: $workflow.input.output_language"));
        assert!(
            !summary_batch.contains("workflow_url: \"${$workflow.input.single_workflow_path}\"")
        );
        assert!(!summary_batch.contains("workflow_context:"));
        assert!(daily_batch.contains(
            "workerName: '${ \"memories-daily-work-summary-single-\" + $workflow.input.output_language }'"
        ));
        assert!(daily_batch.contains("output_language: $workflow.input.output_language"));
        assert!(!daily_batch.contains("workflow_url: \"${$workflow.input.single_workflow_path}\""));
        assert!(!daily_batch.contains("workflow_context:"));
        assert!(daily_script.contains("--output-language"));
        assert!(!daily_script.contains("WORKFLOW_CONTEXT_JSON"));
        assert!(!daily_script.contains("--context"));
        assert!(!daily_script.contains("single_workflow_path"));
        // agent-chat-summary no longer relays prompt context to the
        // summary/daily batches — the named single workers carry the
        // baked prompt, so no `*_system_prompt` / `*_user_tail` plumbing
        // should remain in the orchestrator.
        assert!(!agent_summary.contains("summary_system_prompt"));
        assert!(!agent_summary.contains("summary_user_tail"));
        assert!(!agent_summary.contains("daily_work_summary_system_prompt"));
        assert!(!agent_summary.contains("daily_work_summary_user_tail"));
    }

    #[test]
    fn weekly_and_monthly_work_summary_workflows_support_language_context() {
        let weekly_single =
            include_str!("../workers/weekly-work-summary/weekly-work-summary-single.yaml");
        let weekly_batch =
            include_str!("../workflows/weekly-work-summary/weekly-work-summary-batch.yaml");
        let weekly_script = include_str!("../workflows/weekly-work-summary/run-weekly-summary.sh");
        let monthly_single =
            include_str!("../workers/monthly-work-summary/monthly-work-summary-single.yaml");
        let monthly_batch =
            include_str!("../workflows/monthly-work-summary/monthly-work-summary-batch.yaml");
        let monthly_script =
            include_str!("../workflows/monthly-work-summary/run-monthly-summary.sh");

        for yaml in [weekly_single, monthly_single] {
            assert!(yaml.contains("output_language: { type: string, enum: [\"ja\", \"en\"]"));
            assert!(yaml.contains("prompt_context_missing"));
            assert!(!yaml.contains("system_prompt: |\n"));
        }
        assert!(weekly_single.contains("${ $weekly_work_summary_system_prompt }"));
        assert!(monthly_single.contains("${ $monthly_work_summary_system_prompt }"));
        assert!(weekly_single.contains("{{ weekly_work_summary_user_tail }}"));
        assert!(monthly_single.contains("{{ monthly_work_summary_user_tail }}"));
        assert!(!weekly_single.contains("system prompt で指定された出力言語"));
        assert!(!monthly_single.contains("system prompt で指定された出力言語"));
        assert!(weekly_single.contains("## Target week"));
        assert!(monthly_single.contains("## Target month"));
        assert!(weekly_batch.contains(
            "workerName: '${ \"memories-weekly-work-summary-single-\" + $workflow.input.output_language }'"
        ));
        assert!(weekly_batch.contains("output_language: $workflow.input.output_language"));
        assert!(
            !weekly_batch.contains("workflow_url: \"${$workflow.input.single_workflow_path}\"")
        );
        assert!(!weekly_batch.contains("workflow_context:"));
        assert!(monthly_batch.contains(
            "workerName: '${ \"memories-monthly-work-summary-single-\" + $workflow.input.output_language }'"
        ));
        assert!(monthly_batch.contains("output_language: $workflow.input.output_language"));
        assert!(
            !monthly_batch.contains("workflow_url: \"${$workflow.input.single_workflow_path}\"")
        );
        assert!(!monthly_batch.contains("workflow_context:"));

        for script in [weekly_script, monthly_script] {
            assert!(script.contains("--output-language"));
            assert!(!script.contains("WORKFLOW_CONTEXT_JSON"));
            assert!(!script.contains("--context"));
            assert!(!script.contains("single_workflow_path"));
        }
    }

    #[test]
    fn reflection_single_uses_mode_selected_prompt_variables() {
        let yaml = include_str!("../workers/thread-reflection/thread-reflection-single.yaml");
        assert!(!yaml.contains("$context."));
        assert!(yaml.contains("resolved_prompt_source"));
        assert!(yaml.contains("active_reflection_system_prompt"));
        assert!(yaml.contains("active_reflection_user_tail"));
        assert!(yaml.contains("$reflection_system_prompt"));
        assert!(yaml.contains("$reflection_user_tail"));
        assert!(yaml.contains("prompt_fetch_failed"));
        assert!(yaml.contains("prompt_context_missing"));
    }

    #[test]
    fn reflection_japanese_system_prompt_is_localized() {
        let prompt = include_str!("../workers/thread-reflection/prompts/system_prompt.ja.txt");
        for phrase in [
            "You are an agent",
            "Prime directive",
            "Output format",
            "failure analysis",
            "success_factors and key_decisions",
            "Write every free-text field",
        ] {
            assert!(
                !prompt.contains(phrase),
                "Japanese reflection prompt must not contain English prose phrase: {phrase}"
            );
        }
        assert!(prompt.contains("## 基本方針"));
        assert!(prompt.contains("## 失敗分析"));
        assert!(prompt.contains("## 出力言語"));
    }

    #[test]
    fn reflection_chain_uses_language_workers_without_context_relay() {
        let pipeline = include_str!("../workflows/agent-chat-pipeline/agent-chat-pipeline.yaml");
        let summary = include_str!("../workflows/agent-chat-summary/agent-chat-summary.yaml");
        let batch = include_str!("../workflows/thread-reflection/thread-reflection-batch.yaml");

        assert!(pipeline.contains("workflowUrl: \"${ $workflow.input.agent_chat_summary_yaml }\""));
        assert!(
            summary.contains("workflow_url: \"${ $workflow.input.thread_reflection_batch_yaml }\"")
        );
        assert!(summary.contains("channel: llm_batch"));
        assert!(!summary.contains("$context | tojson"));
        // The reflection batch fans out to the named single worker
        // (prompt baked in worker settings), so agent-chat-summary no
        // longer relays prompt context to it.
        assert!(!summary.contains("reflection_system_prompt"));
        assert!(!summary.contains("reflection_user_tail"));
        assert!(batch.contains(
            "workerName: '${ \"memories-thread-reflection-single-\" + $workflow.input.output_language }'"
        ));
        assert!(!batch.contains("workflow_url: \"${$workflow.input.single_workflow_path}\""));
        assert!(!batch.contains("channel: llm_workflow"));
        assert!(!batch.contains("$context | tojson"));
        assert!(!batch.contains("workflow_context:"));
        assert_eq!(
            batch.matches("memory_kinds: [\"MEMORY_KIND_RAW\"]").count(),
            2
        );
    }

    #[test]
    fn all_summary_layers_support_kanban_statuses() {
        let workflows = [
            include_str!("../workers/thread-summary/thread-summary-single.yaml"),
            include_str!("../workers/daily-work-summary/daily-work-summary-single.yaml"),
            include_str!("../workers/weekly-work-summary/weekly-work-summary-single.yaml"),
            include_str!("../workers/monthly-work-summary/monthly-work-summary-single.yaml"),
        ];
        for workflow in workflows {
            assert!(
                workflow.contains(
                    "\"enum\": [\"resolved\", \"ongoing\", \"in_review\", \"blocked\", \"deferred\", \"abandoned\"]"
                ),
                "summary status schema must expose the kanban-compatible values"
            );
        }

        let prompts = [
            include_str!("../workers/thread-summary/prompts/system_prompt.en.txt"),
            include_str!("../workers/thread-summary/prompts/system_prompt.ja.txt"),
            include_str!("../workers/daily-work-summary/prompts/system_prompt.en.txt"),
            include_str!("../workers/daily-work-summary/prompts/system_prompt.ja.txt"),
            include_str!("../workers/weekly-work-summary/prompts/system_prompt.en.txt"),
            include_str!("../workers/weekly-work-summary/prompts/system_prompt.ja.txt"),
            include_str!("../workers/monthly-work-summary/prompts/system_prompt.en.txt"),
            include_str!("../workers/monthly-work-summary/prompts/system_prompt.ja.txt"),
        ];
        for prompt in prompts {
            assert!(
                ["in_review", "blocked", "deferred"]
                    .iter()
                    .all(|status| prompt.contains(status)),
                "summary prompt must explain every kanban-compatible status"
            );
        }
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
        // Regression guard for the import-branch review finding: a run
        // that imported nothing must not trigger a workflow whose
        // default scope is "the whole user history".
        let r = skip_reason(0, 0).expect("must skip on zero imports");
        assert!(
            r.contains("no memories") || r.contains("entire user history"),
            "reason should explain the zero-import skip: {r}"
        );
    }

    #[test]
    fn skip_reason_prefers_error_message_over_zero_count() {
        // When both conditions apply, surface the error reason first —
        // it's the more actionable signal for the operator.
        let r = skip_reason(2, 0).expect("must skip");
        assert!(r.contains("error"), "reason: {r}");
        assert!(!r.contains("entire user history"), "reason: {r}");
    }
}
