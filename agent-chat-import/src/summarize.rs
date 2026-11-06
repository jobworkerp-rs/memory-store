use crate::common::workflow_input::merge_thread_batch_template;
use anyhow::{Context, Result, anyhow};
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

/// Decide whether the post-import summary workflow should be skipped.
///
/// Two pre-conditions block dispatch:
///
/// 1. The import reported one or more session-level errors. A
///    half-imported thread leaves a stale latest-message window that
///    would otherwise pull incomplete data into the summary; LLM
///    summarization is expensive enough that swallowing partial-failure
///    state silently is worse than asking the operator to rerun.
/// 2. Nothing was actually imported (every session was Skipped or all
///    `memories_imported == 0`). Without `--since`, `merge_template`
///    does not inject `last_message_after_ms` (see `merge_template` below),
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

fn merge_template(
    template: Value,
    user_id: i64,
    last_message_after_ms: Option<i64>,
    output_language: &str,
) -> Result<Value> {
    merge_thread_batch_template(
        template,
        "--summarize-after-*",
        &["summary_user_id"],
        user_id,
        last_message_after_ms,
        output_language,
    )
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

    // Forward `--since` as an absolute `last_message_after_ms` so the summary
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

    fn workflow_yaml(relative: &str) -> serde_yaml::Value {
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
        let body = std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        serde_yaml::from_str(&body).unwrap_or_else(|error| panic!("parse {relative}: {error}"))
    }

    fn step_names(doc: &serde_yaml::Value) -> Vec<&str> {
        doc["do"]
            .as_sequence()
            .unwrap()
            .iter()
            .filter_map(|step| step.as_mapping()?.keys().next()?.as_str())
            .collect()
    }

    fn task<'a>(doc: &'a serde_yaml::Value, name: &str) -> &'a serde_yaml::Value {
        doc["do"]
            .as_sequence()
            .unwrap()
            .iter()
            .find_map(|step| step.get(name))
            .unwrap_or_else(|| panic!("task {name} missing"))
    }

    fn task_text(doc: &serde_yaml::Value, name: &str) -> String {
        serde_yaml::to_string(task(doc, name)).unwrap()
    }

    /// Finds a task at any depth (inside for / try / catch bodies).
    fn nested_task<'a>(doc: &'a serde_yaml::Value, name: &str) -> &'a serde_yaml::Value {
        fn find<'a>(value: &'a serde_yaml::Value, name: &str) -> Option<&'a serde_yaml::Value> {
            match value {
                serde_yaml::Value::Sequence(items) => {
                    items.iter().find_map(|item| find(item, name))
                }
                serde_yaml::Value::Mapping(mapping) => mapping
                    .get(name)
                    .or_else(|| mapping.values().find_map(|item| find(item, name))),
                _ => None,
            }
        }
        find(&doc["do"], name).unwrap_or_else(|| panic!("task {name} missing"))
    }

    /// Evaluates a workflow `${ ... }` expression on the jq engine the
    /// workflows run on, binding each key of `vars` as `$key`.
    fn eval_workflow_jq(
        expression: &serde_yaml::Value,
        vars: serde_json::Value,
    ) -> serde_json::Value {
        let text = expression.as_str().expect("expression string").trim();
        let body = text
            .strip_prefix("${")
            .and_then(|rest| rest.strip_suffix('}'))
            .unwrap_or_else(|| panic!("not a jq expression: {text}"));
        let params = vars
            .as_object()
            .expect("variables object")
            .iter()
            .map(|(name, value)| (name.clone(), std::sync::Arc::new(value.clone())))
            .collect::<std::collections::BTreeMap<_, _>>();
        command_utils::util::jq::execute_jq(serde_json::Value::Null, body, &params)
            .unwrap_or_else(|error| panic!("evaluate {body}: {error}"))
    }

    /// The workflows' label defaults must name the label the importer
    /// writes on subagent Threads.
    fn assert_defaults_to_subagent_label(doc: &serde_yaml::Value, input: &str, relative: &str) {
        let property = &doc["input"]["schema"]["document"]["properties"][input];
        assert_eq!(
            property["default"],
            serde_yaml::Value::Sequence(vec![crate::source::claude_code::SUBAGENT_LABEL.into()]),
            "{relative}: {input}"
        );
    }

    #[test]
    fn period_selection_excludes_subagent_threads_but_explicit_targets_do_not() {
        for (relative, var) in [
            (
                "workflows/thread-summary/thread-summary-batch.yaml",
                "target_threads",
            ),
            (
                "workflows/thread-reflection/thread-reflection-batch.yaml",
                "target_threads",
            ),
            (
                "workflows/personality/thread-personality-batch.yaml",
                "target_threads",
            ),
        ] {
            let doc = workflow_yaml(relative);
            assert_defaults_to_subagent_label(&doc, "exclude_thread_labels", relative);
            let expr = task(&doc, "filterThreads")["set"][var].as_str().unwrap();
            let (explicit, period) = expr.rsplit_once("else").unwrap();
            assert!(
                !explicit.contains("exclude_thread_labels"),
                "{relative}: explicit target ids must bypass the exclusion"
            );
            assert!(
                period.contains("exclude_thread_labels"),
                "{relative}: period/label selection must apply the exclusion"
            );
        }
    }

    #[test]
    fn daily_summary_drops_summaries_of_excluded_source_threads() {
        let relative = "workers/daily-work-summary/daily-work-summary-single.yaml";
        let doc = workflow_yaml(relative);
        assert_defaults_to_subagent_label(&doc, "exclude_thread_labels", relative);
        let steps = step_names(&doc);
        let position = |name: &str| {
            steps
                .iter()
                .position(|s| *s == name)
                .unwrap_or_else(|| panic!("task {name} missing"))
        };
        // The exclusion runs on the freshly fetched memories, before the
        // plan narrows them, so plan and execution runs see the same set.
        assert!(position("fetchSummaryMemories") < position("fetchExcludedSourceThreads"));
        assert!(
            position("fetchExcludedSourceThreads") < position("excludeSummariesOfExcludedThreads")
        );
        assert!(
            position("excludeSummariesOfExcludedThreads") < position("applyPlannedSourceMemories")
        );
        // Summary containers are shared per label set, so the source
        // thread is identified by the summary's `summary:<thread_id>` id,
        // never by the container's labels.
        let exclude = task_text(&doc, "excludeSummariesOfExcludedThreads");
        assert!(exclude.contains("\"summary:\"") && exclude.contains("externalId"));
        let fetch = task_text(&doc, "fetchExcludedSourceThreads");
        assert!(fetch.contains("exclude_thread_labels") && fetch.contains("LABEL_ANY"));
    }

    #[test]
    fn summary_containers_ignore_subagent_labels_everywhere_they_are_derived() {
        let single_path = "workers/thread-summary/thread-summary-single.yaml";
        let batch_path = "workflows/thread-summary/thread-summary-batch.yaml";
        let single = workflow_yaml(single_path);
        let batch = workflow_yaml(batch_path);
        assert_defaults_to_subagent_label(&single, "summary_thread_ignored_labels", single_path);
        assert_defaults_to_subagent_label(&batch, "summary_thread_ignored_labels", batch_path);

        // The single computes the container label set once; the lookup and
        // the creation (labels + description) use it.
        assert!(
            task_text(&single, "resolveContainerLabels").contains("summary_thread_ignored_labels")
        );
        assert!(task_text(&single, "findMemoryThread").contains("$container_source_labels"));
        let single_text = serde_yaml::to_string(&single).unwrap();
        assert_eq!(single_text.matches("$container_source_labels").count(), 3);

        assert!(
            serde_yaml::to_string(&batch).unwrap().contains(
                "summary_thread_ignored_labels: $workflow.input.summary_thread_ignored_labels"
            ),
            "batch must forward the ignored labels to the single worker"
        );
    }

    // ---- merge_template ----

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
    fn import_and_summary_guides_do_not_send_legacy_owner() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        for relative in [
            "workflows/agent-chat-import/run-import.sh",
            "README.md",
            "README_ja.md",
            "workflows/thread-summary/README.md",
            "workflows/thread-summary/README_ja.md",
        ] {
            let path = root.join(relative);
            let body = std::fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            assert!(
                !body.contains("summary_user_id"),
                "{relative} must not send the removed workflow field"
            );
        }
        let script =
            std::fs::read_to_string(root.join("workflows/agent-chat-import/run-import.sh"))
                .unwrap();
        assert!(script.contains("\"$USER_ID\""));
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
            assert!(worker.contains(&format!(
                "source_memory_kind: {{ type: string, default: \"MEMORY_KIND_{input_kind}\" }}"
            )));
            assert!(worker.contains(&format!("memory_kind: \"MEMORY_KIND_{output_kind}\"")));
            assert!(
                worker.contains(
                    "thread_filter: {\n                  user_id: $effective_user_id,\n                  memory_kinds: [$workflow.input.source_memory_kind]"
                ),
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
    fn summary_batches_preserve_generated_skipped_and_failed_outcomes() {
        for (feature, item) in [
            ("thread-summary", "threads"),
            ("daily-work-summary", "dates"),
            ("weekly-work-summary", "weeks"),
            ("monthly-work-summary", "months"),
        ] {
            let yaml = match feature {
                "thread-summary" => {
                    include_str!("../workflows/thread-summary/thread-summary-batch.yaml")
                }
                "daily-work-summary" => {
                    include_str!("../workflows/daily-work-summary/daily-work-summary-batch.yaml")
                }
                "weekly-work-summary" => {
                    include_str!("../workflows/weekly-work-summary/weekly-work-summary-batch.yaml")
                }
                "monthly-work-summary" => {
                    include_str!(
                        "../workflows/monthly-work-summary/monthly-work-summary-batch.yaml"
                    )
                }
                _ => unreachable!(),
            };
            assert!(
                yaml.contains("single_result: \"${ .output | fromjson }\""),
                "{feature} must inspect the single-workflow outcome"
            );
            assert!(
                yaml.contains("single_result.skipped // false"),
                "{feature} must classify skipped as a non-failure"
            );
            assert!(
                yaml.contains("single_result.completed // false"),
                "{feature} must distinguish generated from logical failure"
            );
            assert!(
                yaml.contains("generated_count:")
                    && yaml.contains("skipped_count:")
                    && yaml.contains(&format!("skipped_{item}:")),
                "{feature} must expose generated/skipped outcome counts"
            );
            assert!(
                yaml.contains("succeeded_count:")
                    && yaml.contains(&format!("($generated_{item} | length)"))
                    && yaml.contains(&format!("($skipped_{item} | length)")),
                "{feature} must keep skipped items successful for downstream stages"
            );
        }
    }

    #[test]
    fn thread_summary_single_decides_freshness_before_reading_messages() {
        let single = workflow_yaml("workers/thread-summary/thread-summary-single.yaml");
        let steps = step_names(&single);
        let position = |name: &str| {
            steps
                .iter()
                .position(|step| *step == name)
                .unwrap_or_else(|| panic!("task {name} missing"))
        };
        let ordered = [
            "checkThreadExists",
            "resolveContainerLabels",
            "findMemoryThread",
            "findExistingSummary",
            "checkEligibility",
            "skipIfNotEligible",
            "fetchMessages",
            "checkMessageCount",
        ];
        for pair in ordered.windows(2) {
            assert!(position(pair[0]) < position(pair[1]), "{pair:?}");
        }
    }

    #[test]
    fn thread_summary_single_compares_conversation_time_numerically() {
        let single = workflow_yaml("workers/thread-summary/thread-summary-single.yaml");
        let expression = &task(&single, "checkEligibility")["set"]["eligible"];
        let eligible = |last_message_at: serde_json::Value,
                        description: &str,
                        summary_updated_at: Option<&str>,
                        force: bool| {
            eval_workflow_jq(
                expression,
                json!({
                    "workflow": { "input": { "force_resummarize": force } },
                    "thread": { "data": {
                        "description": description,
                        "lastMessageAt": last_message_at,
                        // Audit time must not decide freshness.
                        "updatedAt": "9999999999999"
                    } },
                    "existing_summary_memory": summary_updated_at
                        .map(|updated_at| json!({ "data": { "updatedAt": updated_at } }))
                }),
            )
        };
        let summarized = Some("1792000000001");
        assert_eq!(
            eligible(json!("1792000000001"), "d", summarized, false),
            json!(false)
        );
        assert_eq!(
            eligible(json!("1792000000000"), "d", summarized, false),
            json!(false)
        );
        assert_eq!(
            eligible(json!("1792000000002"), "d", summarized, false),
            json!(true)
        );
        // Digit counts differ: a plain string comparison would get these wrong.
        assert_eq!(
            eligible(json!("10000000000000"), "d", Some("9999999999999"), false),
            json!(true)
        );
        assert_eq!(
            eligible(json!("9999999999999"), "d", Some("10000000000000"), false),
            json!(false)
        );
        assert_eq!(
            eligible(json!(1792000000005_i64), "d", summarized, false),
            json!(true)
        );
        assert_eq!(
            eligible(json!("1792000000000"), "", summarized, false),
            json!(true)
        );
        assert_eq!(
            eligible(json!("1792000000000"), "d", None, false),
            json!(true)
        );
        assert_eq!(
            eligible(json!("1792000000000"), "d", summarized, true),
            json!(true)
        );
    }

    #[test]
    fn thread_summary_batch_leaves_freshness_to_the_single() {
        let yaml = include_str!("../workflows/thread-summary/thread-summary-batch.yaml");
        assert!(!yaml.contains("MemoryService/FindListByCondition"));
        assert!(yaml.contains("in: \"${ $target_threads }\""));
        assert!(yaml.contains("skipped_threads: \"${ [] }\""));
    }

    #[test]
    fn thread_workflows_preserve_large_ids_as_strings() {
        for (batch, single) in [
            (
                include_str!("../workflows/thread-summary/thread-summary-batch.yaml"),
                include_str!("../workers/thread-summary/thread-summary-single.yaml"),
            ),
            (
                include_str!("../workflows/thread-reflection/thread-reflection-batch.yaml"),
                include_str!("../workers/thread-reflection/thread-reflection-single.yaml"),
            ),
            (
                include_str!("../workflows/personality/thread-personality-batch.yaml"),
                include_str!("../workers/personality/thread-personality-single.yaml"),
            ),
        ] {
            assert!(batch.contains("items: { type: string }"));
            assert!(batch.contains("thread_id: ($thread.id.value | tostring)"));
            assert!(!batch.contains("thread_id: ($thread.id.value | tonumber)"));
            assert!(single.contains("thread_id: { type: string }"));
        }
    }

    #[test]
    fn thread_batch_workflows_use_reserved_proto_replacements_for_message_time_windows() {
        for yaml in [
            include_str!("../workflows/thread-summary/thread-summary-batch.yaml"),
            include_str!("../workflows/thread-reflection/thread-reflection-batch.yaml"),
            include_str!("../workflows/personality/thread-personality-batch.yaml"),
        ] {
            assert!(yaml.contains("last_message_after: $last_message_after_ms"));
            // Batches ignore unknown keys, so a renamed input must fail
            // instead of silently widening the window to the whole history.
            assert!(yaml.contains("- rejectRenamedMessageWindowInputs:"));
            for removed in [
                "updated_after_ms",
                "updated_before_ms",
                "updated_within_hours",
            ] {
                assert_eq!(
                    yaml.matches(removed).count(),
                    1,
                    "renamed window input {removed} may appear only in the rejection guard"
                );
            }
            assert!(
                !yaml.contains("updated_after: $last_message_after_ms"),
                "reserved proto field must not be emitted in a gRPC JSON request"
            );
            assert!(
                !yaml.contains("del(.updated_after)"),
                "reserved proto field must not be removed from a request because it must not be added"
            );
        }

        let summary = include_str!("../workflows/thread-summary/thread-summary-batch.yaml");
        assert!(summary.contains("last_message_before: $last_message_before_ms"));
        assert!(!summary.contains("updated_before: $last_message_before_ms"));
        assert!(!summary.contains("del(.updated_before)"));
    }

    #[test]
    fn agent_chat_summary_runs_daily_after_a_successful_skipped_thread_batch() {
        let yaml = include_str!("../workflows/agent-chat-summary/agent-chat-summary.yaml");
        let thread = yaml.find("                - threadSummaryBatch:").unwrap();
        let daily = yaml.find("                - dailyWorkSummary:").unwrap();
        let reflection = yaml.find("                - reflectionStage:").unwrap();
        assert!(thread < daily);
        let thread_stage = &yaml[thread..daily];
        assert!(
            !thread_stage.contains("skipped"),
            "a skipped child result must not gate the daily stage"
        );
        assert!(thread_stage.contains("thread_summary_result: \"${ .output | fromjson }\""));
        assert!(
            !thread_stage.contains("thread summary stage failed entirely"),
            "thread-summary failures must not prevent the daily stage from using existing sources"
        );
        assert!(thread_stage.contains("thread_summary_succeeded: true"));
        assert!(
            thread_stage.contains("- recordThreadSummaryPartialFailure:")
                && thread_stage.contains("thread_summary_failed: \"${ true }\"")
                && thread_stage.contains("($thread_summary_result.failed_count // 0) > 0"),
            "thread batch item failures must be reported without preventing daily generation"
        );
        let daily_stage = &yaml[daily..reflection];
        assert!(daily_stage.contains("daily_summary_result: \"${ .output | fromjson }\""));
        assert!(daily_stage.contains("($daily_summary_result.failed_count // 0) > 0"));
        assert!(
            daily_stage.contains("- recordDailySummaryPartialFailure:")
                && daily_stage.contains("daily_summary_failed: \"${ true }\""),
            "daily batch item failures must be reported as a partial summary result"
        );
        assert!(
            !daily_stage.contains("skipped_count"),
            "a skipped daily result must not gate the reflection stage"
        );
        assert!(yaml.contains("timeout:\n  after:\n    days: 32"));
        assert!(
            daily_stage.contains(
                "timeout:\n                      after:\n                        days: 31"
            )
        );
        assert!(
            yaml.contains("and ($thread_summary_failed | not)")
                && yaml.contains("and (($daily_summary_failed | not)"),
            "partial thread or daily failures must make the summary output incomplete"
        );
    }

    #[test]
    fn period_rollups_use_fixed_size_source_snapshots() {
        let daily = include_str!("../workers/daily-work-summary/daily-work-summary-single.yaml");
        let weekly = include_str!("../workers/weekly-work-summary/weekly-work-summary-single.yaml");
        let monthly =
            include_str!("../workers/monthly-work-summary/monthly-work-summary-single.yaml");

        for workflow in [daily, weekly, monthly] {
            assert!(workflow.contains("runnerName: COMMAND"));
            assert!(workflow.contains("stdin: \"${ $source_snapshot_json }\""));
            assert!(workflow.contains("shasum -a 256"));
            assert!(workflow.contains("source_fingerprint: $source_fingerprint"));
            // Only a bounded sample of omitted IDs may reach metadata.
            assert!(!workflow.contains("\n                source_memory_ids: ($target_memories"));
            assert!(workflow.contains("| .[0:1024]),"));
            assert!(!workflow.contains("source_thread_ids: ($target_memories"));
        }

        assert!(daily.contains("source_snapshot_version: 1"));

        for workflow in [weekly, monthly] {
            assert!(workflow.contains("source_snapshot_version: 2"));
            assert!(
                workflow.contains("source_fingerprint: (((.memory.data.metadata // \"{}\") | fromjson | .source_fingerprint) // \"\")"),
                "downstream snapshots must include the upstream fingerprint so re-summaries propagate"
            );
        }
    }

    #[test]
    fn period_batches_have_granularity_specific_timeouts() {
        for (yaml, budget) in [
            (
                include_str!("../workflows/daily-work-summary/daily-work-summary-batch.yaml"),
                "days: 31",
            ),
            (
                include_str!("../workflows/weekly-work-summary/weekly-work-summary-batch.yaml"),
                "days: 7",
            ),
            (
                include_str!("../workflows/monthly-work-summary/monthly-work-summary-batch.yaml"),
                "days: 31",
            ),
        ] {
            assert!(yaml.contains(&format!("timeout:\n  after:\n    {budget}")));
            assert!(yaml.contains(&format!("timeout:\n        after:\n          {budget}")));
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

    #[test]
    fn summary_prompts_do_not_ask_for_issue_candidates() {
        for prompt in [
            include_str!("../workers/thread-summary/prompts/system_prompt.en.txt"),
            include_str!("../workers/thread-summary/prompts/system_prompt.ja.txt"),
            include_str!("../workers/thread-summary/prompts/user_tail.en.txt"),
            include_str!("../workers/thread-summary/prompts/user_tail.ja.txt"),
        ] {
            assert!(!prompt.contains("has_unfinished_issue_candidates"));
        }
    }

    #[test]
    fn thread_summary_single_dates_by_conversation_and_rejects_blank_output() {
        let yaml = include_str!("../workers/thread-summary/thread-summary-single.yaml");
        assert!(yaml.contains("created_at: $thread.data.firstMessageAt"));
        assert!(yaml.contains("updated_at: $thread.data.lastMessageAt"));
        assert!(yaml.contains(".updatedAt = $thread.data.lastMessageAt"));
        assert!(yaml.contains("- startedAt: {{ conversation_started_at }}"));
        // Freshness compares conversation times (see the jq evaluation in
        // thread_summary_single_compares_conversation_time_numerically).
        assert!(!yaml.contains("$thread.data.updatedAt"));
        assert!(yaml.contains("extract_reasoning_content: false"));
        assert!(yaml.contains("model: \"${$workflow.input.summary_model}\""));
        // Only blank/malformed output is retried, then the item fails.
        assert!(
            yaml.contains("type: \"https://memories.local/errors/thread-summary/invalid-output\"")
        );
        assert!(yaml.contains("retry:"));
    }

    #[test]
    fn thread_summary_batch_accounts_for_every_target_thread() {
        let yaml = include_str!("../workflows/thread-summary/thread-summary-batch.yaml");
        assert!(yaml.contains("collectUnaccountedThreads"));
        assert!(yaml.contains("insufficient_message_threads: ${ $insufficient_message_threads }"));
        assert!(yaml.contains("summary_model: $workflow.input.summary_model"));
        assert!(yaml.contains("ollama_base_url: $workflow.input.ollama_base_url"));
        assert!(yaml.contains("target_thread_ids:"));
    }

    #[test]
    fn thread_group_summary_batch_refreshes_members_then_calls_language_single() {
        let batch =
            include_str!("../workflows/thread-group-summary/thread-group-summary-batch.yaml");
        assert!(
            batch.contains(
                "required: [group_owner_user_id, memories_grpc_host, memories_grpc_port]"
            )
        );
        assert!(batch.contains("GetThreadGroupCapabilities"));
        assert!(batch.contains("summary_model: $workflow.input.summary_model"));
        // A hard-coded label would make the member refresh relabel summaries
        // created under a custom prefix.
        assert!(
            batch.contains(
                "memory_thread_label_prefix: $workflow.input.memory_thread_label_prefix,"
            )
        );
        assert!(batch.contains(
            "workerName: '${ \"memories-thread-group-summary-single-\" + $workflow.input.output_language }'"
        ));
        assert!(batch.contains("single_result: \"${ .output | fromjson }\""));
        assert!(batch.contains(
            "insufficient_message_thread_ids: ($insufficient_message_thread_ids | unique)"
        ));
        // Members are refreshed one thread at a time before any group is
        // aggregated, so a thread shared by groups is refreshed once.
        let refresh = batch.find("  - refreshMemberSummaries:").unwrap();
        let aggregate = batch.find("  - summarizeEachGroup:").unwrap();
        assert!(refresh < aggregate);
        let refresh_phase = &batch[refresh..aggregate];
        assert!(refresh_phase.contains(
            "workerName: '${ \"memories-thread-summary-single-\" + $workflow.input.output_language }'"
        ));
        // One call per member keeps each call within the run/call default
        // and lets the stream report (n/m) while summaries are generated.
        assert!(!refresh_phase.contains("timeout:"));
        let report = refresh_phase.find("- reportProgress:").unwrap();
        assert!(report < refresh_phase.find("- invokeMemberSingle:").unwrap());
        assert!(batch.contains("- rejectRefreshFailedMembers:"));
        assert!(!batch.contains("stop_on_error"));
        // Aggregation and persistence belong to the single workflow only.
        assert!(!batch.contains("runnerName: LLM"));
        assert!(!batch.contains("MEMORY_KIND_DERIVED_SUMMARY"));
        assert!(batch.contains("collectUnaccountedGroups"));
    }

    #[test]
    fn thread_group_summary_batch_refreshes_each_member_once_and_counts_progress() {
        let batch = workflow_yaml("workflows/thread-group-summary/thread-group-summary-batch.yaml");
        let targets = eval_workflow_jq(
            &nested_task(&batch, "buildMemberRefreshTargets")["set"]["member_refresh_targets"],
            json!({ "owner_member_entries": [
                { "user_id": 7, "thread_id": "3" },
                { "user_id": 1, "thread_id": "1" },
                { "user_id": 7, "thread_id": "2" },
                { "user_id": 7, "thread_id": "3" }
            ] }),
        );
        assert_eq!(
            targets,
            json!([
                { "user_id": 1, "thread_id": "1" },
                { "user_id": 7, "thread_id": "2" },
                { "user_id": 7, "thread_id": "3" }
            ])
        );

        let progress = nested_task(&batch, "reportProgress");
        let at = |index: i64| {
            let vars = json!({ "member_refresh_targets": targets, "member_target_index": index });
            (
                eval_workflow_jq(&progress["set"]["progress_processed"], vars.clone()),
                eval_workflow_jq(&progress["set"]["progress_total"], vars),
            )
        };
        assert_eq!(at(0), (json!(1), json!(3)));
        assert_eq!(at(2), (json!(3), json!(3)));
    }

    /// The batches match the single's skip reason by text; YAML cannot
    /// share the constant, so pin every copy to the single's value.
    #[test]
    fn batches_match_the_single_insufficient_message_skip_reason() {
        let single = workflow_yaml("workers/thread-summary/thread-summary-single.yaml");
        let reason = task(&single, "checkMessageCount")["export"]["as"]["result"]
            .as_str()
            .unwrap();
        for relative in [
            "workflows/thread-summary/thread-summary-batch.yaml",
            "workflows/thread-group-summary/thread-group-summary-batch.yaml",
        ] {
            let path = Path::new(env!("CARGO_MANIFEST_DIR")).join(relative);
            let body = std::fs::read_to_string(&path).unwrap();
            assert!(
                body.contains(&format!("\"{reason}\"")),
                "{relative}: {reason}"
            );
        }
    }

    #[test]
    fn thread_group_summary_batch_classifies_each_member_refresh() {
        let batch = workflow_yaml("workflows/thread-group-summary/thread-group-summary-batch.yaml");
        let classify = &nested_task(&batch, "classifyMemberRefresh")["set"]["member_refreshed"];
        let collect = &nested_task(&batch, "collectMemberRefreshResult")["set"];
        let refresh = |result: serde_json::Value| {
            let refreshed = eval_workflow_jq(classify, json!({ "member_single_result": result }));
            let vars = json!({
                "member_single_result": result,
                "member_refreshed": refreshed,
                "member_target": { "user_id": 7, "thread_id": "2" },
                "insufficient_message_thread_ids": ["9"],
                "refresh_failed_thread_ids": [],
                "refresh_error": null
            });
            (
                eval_workflow_jq(&collect["insufficient_message_thread_ids"], vars.clone()),
                eval_workflow_jq(&collect["refresh_failed_thread_ids"], vars.clone()),
                eval_workflow_jq(&collect["refresh_error"], vars),
            )
        };
        for refreshed in [
            json!({ "completed": true, "skipped": false }),
            json!({ "completed": false, "skipped": true, "skip_reason": "skipped: already up to date" }),
            json!({ "completed": false, "skipped": true, "skip_reason": "skipped: thread not found" }),
        ] {
            assert_eq!(refresh(refreshed), (json!(["9"]), json!([]), json!(null)));
        }
        // Too-short threads never get a summary; the group single drops them.
        assert_eq!(
            refresh(json!({
                "completed": false,
                "skipped": true,
                "skip_reason": "skipped: not enough messages"
            })),
            (json!(["9", "2"]), json!([]), json!(null))
        );
        let (insufficient, failed, error) =
            refresh(json!({ "completed": false, "skipped": false }));
        assert_eq!((insufficient, failed), (json!(["9"]), json!(["2"])));
        assert!(
            error
                .as_str()
                .unwrap()
                .contains("neither completed nor skipped")
        );

        let failure = &nested_task(&batch, "recordMemberRefreshFailure")["set"];
        let vars = json!({
            "member_target": { "user_id": 7, "thread_id": "2" },
            "refresh_failed_thread_ids": ["1"],
            "refresh_error": null,
            "error": { "title": "boom" }
        });
        assert_eq!(
            eval_workflow_jq(&failure["refresh_failed_thread_ids"], vars.clone()),
            json!(["1", "2"])
        );
        assert_eq!(
            eval_workflow_jq(&failure["refresh_error"], vars),
            json!(r#"{"title":"boom"}"#)
        );
    }

    #[test]
    fn thread_group_summary_single_aggregates_one_group_with_baked_prompts() {
        let single =
            include_str!("../workers/thread-group-summary/thread-group-summary-single.yaml");
        assert!(single.contains("group_id: { type: string"));
        assert!(single.contains("output_language: { type: string, enum: [\"ja\", \"en\"]"));
        assert!(single.contains("prompt_context_missing"));
        assert!(single.contains("system_prompt: \"${ $thread_group_summary_system_prompt }\""));
        assert!(single.contains("{{ thread_group_summary_user_tail }}"));
        assert!(!single.contains("system_prompt: >-"));
        assert!(single.contains("runnerName: LLM"));
        assert!(single.contains("model: \"${$workflow.input.summary_model}\""));
        assert!(single.contains("memory_kind: \"MEMORY_KIND_DERIVED_SUMMARY\""));
        assert!(single.contains("\"thread-group-summary:\" + "));
        assert!(single.contains("FindThreadGroupLineage"));
        // Exact per-member lookups instead of scanning each owner's history.
        assert!(single.contains("external_id: (\"summary:\" + $member_thread_id)"));
        assert!(!single.contains("external_id_prefix"));
        assert!(single.contains("index([$thread_id])"));
        assert!(single.contains("snapshot-changed"));
        assert!(
            single.contains("https://memories.local/errors/thread-group-summary/invalid-output")
        );
        // Missing member summaries are a reportable outcome, not an opaque error.
        assert!(single.contains("outcome: \"incomplete_input\""));
        // Member refresh is the batch's job; the single must not nest batches.
        assert!(!single.contains("per_thread_batch_yaml"));
        assert!(!single.contains("runnerName: WORKFLOW"));
    }

    #[test]
    fn agent_chat_summary_runs_optional_group_stage_between_thread_and_daily() {
        let yaml = include_str!("../workflows/agent-chat-summary/agent-chat-summary.yaml");
        let thread = yaml.find("                - threadSummaryBatch:").unwrap();
        let group = yaml
            .find("                - threadGroupSummaryBatch:")
            .unwrap();
        let daily = yaml.find("                - dailyWorkSummary:").unwrap();
        assert!(thread < group && group < daily);
        let group_stage = &yaml[group..daily];
        assert!(group_stage.contains("if: \"${ $thread_group_summary_enabled }\""));
        // The group parent waits on member singles and the group single;
        // it must not hold the llm_batch slot the thread batch stage uses.
        assert!(
            group_stage.contains("channel: \"${ $workflow.input.thread_group_summary_channel }\"")
        );
        assert!(!group_stage.contains("channel: llm_batch"));
        assert!(yaml.contains("default: \"thread_group_summary\""));
        // Both activity bounds are required, otherwise every group is scanned.
        assert!(group_stage.contains("activity_after_ms: $last_message_after_ms_effective"));
        assert!(group_stage.contains("activity_before_ms:"));
        assert!(group_stage.contains("group_owner_user_id: $workflow.input.user_id"));
        assert!(
            group_stage.contains(
                "memory_thread_label_prefix: $workflow.input.memory_thread_label_prefix,"
            )
        );
        assert!(yaml.contains("or $thread_group_summary_failed or $daily_summary_failed"));
        assert!(yaml.contains("($thread_group_summary_enabled | not)"));
    }

    #[test]
    fn summary_entry_scripts_forward_optional_group_stage() {
        let pipeline = include_str!("../workflows/agent-chat-pipeline/agent-chat-pipeline.yaml");
        assert!(pipeline.contains(
            "thread_group_summary_batch_yaml: $workflow.input.thread_group_summary_batch_yaml,"
        ));
        assert!(pipeline.contains(
            "thread_group_summary_channel: $workflow.input.thread_group_summary_channel,"
        ));
        for script in [
            include_str!("../workflows/agent-chat-summary/run-summary.sh"),
            include_str!("../workflows/agent-chat-pipeline/run-pipeline.sh"),
        ] {
            assert!(script.contains("--thread-group-summary-batch-yaml)"));
            assert!(script.contains("--enable-thread-group-summary)"));
            assert!(script.contains("payload[\"thread_group_summary_batch_yaml\"] = tgs_batch"));
            assert!(script.contains("payload[\"thread_group_summary_channel\"] = tgs_channel"));
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
