use crate::common::language::SUPPORTED_LANGUAGES;
use anyhow::{Context as _, Result, anyhow};
use jobworkerp_client::client::helper::UseJobworkerpClientHelper;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::jobworkerp::data::{
    QueueType, ResponseType, RetryPolicy, RetryType, WorkerData,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GenerationWorkerFeature {
    Reflection,
    ThreadSummary,
    ThreadGroupSummary,
    ThreadPersonality,
    UserPersonalityMerge,
    DailyWorkSummary,
    WeeklyWorkSummary,
    MonthlyWorkSummary,
}

impl GenerationWorkerFeature {
    const ALL: [Self; 8] = [
        Self::Reflection,
        Self::ThreadSummary,
        Self::ThreadGroupSummary,
        Self::ThreadPersonality,
        Self::UserPersonalityMerge,
        Self::DailyWorkSummary,
        Self::WeeklyWorkSummary,
        Self::MonthlyWorkSummary,
    ];

    /// Canonical `--feature` token. Failure reports print it so callers can
    /// tell which feature was skipped.
    pub(crate) fn token(self) -> &'static str {
        match self {
            Self::Reflection => "reflection",
            Self::ThreadSummary => "thread-summary",
            Self::ThreadGroupSummary => "thread-group-summary",
            Self::ThreadPersonality => "thread-personality",
            Self::UserPersonalityMerge => "user-personality-merge",
            Self::DailyWorkSummary => "daily-work-summary",
            Self::WeeklyWorkSummary => "weekly-work-summary",
            Self::MonthlyWorkSummary => "monthly-work-summary",
        }
    }

    fn parse(raw: &str) -> Result<Self> {
        let token = raw.trim();
        Self::ALL
            .into_iter()
            .find(|feature| feature.token() == token)
            .ok_or_else(|| {
                anyhow!(
                    "unsupported generation worker feature `{token}`; supported: {}",
                    Self::ALL.map(Self::token).join(", ")
                )
            })
    }

    fn spec(self) -> FeatureSpec {
        match self {
            Self::Reflection => FeatureSpec {
                worker_base_name: "memories-thread-reflection-single",
                workflow_path: "workers/thread-reflection/thread-reflection-single.yaml",
                prompt_dir: "workers/thread-reflection/prompts",
                prompts: &[
                    ("reflection_system_prompt", "system_prompt"),
                    ("reflection_user_tail", "user_tail"),
                ],
            },
            Self::ThreadSummary => FeatureSpec {
                worker_base_name: "memories-thread-summary-single",
                workflow_path: "workers/thread-summary/thread-summary-single.yaml",
                prompt_dir: "workers/thread-summary/prompts",
                prompts: &[
                    ("summary_system_prompt", "system_prompt"),
                    ("summary_user_tail", "user_tail"),
                ],
            },
            Self::ThreadGroupSummary => FeatureSpec {
                worker_base_name: "memories-thread-group-summary-single",
                workflow_path: "workers/thread-group-summary/thread-group-summary-single.yaml",
                prompt_dir: "workers/thread-group-summary/prompts",
                prompts: &[
                    ("thread_group_summary_system_prompt", "system_prompt"),
                    ("thread_group_summary_user_tail", "user_tail"),
                ],
            },
            Self::ThreadPersonality => FeatureSpec {
                worker_base_name: "memories-thread-personality-single",
                workflow_path: "workers/personality/thread-personality-single.yaml",
                prompt_dir: "workers/personality/prompts",
                prompts: &[
                    ("thread_personality_system_prompt", "thread_system_prompt"),
                    ("thread_personality_user_tail", "thread_user_tail"),
                ],
            },
            Self::UserPersonalityMerge => FeatureSpec {
                worker_base_name: "memories-user-personality-merge",
                workflow_path: "workers/personality/user-personality-merge.yaml",
                prompt_dir: "workers/personality/prompts",
                prompts: &[
                    (
                        "user_personality_merge_system_prompt",
                        "merge_system_prompt",
                    ),
                    ("user_personality_merge_user_tail", "merge_user_tail"),
                ],
            },
            Self::DailyWorkSummary => FeatureSpec {
                worker_base_name: "memories-daily-work-summary-single",
                workflow_path: "workers/daily-work-summary/daily-work-summary-single.yaml",
                prompt_dir: "workers/daily-work-summary/prompts",
                prompts: &[
                    ("daily_work_summary_system_prompt", "system_prompt"),
                    ("daily_work_summary_user_tail", "user_tail"),
                ],
            },
            Self::WeeklyWorkSummary => FeatureSpec {
                worker_base_name: "memories-weekly-work-summary-single",
                workflow_path: "workers/weekly-work-summary/weekly-work-summary-single.yaml",
                prompt_dir: "workers/weekly-work-summary/prompts",
                prompts: &[
                    ("weekly_work_summary_system_prompt", "system_prompt"),
                    ("weekly_work_summary_user_tail", "user_tail"),
                ],
            },
            Self::MonthlyWorkSummary => FeatureSpec {
                worker_base_name: "memories-monthly-work-summary-single",
                workflow_path: "workers/monthly-work-summary/monthly-work-summary-single.yaml",
                prompt_dir: "workers/monthly-work-summary/prompts",
                prompts: &[
                    ("monthly_work_summary_system_prompt", "system_prompt"),
                    ("monthly_work_summary_user_tail", "user_tail"),
                ],
            },
        }
    }
}

struct FeatureSpec {
    worker_base_name: &'static str,
    workflow_path: &'static str,
    prompt_dir: &'static str,
    prompts: &'static [(&'static str, &'static str)],
}

#[derive(Debug, Clone)]
struct GenerationWorkerRegistration {
    feature: GenerationWorkerFeature,
    worker_name: String,
    worker_data: WorkerData,
    settings: Value,
}

/// A feature that could not be registered. Other features are still
/// registered so one missing or broken asset does not leave every
/// generation worker unregistered.
#[derive(Debug)]
pub(crate) struct FeatureFailure {
    pub feature: GenerationWorkerFeature,
    pub error: anyhow::Error,
}

impl std::fmt::Display for FeatureFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // `{:#}` keeps the whole context chain (which names the path) and the
        // newline replacement keeps each failure a single parseable line.
        let error = format!("{:#}", self.error).replace('\n', " ");
        write!(f, "{}: {error}", self.feature.token())
    }
}

#[derive(Debug)]
struct RegistrationPlan {
    registrations: Vec<GenerationWorkerRegistration>,
    failures: Vec<FeatureFailure>,
}

#[derive(Debug)]
pub(crate) struct UpsertReport {
    /// Workers upserted, or planned when running as a dry run.
    pub registered: Vec<String>,
    pub failures: Vec<FeatureFailure>,
}

// Stable output contract for external callers: tools that run this command
// parse these line prefixes to report which features were registered or
// skipped. Changing them is a breaking change.
/// Prefix of each stdout line naming an upserted worker.
pub const UPSERTED_PREFIX: &str = "upserted generation worker: ";
/// Prefix of each stdout line naming a worker a dry run would upsert.
pub const DRY_RUN_PREFIX: &str = "[dry-run] would upsert generation worker: ";
/// Prefix of each stderr line naming a skipped feature (`<token>: <error>`).
pub const SKIPPED_FEATURE_PREFIX: &str = "warning: skipped generation feature ";
/// Marker in the final error line: `<n> generation feature(s) failed: <tokens>; <m> worker(s) registered`.
pub const FAILURE_SUMMARY_MARKER: &str = " generation feature(s) failed: ";

/// Console form of an [`UpsertReport`], built from the stable prefixes above.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RenderedUpsertReport {
    pub stdout: Vec<String>,
    pub stderr: Vec<String>,
    /// Set when any feature failed; the command must exit non-zero with it.
    pub failure: Option<String>,
}

impl UpsertReport {
    pub(crate) fn render(&self, dry_run: bool) -> RenderedUpsertReport {
        let prefix = if dry_run {
            DRY_RUN_PREFIX
        } else {
            UPSERTED_PREFIX
        };
        let stdout = self
            .registered
            .iter()
            .map(|name| format!("{prefix}{name}"))
            .collect();
        let stderr = self
            .failures
            .iter()
            .map(|failure| format!("{SKIPPED_FEATURE_PREFIX}{failure}"))
            .collect();
        let failure = (!self.failures.is_empty()).then(|| {
            let tokens: Vec<_> = self.failures.iter().map(|f| f.feature.token()).collect();
            format!(
                "{}{FAILURE_SUMMARY_MARKER}{}; {} worker(s) registered",
                self.failures.len(),
                tokens.join(", "),
                self.registered.len()
            )
        });
        RenderedUpsertReport {
            stdout,
            stderr,
            failure,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct UpsertGenerationWorkersArgs {
    pub repo_root: PathBuf,
    pub channel: Option<String>,
    pub timeout_sec: u32,
    pub features: Vec<GenerationWorkerFeature>,
    pub languages: Vec<String>,
}

/// Default base directory for generation workers when `--repo-root` is not
/// given. Resolution order: `MEMORY_REPO_ROOT` env > crate directory baked
/// at build time. The base is the `agent-chat-import` crate directory and
/// worker registration reads `workers/<feature>/...` below it.
pub(crate) fn resolve_repo_root() -> PathBuf {
    std::env::var("MEMORY_REPO_ROOT")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")))
}

pub(crate) fn parse_language_selection(raw: &str) -> Result<Vec<String>> {
    let trimmed = raw.trim();
    if trimmed == "all" {
        return Ok(SUPPORTED_LANGUAGES
            .iter()
            .map(|s| (*s).to_string())
            .collect());
    }
    if SUPPORTED_LANGUAGES.contains(&trimmed) {
        return Ok(vec![trimmed.to_string()]);
    }
    Err(anyhow!(
        "unsupported language `{trimmed}`; supported: ja, en, all"
    ))
}

/// Resolve a `--feature` value into concrete features. Selection-layer
/// aliases (`all`, and `personality` = both personality layers) are
/// owned here; `GenerationWorkerFeature::parse` only knows the canonical
/// one-to-one tokens.
pub(crate) fn parse_feature_selection(raw: &str) -> Result<Vec<GenerationWorkerFeature>> {
    let trimmed = raw.trim();
    if trimmed == "all" {
        return Ok(GenerationWorkerFeature::ALL.to_vec());
    }
    if trimmed == "personality" {
        return Ok(vec![
            GenerationWorkerFeature::ThreadPersonality,
            GenerationWorkerFeature::UserPersonalityMerge,
        ]);
    }
    Ok(vec![GenerationWorkerFeature::parse(trimmed)?])
}

fn worker_name_for_feature(feature: GenerationWorkerFeature, lang: &str) -> Result<String> {
    if !SUPPORTED_LANGUAGES.contains(&lang) {
        return Err(anyhow!(
            "unsupported language `{lang}`; supported: {}",
            SUPPORTED_LANGUAGES.join(", ")
        ));
    }
    Ok(format!("{}-{lang}", feature.spec().worker_base_name))
}

fn build_generation_worker_registrations(
    repo_root: &Path,
    channel: Option<&str>,
    features: &[GenerationWorkerFeature],
    languages: &[String],
) -> RegistrationPlan {
    let mut plan = RegistrationPlan {
        registrations: Vec::new(),
        failures: Vec::new(),
    };
    for &feature in features {
        // All languages or none: a half-registered feature would make the
        // batch's `-<lang>` dispatch fail only for some output languages.
        let built: Result<Vec<_>> = languages
            .iter()
            .map(|lang| build_registration(repo_root, channel, feature, lang))
            .collect();
        match built {
            Ok(registrations) => plan.registrations.extend(registrations),
            Err(error) => plan.failures.push(FeatureFailure { feature, error }),
        }
    }
    plan
}

fn build_registration(
    repo_root: &Path,
    channel: Option<&str>,
    feature: GenerationWorkerFeature,
    lang: &str,
) -> Result<GenerationWorkerRegistration> {
    let spec = feature.spec();
    let worker_name = worker_name_for_feature(feature, lang)?;
    let workflow_data = read_non_empty(repo_root.join(spec.workflow_path), "workflow yaml")?;
    let mut workflow_context = serde_json::Map::from_iter([
        ("prompt_source".to_string(), json!("embedded_context")),
        ("prompt_base_url".to_string(), json!("")),
    ]);
    let prompt_dir = repo_root.join(spec.prompt_dir);
    for (context_key, role) in spec.prompts {
        let prompt_path = prompt_dir.join(format!("{role}.{lang}.txt"));
        workflow_context.insert(
            (*context_key).to_string(),
            json!(read_non_empty(prompt_path, "prompt file")?),
        );
    }

    let settings = json!({
        "workflow_data": workflow_data,
        "workflow_context": Value::Object(workflow_context).to_string(),
    });

    let worker_data = WorkerData {
        name: worker_name.clone(),
        description: format!("{worker_name} lang-worker generation workflow"),
        runner_id: None,
        runner_settings: Vec::new(),
        periodic_interval: 0,
        channel: channel.map(str::to_string),
        queue_type: QueueType::Normal as i32,
        response_type: ResponseType::Direct as i32,
        store_success: false,
        store_failure: true,
        use_static: false,
        retry_policy: Some(RetryPolicy {
            r#type: RetryType::Exponential as i32,
            interval: 800,
            max_interval: 60_000,
            max_retry: 0,
            basis: 2.0,
        }),
        broadcast_results: false,
    };
    Ok(GenerationWorkerRegistration {
        feature,
        worker_name,
        worker_data,
        settings,
    })
}

fn read_non_empty(path: PathBuf, label: &str) -> Result<String> {
    let body = std::fs::read_to_string(&path)
        .with_context(|| format!("read {label} {}", path.display()))?;
    if body.trim().is_empty() {
        return Err(anyhow!("{label} {} is empty", path.display()));
    }
    Ok(body)
}

/// Upsert every buildable worker. Features whose assets fail to load, or
/// whose upsert fails, are reported in [`UpsertReport::failures`] instead
/// of aborting the rest. `dry_run` builds the plan without connecting to
/// jobworkerp.
pub(crate) async fn upsert_generation_workers(
    args: UpsertGenerationWorkersArgs,
    dry_run: bool,
) -> Result<UpsertReport> {
    let RegistrationPlan {
        registrations,
        mut failures,
    } = build_generation_worker_registrations(
        &args.repo_root,
        args.channel.as_deref(),
        &args.features,
        &args.languages,
    );
    // Nothing to upsert (dry run, or every feature failed to build): skip the
    // connection; the rendered report still exits non-zero on failures.
    if dry_run || registrations.is_empty() {
        return Ok(UpsertReport {
            registered: registrations.into_iter().map(|r| r.worker_name).collect(),
            failures,
        });
    }

    let client = JobworkerpClientWrapper::new_by_env(Some(args.timeout_sec)).await?;
    let metadata = Arc::new(HashMap::new());
    let mut registered = Vec::with_capacity(registrations.len());
    for registration in registrations {
        // After one language of a feature fails, skip its remaining
        // languages so the feature is reported once.
        if failures.iter().any(|f| f.feature == registration.feature) {
            continue;
        }
        let result = client
            .register_worker(
                None,
                metadata.clone(),
                "WORKFLOW",
                registration.worker_data,
                Some(&registration.settings),
            )
            .await
            .with_context(|| format!("upsert worker {}", registration.worker_name));
        match result {
            Ok(_) => registered.push(registration.worker_name),
            Err(error) => failures.push(FeatureFailure {
                feature: registration.feature,
                error,
            }),
        }
    }
    Ok(UpsertReport {
        registered,
        failures,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn built(plan: RegistrationPlan) -> Vec<GenerationWorkerRegistration> {
        assert!(plan.failures.is_empty(), "{:?}", plan.failures);
        plan.registrations
    }

    #[test]
    fn worker_name_for_feature_uses_language_suffix() {
        assert_eq!(
            worker_name_for_feature(GenerationWorkerFeature::Reflection, "ja").unwrap(),
            "memories-thread-reflection-single-ja"
        );
        assert_eq!(
            worker_name_for_feature(GenerationWorkerFeature::MonthlyWorkSummary, "en").unwrap(),
            "memories-monthly-work-summary-single-en"
        );
        assert_eq!(
            worker_name_for_feature(GenerationWorkerFeature::ThreadPersonality, "ja").unwrap(),
            "memories-thread-personality-single-ja"
        );
        assert_eq!(
            worker_name_for_feature(GenerationWorkerFeature::UserPersonalityMerge, "en").unwrap(),
            "memories-user-personality-merge-en"
        );
    }

    #[test]
    fn worker_name_rejects_unsupported_language() {
        let err = worker_name_for_feature(GenerationWorkerFeature::Reflection, "../en")
            .expect_err("path-like lang must be rejected");
        assert!(err.to_string().contains("unsupported language"));
    }

    #[test]
    fn builds_reflection_registration_with_prompt_context() {
        let regs = built(build_generation_worker_registrations(
            &resolve_repo_root(),
            None,
            &[GenerationWorkerFeature::Reflection],
            &["ja".to_string()],
        ));
        assert_eq!(regs.len(), 1);
        let reg = &regs[0];
        assert_eq!(reg.worker_name, "memories-thread-reflection-single-ja");
        assert_eq!(reg.worker_data.channel, None);
        let context = reg.settings["workflow_context"].as_str().unwrap();
        assert!(context.contains("\"prompt_source\":\"embedded_context\""));
        assert!(context.contains("reflection_system_prompt"));
        assert!(context.contains("reflection_user_tail"));
        assert!(!context.contains("http_raw"));
    }

    #[test]
    fn builds_all_summary_registrations() {
        let regs = built(build_generation_worker_registrations(
            &resolve_repo_root(),
            Some("workflow_lang"),
            &[
                GenerationWorkerFeature::ThreadSummary,
                GenerationWorkerFeature::DailyWorkSummary,
                GenerationWorkerFeature::WeeklyWorkSummary,
                GenerationWorkerFeature::MonthlyWorkSummary,
            ],
            &["en".to_string()],
        ));
        let names: Vec<_> = regs.iter().map(|r| r.worker_name.as_str()).collect();
        assert_eq!(
            names,
            [
                "memories-thread-summary-single-en",
                "memories-daily-work-summary-single-en",
                "memories-weekly-work-summary-single-en",
                "memories-monthly-work-summary-single-en",
            ]
        );
        for reg in regs {
            assert_eq!(reg.worker_data.channel.as_deref(), Some("workflow_lang"));
            let context = reg.settings["workflow_context"].as_str().unwrap();
            assert!(context.contains("_system_prompt"));
            assert!(context.contains("_user_tail"));
            assert!(!context.contains("http_raw"));
        }
    }

    #[test]
    fn builds_thread_group_summary_registration_with_prompt_context() {
        assert_eq!(
            parse_feature_selection("thread-group-summary").unwrap(),
            vec![GenerationWorkerFeature::ThreadGroupSummary]
        );
        assert!(
            parse_feature_selection("all")
                .unwrap()
                .contains(&GenerationWorkerFeature::ThreadGroupSummary)
        );
        let regs = built(build_generation_worker_registrations(
            &resolve_repo_root(),
            None,
            &[GenerationWorkerFeature::ThreadGroupSummary],
            &["ja".to_string(), "en".to_string()],
        ));
        let names: Vec<_> = regs.iter().map(|r| r.worker_name.as_str()).collect();
        assert_eq!(
            names,
            [
                "memories-thread-group-summary-single-ja",
                "memories-thread-group-summary-single-en",
            ]
        );
        for reg in regs {
            let context = reg.settings["workflow_context"].as_str().unwrap();
            assert!(context.contains("thread_group_summary_system_prompt"));
            assert!(context.contains("thread_group_summary_user_tail"));
            assert!(
                reg.settings["workflow_data"]
                    .as_str()
                    .unwrap()
                    .contains("name: thread-group-summary-single")
            );
        }
    }

    #[test]
    fn personality_feature_alias_expands_both_layers() {
        assert_eq!(
            parse_feature_selection("personality").unwrap(),
            vec![
                GenerationWorkerFeature::ThreadPersonality,
                GenerationWorkerFeature::UserPersonalityMerge
            ]
        );
    }

    #[test]
    fn builds_personality_registrations_with_prompt_context() {
        let regs = built(build_generation_worker_registrations(
            &resolve_repo_root(),
            Some("workflow_lang"),
            &[
                GenerationWorkerFeature::ThreadPersonality,
                GenerationWorkerFeature::UserPersonalityMerge,
            ],
            &["ja".to_string(), "en".to_string()],
        ));
        let names: Vec<_> = regs.iter().map(|r| r.worker_name.as_str()).collect();
        assert_eq!(
            names,
            [
                "memories-thread-personality-single-ja",
                "memories-thread-personality-single-en",
                "memories-user-personality-merge-ja",
                "memories-user-personality-merge-en",
            ]
        );
        assert!(
            regs[0].settings["workflow_context"]
                .as_str()
                .unwrap()
                .contains("thread_personality_system_prompt")
        );
        assert!(
            regs[0].settings["workflow_context"]
                .as_str()
                .unwrap()
                .contains("thread_personality_user_tail")
        );
        assert!(
            regs[2].settings["workflow_context"]
                .as_str()
                .unwrap()
                .contains("user_personality_merge_system_prompt")
        );
        assert!(
            regs[2].settings["workflow_context"]
                .as_str()
                .unwrap()
                .contains("user_personality_merge_user_tail")
        );
    }

    fn generation_yaml_files() -> Vec<PathBuf> {
        fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
            for entry in std::fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    walk(&path, out);
                } else if path.extension().is_some_and(|ext| ext == "yaml") {
                    out.push(path);
                }
            }
        }
        let root = resolve_repo_root();
        let mut files = Vec::new();
        walk(&root.join("workers"), &mut files);
        walk(&root.join("workflows"), &mut files);
        for feature in GenerationWorkerFeature::ALL {
            let single = root.join(feature.spec().workflow_path);
            assert!(files.contains(&single), "{} not scanned", single.display());
        }
        files
    }

    /// Generation workflows must run on a bare jobworkerp + memories setup:
    /// they may only call the language workers registered here and must
    /// configure every LLM call inline.
    #[test]
    fn generation_workflows_depend_only_on_registered_language_workers() {
        for path in generation_yaml_files() {
            let body = std::fs::read_to_string(&path).unwrap();
            let name = path.display();
            for line in body.lines().filter(|l| l.contains("workerName:")) {
                assert!(
                    line.contains("memories-") && line.contains("$workflow.input.output_language"),
                    "{name} may only call the registered language workers: {line}"
                );
            }
            if body.contains("using: completion") {
                assert!(body.contains("runnerName: LLM"), "{name}");
                assert!(
                    body.contains("ollama:"),
                    "{name} must configure the LLM inline"
                );
            }
        }
    }

    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.path().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    /// A copy of the shipped `workers/` tree that a test can break
    /// without touching the checkout.
    fn workers_copy() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        copy_tree(
            &resolve_repo_root().join("workers"),
            &dir.path().join("workers"),
        );
        dir
    }

    fn all_languages() -> Vec<String> {
        vec!["ja".to_string(), "en".to_string()]
    }

    fn upsert_args(repo_root: &Path) -> UpsertGenerationWorkersArgs {
        UpsertGenerationWorkersArgs {
            repo_root: repo_root.to_path_buf(),
            channel: Some("llm_workflow".to_string()),
            timeout_sec: 1,
            features: GenerationWorkerFeature::ALL.to_vec(),
            languages: all_languages(),
        }
    }

    #[test]
    fn token_round_trips_parse() {
        for feature in GenerationWorkerFeature::ALL {
            assert_eq!(
                GenerationWorkerFeature::parse(feature.token()).unwrap(),
                feature
            );
        }
    }

    #[test]
    fn skips_feature_with_missing_prompt_dir_and_keeps_others() {
        let root = workers_copy();
        std::fs::remove_dir_all(root.path().join("workers/thread-group-summary/prompts")).unwrap();

        let plan = build_generation_worker_registrations(
            root.path(),
            None,
            &GenerationWorkerFeature::ALL,
            &all_languages(),
        );

        assert_eq!(plan.failures.len(), 1);
        assert_eq!(
            plan.failures[0].feature,
            GenerationWorkerFeature::ThreadGroupSummary
        );
        let message = format!("{:#}", plan.failures[0].error);
        assert!(
            message.contains("thread-group-summary/prompts/system_prompt.ja.txt"),
            "{message}"
        );
        assert_eq!(plan.registrations.len(), 14);
        assert!(plan.registrations.iter().all(|r| {
            !r.worker_name
                .starts_with("memories-thread-group-summary-single")
        }));
    }

    #[test]
    fn feature_with_one_missing_language_is_skipped_entirely() {
        let root = workers_copy();
        std::fs::remove_file(
            root.path()
                .join("workers/thread-summary/prompts/system_prompt.en.txt"),
        )
        .unwrap();

        let plan = build_generation_worker_registrations(
            root.path(),
            None,
            &[
                GenerationWorkerFeature::ThreadSummary,
                GenerationWorkerFeature::Reflection,
            ],
            &all_languages(),
        );

        assert_eq!(plan.failures.len(), 1);
        assert_eq!(
            plan.failures[0].feature,
            GenerationWorkerFeature::ThreadSummary
        );
        let names: Vec<_> = plan
            .registrations
            .iter()
            .map(|r| r.worker_name.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "memories-thread-reflection-single-ja",
                "memories-thread-reflection-single-en",
            ]
        );
    }

    #[tokio::test]
    async fn dry_run_plans_every_worker_without_connecting() {
        let report = upsert_generation_workers(upsert_args(&resolve_repo_root()), true)
            .await
            .unwrap();
        assert!(report.failures.is_empty());
        assert_eq!(
            report.registered.len(),
            GenerationWorkerFeature::ALL.len() * 2
        );
        assert!(
            report
                .registered
                .contains(&"memories-thread-group-summary-single-en".to_string())
        );
    }

    #[tokio::test]
    async fn dry_run_reports_skipped_features_alongside_planned_workers() {
        let root = workers_copy();
        std::fs::remove_dir_all(root.path().join("workers/thread-group-summary")).unwrap();

        let report = upsert_generation_workers(upsert_args(root.path()), true)
            .await
            .unwrap();

        assert_eq!(report.registered.len(), 14);
        assert_eq!(report.failures.len(), 1);
        let output = report.render(true);
        assert_eq!(output.stdout.len(), 14);
        assert!(
            output
                .stdout
                .iter()
                .all(|line| line.starts_with("[dry-run] would upsert generation worker: "))
        );
        assert_eq!(output.stderr.len(), 1);
        assert!(
            output.stderr[0]
                .starts_with("warning: skipped generation feature thread-group-summary: "),
            "{}",
            output.stderr[0]
        );
        assert!(!output.stderr[0].contains('\n'));
        assert_eq!(
            output.failure.as_deref(),
            Some("1 generation feature(s) failed: thread-group-summary; 14 worker(s) registered")
        );
    }

    #[test]
    fn successful_report_renders_without_failure() {
        let report = UpsertReport {
            registered: vec!["memories-thread-summary-single-ja".to_string()],
            failures: Vec::new(),
        };
        let output = report.render(false);
        assert_eq!(
            output.stdout,
            ["upserted generation worker: memories-thread-summary-single-ja"]
        );
        assert!(output.stderr.is_empty());
        assert!(output.failure.is_none());
    }

    #[tokio::test]
    async fn all_features_failing_is_an_error() {
        let root = tempfile::tempdir().unwrap();
        // Not a dry run: with nothing buildable it must fail without trying
        // to connect to jobworkerp.
        let report = upsert_generation_workers(upsert_args(root.path()), false)
            .await
            .expect("an all-failed plan is reported, not raised");
        assert!(report.registered.is_empty());
        let failure = report
            .render(false)
            .failure
            .expect("every feature failed, so the command must exit non-zero");
        assert!(
            failure.starts_with(&format!(
                "{}{FAILURE_SUMMARY_MARKER}",
                GenerationWorkerFeature::ALL.len()
            )),
            "{failure}"
        );
        assert!(failure.contains("thread-summary"), "{failure}");
        assert!(failure.ends_with("; 0 worker(s) registered"), "{failure}");
    }

    #[test]
    fn resolve_repo_root_prefers_env_override() {
        // Sets/removes a process-global env var; the var is removed before
        // returning so sibling tests that call `resolve_repo_root()` keep
        // seeing the crate-relative default.
        unsafe {
            std::env::set_var("MEMORY_REPO_ROOT", "/opt/memories");
        }
        let resolved = resolve_repo_root();
        unsafe {
            std::env::remove_var("MEMORY_REPO_ROOT");
        }
        assert_eq!(resolved, PathBuf::from("/opt/memories"));
    }

    #[test]
    fn resolve_repo_root_ignores_blank_env_and_falls_back() {
        unsafe {
            std::env::set_var("MEMORY_REPO_ROOT", "   ");
        }
        let resolved = resolve_repo_root();
        unsafe {
            std::env::remove_var("MEMORY_REPO_ROOT");
        }
        // Blank value is ignored so local development keeps using this crate.
        assert_eq!(resolved, PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    }
}
