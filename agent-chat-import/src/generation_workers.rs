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
    ThreadPersonality,
    UserPersonalityMerge,
    DailyWorkSummary,
    WeeklyWorkSummary,
    MonthlyWorkSummary,
}

impl GenerationWorkerFeature {
    const ALL: [Self; 7] = [
        Self::Reflection,
        Self::ThreadSummary,
        Self::ThreadPersonality,
        Self::UserPersonalityMerge,
        Self::DailyWorkSummary,
        Self::WeeklyWorkSummary,
        Self::MonthlyWorkSummary,
    ];

    fn parse(raw: &str) -> Result<Self> {
        match raw.trim() {
            "reflection" => Ok(Self::Reflection),
            "thread-summary" => Ok(Self::ThreadSummary),
            "thread-personality" => Ok(Self::ThreadPersonality),
            "user-personality-merge" => Ok(Self::UserPersonalityMerge),
            "daily-work-summary" => Ok(Self::DailyWorkSummary),
            "weekly-work-summary" => Ok(Self::WeeklyWorkSummary),
            "monthly-work-summary" => Ok(Self::MonthlyWorkSummary),
            other => Err(anyhow!(
                "unsupported generation worker feature `{other}`; supported: reflection, thread-summary, thread-personality, user-personality-merge, daily-work-summary, weekly-work-summary, monthly-work-summary"
            )),
        }
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
    worker_name: String,
    worker_data: WorkerData,
    settings: Value,
}

#[derive(Debug, Clone)]
pub(crate) struct UpsertGenerationWorkersArgs {
    pub repo_root: PathBuf,
    pub channel: String,
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
    channel: &str,
    features: &[GenerationWorkerFeature],
    languages: &[String],
) -> Result<Vec<GenerationWorkerRegistration>> {
    let mut registrations = Vec::new();
    for feature in features {
        for lang in languages {
            registrations.push(build_registration(repo_root, channel, *feature, lang)?);
        }
    }
    Ok(registrations)
}

fn build_registration(
    repo_root: &Path,
    channel: &str,
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
        channel: Some(channel.to_string()),
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

pub(crate) async fn upsert_generation_workers(
    args: UpsertGenerationWorkersArgs,
) -> Result<Vec<String>> {
    let registrations = build_generation_worker_registrations(
        &args.repo_root,
        &args.channel,
        &args.features,
        &args.languages,
    )?;
    let client = JobworkerpClientWrapper::new_by_env(Some(args.timeout_sec)).await?;
    let metadata = Arc::new(HashMap::new());
    let mut registered = Vec::with_capacity(registrations.len());
    for registration in registrations {
        client
            .register_worker(
                None,
                metadata.clone(),
                "WORKFLOW",
                registration.worker_data,
                Some(&registration.settings),
            )
            .await
            .with_context(|| format!("upsert worker {}", registration.worker_name))?;
        registered.push(registration.worker_name);
    }
    Ok(registered)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let regs = build_generation_worker_registrations(
            &resolve_repo_root(),
            "workflow_lang",
            &[GenerationWorkerFeature::Reflection],
            &["ja".to_string()],
        )
        .unwrap();
        assert_eq!(regs.len(), 1);
        let reg = &regs[0];
        assert_eq!(reg.worker_name, "memories-thread-reflection-single-ja");
        assert_eq!(reg.worker_data.channel.as_deref(), Some("workflow_lang"));
        let context = reg.settings["workflow_context"].as_str().unwrap();
        assert!(context.contains("\"prompt_source\":\"embedded_context\""));
        assert!(context.contains("reflection_system_prompt"));
        assert!(context.contains("reflection_user_tail"));
        assert!(!context.contains("http_raw"));
    }

    #[test]
    fn builds_all_summary_registrations() {
        let regs = build_generation_worker_registrations(
            &resolve_repo_root(),
            "workflow_lang",
            &[
                GenerationWorkerFeature::ThreadSummary,
                GenerationWorkerFeature::DailyWorkSummary,
                GenerationWorkerFeature::WeeklyWorkSummary,
                GenerationWorkerFeature::MonthlyWorkSummary,
            ],
            &["en".to_string()],
        )
        .unwrap();
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
            let context = reg.settings["workflow_context"].as_str().unwrap();
            assert!(context.contains("_system_prompt"));
            assert!(context.contains("_user_tail"));
            assert!(!context.contains("http_raw"));
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
        let regs = build_generation_worker_registrations(
            &resolve_repo_root(),
            "workflow_lang",
            &[
                GenerationWorkerFeature::ThreadPersonality,
                GenerationWorkerFeature::UserPersonalityMerge,
            ],
            &["ja".to_string(), "en".to_string()],
        )
        .unwrap();
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
