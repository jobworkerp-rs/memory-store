//! Every workflow document this repository ships must be a valid
//! jobworkerp workflow: the WORKFLOW runner validates it and deserializes
//! it into its workflow types, so a document that only parses as YAML
//! (e.g. `try: { do: [...] }` where a task list is required) fails to load
//! at runtime. Checked here against a copy of the schema the
//! runner validates with (`workflow_minimal_fix.json`, not the
//! code-generation `workflow.yaml`, which rejects valid documents):
//! the workflows as memories registers them (worker YAMLs rendered with
//! their `$file` includes), and every standalone workflow file.

use crate::infra::embedding_dispatch::ImageSearchMode;
use crate::infra::embedding_space::SpaceId;
use crate::infra::embedding_space::plan::{EnabledWorkers, registration_overrides};
use crate::infra::embedding_space::registration::render_yaml;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

const SCHEMA_COPY: &str = "testdata/jobworkerp-workflow-schema.json";

/// The validator the WORKFLOW runner builds when it loads a workflow
/// (draft 2020-12 over its validation schema).
fn schema_validator() -> jsonschema::Validator {
    let raw =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(SCHEMA_COPY)).unwrap();
    let schema: serde_json::Value = serde_json::from_str(&raw).unwrap();
    jsonschema::draft202012::new(&schema).expect("the jobworkerp workflow schema compiles")
}

/// Placeholder values as registration supplies them (callback endpoint
/// included, which registration takes from the environment).
fn overrides() -> HashMap<String, String> {
    let mut o = registration_overrides(Some(&SpaceId("ab".repeat(32))));
    o.insert("MEMORY_GRPC_HOST".into(), "127.0.0.1".into());
    o.insert("MEMORY_GRPC_PORT".into(), "9010".into());
    o
}

/// Schema violations of one workflow document (YAML text), as messages.
fn violations(validator: &jsonschema::Validator, name: &str, yaml: &str) -> Vec<String> {
    let doc: serde_yaml::Value = match serde_yaml::from_str(yaml) {
        Ok(doc) => doc,
        Err(e) => return vec![format!("{name}: not YAML: {e}")],
    };
    let json = match serde_json::to_value(doc) {
        Ok(json) => json,
        Err(e) => return vec![format!("{name}: not JSON-compatible: {e}")],
    };
    validator
        .iter_errors(&json)
        .map(|e| {
            // The message embeds the whole failing instance; keep its head.
            let message: String = e.to_string().chars().take(200).collect();
            format!("{name}: at {}: {message}", e.instance_path())
        })
        .collect()
}

/// `workflow_data` values (the inlined workflow text) of WORKFLOW workers.
fn workflow_data(value: &serde_yaml::Value, out: &mut Vec<String>) {
    match value {
        serde_yaml::Value::Mapping(map) => {
            for (k, v) in map {
                if k.as_str() == Some("workflow_data") {
                    if let Some(text) = v.as_str() {
                        out.push(text.to_string());
                    } else {
                        out.push(serde_yaml::to_string(v).unwrap());
                    }
                } else {
                    workflow_data(v, out);
                }
            }
        }
        serde_yaml::Value::Sequence(items) => items.iter().for_each(|v| workflow_data(v, out)),
        _ => {}
    }
}

#[tokio::test]
async fn registered_workflows_conform_to_the_jobworkerp_schema() {
    let validator = schema_validator();
    let overrides = overrides();
    let plan = EnabledWorkers {
        auto_embedding: true,
        image_search_mode: ImageSearchMode::Multimodal,
        reflection_dispatch: true,
        rag_tools: true,
    }
    .plan();
    let mut errors = Vec::new();
    let mut checked = 0;
    for path in plan.worker_yamls.iter().chain(plan.manifests.iter()) {
        let (rendered, _) = render_yaml(path, &overrides).await.unwrap();
        let doc: serde_yaml::Value = serde_yaml::from_str(&rendered).unwrap();
        let mut workflows = Vec::new();
        workflow_data(&doc, &mut workflows);
        for (i, wf) in workflows.iter().enumerate() {
            checked += 1;
            errors.extend(violations(
                &validator,
                &format!("{} workflow #{i}", path.display()),
                wf,
            ));
        }
    }
    assert!(checked >= 8, "only {checked} registered workflows found");
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// Workflow files of this repository: every `*.yaml` whose top level is
/// a workflow `document`.
fn workflow_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().and_then(|e| e.to_str()) == Some("yaml")
                && std::fs::read_to_string(&path)
                    .is_ok_and(|raw| raw.lines().any(|l| l.starts_with("document:")))
            {
                out.push(path);
            }
        }
    }
    let root = repo_root();
    let mut out = Vec::new();
    for dir in [
        "workflows",
        "agent-chat-import/workflows",
        "agent-chat-import/workers",
    ] {
        walk(&root.join(dir), &mut out);
    }
    out.sort();
    out
}

#[test]
fn workflow_files_conform_to_the_jobworkerp_schema() {
    let validator = schema_validator();
    let overrides = overrides();
    let files = workflow_files();
    assert!(
        files.len() >= 20,
        "only {} workflow files found",
        files.len()
    );
    let mut errors = Vec::new();
    for path in &files {
        let raw = std::fs::read_to_string(path).unwrap();
        match jobworkerp_client::client::yaml_common::expand_env_with_overrides(&raw, &overrides) {
            Ok(expanded) => {
                errors.extend(violations(
                    &validator,
                    &path.display().to_string(),
                    &expanded,
                ));
            }
            Err(e) => errors.push(format!("{}: placeholder expansion: {e:#}", path.display())),
        }
    }
    assert!(errors.is_empty(), "{}", errors.join("\n"));
}

/// The copy must follow jobworkerp's validation schema; checked when the
/// jobworkerp checkout that hosts this repository is present.
#[test]
fn schema_copy_matches_the_jobworkerp_checkout() {
    let upstream = repo_root().join("../runner/schema/workflow_minimal_fix.json");
    let Ok(upstream) = std::fs::read_to_string(&upstream) else {
        return;
    };
    let copy =
        std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(SCHEMA_COPY)).unwrap();
    assert!(
        copy == upstream,
        "infra/{SCHEMA_COPY} is out of date with runner/schema/workflow_minimal_fix.json"
    );
}
