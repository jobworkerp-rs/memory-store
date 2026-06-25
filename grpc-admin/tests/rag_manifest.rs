//! Static structural validation of the RAG manifest and its referenced
//! workflow YAMLs. Live registration against jobworkerp is left for
//! `--ignored` E2E runs (see `docs/rag-tools-spec.md`).

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

fn workflows_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crate dir has a parent")
        .join("workflows")
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// Parse the manifest once per test process; every test reads the same
/// bundled file so caching avoids 4x the file I/O without altering
/// semantics (the YAML is static for the duration of the run).
fn manifest() -> &'static serde_yaml::Value {
    static CACHE: OnceLock<serde_yaml::Value> = OnceLock::new();
    CACHE.get_or_init(|| {
        // %{VAR} placeholders sit at YAML positions where the raw `%`
        // is not a legal YAML token, so `expand_env` MUST run before
        // serde_yaml::from_str.
        // SAFETY: this closure runs at most once per process via
        // OnceLock::get_or_init, so concurrent set_var from other tests
        // is impossible by construction. Set required vars only when
        // missing so a real env (e.g. dot.env loaded earlier) isn't
        // overwritten.
        unsafe {
            for (k, v) in [
                ("MEMORY_GRPC_HOST", "test-host"),
                ("MEMORY_GRPC_PORT", "12345"),
            ] {
                if std::env::var_os(k).is_none() {
                    std::env::set_var(k, v);
                }
            }
        }
        let path = workflows_dir().join("rag-tools-manifest.yaml");
        let raw = read(&path);
        let expanded = jobworkerp_client::client::yaml_common::expand_env(&raw)
            .expect("manifest YAML env expansion must succeed");
        serde_yaml::from_str(&expanded).expect("manifest YAML must parse")
    })
}

fn cached_workflow(rel: &'static str, cache: &'static OnceLock<String>) -> &'static String {
    cache.get_or_init(|| read(&workflows_dir().join(rel)))
}

fn recall_memories_workflow() -> &'static String {
    static CACHE: OnceLock<String> = OnceLock::new();
    cached_workflow("rag/search-memories.yaml", &CACHE)
}

fn find_conversations_workflow() -> &'static String {
    static CACHE: OnceLock<String> = OnceLock::new();
    cached_workflow("rag/search-threads.yaml", &CACHE)
}

fn expand_memory_context_workflow() -> &'static String {
    static CACHE: OnceLock<String> = OnceLock::new();
    cached_workflow("rag/get-surrounding-memories.yaml", &CACHE)
}

fn worker_entries() -> &'static Vec<serde_yaml::Value> {
    manifest()
        .get("workers")
        .and_then(|w| w.get("entries"))
        .and_then(|e| e.as_sequence())
        .expect("workers.entries must be a sequence")
}

#[test]
fn manifest_has_expected_workers() {
    let entries = worker_entries();

    let names: Vec<&str> = entries
        .iter()
        .filter_map(|e| e.get("name").and_then(|n| n.as_str()))
        .collect();
    assert_eq!(
        names,
        [
            "recall_memories",
            "find_conversations",
            "expand_memory_context"
        ],
        "RAG worker name set / order changed — update the function set targets too"
    );

    // 40-char floor: roughly one short English sentence. Long enough to
    // catch a worker registered with an empty / placeholder description
    // (which yields an unusable LLM tool spec) without policing prose
    // style. If a real description ever needs to be shorter, lower this
    // and add a justifying comment.
    const MIN_DESCRIPTION_LEN: usize = 40;
    for entry in entries {
        let name = entry.get("name").and_then(|n| n.as_str()).unwrap();
        let description = entry
            .get("description")
            .and_then(|d| d.as_str())
            .unwrap_or_else(|| panic!("worker {name} missing description"));
        assert!(
            description.trim().len() >= MIN_DESCRIPTION_LEN,
            "worker {name} description too short to brief an LLM: {description:?}"
        );
    }
}

#[test]
fn manifest_workflow_workers_reference_existing_yaml_files() {
    let dir = workflows_dir();
    for entry in worker_entries() {
        let runner = entry.get("runner").and_then(|r| r.as_str()).unwrap();
        if runner != "WORKFLOW" {
            continue;
        }
        let name = entry.get("name").and_then(|n| n.as_str()).unwrap();
        let file = entry
            .get("settings")
            .and_then(|s| s.get("workflow_data"))
            .and_then(|w| w.get("$file"))
            .and_then(|f| f.as_str())
            .unwrap_or_else(|| panic!("WORKFLOW worker {name} missing $file include"));
        assert!(
            dir.join(file).is_file(),
            "WORKFLOW worker {name} references missing file {file}"
        );
    }
}

/// All RAG workers must be WORKFLOWs. Direct GRPC workers expose
/// `GrpcArgs` (method / json_body / metadata) to the LLM, which would
/// silently swallow structured fields like memory_id and fire
/// zero-valued requests. The wrapping WORKFLOWs translate clean input
/// schemas into json_body server-side.
#[test]
fn manifest_workers_are_all_workflows() {
    for entry in worker_entries() {
        let name = entry.get("name").and_then(|n| n.as_str()).unwrap();
        let runner = entry.get("runner").and_then(|r| r.as_str()).unwrap();
        assert_eq!(
            runner, "WORKFLOW",
            "RAG worker {name} must be WORKFLOW, got {runner}"
        );
    }
}

#[test]
fn manifest_function_set_targets_match_workers() {
    let function_sets = manifest()
        .get("function_sets")
        .and_then(|f| f.get("entries"))
        .and_then(|e| e.as_sequence())
        .expect("function_sets.entries must be a sequence");

    assert_eq!(function_sets.len(), 1, "expected exactly one function set");
    let fs = &function_sets[0];
    assert_eq!(
        fs.get("name").and_then(|n| n.as_str()),
        Some("memory-recall")
    );

    let targets = fs
        .get("targets")
        .and_then(|t| t.as_sequence())
        .expect("targets must be a sequence");
    let target_names: Vec<&str> = targets
        .iter()
        .filter_map(|t| t.get("name").and_then(|n| n.as_str()))
        .collect();
    assert_eq!(
        target_names,
        [
            "recall_memories",
            "find_conversations",
            "expand_memory_context"
        ]
    );

    // RUNNER targets bypass the per-worker description we crafted, so an
    // accidental swap would silently degrade LLM tool quality.
    for target in targets {
        let ty = target.get("type").and_then(|t| t.as_str()).unwrap();
        assert_eq!(
            ty, "WORKER",
            "RAG function set targets must all be WORKER targets, got {ty}"
        );
    }
}

#[test]
fn recall_memories_workflow_yaml_has_required_pieces() {
    let yaml = recall_memories_workflow();

    assert!(yaml.contains(r#"dsl: "1.0.0-jobworkerp""#));
    let placeholder = infra::infra::embedding_dispatch::MM_EMBEDDING_WORKER_PLACEHOLDER;
    assert!(
        yaml.contains(placeholder) && yaml.contains("using: embed_text"),
        "must reference the shared mm-embedding worker (env placeholder) via embed_text"
    );
    assert!(
        yaml.contains("MemoryVectorService/HybridSearch"),
        "must call MemoryVectorService HybridSearch"
    );
    assert!(
        yaml.contains("using: streaming"),
        "HybridSearch is server-streaming; the GRPC runner must be invoked with using: streaming"
    );
    assert!(
        yaml.contains("user_id: $workflow.input.user_id"),
        "user_id must be injected into options.filter from workflow input"
    );
    // The auto-embedding workflow ran into DSL §Runtime Expressions
    // corner cases when json_body was assembled by string interpolation;
    // the project rule is to build the JSON in jq and serialize once
    // with `tojson`. See workflows/auto-embedding.yaml for the precedent.
    assert!(
        yaml.contains("| tojson"),
        "json_body must be built with a single jq expression and serialized via tojson"
    );
}

#[test]
fn find_conversations_workflow_yaml_has_required_pieces() {
    let yaml = find_conversations_workflow();

    assert!(yaml.contains(r#"dsl: "1.0.0-jobworkerp""#));
    let placeholder = infra::infra::embedding_dispatch::MM_EMBEDDING_WORKER_PLACEHOLDER;
    assert!(yaml.contains(placeholder) && yaml.contains("using: embed_text"));
    assert!(yaml.contains("ThreadVectorService/HybridSearch"));
    assert!(yaml.contains("using: streaming"));
    assert!(yaml.contains("user_id: $workflow.input.user_id"));
    assert!(yaml.contains("| tojson"));
}

#[test]
fn expand_memory_context_workflow_yaml_has_required_pieces() {
    let yaml = expand_memory_context_workflow();

    assert!(yaml.contains(r#"dsl: "1.0.0-jobworkerp""#));
    assert!(
        yaml.contains("MemoryVectorService/GetSurroundingMemories"),
        "must call GetSurroundingMemories"
    );
    assert!(
        yaml.contains("using: unary"),
        "GetSurroundingMemories is unary, not server-streaming"
    );
    assert!(
        yaml.contains("memory_id: $workflow.input.memory_id"),
        "memory_id must be sourced from workflow input"
    );
    assert!(
        yaml.contains("thread_id: $workflow.input.thread_id"),
        "thread_id must be sourced from workflow input"
    );
    assert!(yaml.contains("| tojson"));
}
