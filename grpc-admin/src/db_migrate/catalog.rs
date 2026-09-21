use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const THREAD_MESSAGE_TIMES_V1_ID: &str = "thread-message-times-v1";
pub const THREAD_MESSAGE_TIMES_V1_GENERATION: u32 = 1;
pub const THREAD_MESSAGE_TIMES_V1_IDENTITY: &str = "thread-message-times-v1@1";

pub const THREAD_GROUPS_CANONICAL_KEYS_V1_ID: &str = "thread-groups-canonical-keys-v1";
pub const THREAD_GROUPS_CANONICAL_KEYS_V1_GENERATION: u32 = 1;
pub const THREAD_GROUPS_CANONICAL_KEYS_V1_IDENTITY: &str = "thread-groups-canonical-keys-v1@1";

const CATALOG_JSON: &str = include_str!("../../../infra/atlas/post-migration-tasks.json");
const HISTORY_JSON: &str = include_str!("../../../infra/atlas/post-migration-task-history.json");

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TaskCatalog {
    pub tasks: Vec<TaskCatalogEntry>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct TaskCatalogEntry {
    pub id: String,
    pub generation: u32,
    pub description: String,
    pub canonical_definition: serde_json::Value,
    pub canonical_definition_digest: String,
    pub backends: Vec<String>,
    pub introduced_by_schema_version: String,
    pub completion_required_by_schema_version: Option<String>,
    pub dependencies: Vec<String>,
    pub maintenance_window_required: bool,
    pub implementation: String,
    pub lifecycle: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct TaskHistory {
    entries: Vec<TaskHistoryEntry>,
    lifecycle_transitions: Vec<TaskHistoryTransition>,
    previous_history_digest: Option<String>,
    history_digest: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct TaskHistoryEntry {
    identity: String,
    canonical_definition_digest: String,
}

/// One append-only ledger event. A `registered` transition adds exactly one
/// catalog entry; a `retired` transition only changes the entry's lifecycle.
/// `previous_history_digest` anchors the transition to the ledger state it
/// was appended to, so every prior ledger state can be re-derived and
/// re-hashed from the file alone (rewrites, removals, and non-prefix
/// updates therefore fail the digest chain).
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct TaskHistoryTransition {
    identity: String,
    canonical_definition_digest: String,
    kind: String,
    previous_history_digest: String,
}

impl TaskCatalogEntry {
    pub fn identity(&self) -> String {
        format!("{}@{}", self.id, self.generation)
    }

    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.generation == 0 {
            bail!("task catalog identity must contain a non-empty id and positive generation");
        }
        if self.backends.is_empty()
            || self
                .backends
                .iter()
                .any(|backend| backend != "sqlite" && backend != "postgres")
        {
            bail!("task {} has unsupported or empty backends", self.identity());
        }
        if self.lifecycle != "active" {
            bail!("task {} is not active", self.identity());
        }
        if !super::has_registered_implementation(&self.implementation) {
            bail!("task {} has an unknown implementation", self.identity());
        }
        if self.completion_required_by_schema_version.is_none() {
            bail!("task {} must not be optional", self.identity());
        }
        validate_schema_version(&self.introduced_by_schema_version)?;
        validate_schema_version(
            self.completion_required_by_schema_version
                .as_deref()
                .expect("checked above"),
        )?;
        let actual = canonical_definition_digest(&self.canonical_definition)?;
        if actual != self.canonical_definition_digest {
            bail!(
                "task {} canonical definition digest mismatch",
                self.identity()
            );
        }
        Ok(())
    }
}

pub fn load_catalog() -> Result<TaskCatalog> {
    let catalog: TaskCatalog =
        serde_json::from_str(CATALOG_JSON).context("parsing post-migration task catalog")?;
    if catalog.tasks.is_empty() {
        bail!("post-migration task catalog must not be empty");
    }
    for task in &catalog.tasks {
        task.validate()?;
    }
    validate_dependencies(&catalog)?;
    validate_history(&catalog)?;
    Ok(catalog)
}

/// Reject an entry unless it is byte-for-byte equivalent (after JSON parsing)
/// to the immutable entry compiled into this release.
///
/// Validating a caller-supplied entry alone is insufficient: a forged entry
/// can have a self-consistent digest while changing its backend, dependency,
/// or implementation contract.
pub fn validate_fixed_catalog_entry(entry: &TaskCatalogEntry) -> Result<()> {
    entry.validate()?;
    let fixed = load_catalog()?
        .tasks
        .into_iter()
        .find(|candidate| candidate.id == entry.id && candidate.generation == entry.generation)
        .with_context(|| {
            format!(
                "task {} is not registered in the fixed catalog",
                entry.identity()
            )
        })?;
    if *entry != fixed {
        bail!(
            "task {} differs from its fixed catalog definition",
            entry.identity()
        );
    }
    Ok(())
}

/// Select tasks available for a schema/backend pair, expanding their
/// dependency closure in deterministic topological order.
pub fn select_tasks_for_schema_version(
    catalog: &TaskCatalog,
    schema_version: &str,
    backend: &str,
) -> Result<Vec<TaskCatalogEntry>> {
    validate_schema_version(schema_version)?;
    if backend != "sqlite" && backend != "postgres" {
        return Ok(Vec::new());
    }
    for task in &catalog.tasks {
        task.validate()?;
    }
    validate_dependencies(catalog)?;

    let all = catalog
        .tasks
        .iter()
        .map(|task| (task.identity(), task))
        .collect::<BTreeMap<_, _>>();
    let selected = catalog
        .tasks
        .iter()
        .filter(|task| {
            task.backends.iter().any(|candidate| candidate == backend)
                && task.introduced_by_schema_version.as_str() <= schema_version
        })
        .map(TaskCatalogEntry::identity)
        .collect::<BTreeSet<_>>();
    let mut ordered = Vec::with_capacity(selected.len());
    let mut visiting = BTreeSet::new();
    let mut completed = BTreeSet::new();
    for identity in &selected {
        visit_selected_task(
            identity,
            &all,
            &selected,
            &mut visiting,
            &mut completed,
            &mut ordered,
        )?;
    }
    Ok(ordered)
}

fn validate_dependencies(catalog: &TaskCatalog) -> Result<()> {
    let identities = catalog
        .tasks
        .iter()
        .map(TaskCatalogEntry::identity)
        .collect::<BTreeSet<_>>();
    if identities.len() != catalog.tasks.len() {
        bail!("task catalog contains duplicate task identities");
    }
    let all = catalog
        .tasks
        .iter()
        .map(|task| (task.identity(), task))
        .collect::<BTreeMap<_, _>>();
    let mut visiting = BTreeSet::new();
    let mut completed = BTreeSet::new();
    let mut ignored_order = Vec::new();
    for identity in &identities {
        visit_selected_task(
            identity,
            &all,
            &identities,
            &mut visiting,
            &mut completed,
            &mut ignored_order,
        )?;
    }
    Ok(())
}

fn visit_selected_task(
    identity: &str,
    all: &BTreeMap<String, &TaskCatalogEntry>,
    selected: &BTreeSet<String>,
    visiting: &mut BTreeSet<String>,
    completed: &mut BTreeSet<String>,
    ordered: &mut Vec<TaskCatalogEntry>,
) -> Result<()> {
    if completed.contains(identity) {
        return Ok(());
    }
    if !visiting.insert(identity.to_string()) {
        bail!("task catalog contains a cyclic dependency at {identity}");
    }
    let task = all
        .get(identity)
        .context("task catalog dependency does not exist")?;
    let mut dependencies = task.dependencies.clone();
    dependencies.sort_unstable();
    dependencies.dedup();
    if dependencies.len() != task.dependencies.len() {
        bail!(
            "task {} declares a dependency more than once",
            task.identity()
        );
    }
    for dependency in dependencies {
        if !all.contains_key(&dependency) {
            bail!(
                "task {} depends on missing task {dependency}",
                task.identity()
            );
        }
        if !selected.contains(&dependency) {
            bail!(
                "task {} depends on task {dependency} that is unavailable for this schema/backend",
                task.identity()
            );
        }
        visit_selected_task(&dependency, all, selected, visiting, completed, ordered)?;
    }
    visiting.remove(identity);
    completed.insert(identity.to_string());
    ordered.push((*task).clone());
    Ok(())
}

fn validate_history(catalog: &TaskCatalog) -> Result<()> {
    let history: TaskHistory =
        serde_json::from_str(HISTORY_JSON).context("parsing post-migration task history")?;
    let canonical = serde_json::json!({
        "entries": history.entries.clone(),
        "lifecycle_transitions": history.lifecycle_transitions.clone(),
        "previous_history_digest": history.previous_history_digest.clone(),
    });
    if canonical_definition_digest(&canonical)? != history.history_digest {
        bail!("post-migration task history digest mismatch");
    }
    for task in &catalog.tasks {
        let found = history.entries.iter().any(|entry| {
            entry.identity == task.identity()
                && entry.canonical_definition_digest == task.canonical_definition_digest
        });
        if !found {
            bail!(
                "task {} is absent from immutable task history",
                task.identity()
            );
        }
    }
    validate_history_chain(&history)?;
    Ok(())
}

/// Re-derive every prior ledger state from the file and verify its digest
/// against the chain anchor of the transition that consumed it. This keeps
/// the ledger append-only: rewriting an entry, dropping an entry, or
/// injecting a non-prefix update breaks one of the re-derived digests.
fn validate_history_chain(history: &TaskHistory) -> Result<()> {
    if history.lifecycle_transitions.is_empty() {
        if history.previous_history_digest.is_some() {
            bail!("initial task history must not declare a previous digest");
        }
        return Ok(());
    }
    // The current state's link must be the one its last transition consumed.
    let last_transition = history.lifecycle_transitions.last().expect("non-empty");
    if history.previous_history_digest.as_deref()
        != Some(last_transition.previous_history_digest.as_str())
    {
        bail!("task history previous digest does not match the last lifecycle transition");
    }
    let mut entries = history.entries.clone();
    let mut transitions = history.lifecycle_transitions.clone();
    while let Some(transition) = transitions.pop() {
        if transition.kind == "registered" {
            let registered = entries.pop().with_context(|| {
                format!(
                    "task history transition for {} has no matching entry",
                    transition.identity
                )
            })?;
            if registered.identity != transition.identity
                || registered.canonical_definition_digest != transition.canonical_definition_digest
            {
                bail!(
                    "task history transition for {} does not match its registered entry",
                    transition.identity
                );
            }
        } else if transition.kind != "retired" {
            bail!("unknown task history transition kind: {}", transition.kind);
        }
        let predecessor = serde_json::json!({
            "entries": entries,
            "lifecycle_transitions": transitions,
            "previous_history_digest":
                transitions.last().map(|t| t.previous_history_digest.clone()),
        });
        if canonical_definition_digest(&predecessor)? != transition.previous_history_digest {
            bail!(
                "task history chain digest mismatch at the transition for {}",
                transition.identity
            );
        }
    }
    Ok(())
}

pub fn thread_groups_canonical_keys_v1() -> Result<TaskCatalogEntry> {
    let task = load_catalog()?
        .tasks
        .into_iter()
        .find(|task| {
            task.id == THREAD_GROUPS_CANONICAL_KEYS_V1_ID
                && task.generation == THREAD_GROUPS_CANONICAL_KEYS_V1_GENERATION
        })
        .context("thread-groups-canonical-keys-v1@1 is missing from task catalog")?;
    if task.identity() != THREAD_GROUPS_CANONICAL_KEYS_V1_IDENTITY {
        bail!("thread-groups-canonical-keys-v1 catalog identity is invalid");
    }
    Ok(task)
}

pub fn thread_message_times_v1() -> Result<TaskCatalogEntry> {
    let task = load_catalog()?
        .tasks
        .into_iter()
        .find(|task| {
            task.id == THREAD_MESSAGE_TIMES_V1_ID
                && task.generation == THREAD_MESSAGE_TIMES_V1_GENERATION
        })
        .context("thread-message-times-v1@1 is missing from task catalog")?;
    if task.identity() != THREAD_MESSAGE_TIMES_V1_IDENTITY {
        bail!("thread-message-times-v1 catalog identity is invalid");
    }
    Ok(task)
}

pub fn canonical_definition_digest(definition: &serde_json::Value) -> Result<String> {
    let normalized = canonical_json(definition)?;
    let digest = Sha256::digest(normalized.as_bytes());
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn canonical_json(value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {
            serde_json::to_string(value).context("serializing scalar in canonical task definition")
        }
        serde_json::Value::Array(values) => values
            .iter()
            .map(canonical_json)
            .collect::<Result<Vec<_>>>()
            .map(|values| format!("[{}]", values.join(","))),
        serde_json::Value::Object(values) => {
            let mut keys: Vec<&String> = values.keys().collect();
            keys.sort_unstable();
            keys.into_iter()
                .map(|key| {
                    let encoded_key = serde_json::to_string(key)
                        .context("serializing key in canonical task definition")?;
                    let encoded_value = canonical_json(
                        values
                            .get(key)
                            .expect("key collected from this map must still exist"),
                    )?;
                    Ok(format!("{encoded_key}:{encoded_value}"))
                })
                .collect::<Result<Vec<_>>>()
                .map(|values| format!("{{{}}}", values.join(",")))
        }
    }
}

fn validate_schema_version(version: &str) -> Result<()> {
    if version.len() != 14 || !version.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("schema version must be a 14-digit ASCII timestamp");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        HISTORY_JSON, TaskCatalog, TaskCatalogEntry, TaskHistory, canonical_definition_digest,
        load_catalog, select_tasks_for_schema_version, thread_groups_canonical_keys_v1,
        thread_message_times_v1, validate_fixed_catalog_entry, validate_history_chain,
    };
    use crate::db_migrate::has_registered_implementation;
    use serde_json::json;

    #[test]
    fn catalog_is_valid_and_contains_the_required_tasks() {
        let catalog = load_catalog().expect("catalog must be valid");
        assert_eq!(catalog.tasks.len(), 2);
        assert_eq!(
            thread_message_times_v1().unwrap().identity(),
            "thread-message-times-v1@1"
        );
        assert_eq!(
            thread_groups_canonical_keys_v1().unwrap().identity(),
            "thread-groups-canonical-keys-v1@1"
        );
    }

    #[test]
    fn new_task_is_selected_for_both_backends_at_its_schema_version() {
        let catalog = load_catalog().unwrap();
        for backend in ["sqlite", "postgres"] {
            let selected =
                select_tasks_for_schema_version(&catalog, "20260920000001", backend).unwrap();
            assert_eq!(
                selected
                    .iter()
                    .map(TaskCatalogEntry::identity)
                    .collect::<Vec<_>>(),
                vec![
                    "thread-groups-canonical-keys-v1@1",
                    "thread-message-times-v1@1"
                ]
            );
        }
        // The task is introduced by the ThreadGroup schema version, so
        // earlier schema versions must not select it.
        for backend in ["sqlite", "postgres"] {
            let selected =
                select_tasks_for_schema_version(&catalog, "20260803000003", backend).unwrap();
            assert_eq!(
                selected
                    .iter()
                    .map(TaskCatalogEntry::identity)
                    .collect::<Vec<_>>(),
                vec!["thread-message-times-v1@1"]
            );
        }
    }

    #[test]
    fn canonical_digest_is_independent_of_json_object_order() {
        let a = json!({"z": ["b", "a"], "a": true});
        let b = json!({"a": true, "z": ["b", "a"]});
        assert_eq!(
            canonical_definition_digest(&a).unwrap(),
            canonical_definition_digest(&b).unwrap()
        );
    }

    #[test]
    fn every_catalog_implementation_has_a_fixed_registry_entry() {
        for task in load_catalog().unwrap().tasks {
            assert!(has_registered_implementation(&task.implementation));
        }
    }

    fn task(id: &str, dependencies: &[&str]) -> TaskCatalogEntry {
        let canonical_definition = json!({"id": id});
        TaskCatalogEntry {
            id: id.to_string(),
            generation: 1,
            description: id.to_string(),
            canonical_definition_digest: canonical_definition_digest(&canonical_definition)
                .unwrap(),
            canonical_definition,
            backends: vec!["sqlite".to_string()],
            introduced_by_schema_version: "20260803000003".to_string(),
            completion_required_by_schema_version: Some("20260803000003".to_string()),
            dependencies: dependencies
                .iter()
                .map(|dependency| (*dependency).to_string())
                .collect(),
            maintenance_window_required: true,
            implementation: "thread_message_times_v1::ThreadMessageTimesV1Task".to_string(),
            lifecycle: "active".to_string(),
        }
    }

    #[test]
    fn selected_tasks_include_dependency_closure_in_stable_topological_order() {
        let catalog = TaskCatalog {
            tasks: vec![task("dependent", &["base@1"]), task("base", &[])],
        };
        let selected =
            select_tasks_for_schema_version(&catalog, "20260803000003", "sqlite").unwrap();
        assert_eq!(
            selected
                .iter()
                .map(TaskCatalogEntry::identity)
                .collect::<Vec<_>>(),
            vec!["base@1", "dependent@1"]
        );
    }

    #[test]
    fn catalog_rejects_missing_or_cyclic_dependencies() {
        let missing = TaskCatalog {
            tasks: vec![task("dependent", &["missing@1"])],
        };
        assert!(select_tasks_for_schema_version(&missing, "20260803000003", "sqlite").is_err());

        let cyclic = TaskCatalog {
            tasks: vec![task("a", &["b@1"]), task("b", &["a@1"])],
        };
        assert!(select_tasks_for_schema_version(&cyclic, "20260803000003", "sqlite").is_err());
    }

    #[test]
    fn committed_task_history_chain_is_valid() {
        let history: TaskHistory =
            serde_json::from_str(HISTORY_JSON).expect("committed task history must parse");
        validate_history_chain(&history).unwrap();
    }

    #[test]
    fn history_chain_rejects_rewrites_and_non_prefix_updates() {
        use super::{TaskHistory, TaskHistoryEntry, TaskHistoryTransition};

        let entry = |identity: &str, digest: &str| TaskHistoryEntry {
            identity: identity.to_string(),
            canonical_definition_digest: digest.to_string(),
        };
        let digest_of = |entries: &[TaskHistoryEntry],
                         transitions: &[TaskHistoryTransition],
                         previous: Option<String>| {
            canonical_definition_digest(&json!({
                "entries": entries,
                "lifecycle_transitions": transitions,
                "previous_history_digest": previous,
            }))
            .unwrap()
        };

        let first = entry("a@1", "digest-a");
        let second = entry("b@1", "digest-b");
        let genesis_digest = digest_of(std::slice::from_ref(&first), &[], None);
        let transition = TaskHistoryTransition {
            identity: "b@1".to_string(),
            canonical_definition_digest: "digest-b".to_string(),
            kind: "registered".to_string(),
            previous_history_digest: genesis_digest.clone(),
        };
        let valid = TaskHistory {
            entries: vec![first.clone(), second.clone()],
            lifecycle_transitions: vec![transition.clone()],
            previous_history_digest: Some(genesis_digest.clone()),
            history_digest: digest_of(
                &[first.clone(), second],
                std::slice::from_ref(&transition),
                Some(genesis_digest.clone()),
            ),
        };
        validate_history_chain(&valid).unwrap();

        // Rewriting the registered entry's digest breaks the chain.
        let mut rewritten = valid.clone();
        rewritten.entries[1].canonical_definition_digest = "forged".to_string();
        assert!(validate_history_chain(&rewritten).is_err());

        // Dropping an entry (non-prefix update) breaks the chain.
        let mut shortened = valid.clone();
        shortened.entries.remove(0);
        assert!(validate_history_chain(&shortened).is_err());

        // A transition anchored to the wrong predecessor digest is rejected.
        let mut wrong_link = valid;
        wrong_link.lifecycle_transitions[0].previous_history_digest = "0".repeat(64);
        assert!(validate_history_chain(&wrong_link).is_err());
    }

    #[test]
    fn fixed_catalog_validation_rejects_a_self_consistent_forged_entry() {
        let entry = thread_message_times_v1().unwrap();
        validate_fixed_catalog_entry(&entry).unwrap();
        let entry = thread_groups_canonical_keys_v1().unwrap();
        validate_fixed_catalog_entry(&entry).unwrap();

        let mut forged = entry.clone();
        forged.backends = vec!["postgres".to_string()];
        assert!(validate_fixed_catalog_entry(&forged).is_err());

        let mut forged = entry.clone();
        forged.dependencies = vec!["unrelated@1".to_string()];
        assert!(validate_fixed_catalog_entry(&forged).is_err());

        let mut forged = entry.clone();
        forged.implementation = "forged::Task".to_string();
        assert!(validate_fixed_catalog_entry(&forged).is_err());

        let mut forged = entry.clone();
        forged.introduced_by_schema_version = "20260803000001".to_string();
        assert!(validate_fixed_catalog_entry(&forged).is_err());

        let mut forged = entry;
        forged.canonical_definition = json!({"forged": true});
        forged.canonical_definition_digest =
            canonical_definition_digest(&forged.canonical_definition).unwrap();
        assert!(validate_fixed_catalog_entry(&forged).is_err());
    }
}
