//! Registration of memories' workers into jobworkerp.
//!
//! Registration runs in the background after the startup decision passed
//! and retries until every enabled worker is registered; only then do
//! dispatch and query embedding proceed. Afterwards, workers of other
//! spaces sharing the same base names are deleted so no stale model
//! stays loaded.

use super::SpaceId;
use super::workers::{stale_space_workers, template_overrides};
use crate::infra::jobworkerp_ops::{JobworkerpConnector, JobworkerpOps};
use anyhow::{Context as _, Result};
use arc_swap::ArcSwapOption;
use jobworkerp_client::client::yaml_common;
use jobworkerp_client::jobworkerp::data::WorkerId;
use prost_reflect::MessageDescriptor;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Default wait between registration attempts.
pub const DEFAULT_RETRY_INTERVAL: Duration = Duration::from_secs(10);

/// Whether any registry in this process completed registration. Read by
/// query paths that hold a raw jobworkerp client instead of a registry.
static REGISTRATION_COMPLETE: AtomicBool = AtomicBool::new(false);

pub fn registration_complete() -> bool {
    REGISTRATION_COMPLETE.load(Ordering::SeqCst)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationStatus {
    Registered,
    Pending,
}

impl RegistrationStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Registered => "registered",
            Self::Pending => "pending",
        }
    }
}

/// What a process registers. Every entry is required for completion.
#[derive(Debug, Clone, Default)]
pub struct RegistrationPlan {
    /// Workers YAML files, registered in order (a later file's workflows
    /// may reference workers of an earlier one).
    pub worker_yamls: Vec<PathBuf>,
    /// Manifest YAML files (workers plus function sets).
    pub manifests: Vec<PathBuf>,
    /// Base names whose other-space workers are deleted afterwards.
    pub cleanup_bases: Vec<String>,
}

impl RegistrationPlan {
    /// Add a workers YAML unless already planned.
    pub fn add_worker_yaml(&mut self, path: PathBuf) {
        if !self.worker_yamls.contains(&path) {
            self.worker_yamls.push(path);
        }
    }
}

/// Result of a completed registration.
pub struct Registered {
    pub ops: Arc<dyn JobworkerpOps>,
    pub worker_ids: HashMap<String, WorkerId>,
    /// WORKFLOW `run` args descriptor, for encoding workflow job args.
    pub workflow_args_descriptor: Option<MessageDescriptor>,
}

pub struct WorkerRegistry {
    connector: Arc<dyn JobworkerpConnector>,
    plan: RegistrationPlan,
    space: Option<SpaceId>,
    overrides: HashMap<String, String>,
    retry_interval: Duration,
    state: ArcSwapOption<Registered>,
}

impl WorkerRegistry {
    /// `extra_overrides` are placeholder values supplied on top of the
    /// space suffix (e.g. the query prefix literal).
    pub fn new(
        connector: Arc<dyn JobworkerpConnector>,
        plan: RegistrationPlan,
        space: Option<SpaceId>,
        extra_overrides: HashMap<String, String>,
        retry_interval: Duration,
    ) -> Arc<Self> {
        let mut overrides = template_overrides(space.as_ref());
        overrides.extend(extra_overrides);
        Arc::new(Self {
            connector,
            plan,
            space,
            overrides,
            retry_interval,
            state: ArcSwapOption::empty(),
        })
    }

    pub fn registered(&self) -> Option<Arc<Registered>> {
        self.state.load_full()
    }

    pub fn status(&self) -> RegistrationStatus {
        if self.state.load().is_some() {
            RegistrationStatus::Registered
        } else {
            RegistrationStatus::Pending
        }
    }

    /// Retry [`Self::register_once`] until it succeeds.
    pub fn spawn_until_registered(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            loop {
                match this.register_once().await {
                    Ok(()) => return,
                    Err(e) => {
                        tracing::warn!(
                            retry_in_secs = this.retry_interval.as_secs(),
                            "embedding worker registration incomplete: {e:#}"
                        );
                        tokio::time::sleep(this.retry_interval).await;
                    }
                }
            }
        })
    }

    /// One registration attempt. Succeeds only when every planned
    /// document registered; a partial success leaves the state pending.
    pub async fn register_once(&self) -> Result<()> {
        if self.state.load().is_some() {
            return Ok(());
        }
        let ops = self.connector.connect().await?;
        let mut worker_ids = HashMap::new();
        for path in &self.plan.worker_yamls {
            let (raw, base_dir) = render_yaml(path, &self.overrides).await?;
            let ids = ops
                .register_workers_from_yaml_str(&raw, &base_dir)
                .await
                .with_context(|| format!("registering workers from {}", path.display()))?;
            worker_ids.extend(ids);
        }
        for path in &self.plan.manifests {
            let (raw, base_dir) = render_yaml(path, &self.overrides).await?;
            let ids = ops
                .register_manifest_from_yaml_str(&raw, &base_dir)
                .await
                .with_context(|| format!("registering manifest {}", path.display()))?;
            worker_ids.extend(ids);
        }
        let workflow_args_descriptor = ops.workflow_run_args_descriptor().await?;
        self.state.store(Some(Arc::new(Registered {
            ops: ops.clone(),
            worker_ids,
            workflow_args_descriptor,
        })));
        REGISTRATION_COMPLETE.store(true, Ordering::SeqCst);
        tracing::info!("embedding workers registered");
        self.delete_stale_workers(ops.as_ref()).await;
        Ok(())
    }

    /// Best effort: a failure is only logged, since serving does not
    /// depend on it.
    async fn delete_stale_workers(&self, ops: &dyn JobworkerpOps) {
        let Some(space) = &self.space else {
            return;
        };
        for base in &self.plan.cleanup_bases {
            let names = match ops.find_worker_names_with_prefix(base).await {
                Ok(names) => names,
                Err(e) => {
                    tracing::warn!(base, "listing workers for cleanup failed: {e:#}");
                    continue;
                }
            };
            for name in stale_space_workers(base, space, &names) {
                match ops.delete_worker_by_name(&name).await {
                    Ok(_) => {
                        tracing::info!(worker = %name, "deleted worker of another embedding space")
                    }
                    Err(e) => tracing::warn!(worker = %name, "deleting stale worker failed: {e:#}"),
                }
            }
        }
    }
}

/// Expand placeholders (with `overrides`) in a YAML file and the files it
/// includes via `$file:`, returning the rendered document and its base
/// directory. jobworkerp-client does not apply overrides to included
/// files, so includes are inlined here.
pub async fn render_yaml(
    path: &Path,
    overrides: &HashMap<String, String>,
) -> Result<(String, PathBuf)> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .with_context(|| format!("failed to read YAML at {}", path.display()))?;
    let base_dir = path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);
    // Expansion runs before parsing: an unquoted `%{...}` is not valid YAML.
    let expanded = yaml_common::expand_env_with_overrides(&raw, overrides)
        .with_context(|| format!("env expansion failed on {}", path.display()))?;
    let mut doc: serde_yaml::Value = serde_yaml::from_str(&expanded)
        .with_context(|| format!("YAML parse error in {}", path.display()))?;
    yaml_common::resolve_includes_with_overrides(&mut doc, &base_dir, overrides)
        .await
        .with_context(|| format!("resolving $file: includes of {}", path.display()))?;
    Ok((serde_yaml::to_string(&doc)?, base_dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::jobworkerp_ops::fake::FakeJobworkerp;
    use std::sync::atomic::Ordering;

    fn space() -> SpaceId {
        SpaceId("ab".repeat(32))
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        plan: RegistrationPlan,
    }

    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("wf.yaml"),
            "do:\n  - step:\n      run:\n        worker:\n          name: \"mm%{MEMORY_EMBEDDING_SPACE_SUFFIX}\"\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("workers.yaml"),
            "workers:\n  - name: \"mm%{MEMORY_EMBEDDING_SPACE_SUFFIX}\"\n    runner: R\n  - name: \"flow%{MEMORY_EMBEDDING_SPACE_SUFFIX}\"\n    runner: WORKFLOW\n    settings:\n      workflow_data:\n        $file: wf.yaml\n  - name: callback\n    runner: GRPC\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("manifest.yaml"),
            "workers:\n  entries:\n    - name: recall\n      runner: WORKFLOW\n",
        )
        .unwrap();
        let plan = RegistrationPlan {
            worker_yamls: vec![dir.path().join("workers.yaml")],
            manifests: vec![dir.path().join("manifest.yaml")],
            cleanup_bases: vec!["mm".into(), "flow".into()],
        };
        Fixture { _dir: dir, plan }
    }

    fn registry(fake: &Arc<FakeJobworkerp>, plan: RegistrationPlan) -> Arc<WorkerRegistry> {
        WorkerRegistry::new(
            Arc::new(fake.clone()),
            plan,
            Some(space()),
            HashMap::new(),
            Duration::from_millis(10),
        )
    }

    #[tokio::test]
    async fn registers_space_scoped_names_including_inlined_workflows() {
        let fx = fixture();
        let fake = FakeJobworkerp::new();
        let reg = registry(&fake, fx.plan.clone());
        assert_eq!(reg.status(), RegistrationStatus::Pending);
        reg.register_once().await.unwrap();
        assert_eq!(reg.status(), RegistrationStatus::Registered);
        let suffix = format!("-{}", &space().0[..16]);
        let names = fake.worker_names();
        assert!(names.contains(&format!("mm{suffix}")), "{names:?}");
        assert!(names.contains(&format!("flow{suffix}")));
        assert!(names.contains(&"callback".to_string()));
        assert!(names.contains(&"recall".to_string()));
        let rendered = fake.registered_yamls.lock().unwrap()[0].clone();
        assert!(
            !rendered.contains("%{"),
            "no placeholder may survive rendering: {rendered}"
        );
        assert!(rendered.contains(&format!("mm{suffix}")));
    }

    #[tokio::test]
    async fn partial_registration_stays_pending_and_retries_to_completion() {
        let fx = fixture();
        let fake = FakeJobworkerp::new();
        fake.fail_documents_containing("recall");
        let reg = registry(&fake, fx.plan.clone());
        assert!(reg.register_once().await.is_err());
        assert_eq!(
            reg.status(),
            RegistrationStatus::Pending,
            "workers registered but manifest failed is not complete"
        );
        fake.clear_failures();
        reg.spawn_until_registered().await.unwrap();
        assert_eq!(reg.status(), RegistrationStatus::Registered);
    }

    #[tokio::test]
    async fn unreachable_jobworkerp_is_retried_in_background() {
        let fx = fixture();
        let fake = FakeJobworkerp::new();
        fake.connect_failures.store(3, Ordering::SeqCst);
        let reg = registry(&fake, fx.plan.clone());
        reg.spawn_until_registered().await.unwrap();
        assert_eq!(reg.status(), RegistrationStatus::Registered);
        assert_eq!(fake.connect_calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn deletes_other_space_workers_only_after_completion() {
        let fx = fixture();
        let fake = FakeJobworkerp::new();
        fake.seed_workers(&[
            "mm",
            "mm-0000000000000000",
            "flow-1111111111111111",
            "mm-other",
        ]);
        fake.fail_documents_containing("recall");
        let reg = registry(&fake, fx.plan.clone());
        assert!(reg.register_once().await.is_err());
        assert!(fake.deleted.lock().unwrap().is_empty());

        fake.clear_failures();
        reg.register_once().await.unwrap();
        let mut deleted = fake.deleted.lock().unwrap().clone();
        deleted.sort();
        assert_eq!(
            deleted,
            vec!["flow-1111111111111111", "mm", "mm-0000000000000000"]
        );
        assert!(fake.worker_names().contains(&"mm-other".to_string()));
    }
}
