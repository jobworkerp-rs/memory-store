//! The jobworkerp operations memories depends on, behind a trait so the
//! embedding dispatchers can be exercised against an in-process fake
//! (registration retries, partial failures, connection loss) without a
//! running jobworkerp.

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use jobworkerp_client::client::UseJobworkerpClient;
use jobworkerp_client::client::helper::UseJobworkerpClientHelper;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::client::{manifest_yaml, worker_yaml};
use jobworkerp_client::jobworkerp::data::{JobId, WorkerData, WorkerId};
use jobworkerp_client::jobworkerp::service::{FindWorkerListRequest, JobRequest, job_request};
use jobworkerp_client::proto::JobworkerpProto;
use prost_reflect::MessageDescriptor;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

#[async_trait]
pub trait JobworkerpOps: Send + Sync {
    /// Upsert every worker defined in an already-rendered workers YAML
    /// and return the registered ids keyed by worker name. `base_dir`
    /// resolves any remaining `$file:` includes.
    async fn register_workers_from_yaml_str(
        &self,
        raw_yaml: &str,
        base_dir: &Path,
    ) -> Result<HashMap<String, WorkerId>>;

    /// Upsert the workers and function sets of an already-rendered
    /// manifest YAML and return the registered worker ids.
    async fn register_manifest_from_yaml_str(
        &self,
        raw_yaml: &str,
        base_dir: &Path,
    ) -> Result<HashMap<String, WorkerId>>;

    /// Names of every worker whose name starts with `prefix`.
    async fn find_worker_names_with_prefix(&self, prefix: &str) -> Result<Vec<String>>;

    /// Delete a worker by name. `Ok(false)` when it does not exist.
    async fn delete_worker_by_name(&self, name: &str) -> Result<bool>;

    /// Args descriptor of the WORKFLOW runner's `run` method, used to
    /// encode workflow job args. `None` when the runner has no schema.
    async fn workflow_run_args_descriptor(&self) -> Result<Option<MessageDescriptor>>;

    /// Enqueue a job without waiting for its result.
    async fn enqueue(
        &self,
        request: JobRequest,
    ) -> std::result::Result<Option<JobId>, tonic::Status>;

    /// Run `using` on the named worker, wait for the result, and return
    /// the decoded output as JSON. A non-success job is an error.
    async fn run_worker_job(
        &self,
        worker_name: &str,
        using: &str,
        args_json: &serde_json::Value,
        timeout_sec: u32,
    ) -> Result<serde_json::Value>;
}

/// Opens a [`JobworkerpOps`] session. Connecting is separate from the
/// operations so a failed connection can be retried later.
#[async_trait]
pub trait JobworkerpConnector: Send + Sync {
    async fn connect(&self) -> Result<Arc<dyn JobworkerpOps>>;
}

/// Connects to the jobworkerp at `JOBWORKERP_ADDR`.
pub struct EnvJobworkerpConnector {
    pub request_timeout_sec: u32,
}

impl Default for EnvJobworkerpConnector {
    fn default() -> Self {
        Self {
            request_timeout_sec: 30,
        }
    }
}

#[async_trait]
impl JobworkerpConnector for EnvJobworkerpConnector {
    async fn connect(&self) -> Result<Arc<dyn JobworkerpOps>> {
        let addr = std::env::var("JOBWORKERP_ADDR")
            .map_err(|_| anyhow::anyhow!("JOBWORKERP_ADDR is not set"))?;
        let client = JobworkerpClientWrapper::new(&addr, Some(self.request_timeout_sec)).await?;
        Ok(Arc::new(ClientJobworkerpOps::new(client)))
    }
}

/// [`JobworkerpOps`] over a real jobworkerp gRPC client.
pub struct ClientJobworkerpOps {
    client: JobworkerpClientWrapper,
    /// Per-(worker, using) cache of the worker row and the method's args
    /// descriptor. Query embedding is a per-request search path, so the
    /// two resolving round-trips should not run on every query.
    #[allow(clippy::type_complexity)]
    query_resolve_cache:
        tokio::sync::Mutex<HashMap<(String, String), (WorkerData, Option<MessageDescriptor>)>>,
}

impl ClientJobworkerpOps {
    pub fn new(client: JobworkerpClientWrapper) -> Self {
        Self {
            client,
            query_resolve_cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn resolve_worker(
        &self,
        worker_name: &str,
        using: &str,
    ) -> Result<(WorkerData, Option<MessageDescriptor>)> {
        let cache_key = (worker_name.to_string(), using.to_string());
        let mut cache = self.query_resolve_cache.lock().await;
        if let Some(hit) = cache.get(&cache_key) {
            return Ok(hit.clone());
        }
        let (_, worker_data) = self
            .client
            .find_worker_by_name(None, Arc::new(HashMap::new()), worker_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("embedding worker not registered: {worker_name}"))?;
        let (_, args_desc, _) = JobworkerpProto::find_runner_descriptors_by_worker(
            self.client.jobworkerp_client(),
            job_request::Worker::WorkerName(worker_name.to_string()),
            Some(using),
        )
        .await
        .with_context(|| format!("resolving {using} args descriptor failed"))?;
        let entry = (worker_data, args_desc);
        cache.insert(cache_key, entry.clone());
        Ok(entry)
    }
}

#[async_trait]
impl JobworkerpOps for ClientJobworkerpOps {
    async fn register_workers_from_yaml_str(
        &self,
        raw_yaml: &str,
        base_dir: &Path,
    ) -> Result<HashMap<String, WorkerId>> {
        worker_yaml::register_workers_from_yaml_str(
            &self.client,
            None,
            Arc::new(HashMap::new()),
            raw_yaml,
            base_dir,
        )
        .await
    }

    async fn register_manifest_from_yaml_str(
        &self,
        raw_yaml: &str,
        base_dir: &Path,
    ) -> Result<HashMap<String, WorkerId>> {
        manifest_yaml::register_manifest_from_yaml_str(
            &self.client,
            None,
            Arc::new(HashMap::new()),
            raw_yaml,
            base_dir,
        )
        .await
        .map(|r| r.workers)
    }

    async fn find_worker_names_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        use futures::StreamExt as _;
        let request = FindWorkerListRequest {
            name_filter: Some(prefix.to_string()),
            ..Default::default()
        };
        let mut stream = self
            .client
            .jobworkerp_client()
            .worker_client()
            .await
            .find_list(tonic::Request::new(request))
            .await?
            .into_inner();
        let mut names = Vec::new();
        while let Some(worker) = stream.next().await {
            if let Some(data) = worker?.data
                && data.name.starts_with(prefix)
            {
                names.push(data.name);
            }
        }
        Ok(names)
    }

    async fn delete_worker_by_name(&self, name: &str) -> Result<bool> {
        self.client
            .delete_worker_by_name(None, Arc::new(HashMap::new()), name)
            .await
    }

    async fn workflow_run_args_descriptor(&self) -> Result<Option<MessageDescriptor>> {
        let (_, wf_rdata) = self
            .client
            .find_runner_or_error(None, Arc::new(HashMap::new()), "WORKFLOW")
            .await?;
        JobworkerpProto::parse_job_args_schema_descriptor(&wf_rdata, Some("run"))
    }

    async fn enqueue(
        &self,
        request: JobRequest,
    ) -> std::result::Result<Option<JobId>, tonic::Status> {
        self.client
            .jobworkerp_client()
            .job_client()
            .await
            .enqueue(tonic::Request::new(request))
            .await
            .map(|resp| resp.into_inner().id)
    }

    async fn run_worker_job(
        &self,
        worker_name: &str,
        using: &str,
        args_json: &serde_json::Value,
        timeout_sec: u32,
    ) -> Result<serde_json::Value> {
        // Job args MUST be protobuf-encoded when the method has a schema:
        // raw JSON bytes make the runner fail with "buffer underflow".
        let (worker_data, args_desc) = self.resolve_worker(worker_name, using).await?;
        let args_bytes = match args_desc {
            Some(desc) => JobworkerpProto::json_value_to_message(desc, args_json, true, true)
                .with_context(|| format!("encoding {using} args failed"))?,
            None => serde_json::to_vec(args_json)?,
        };
        let result = self
            .client
            .enqueue_and_get_result_worker_job(
                None,
                Arc::new(HashMap::new()),
                &worker_data,
                args_bytes,
                u64::from(timeout_sec),
                None,
                Some(jobworkerp_client::jobworkerp::data::Priority::High),
                Some(using),
            )
            .await
            .with_context(|| format!("{using} job failed"))?;

        // A non-success job yields an empty output that would otherwise
        // decode to "" and surface as a confusing shape error.
        use jobworkerp_client::jobworkerp::data::ResultStatus;
        if result.status() != ResultStatus::Success {
            let err_body = result
                .output
                .as_ref()
                .map(|o| String::from_utf8_lossy(&o.items).into_owned())
                .unwrap_or_default();
            anyhow::bail!(
                "{using} job did not succeed (status={:?}): {err_body}",
                result.status()
            );
        }
        JobworkerpProto::resolve_result_output_to_json(
            self.client.jobworkerp_client(),
            worker_name,
            &result,
            Some(using),
        )
        .await
        .with_context(|| format!("decoding {using} result failed"))
    }
}

/// In-process [`JobworkerpOps`] for tests: registers the worker names
/// found in rendered YAML, records calls, and can be told to fail
/// connecting or registering.
#[cfg(any(test, feature = "test-helper"))]
pub mod fake {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};

    #[derive(Default)]
    pub struct FakeJobworkerp {
        /// Registration of a document containing any of these substrings
        /// fails.
        pub failing_markers: Mutex<Vec<String>>,
        /// Number of upcoming `connect` calls that fail.
        pub connect_failures: AtomicUsize,
        pub connect_calls: AtomicUsize,
        pub registered: Mutex<HashMap<String, WorkerId>>,
        /// Rendered YAML documents in registration order.
        pub registered_yamls: Mutex<Vec<String>>,
        pub deleted: Mutex<Vec<String>>,
        pub enqueued: Mutex<Vec<JobRequest>>,
        /// Output returned by `run_worker_job`.
        pub job_output: Mutex<serde_json::Value>,
        pub job_calls: Mutex<Vec<(String, String, serde_json::Value)>>,
        next_id: AtomicI64,
    }

    impl FakeJobworkerp {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        pub fn worker_id(&self, name: &str) -> Option<WorkerId> {
            self.registered.lock().unwrap().get(name).copied()
        }

        pub fn worker_names(&self) -> Vec<String> {
            let mut names: Vec<String> = self.registered.lock().unwrap().keys().cloned().collect();
            names.sort();
            names
        }

        /// Pre-register workers (e.g. ones left by a previous space).
        pub fn seed_workers(&self, names: &[&str]) {
            for name in names {
                self.upsert(name);
            }
        }

        pub fn fail_documents_containing(&self, marker: &str) {
            self.failing_markers
                .lock()
                .unwrap()
                .push(marker.to_string());
        }

        pub fn clear_failures(&self) {
            self.failing_markers.lock().unwrap().clear();
        }

        fn upsert(&self, name: &str) -> WorkerId {
            *self
                .registered
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_insert_with(|| WorkerId {
                    value: self.next_id.fetch_add(1, Ordering::SeqCst) + 1,
                })
        }

        fn register_document(
            &self,
            raw_yaml: &str,
            names_at: &[&str],
        ) -> Result<HashMap<String, WorkerId>> {
            self.registered_yamls
                .lock()
                .unwrap()
                .push(raw_yaml.to_string());
            if self
                .failing_markers
                .lock()
                .unwrap()
                .iter()
                .any(|m| raw_yaml.contains(m.as_str()))
            {
                anyhow::bail!("fake registration failure");
            }
            let doc: serde_yaml::Value = serde_yaml::from_str(raw_yaml)?;
            let list = names_at
                .iter()
                .try_fold(&doc, |v, key| v.get(*key))
                .and_then(|w| w.as_sequence())
                .ok_or_else(|| anyhow::anyhow!("no worker list"))?;
            Ok(list
                .iter()
                .filter_map(|w| w.get("name").and_then(|n| n.as_str()))
                .map(|name| (name.to_string(), self.upsert(name)))
                .collect())
        }
    }

    #[async_trait]
    impl JobworkerpConnector for Arc<FakeJobworkerp> {
        async fn connect(&self) -> Result<Arc<dyn JobworkerpOps>> {
            self.connect_calls.fetch_add(1, Ordering::SeqCst);
            let pending = self.connect_failures.load(Ordering::SeqCst);
            if pending > 0 {
                self.connect_failures.store(pending - 1, Ordering::SeqCst);
                anyhow::bail!("fake jobworkerp unreachable");
            }
            Ok(Arc::new(self.clone()))
        }
    }

    #[async_trait]
    impl JobworkerpOps for Arc<FakeJobworkerp> {
        async fn register_workers_from_yaml_str(
            &self,
            raw_yaml: &str,
            _base_dir: &Path,
        ) -> Result<HashMap<String, WorkerId>> {
            self.register_document(raw_yaml, &["workers"])
        }

        async fn register_manifest_from_yaml_str(
            &self,
            raw_yaml: &str,
            _base_dir: &Path,
        ) -> Result<HashMap<String, WorkerId>> {
            self.register_document(raw_yaml, &["workers", "entries"])
        }

        async fn find_worker_names_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
            Ok(self
                .worker_names()
                .into_iter()
                .filter(|n| n.starts_with(prefix))
                .collect())
        }

        async fn delete_worker_by_name(&self, name: &str) -> Result<bool> {
            self.deleted.lock().unwrap().push(name.to_string());
            Ok(self.registered.lock().unwrap().remove(name).is_some())
        }

        async fn workflow_run_args_descriptor(&self) -> Result<Option<MessageDescriptor>> {
            Ok(None)
        }

        async fn enqueue(
            &self,
            request: JobRequest,
        ) -> std::result::Result<Option<JobId>, tonic::Status> {
            let mut enqueued = self.enqueued.lock().unwrap();
            enqueued.push(request);
            Ok(Some(JobId {
                value: enqueued.len() as i64,
            }))
        }

        async fn run_worker_job(
            &self,
            worker_name: &str,
            using: &str,
            args_json: &serde_json::Value,
            _timeout_sec: u32,
        ) -> Result<serde_json::Value> {
            if !self.registered.lock().unwrap().contains_key(worker_name) {
                anyhow::bail!("embedding worker not registered: {worker_name}");
            }
            self.job_calls.lock().unwrap().push((
                worker_name.to_string(),
                using.to_string(),
                args_json.clone(),
            ));
            Ok(self.job_output.lock().unwrap().clone())
        }
    }
}
