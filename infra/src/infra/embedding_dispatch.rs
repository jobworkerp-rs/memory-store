//! Shared core for memory and thread embedding job dispatchers.
//!
//! Both dispatchers register the same kind of workers via YAML, encode
//! identical workflow job args (only the ID field name differs), and run
//! the same enqueue / cache-args-descriptor flow against jobworkerp. This
//! module factors that logic so the per-dispatcher modules can stay thin.

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use jobworkerp_client::client::UseJobworkerpClient;
use jobworkerp_client::client::helper::UseJobworkerpClientHelper;
use jobworkerp_client::client::worker_yaml;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::jobworkerp::data::{JobId, WorkerId};
use jobworkerp_client::jobworkerp::service::{JobRequest, job_request};
use jobworkerp_client::proto::JobworkerpProto;
use prost_reflect::MessageDescriptor;
use serde_json::json;
use smallvec::{SmallVec, smallvec};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

/// Re-exports so downstream crates (notably `app`) that hold an
/// `EmbeddingDispatch` but do not directly depend on `jobworkerp-client`
/// or `tonic` can still construct `JobId` / `tonic::Status` values — for
/// tests, stubs, and similar authoring.
pub use jobworkerp_client::jobworkerp::data::JobId as EmbeddingJobId;
pub use tonic::Status as EmbeddingDispatchStatus;

/// Env var that selects the MultimodalEmbeddingRunner worker name. The
/// single source of truth shared by every embedding path: the storage
/// workflow YAMLs reference it through [`MM_EMBEDDING_WORKER_PLACEHOLDER`]
/// and the Rust query paths resolve it through
/// [`mm_embedding_worker_name`]. Keeping both sides on this one env var
/// is what guarantees query vectors and stored vectors are always
/// produced by the same worker (and thus the same model space).
pub const MM_EMBEDDING_WORKER_ENV: &str = "MEMORY_MM_EMBEDDING_WORKER";

/// Default worker name when [`MM_EMBEDDING_WORKER_ENV`] is unset.
pub const MM_EMBEDDING_WORKER_DEFAULT: &str = "memories-mm-embedding";

/// The `%{ENV:-default}` placeholder the storage workflow YAMLs embed so
/// jobworkerp's `expand_env` resolves the worker name from the same env
/// var the Rust query paths use. Kept here so the YAML-pinning tests can
/// assert the literal without re-typing it. This is a literal because
/// `concat!` only accepts literal tokens (not `const` identifiers), so
/// the env name / default appear here too; `placeholder_matches_env_and_default`
/// asserts it stays in sync with [`MM_EMBEDDING_WORKER_ENV`] /
/// [`MM_EMBEDDING_WORKER_DEFAULT`].
pub const MM_EMBEDDING_WORKER_PLACEHOLDER: &str =
    "%{MEMORY_MM_EMBEDDING_WORKER:-memories-mm-embedding}";

/// Resolve the mm-embedding worker name from [`MM_EMBEDDING_WORKER_ENV`],
/// falling back to [`MM_EMBEDDING_WORKER_DEFAULT`]. Used by every Rust
/// query-embed path (memory_vector SearchSemantic / SearchByMedia, the
/// startup dimension probe, and reflection F-S8) so they all hit the same
/// worker the storage YAMLs registered.
pub fn mm_embedding_worker_name() -> String {
    std::env::var(MM_EMBEDDING_WORKER_ENV)
        .unwrap_or_else(|_| MM_EMBEDDING_WORKER_DEFAULT.to_string())
}

/// Which embedding pipelines a deployment runs. `none` keeps text-only
/// semantic search (the historical behaviour); the image modes add an
/// image embedding pipeline and, for the caption variants, a VLM caption
/// step. Read once from `MEMORY_IMAGE_SEARCH_MODE`; the default is the
/// safe text-only `None` so an unconfigured deployment never tries to
/// reach an image embedding worker that was never registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ImageSearchMode {
    /// Text-only semantic search. No image embedding, no caption.
    #[default]
    None,
    /// Image embedding only (cross-modal vectors), no VLM caption.
    Multimodal,
    /// VLM caption only: caption text is embedded, no direct image vector.
    VlmCaption,
    /// Both an image vector and a VLM caption (caption is also embedded).
    Both,
}

impl ImageSearchMode {
    /// Parse the mode label. Unknown / empty values fall back to the
    /// safe text-only `None` so a typo cannot silently enable an image
    /// pipeline whose workers are not registered. Case-insensitive.
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "multimodal" => Self::Multimodal,
            "vlm_caption" => Self::VlmCaption,
            "both" => Self::Both,
            _ => Self::None,
        }
    }

    /// Read `MEMORY_IMAGE_SEARCH_MODE` (default text-only `None`).
    pub fn from_env() -> Self {
        std::env::var("MEMORY_IMAGE_SEARCH_MODE")
            .map(|v| Self::parse(&v))
            .unwrap_or_default()
    }

    /// The canonical lowercase label, the exact inverse of `parse`.
    /// Kept paired so the workflow job-args string and `parse` cannot
    /// drift apart.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Multimodal => "multimodal",
            Self::VlmCaption => "vlm_caption",
            Self::Both => "both",
        }
    }

    /// Whether any image-side pipeline is active (i.e. not text-only).
    pub fn is_image_enabled(self) -> bool {
        self != Self::None
    }
}

/// The workflow worker that handles `DispatchKind::Text` jobs.
pub const TEXT_WORKFLOW_WORKER: &str = "memories-auto-embedding";
/// The workflow worker that handles `DispatchKind::Media` jobs.
pub const IMAGE_WORKFLOW_WORKER: &str = "memories-auto-image-embedding";

/// Which embedding pipeline a memory row should be (re)dispatched for.
/// Independent axes — a memory with both text and an image attachment
/// gets both kinds, dispatched to two separate workflow workers that
/// each manage their own `vector_kind` rows (so they cannot clobber each
/// other's vectors).
///
/// This is intentionally a 2-value enum (no `Unspecified`): it is the
/// *output* of `dispatch_kinds`, which never yields the proto default.
/// The wire `DispatchKind` (UNSPECIFIED/TEXT/MEDIA) is bridged via
/// `try_from(i32)` / `as_wire()` so callers that filter by the proto
/// enum can convert without leaking the UNSPECIFIED case into dispatch
/// logic.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum DispatchKind {
    /// `memory.content` → `embed_text` (all chunks) → `vector_kind=text`.
    Text,
    /// The linked `media_object` → `embed_image` (+ optional VLM caption
    /// depending on `ImageSearchMode`) via the image workflow.
    Media,
}

impl DispatchKind {
    /// The wire enum discriminant (1:1 with proto `DispatchKind`).
    /// `Text=1`, `Media=2`. UNSPECIFIED(0) is unrepresentable by design.
    pub fn as_wire(self) -> i32 {
        match self {
            DispatchKind::Text => 1,
            DispatchKind::Media => 2,
        }
    }
}

impl TryFrom<i32> for DispatchKind {
    type Error = ();
    /// Map a wire `DispatchKind` discriminant. UNSPECIFIED(0) and any
    /// out-of-range value are rejected (`Err(())`) so callers must
    /// handle the "not a real dispatch kind" case explicitly rather
    /// than silently defaulting.
    fn try_from(v: i32) -> std::result::Result<Self, ()> {
        match v {
            1 => Ok(DispatchKind::Text),
            2 => Ok(DispatchKind::Media),
            _ => Err(()),
        }
    }
}

/// Decide which embedding pipelines a memory row should be dispatched to.
///
/// Two independent axes:
/// - **Text**: `role ∈ {USER,ASSISTANT,SYSTEM} ∧ content_type ≠ TOOL ∧
///   non-empty content`. TOOL content is excluded to keep tool-call /
///   tool-output previews out of the text vector space (the historical
///   `content_type=TEXT` narrowing carried this intent; role alone would
///   let ASSISTANT-role tool_use rows pollute the index).
/// - **Media**: the linked `media_object.kind == IMAGE` ∧ its
///   `storage_backend ∉ {unresolvable, inline}` ∧ `mode != none`.
///   Independent of `content_type` (any content_type may carry media —
///   a TOOL memory's screenshot is still embeddable). AUDIO/VIDEO are
///   out of scope. `unresolvable` has no bytes to embed (promoted later);
///   `inline` is a test-only backend the embedding workflow cannot read.
///
/// `ROLE_REFLECTION` is excluded via the role allow-list: reflection
/// memories travel through their own dispatchers, and dispatching them
/// here too would double-dispatch with status-update gaps.
/// Whether a memory's linked media would be dispatched to the image
/// (Media) pipeline. The three conjuncts are exactly the Media axis of
/// [`dispatch_kinds`]: `kind == IMAGE` ∧ `storage_backend ∉
/// {unresolvable, inline}` ∧ image mode enabled. `unresolvable` has no
/// bytes to embed; `inline` is a test-only backend the embedding
/// workflow cannot read; `mode=none` disables the image pipeline
/// entirely. Exposed as a standalone predicate so callers that must
/// reason about "will this media produce image/caption vectors?" (e.g.
/// the Update path deciding whether old image rows are now orphaned)
/// share one definition with `dispatch_kinds` instead of re-deriving a
/// subset of the conditions.
pub fn media_axis_dispatchable(
    media_kind: Option<i32>,
    media_storage_backend: Option<&str>,
    mode: ImageSearchMode,
) -> bool {
    use protobuf::llm_memory::data::ContentType;
    let is_image = matches!(
        media_kind.and_then(|k| ContentType::try_from(k).ok()),
        Some(ContentType::Image)
    );
    let media_embeddable = !matches!(media_storage_backend, Some("unresolvable") | Some("inline"));
    is_image && media_embeddable && mode.is_image_enabled()
}

pub fn dispatch_kinds(
    content: &str,
    role: i32,
    content_type: i32,
    media_kind: Option<i32>,
    media_storage_backend: Option<&str>,
    mode: ImageSearchMode,
) -> SmallVec<[DispatchKind; 2]> {
    use protobuf::llm_memory::data::{ContentType, MessageRole};
    let role_ok = matches!(
        MessageRole::try_from(role),
        Ok(MessageRole::RoleUser
            | MessageRole::RoleAssistant
            | MessageRole::RoleSystem
            | MessageRole::RoleReflection)
    );
    if !role_ok {
        return SmallVec::new();
    }
    let mut kinds: SmallVec<[DispatchKind; 2]> = smallvec![];
    let is_tool = matches!(ContentType::try_from(content_type), Ok(ContentType::Tool));
    if !is_tool && !content.trim().is_empty() {
        kinds.push(DispatchKind::Text);
    }
    if media_axis_dispatchable(media_kind, media_storage_backend, mode) {
        kinds.push(DispatchKind::Media);
    }
    kinds
}

/// A memory row resolved for dispatch: which pipelines to run, the
/// content for the text pipeline, and the linked media for the image
/// pipeline. The app layer evaluates `dispatch_kinds` and builds this.
#[derive(Debug, Clone)]
pub struct DispatchTarget {
    pub memory_id: i64,
    /// May be empty (an image-only memory still dispatches `Media`).
    pub content: String,
    /// The pipelines to run; if empty the target is skipped entirely.
    pub kinds: SmallVec<[DispatchKind; 2]>,
    /// Set when `kinds` contains `Media` — the image workflow resolves it.
    pub media_object_id: Option<i64>,
    /// Forwarded to the image workflow so it can branch image vs caption.
    pub image_search_mode: ImageSearchMode,
}

impl DispatchTarget {
    /// Construct from a memory row plus its (optional) linked media,
    /// evaluating `dispatch_kinds`. Returns `None` when no pipeline
    /// applies (so callers can skip without spawning a task).
    #[allow(clippy::too_many_arguments)]
    pub fn from_memory(
        memory_id: i64,
        content: &str,
        role: i32,
        content_type: i32,
        media_object_id: Option<i64>,
        media_kind: Option<i32>,
        media_storage_backend: Option<&str>,
        mode: ImageSearchMode,
    ) -> Option<Self> {
        let kinds = dispatch_kinds(
            content,
            role,
            content_type,
            media_kind,
            media_storage_backend,
            mode,
        );
        if kinds.is_empty() {
            return None;
        }
        Some(Self {
            memory_id,
            content: content.to_string(),
            kinds,
            media_object_id,
            image_search_mode: mode,
        })
    }
}

/// Classified failure modes of `EmbeddingDispatch::dispatch`. The variants
/// line up with the three stages where a dispatch can fail — init of the
/// lazy jobworkerp client, protobuf encoding of the job args, and the
/// actual enqueue RPC — so callers can distinguish transient per-job
/// failures (Encode / Enqueue) from a permanent blocker (Init).
#[derive(Debug, thiserror::Error)]
pub enum DispatchError {
    #[error("init failed: {0}")]
    Init(anyhow::Error),
    #[error("encode failed: {0}")]
    Encode(anyhow::Error),
    #[error("enqueue failed: {0}")]
    Enqueue(tonic::Status),
}

/// Minimal surface of the dispatcher used by the `redispatch_embeddings`
/// recovery API. Extracted as a trait so the app layer can verify the
/// dispatched/failed/skipped counting logic with a stub — the concrete
/// dispatcher holds a jobworkerp client which is impractical to stand up
/// in a unit test.
#[async_trait]
pub trait EmbeddingDispatch: Send + Sync {
    /// Submit a text embedding job. Returns `Ok(None)` if content was
    /// empty (skipped), `Ok(Some(JobId))` on success. This is the legacy
    /// single-worker text path used directly by thread / reflection
    /// dispatchers and by `redispatch_embeddings` for the text kind.
    async fn dispatch(
        &self,
        target_id: i64,
        content: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError>;

    /// Dispatch every kind in `target` (text → text workflow, media →
    /// image workflow). Returns one result per enqueued kind.
    ///
    /// Default implementation: text-only fallback to `dispatch`. This
    /// keeps thread / reflection dispatchers and unit-test stubs working
    /// unchanged — they never emit `Media`, so the default never needs
    /// the image route. `EmbeddingJobDispatcher` overrides this to route
    /// `Media` to the image workflow worker.
    async fn dispatch_target(
        &self,
        target: &DispatchTarget,
    ) -> Vec<std::result::Result<Option<JobId>, DispatchError>> {
        let mut out = Vec::with_capacity(target.kinds.len());
        for kind in &target.kinds {
            match kind {
                DispatchKind::Text => {
                    out.push(self.dispatch(target.memory_id, &target.content).await);
                }
                DispatchKind::Media => {
                    // A stub / text-only dispatcher cannot route image
                    // jobs. Surface it rather than silently dropping the
                    // media embedding.
                    out.push(Err(DispatchError::Enqueue(tonic::Status::unimplemented(
                        "this dispatcher does not support the image pipeline",
                    ))));
                }
            }
        }
        out
    }
}

/// Configuration for an embedding dispatcher. Worker-specific settings
/// (runner_settings, response_type, retry policy, ...) live in the YAML
/// referenced by `workers_yaml_path`; this struct only carries values
/// needed at job-args construction and dispatch time.
///
/// Note: the `embedding_model` label persisted alongside each vector
/// record is sourced from the runner's `model_info.model_name` inside
/// the workflow YAML, not from this config — that keeps the metadata in
/// sync with the model that actually produced the vector.
pub struct EmbeddingConfig {
    pub timeout_sec: u32,
    pub max_content_len: usize,
    /// Primary YAML defining the dispatcher's workflow worker plus any
    /// workers it owns exclusively.
    pub workers_yaml_path: PathBuf,
    /// YAMLs to register *before* `workers_yaml_path`. Used by the thread
    /// dispatcher to pull in the shared `memories-mm-embedding` worker
    /// from the memory YAML rather than redefining it (which would race
    /// against the memory dispatcher under last-write-wins).
    pub prerequisite_yaml_paths: Vec<PathBuf>,
    /// Extra workflow worker names to resolve to `WorkerId` during lazy
    /// init, on top of `DispatchSpec.target_worker_name`. The memory
    /// dispatcher uses this to also resolve the image workflow worker so
    /// it can route `DispatchKind::Media` to a second worker. Resolution
    /// is best-effort: a name absent from the registered set is skipped
    /// (it may be a mode-gated worker that this deployment did not
    /// register), so the text path is never blocked by an image worker
    /// that is intentionally not present.
    pub extra_worker_names: Vec<&'static str>,
}

impl EmbeddingConfig {
    /// Read the shared `MEMORY_EMBEDDING_*` knobs plus a caller-supplied
    /// env var for the workers YAML path.
    pub fn from_env(workers_yaml_env: &str, workers_yaml_default: &str) -> Result<Self> {
        Ok(Self {
            timeout_sec: std::env::var("MEMORY_EMBEDDING_TIMEOUT_SEC")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),
            max_content_len: std::env::var("MEMORY_EMBEDDING_MAX_CONTENT_LEN")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8192),
            workers_yaml_path: std::env::var(workers_yaml_env)
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from(workers_yaml_default)),
            prerequisite_yaml_paths: Vec::new(),
            extra_worker_names: Vec::new(),
        })
    }
}

/// Constants distinguishing a memory dispatcher from a thread dispatcher.
/// `id_field_name` is the JSON key under which the target id is sent to
/// the workflow runner (the workflow YAML reads it back out).
pub struct DispatchSpec {
    pub target_worker_name: &'static str,
    pub id_field_name: &'static str,
}

/// Lazy-initialized state shared by both dispatcher kinds.
struct DispatcherInner {
    client: JobworkerpClientWrapper,
    /// WorkerId of the primary workflow worker (`spec.target_worker_name`)
    /// — avoids find_by_name on every enqueue.
    worker_id: WorkerId,
    /// WorkerIds of `config.extra_worker_names` resolved at init.
    /// Best-effort: a name not present in the registered set is absent
    /// here (a mode-gated worker this deployment did not register), so
    /// routing to it later fails loudly rather than blocking init.
    extra_worker_ids: HashMap<&'static str, WorkerId>,
    /// WorkflowRunArgs schema descriptor, cached for repeat encoding.
    args_descriptor: Option<MessageDescriptor>,
    /// Lazy per-(worker, using) cache for `query_embed`: the WorkerData
    /// and the method's args descriptor. SearchSemantic / SearchByMedia
    /// are a per-request hot path, so resolving the worker + descriptor
    /// (two gRPC round-trips) only once per (worker, using) — instead of
    /// every query — matters as search QPS grows. Mutex (not OnceCell)
    /// because the key set is small but not known until first use.
    #[allow(clippy::type_complexity)]
    query_resolve_cache: tokio::sync::Mutex<
        HashMap<
            (String, String),
            (
                jobworkerp_client::jobworkerp::data::WorkerData,
                Option<MessageDescriptor>,
            ),
        >,
    >,
}

/// Core dispatcher logic shared by memory and thread dispatchers. Public
/// dispatchers wrap this and add their own `spawn_*` convenience methods.
pub struct EmbeddingDispatcherCore {
    config: EmbeddingConfig,
    spec: DispatchSpec,
    inner: tokio::sync::OnceCell<DispatcherInner>,
}

impl EmbeddingDispatcherCore {
    pub fn new(config: EmbeddingConfig, spec: DispatchSpec) -> Self {
        Self {
            config,
            spec,
            inner: tokio::sync::OnceCell::new(),
        }
    }

    /// Read-only accessor for tests in sibling modules that need to verify
    /// how a dispatcher composed its config (e.g. that the thread
    /// dispatcher promoted the memory YAML to a prerequisite).
    #[cfg(test)]
    pub(crate) fn config_for_test(&self) -> &EmbeddingConfig {
        &self.config
    }

    /// Force the lazy init path now so YAML parse / schema validation /
    /// jobworkerp connectivity errors surface at app startup instead of
    /// being deferred to the first fire-and-forget `dispatch()` call,
    /// where they would only show up as a `tracing::error!` log buried
    /// in a `tokio::spawn` task. Callers that handle the `Err` may then
    /// disable auto-embedding for the rest of the process lifetime.
    pub async fn ensure_initialized(&self) -> Result<()> {
        self.get_or_init().await.map(|_| ())
    }

    /// Lazy initialization: connect to jobworkerp, register all workers
    /// from the YAML, and cache the WorkerId of the workflow entry point.
    /// On Err, the OnceCell remains uninitialized and retries on next
    /// call (tokio 1.22+).
    async fn get_or_init(&self) -> Result<&DispatcherInner> {
        self.inner
            .get_or_try_init(|| async {
                let client = JobworkerpClientWrapper::new_by_env(Some(30)).await?;
                let metadata = Arc::new(HashMap::new());

                // Register prerequisite YAMLs first so workers shared
                // with another dispatcher (e.g. `memories-mm-embedding`
                // owned by the memory YAML) are present before the
                // primary YAML's workflow references them. Their
                // registered WorkerIds are kept too: the image workflow
                // worker (`memories-auto-image-embedding`) lives in a
                // prerequisite YAML, and `extra_worker_ids` must resolve
                // it from there — not only from the primary YAML.
                let mut all_registered: HashMap<String, WorkerId> = HashMap::new();
                for prereq_path in &self.config.prerequisite_yaml_paths {
                    let reg = worker_yaml::register_workers_from_yaml(
                        &client,
                        None,
                        metadata.clone(),
                        prereq_path,
                    )
                    .await
                    .with_context(|| {
                        format!(
                            "failed to register prerequisite workers from {}",
                            prereq_path.display()
                        )
                    })?;
                    all_registered.extend(reg);
                }

                let registered = worker_yaml::register_workers_from_yaml(
                    &client,
                    None,
                    metadata.clone(),
                    &self.config.workers_yaml_path,
                )
                .await
                .with_context(|| {
                    format!(
                        "failed to register {} workers from {}",
                        self.spec.target_worker_name,
                        self.config.workers_yaml_path.display()
                    )
                })?;
                all_registered.extend(registered);
                let worker_id = *all_registered
                    .get(self.spec.target_worker_name)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "{} missing from {}",
                            self.spec.target_worker_name,
                            self.config.workers_yaml_path.display()
                        )
                    })?;

                // Best-effort resolve of the extra workflow workers (e.g.
                // the image workflow, which lives in a prerequisite YAML).
                // Absence is not an init error: an image-mode worker is
                // intentionally not registered in a text-only deployment,
                // and the text path must still come up. Routing to a
                // missing name fails loudly at enqueue time.
                let mut extra_worker_ids = HashMap::new();
                for name in &self.config.extra_worker_names {
                    if let Some(id) = all_registered.get(*name) {
                        extra_worker_ids.insert(*name, *id);
                    }
                }

                let (_, wf_rdata) = client
                    .find_runner_or_error(None, metadata, "WORKFLOW")
                    .await?;
                let args_descriptor =
                    JobworkerpProto::parse_job_args_schema_descriptor(&wf_rdata, Some("run"))?;

                Ok(DispatcherInner {
                    client,
                    worker_id,
                    extra_worker_ids,
                    args_descriptor,
                    query_resolve_cache: tokio::sync::Mutex::new(HashMap::new()),
                })
            })
            .await
    }

    /// Truncate content to max_content_len Unicode characters.
    fn truncate_content<'a>(&self, content: &'a str) -> std::borrow::Cow<'a, str> {
        if content.chars().count() > self.config.max_content_len {
            std::borrow::Cow::Owned(
                content
                    .chars()
                    .take(self.config.max_content_len)
                    .collect::<String>(),
            )
        } else {
            std::borrow::Cow::Borrowed(content)
        }
    }

    fn build_job_args_json(&self, target_id: i64, content: &str) -> serde_json::Value {
        // The `input` field is itself a JSON-encoded string because the
        // WORKFLOW runner accepts a single string-typed parameter. The
        // workflow itself reads `embedding_model` from the runner output
        // (`model_info.model_name`), so it does not appear here.
        let inner = serde_json::json!({
            self.spec.id_field_name: target_id,
            "content": content,
        });
        serde_json::json!({ "input": inner.to_string() })
    }

    /// Encode the workflow `input` JSON and enqueue it against
    /// `worker_id`. Shared by the legacy `dispatch` (text path used by
    /// memory / thread / reflection) and the kind-routed memory path.
    /// `worker_label` is only for logs.
    async fn enqueue_workflow_job(
        &self,
        inner: &DispatcherInner,
        worker_id: WorkerId,
        worker_label: &str,
        job_args_json: &serde_json::Value,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        let args_bytes = match &inner.args_descriptor {
            Some(desc) => match JobworkerpProto::json_value_to_message(
                desc.clone(),
                job_args_json,
                true,
                true,
            ) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::error!(
                        target_worker = worker_label,
                        "Failed to encode embedding job args: {e}"
                    );
                    return Err(DispatchError::Encode(e));
                }
            },
            None => job_args_json.to_string().into_bytes(),
        };

        let job_request = JobRequest {
            args: args_bytes,
            timeout: Some((self.config.timeout_sec as u64) * 1000),
            worker: Some(job_request::Worker::WorkerId(worker_id)),
            priority: Some(jobworkerp_client::jobworkerp::data::Priority::Low as i32),
            using: Some("run".to_string()),
            ..Default::default()
        };

        match inner
            .client
            .jobworkerp_client()
            .job_client()
            .await
            .enqueue(tonic::Request::new(job_request))
            .await
        {
            Ok(resp) => {
                let job_id = resp.into_inner().id;
                tracing::debug!(
                    target_worker = worker_label,
                    "Embedding job enqueued: job_id={:?}",
                    job_id
                );
                Ok(job_id)
            }
            Err(e) => {
                tracing::error!(
                    target_worker = worker_label,
                    "Failed to enqueue embedding job: {e}"
                );
                Err(DispatchError::Enqueue(e))
            }
        }
    }

    /// Dispatch logic shared by both kinds. Empty content returns
    /// `Ok(None)` without connecting to jobworkerp.
    ///
    /// This is the legacy single-worker text path. The memory dispatcher's
    /// image / kind-routed path uses `dispatch_to_worker` instead; thread
    /// and reflection dispatchers keep using this unchanged.
    pub async fn dispatch(
        &self,
        target_id: i64,
        content: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        if content.is_empty() {
            return Ok(None);
        }
        let inner = match self.get_or_init().await {
            Ok(inner) => inner,
            Err(e) => {
                tracing::error!(
                    target_worker = self.spec.target_worker_name,
                    "embedding dispatcher init failed: {e}"
                );
                return Err(DispatchError::Init(e));
            }
        };

        let content = self.truncate_content(content);
        let job_args_json = self.build_job_args_json(target_id, &content);
        self.enqueue_workflow_job(
            inner,
            inner.worker_id,
            self.spec.target_worker_name,
            &job_args_json,
        )
        .await
    }

    /// Enqueue a pre-built workflow `input` JSON to a named workflow
    /// worker. The name must be one of `config.extra_worker_names` (or
    /// `spec.target_worker_name`); a name not resolved at init (e.g. an
    /// image worker absent in a text-only deployment) returns an
    /// `Enqueue` error rather than silently dropping the job. Used by the
    /// memory dispatcher to route `DispatchKind::{Text,Media}` to two
    /// different workflow workers.
    pub async fn dispatch_to_worker(
        &self,
        worker_name: &str,
        job_args_json: &serde_json::Value,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        let inner = match self.get_or_init().await {
            Ok(inner) => inner,
            Err(e) => {
                tracing::error!(
                    target_worker = worker_name,
                    "embedding dispatcher init failed: {e}"
                );
                return Err(DispatchError::Init(e));
            }
        };
        let worker_id = if worker_name == self.spec.target_worker_name {
            inner.worker_id
        } else if let Some(id) = inner.extra_worker_ids.get(worker_name) {
            *id
        } else {
            // The worker was not registered (mode-gated worker missing in
            // this deployment, or a name typo). Surface it as an enqueue
            // failure so the fire-and-forget caller logs it, instead of a
            // silent no-op that loses the embedding.
            return Err(DispatchError::Enqueue(tonic::Status::not_found(format!(
                "workflow worker not registered: {worker_name}"
            ))));
        };
        self.enqueue_workflow_job(inner, worker_id, worker_name, job_args_json)
            .await
    }

    /// Build the workflow `input` JSON for the text path:
    /// `{"input": "{\"<id_field>\": <id>, \"content\": \"...\"}"}`.
    /// `content` is truncated to `max_content_len`.
    pub fn build_text_job_args(&self, target_id: i64, content: &str) -> serde_json::Value {
        let content = self.truncate_content(content);
        self.build_job_args_json(target_id, &content)
    }

    /// Synchronously embed a query in the SAME model space as the stored
    /// vectors, by submitting the embedding worker (`memories-mm-embedding`)
    /// as a job and waiting for the result. Used by
    /// SearchSemantic / SearchByMedia and the startup dimension probe.
    ///
    /// `using` is `"embed_text"` or `"embed_image"`; `args_json` is the
    /// method's args message as JSON (e.g. `{"text": "..."}` or
    /// `{"items": [{"url": "...", "label": "..."}]}`). Returns every
    /// embedding row in the result plus the runner's reported embedding
    /// dimension (one row for a short query / per image).
    pub async fn query_embed(
        &self,
        embed_worker_name: &str,
        using: &str,
        args_json: &serde_json::Value,
    ) -> Result<QueryEmbedding> {
        let inner = self
            .get_or_init()
            .await
            .context("embedding dispatcher init failed (query_embed)")?;
        let client = inner.client.jobworkerp_client();
        let metadata = Arc::new(HashMap::new());

        // Resolve (worker, args-descriptor) once per (worker, using) and
        // cache: this is a per-request search path, so the two gRPC
        // round-trips below should not run on every query. The args
        // descriptor is needed because job args MUST be protobuf-encoded
        // (raw JSON bytes make the runner fail with "buffer underflow").
        let cache_key = (embed_worker_name.to_string(), using.to_string());
        let (worker_data, args_desc) = {
            let mut cache = inner.query_resolve_cache.lock().await;
            if let Some(hit) = cache.get(&cache_key) {
                hit.clone()
            } else {
                let (_, worker_data) = inner
                    .client
                    .find_worker_by_name(None, metadata.clone(), embed_worker_name)
                    .await?
                    .ok_or_else(|| {
                        anyhow::anyhow!("embedding worker not registered: {embed_worker_name}")
                    })?;
                let (_, args_desc, _) = JobworkerpProto::find_runner_descriptors_by_worker(
                    client,
                    job_request::Worker::WorkerName(embed_worker_name.to_string()),
                    Some(using),
                )
                .await
                .with_context(|| format!("resolving {using} args descriptor failed"))?;
                let entry = (worker_data, args_desc);
                cache.insert(cache_key, entry.clone());
                entry
            }
        };

        let args_bytes = match args_desc {
            Some(desc) => JobworkerpProto::json_value_to_message(desc, args_json, true, true)
                .with_context(|| format!("encoding {using} args failed"))?,
            // No proto schema (raw-bytes runner) — fall back to JSON.
            None => serde_json::to_vec(args_json)?,
        };

        let result = inner
            .client
            .enqueue_and_get_result_worker_job(
                None,
                metadata,
                &worker_data,
                args_bytes,
                self.config.timeout_sec,
                None,
                Some(jobworkerp_client::jobworkerp::data::Priority::High),
                Some(using),
            )
            .await
            .with_context(|| format!("query embed job failed ({using})"))?;

        // A non-Success job (bad settings / runner error / timeout)
        // yields an empty output, which would otherwise decode to "" and
        // surface as a confusing "no embeddings array". Fail loudly with
        // the runner's error text instead.
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

        let out = JobworkerpProto::resolve_result_output_to_json(
            client,
            embed_worker_name,
            &result,
            Some(using),
        )
        .await
        .with_context(|| format!("decoding {using} result failed"))?;

        // MmEmbeddingResult: { embeddings: [{ values: [f32], ... }],
        // model_info: { embedding_dimension: u32, model_name } }.
        // `resolve_result_output_to_json` may return the message either
        // bare or wrapped in a single-element array (it builds an array
        // and unwraps len==1) — accept both shapes.
        let body = match out.as_array() {
            Some(arr) if arr.len() == 1 => &arr[0],
            _ => &out,
        };
        let embeddings = body
            .get("embeddings")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "{using} result has no `embeddings` array; raw decoded \
                     output = {out}"
                )
            })?;
        let mut vectors: Vec<Vec<f32>> = Vec::with_capacity(embeddings.len());
        for e in embeddings {
            let values = e
                .get("values")
                .and_then(|v| v.as_array())
                .ok_or_else(|| anyhow::anyhow!("{using} embedding row has no `values`"))?;
            let v: Vec<f32> = values
                .iter()
                .map(|n| n.as_f64().map(|f| f as f32))
                .collect::<Option<_>>()
                .ok_or_else(|| anyhow::anyhow!("{using} `values` is not all numeric"))?;
            vectors.push(v);
        }
        if vectors.is_empty() {
            anyhow::bail!("{using} returned no embedding rows");
        }
        let dimension = body
            .get("model_info")
            .and_then(|m| m.get("embedding_dimension"))
            .and_then(|d| d.as_u64())
            .map(|d| d as usize)
            // model_info is optional in the result; fall back to the
            // length of the first vector so the probe still has a value.
            .unwrap_or_else(|| vectors[0].len());
        let model_name = body
            .get("model_info")
            .and_then(|m| m.get("model_name"))
            .and_then(|n| n.as_str())
            .map(str::to_owned);
        Ok(QueryEmbedding {
            vectors,
            dimension,
            model_name,
        })
    }

    /// Convenience: embed a single short text query (one row expected).
    pub async fn query_embed_text(
        &self,
        embed_worker_name: &str,
        text: &str,
    ) -> Result<QueryEmbedding> {
        self.query_embed(embed_worker_name, "embed_text", &json!({ "text": text }))
            .await
    }

    /// Convenience: embed a single image query by URL (one row expected).
    pub async fn query_embed_image_url(
        &self,
        embed_worker_name: &str,
        url: &str,
    ) -> Result<QueryEmbedding> {
        self.query_embed(
            embed_worker_name,
            "embed_image",
            &json!({ "items": [{ "url": url }] }),
        )
        .await
    }
}

/// Result of a synchronous query embedding.
#[derive(Debug, Clone)]
pub struct QueryEmbedding {
    /// One vector per embedding row (one for a short text query / per
    /// input image). SearchSemantic uses `vectors[0]`; multi-image is
    /// not used yet but the shape supports it.
    pub vectors: Vec<Vec<f32>>,
    /// The runner-reported embedding dimension (for the startup probe /
    /// MEMORY_VECTOR_SIZE consistency check).
    pub dimension: usize,
    /// The runner-reported model name, if present.
    pub model_name: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `MM_EMBEDDING_WORKER_PLACEHOLDER` re-states the env name and default
    /// as a literal (jobworkerp's `%{...}` syntax can't be built from
    /// `const` identifiers via `concat!`). This guards that literal against
    /// drifting from `MM_EMBEDDING_WORKER_ENV` / `MM_EMBEDDING_WORKER_DEFAULT`
    /// — without it, changing the env name would silently split the Rust
    /// query path (reads the new env) from the YAML registration (still
    /// expands the old placeholder).
    #[test]
    fn placeholder_matches_env_and_default() {
        assert_eq!(
            MM_EMBEDDING_WORKER_PLACEHOLDER,
            format!("%{{{MM_EMBEDDING_WORKER_ENV}:-{MM_EMBEDDING_WORKER_DEFAULT}}}")
        );
    }

    fn test_core(spec: DispatchSpec, max_content_len: usize) -> EmbeddingDispatcherCore {
        EmbeddingDispatcherCore::new(
            EmbeddingConfig {
                timeout_sec: 60,
                max_content_len,
                workers_yaml_path: PathBuf::from("nonexistent.yaml"),
                prerequisite_yaml_paths: Vec::new(),
                extra_worker_names: Vec::new(),
            },
            spec,
        )
    }

    fn memory_spec() -> DispatchSpec {
        DispatchSpec {
            target_worker_name: "memories-auto-embedding",
            id_field_name: "memory_id",
        }
    }

    fn thread_spec() -> DispatchSpec {
        DispatchSpec {
            target_worker_name: "memories-auto-thread-embedding",
            id_field_name: "thread_id",
        }
    }

    #[test]
    fn media_axis_dispatchable_conjuncts() {
        use protobuf::llm_memory::data::ContentType;
        let img = ContentType::Image as i32;
        let url = ContentType::Url as i32;

        // All three conjuncts satisfied → dispatchable.
        assert!(media_axis_dispatchable(
            Some(img),
            Some("s3"),
            ImageSearchMode::Multimodal
        ));
        assert!(media_axis_dispatchable(
            Some(img),
            Some("file"),
            ImageSearchMode::Both
        ));

        // kind != IMAGE → never dispatchable regardless of backend/mode.
        assert!(!media_axis_dispatchable(
            Some(url),
            Some("s3"),
            ImageSearchMode::Multimodal
        ));
        assert!(!media_axis_dispatchable(
            None,
            Some("s3"),
            ImageSearchMode::Multimodal
        ));

        // IMAGE but non-embeddable backend → not dispatchable. This is
        // the case the kind-only check missed (review P2 follow-up).
        assert!(!media_axis_dispatchable(
            Some(img),
            Some("unresolvable"),
            ImageSearchMode::Multimodal
        ));
        assert!(!media_axis_dispatchable(
            Some(img),
            Some("inline"),
            ImageSearchMode::Multimodal
        ));

        // IMAGE + embeddable backend but image mode off → not
        // dispatchable (no Media pipeline runs at all).
        assert!(!media_axis_dispatchable(
            Some(img),
            Some("s3"),
            ImageSearchMode::None
        ));
    }

    #[test]
    fn truncate_content_ascii() {
        let c = test_core(memory_spec(), 5);
        assert_eq!(c.truncate_content("hello").as_ref(), "hello");
        assert_eq!(c.truncate_content("hello world").as_ref(), "hello");
        assert_eq!(c.truncate_content("").as_ref(), "");
    }

    #[test]
    fn truncate_content_multibyte() {
        let c = test_core(memory_spec(), 5);
        assert_eq!(
            c.truncate_content("あいうえおかきく").as_ref(),
            "あいうえお"
        );
        assert_eq!(c.truncate_content("あいうえお").as_ref(), "あいうえお");
        assert_eq!(c.truncate_content("abcあいうえお").as_ref(), "abcあい");
    }

    #[test]
    fn truncate_content_emoji() {
        let c = test_core(memory_spec(), 3);
        let truncated = c.truncate_content("🎉🎊🎈🎁");
        assert_eq!(truncated.chars().count(), 3);
        assert_eq!(truncated.as_ref(), "🎉🎊🎈");
    }

    #[test]
    fn build_job_args_uses_memory_id_for_memory_spec() {
        let c = test_core(memory_spec(), 8192);
        let json = c.build_job_args_json(12345, "hello");
        let input: serde_json::Value =
            serde_json::from_str(json["input"].as_str().unwrap()).unwrap();
        assert_eq!(input["memory_id"], 12345);
        assert_eq!(input["content"], "hello");
        // embedding_model is no longer a workflow input; it is read from
        // the runner's `model_info.model_name` inside the workflow YAML.
        assert!(input.get("embedding_model").is_none());
    }

    #[test]
    fn build_job_args_uses_thread_id_for_thread_spec() {
        let c = test_core(thread_spec(), 8192);
        let json = c.build_job_args_json(999, "hi");
        let input: serde_json::Value =
            serde_json::from_str(json["input"].as_str().unwrap()).unwrap();
        assert_eq!(input["thread_id"], 999);
        assert!(input.get("memory_id").is_none());
        assert!(input.get("embedding_model").is_none());
    }

    #[test]
    fn image_search_mode_parse_known_values() {
        assert_eq!(ImageSearchMode::parse("none"), ImageSearchMode::None);
        assert_eq!(
            ImageSearchMode::parse("multimodal"),
            ImageSearchMode::Multimodal
        );
        assert_eq!(
            ImageSearchMode::parse("vlm_caption"),
            ImageSearchMode::VlmCaption
        );
        assert_eq!(ImageSearchMode::parse("both"), ImageSearchMode::Both);
    }

    #[test]
    fn image_search_mode_parse_is_case_insensitive_and_trims() {
        assert_eq!(
            ImageSearchMode::parse("  MultiModal "),
            ImageSearchMode::Multimodal
        );
        assert_eq!(ImageSearchMode::parse("BOTH"), ImageSearchMode::Both);
    }

    #[test]
    fn image_search_mode_unknown_falls_back_to_none() {
        // An unknown / typo value must not silently enable an image
        // pipeline whose workers were never registered.
        assert_eq!(ImageSearchMode::parse(""), ImageSearchMode::None);
        assert_eq!(ImageSearchMode::parse("multimoda"), ImageSearchMode::None);
        assert_eq!(ImageSearchMode::parse("caption"), ImageSearchMode::None);
        assert_eq!(ImageSearchMode::default(), ImageSearchMode::None);
    }

    #[test]
    fn image_search_mode_is_image_enabled() {
        assert!(!ImageSearchMode::None.is_image_enabled());
        assert!(ImageSearchMode::Multimodal.is_image_enabled());
        assert!(ImageSearchMode::VlmCaption.is_image_enabled());
        assert!(ImageSearchMode::Both.is_image_enabled());
    }

    #[tokio::test]
    async fn dispatch_skips_empty_content() {
        // SAFETY: this is the only env access in this test and it just removes a var.
        unsafe { std::env::remove_var("JOBWORKERP_ADDR") };
        let c = test_core(memory_spec(), 8192);
        let result = c.dispatch(1, "").await;
        assert!(matches!(result, Ok(None)));
    }

    /// Live integration: synchronously embed a text query via the real
    /// `memories-mm-embedding` worker on a running jobworkerp, exercising
    /// the SearchSemantic / startup-probe path end to end (job submit →
    /// MmEmbeddingResult decode → QueryEmbedding). Also records the
    /// runner's real `embedding_dimension` so MEMORY_VECTOR_SIZE can be
    /// confirmed against the live model. Self-skips when JOBWORKERP_ADDR is unset so
    /// CI / offline runs are unaffected; run with:
    ///   JOBWORKERP_ADDR=http://127.0.0.1:9000 \
    ///     cargo test -p infra query_embed -- --ignored --test-threads=1
    #[tokio::test]
    #[ignore = "requires a running jobworkerp with memories-mm-embedding; \
                set JOBWORKERP_ADDR and run with --ignored"]
    async fn query_embed_text_roundtrip_live() {
        if std::env::var("JOBWORKERP_ADDR").is_err() {
            eprintln!(
                "skipping: JOBWORKERP_ADDR not set (start jobworkerp with \
                 the MultimodalEmbeddingRunner plugin, then \
                 JOBWORKERP_ADDR=http://127.0.0.1:9000 cargo test -p infra \
                 query_embed -- --ignored)"
            );
            return;
        }
        // Use the real auto-embedding-workers.yaml so the actual
        // mm-embedding worker is registered (the query path resolves it by
        // name). Resolve the name through the single env source so this
        // test follows a MEMORY_MM_EMBEDDING_WORKER override too.
        let config = crate::infra::memory_vector::dispatcher::auto_embedding_config_from_env()
            .expect("auto_embedding_config_from_env");
        let core = EmbeddingDispatcherCore::new(config, memory_spec());
        let worker = mm_embedding_worker_name();

        let emb = core
            .query_embed_text(&worker, "a small grey cat sitting on a window sill")
            .await
            .expect("query_embed_text against the live mm-embedding worker");

        assert!(
            !emb.vectors.is_empty() && !emb.vectors[0].is_empty(),
            "the runner must return at least one non-empty vector"
        );
        assert_eq!(
            emb.vectors[0].len(),
            emb.dimension,
            "vector length must equal the reported embedding_dimension"
        );
        // L2-normalized per the runner contract (~1.0). Loose bound: the
        // point is that it is a real unit-ish embedding, not zeros.
        let norm: f32 = emb.vectors[0].iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!(
            (0.5..=1.5).contains(&norm),
            "embedding norm {norm} is implausible (expected ~1.0)"
        );
        // Surface the real dimension so MEMORY_VECTOR_SIZE can be set to
        // match the loaded model. Visible with --nocapture.
        eprintln!(
            "mm-embedding live: model={:?} embedding_dimension={}",
            emb.model_name, emb.dimension
        );
    }
}
