//! Shared importer for sources that produce `CanonicalEntry`.
//!
//! Server-side resolution against `BatchMemoryInput.parent_external_ids`
//! is the single source of truth for parent links.

use crate::client::ImportClient;
use crate::common::canonical::{strip_nul_bytes, strip_nul_bytes_in_value};
use crate::events::ImportEventSink;
use crate::source::{
    CanonicalEntry, CanonicalEntryStream, CanonicalSession, ChatSource, ReadSessionOutcome,
    SourceDiagnostics, StreamItem, ThreadGroupConfidence, ThreadGroupEvidenceKind,
    ThreadGroupIdentityScope, ThreadGroupPolarity, ThreadGroupSourceIdentity,
};
use anyhow::Result;
use common::external_id::{EXTERNAL_ID_MAX_BYTES, namespace_for_external_id, owner_scoped};
use futures::stream::{StreamExt, TryStreamExt};
use prost::Message;
use protobuf::llm_memory::data::{
    MemoryData, MemoryId, MemoryKind, ThreadData, ThreadEvidenceConfidence, ThreadEvidenceKind,
    ThreadGroupEndpoint, ThreadGroupObservationInput, ThreadId, ThreadObservationPolarity, UserId,
};
use protobuf::llm_memory::service::add_memories_batch_request::ThreadTarget as PbThreadTarget;
use protobuf::llm_memory::service::{
    AddLabelsRequest, AddMemoriesBatchRequest, BatchMemoryInput as PbBatchMemoryInput,
    RecordThreadGroupObservationsRequest, ThreadUpsertByChannel, UpdateMemoryParentsRequest,
    UpdateMemoryParentsResponse, UpdateMemoryParentsSkipReason as SkipReason,
};
use std::collections::{HashMap, HashSet};

/// Per-run ThreadGroup write gate. `--no-thread-group-writes` disables
/// observation / reconciliation writes for the process without disabling
/// the suppression check (the source identity is still sent on the batch
/// request). Default enabled.
static THREAD_GROUP_WRITES_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

pub fn set_thread_group_writes_enabled(enabled: bool) {
    THREAD_GROUP_WRITES_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn thread_group_writes_enabled() -> bool {
    THREAD_GROUP_WRITES_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Maximum entries packed into a single AddMemoriesBatch call. Spec §3.2.9.
const CHUNK_MAX_ENTRIES: usize = 500;

/// Soft cap on the prost-encoded size of a single AddMemoriesBatchRequest.
/// Sized below the tonic 16 MiB - 1 frame limit so request framing
/// metadata (tonic headers, grpc length prefix) plus a per-import
/// overhead margin still fit. Spec §3.2.9 / open-issue A5.
const CHUNK_MAX_BYTES: usize = 12 * 1024 * 1024;

/// Independent per-entry `UpdateMemoryParents` RPCs, run concurrently to
/// amortise RTT for chunks with many duplicates.
const REWIRE_CONCURRENCY: usize = 8;

struct RewireJob {
    external_id: String,
    memory_id: MemoryId,
    request: UpdateMemoryParentsRequest,
}

#[derive(Debug, Clone)]
struct ExistingMemory {
    memory_id: MemoryId,
    parent_ids_empty: bool,
    thread_ids: Vec<ThreadId>,
}

async fn lookup_opencode_memories(
    client: &dyn ImportClient,
    entries: &[CanonicalEntry],
    user_id: i64,
    target_thread_id: Option<ThreadId>,
) -> Result<(
    Vec<CanonicalEntry>,
    HashMap<String, ExistingMemory>,
    Option<ThreadId>,
)> {
    let mut new_entries = Vec::with_capacity(entries.len());
    let mut existing = HashMap::new();
    for entry in entries {
        let Some(found) = client
            .find_memory_by_external_id(entry.external_id.clone())
            .await?
        else {
            new_entries.push(entry.clone());
            continue;
        };
        let memory = found
            .memory
            .ok_or_else(|| anyhow::anyhow!("exact lookup returned no memory"))?;
        let data = memory
            .data
            .ok_or_else(|| anyhow::anyhow!("exact lookup returned no memory data"))?;
        if data.external_id.as_deref() != Some(entry.external_id.as_str()) {
            return Err(anyhow::anyhow!(
                "exact lookup returned a different external_id"
            ));
        }
        let owner = data
            .user_id
            .ok_or_else(|| anyhow::anyhow!("exact lookup returned no memory user_id"))?;
        if owner.value != user_id {
            return Err(anyhow::anyhow!(
                "external_id collision belongs to user {}",
                owner.value
            ));
        }
        if data.thread_ids.is_empty() {
            return Err(anyhow::anyhow!(
                "exact lookup returned no thread membership for {}",
                entry.external_id
            ));
        }
        let Some(target_thread_id) = target_thread_id else {
            return Err(anyhow::anyhow!(
                "external_id collision has no matching OpenCode thread: {}",
                entry.external_id
            ));
        };
        if !data
            .thread_ids
            .iter()
            .any(|thread_id| thread_id.value == target_thread_id.value)
        {
            return Err(anyhow::anyhow!(
                "external_id collision belongs to a different thread: {}",
                entry.external_id
            ));
        }
        let memory_id = memory
            .id
            .ok_or_else(|| anyhow::anyhow!("exact lookup returned no memory id"))?;
        existing.insert(
            entry.external_id.clone(),
            ExistingMemory {
                memory_id,
                parent_ids_empty: data.parent_ids.is_empty(),
                thread_ids: data.thread_ids,
            },
        );
    }
    Ok((new_entries, existing, target_thread_id))
}

struct OpenCodeDuplicatePreflight {
    entries_stream: crate::source::CanonicalEntryStream,
    duplicates: Vec<(CanonicalEntry, ExistingMemory)>,
    thread_id: Option<ThreadId>,
    duplicate_count: usize,
}

async fn preflight_opencode_duplicates(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    entries_stream: crate::source::CanonicalEntryStream,
    user_id: i64,
    thread_channel_snapshot: &HashMap<String, ThreadId>,
) -> std::result::Result<OpenCodeDuplicatePreflight, String> {
    let collected = entries_stream.collect::<Vec<_>>();
    let mut entries = Vec::with_capacity(collected.len());
    for item in collected {
        if let StreamItem::Entry(entry) = item {
            entries.push(entry);
        }
    }
    let mut namespaced = entries;
    for entry in &mut namespaced {
        namespace_entry_external_ids(entry, &session.source_id, user_id);
    }

    let target_thread = if namespaced.is_empty() {
        None
    } else {
        // Snapshot first (one scan per run); fall back to the authoritative
        // per-channel lookup on miss so threads created after the snapshot
        // are still resolved. Ambiguous channels are omitted from the
        // snapshot and reported by that lookup.
        match thread_channel_snapshot.get(&session.channel) {
            Some(thread_id) => Some(*thread_id),
            None => match client
                .find_thread_by_channel_and_user_id(session.channel.clone(), user_id)
                .await
            {
                Ok(thread_id) => thread_id,
                Err(error) => {
                    return Err(format!("OpenCode target thread lookup failed: {error}"));
                }
            },
        }
    };

    match lookup_opencode_memories(client, &namespaced, user_id, target_thread).await {
        Ok((new_entries, existing, thread_id)) => {
            let duplicates = namespaced
                .iter()
                .filter_map(|entry| {
                    existing
                        .get(&entry.external_id)
                        .cloned()
                        .map(|memory| (entry.clone(), memory))
                })
                .collect();
            let duplicate_count = namespaced.len() - new_entries.len();
            Ok(OpenCodeDuplicatePreflight {
                entries_stream: crate::source::CanonicalEntryStream::from_vec(new_entries),
                duplicates,
                thread_id,
                duplicate_count,
            })
        }
        Err(error) => Err(format!("OpenCode duplicate lookup failed: {error}")),
    }
}

/// Idempotently apply the session labels to an already-existing OpenCode
/// thread. Returns `Err` with the caller-facing error string on failure.
async fn apply_opencode_labels(
    client: &dyn ImportClient,
    thread_id: ThreadId,
    labels: &[String],
) -> Result<(), String> {
    if labels.is_empty() {
        return Ok(());
    }
    client
        .add_labels(AddLabelsRequest {
            thread_id: Some(thread_id),
            labels: labels.to_vec(),
        })
        .await
        .map_err(|error| format!("ThreadService.AddLabels failed: {error}"))
}

async fn rewire_opencode_duplicates(
    client: &dyn ImportClient,
    duplicates: &[(CanonicalEntry, ExistingMemory)],
    thread_id: ThreadId,
    user_id: i64,
    result: &mut CanonicalSessionResult,
) -> Result<(), String> {
    for (entry, existing) in duplicates {
        // A shared memory must not be rewritten from one session's view.
        if !existing.parent_ids_empty || existing.thread_ids.len() != 1 {
            continue;
        }
        if entry.parent_external_ids.is_empty() {
            continue;
        }
        let mut parent_ids = Vec::new();
        let mut seen = HashSet::new();
        for external_id in &entry.parent_external_ids {
            let Some(found) = client
                .find_memory_by_external_id(external_id.clone())
                .await
                .map_err(|e| format!("OpenCode parent lookup failed: {e}"))?
            else {
                continue;
            };
            let memory = found
                .memory
                .ok_or_else(|| "exact parent lookup returned no memory".to_string())?;
            let data = memory
                .data
                .ok_or_else(|| "exact parent lookup returned no memory data".to_string())?;
            if data.external_id.as_deref() != Some(external_id.as_str()) {
                return Err(format!(
                    "exact parent lookup returned a different external_id for {external_id}"
                ));
            }
            if data.user_id.map(|id| id.value) != Some(user_id) {
                return Err(format!(
                    "OpenCode parent collision belongs to another user for {external_id}"
                ));
            }
            if !data.thread_ids.iter().any(|id| id.value == thread_id.value) {
                continue;
            }
            let Some(parent_id) = memory.id else {
                return Err("exact parent lookup returned no memory id".to_string());
            };
            if seen.insert(parent_id.value) {
                parent_ids.push(parent_id);
            }
        }
        if parent_ids.is_empty() {
            continue;
        }
        let response = client
            .update_memory_parents(UpdateMemoryParentsRequest {
                thread_id: Some(thread_id),
                memory_id: Some(existing.memory_id),
                parent_ids,
                force_overwrite_when_shared: false,
                force_overwrite_when_non_empty: false,
            })
            .await
            .map_err(|e| {
                format!(
                    "UpdateMemoryParents for OpenCode duplicate {} failed: {e}",
                    entry.external_id
                )
            })?;
        if response.rewired {
            result.memories_rewired += 1;
        }
    }
    Ok(())
}

/// Shared by `run_import` and `flush_chunk`: for every non-created outcome
/// whose parent_ids were empty and got resolved server-side, issue a
/// rewire RPC so the memory picks up its resolved parents. Each rewire
/// targets a different memory_id, so they're independent on the server
/// side and safe to run with bounded concurrency. Updates
/// `result.memories_imported` / `memories_skipped_duplicate` /
/// `memories_rewired` in place; returns `Err` on the first RPC failure.
async fn run_rewire_pass(
    client: &dyn ImportClient,
    entries: &[CanonicalEntry],
    outcomes: &[protobuf::llm_memory::service::AddMemoryOutcome],
    batch_thread_id: ThreadId,
    result: &mut CanonicalSessionResult,
) -> Result<(), String> {
    let mut rewire_jobs: Vec<RewireJob> = Vec::new();
    for (entry, outcome) in entries.iter().zip(outcomes.iter()) {
        if outcome.created {
            result.memories_imported += 1;
            continue;
        }
        result.memories_skipped_duplicate += 1;
        if !outcome.existing_parent_ids_empty || outcome.resolved_parent_ids.is_empty() {
            continue;
        }
        let Some(memory_id) = outcome.memory_id else {
            continue;
        };
        rewire_jobs.push(RewireJob {
            external_id: entry.external_id.clone(),
            memory_id,
            request: UpdateMemoryParentsRequest {
                thread_id: Some(batch_thread_id),
                memory_id: Some(memory_id),
                parent_ids: outcome.resolved_parent_ids.clone(),
                force_overwrite_when_shared: false,
                force_overwrite_when_non_empty: false,
            },
        });
    }

    let rewire_outcomes: Vec<(
        String,
        MemoryId,
        anyhow::Result<UpdateMemoryParentsResponse>,
    )> = futures::stream::iter(rewire_jobs.into_iter().map(|job| async move {
        let r = client.update_memory_parents(job.request).await;
        (job.external_id, job.memory_id, r)
    }))
    .buffer_unordered(REWIRE_CONCURRENCY)
    .collect()
    .await;

    for (external_id, memory_id, r) in rewire_outcomes {
        match r {
            Ok(resp) if resp.rewired => {
                result.memories_rewired += 1;
            }
            Ok(resp) => {
                let reason = SkipReason::try_from(resp.skip_reason)
                    .unwrap_or(SkipReason::UpdateMemoryParentsSkipUnspecified);
                tracing::warn!(
                    external_id = external_id.as_str(),
                    memory_id = memory_id.value,
                    skip_reason = ?reason,
                    "UpdateMemoryParents skipped by server-side guard"
                );
            }
            Err(e) => {
                return Err(format!("UpdateMemoryParents for {external_id} failed: {e}"));
            }
        }
    }
    Ok(())
}

/// Per-RPC sizing limits forwarded into the streaming importer. Carried
/// as a value so CLI flags / env config can shrink the defaults — a
/// large `max_entries` / `max_bytes` means a single cnpg transaction
/// can hold locks for tens of seconds, so callers running against a
/// loaded server typically want smaller limits than the legacy
/// `(500, 12 MiB)` constants.
#[derive(Debug, Clone, Copy)]
pub struct ChunkLimits {
    pub max_entries: usize,
    pub max_bytes: usize,
}

impl Default for ChunkLimits {
    fn default() -> Self {
        Self {
            max_entries: CHUNK_MAX_ENTRIES,
            max_bytes: CHUNK_MAX_BYTES,
        }
    }
}

/// Per-entry filter applied by the runner — `--since` only. Source
/// filters (e.g. `--include-types`) have already been consumed by the
/// time `run_import` sees `entries`. Spec §4.3.
pub fn is_entry_importable(entry: &CanonicalEntry, since_millis: Option<i64>) -> bool {
    match since_millis {
        Some(since) => entry.timestamp_ms >= since,
        None => true,
    }
}

/// Tracking record for a single session import. Mirrors the legacy
/// shape so summaries can be emitted unchanged.
#[derive(Debug, Default, Clone)]
pub struct CanonicalSessionResult {
    pub session_id: String,
    pub session_key: Option<String>,
    pub thread_id: Option<i64>,
    pub thread_created: bool,
    pub memories_imported: usize,
    pub memories_skipped_duplicate: usize,
    pub memories_skipped_filtered: usize,
    pub memories_deferred: usize,
    pub memories_skipped_ignored: usize,
    pub memories_skipped_warning: usize,
    pub ignored_by_reason: std::collections::BTreeMap<String, usize>,
    pub warning_exclusions_by_reason: std::collections::BTreeMap<String, usize>,
    pub warnings_by_reason: std::collections::BTreeMap<String, usize>,
    pub memories_rewired: usize,
    /// Attachments turned into `media_object` references via
    /// `MediaService`. Counts Upload + Register successes.
    pub media_objects_linked: usize,
    /// Attachments whose `MediaService` resolution failed; the entry is
    /// still imported but `metadata.attachment` is left intact.
    pub media_link_failures: usize,
    pub error: Option<String>,
    pub skip_reason: Option<String>,
}

impl CanonicalSessionResult {
    fn bump_reason(&mut self, reason: &str, count: usize) {
        *self
            .warnings_by_reason
            .entry(reason.to_string())
            .or_default() += count;
    }

    fn warnings_by_reason_entry(&mut self, reason: &str) {
        self.bump_reason(reason, 1);
    }

    /// Entries confirmed present by this logical session attempt. Duplicate
    /// upserts count because a retry may be completing a thread created by a
    /// previous partial attempt.
    pub fn cumulative_imported_count(&self) -> usize {
        self.memories_imported + self.memories_skipped_duplicate
    }
}

fn apply_source_diagnostics(result: &mut CanonicalSessionResult, diagnostics: SourceDiagnostics) {
    result.memories_deferred += diagnostics.deferred;
    for (reason, count) in diagnostics.ignored_by_reason {
        result.memories_skipped_ignored += count;
        *result.ignored_by_reason.entry(reason).or_default() += count;
    }
    for (reason, count) in diagnostics.warning_exclusions_by_reason {
        result.memories_skipped_warning += count;
        *result
            .warning_exclusions_by_reason
            .entry(reason)
            .or_default() += count;
    }
    for (reason, count) in diagnostics.warnings_by_reason {
        result.bump_reason(&reason, count);
    }
}

fn emit_session_completion(
    result: &mut CanonicalSessionResult,
    event_sink: Option<&dyn ImportEventSink>,
) {
    let (Some(sink), Some(session_key), Some(thread_id)) =
        (event_sink, result.session_key.as_deref(), result.thread_id)
    else {
        return;
    };
    let success = result.error.is_none();
    if let Err(error) = sink.session_completed(
        session_key,
        thread_id,
        result.cumulative_imported_count(),
        success,
    ) {
        result.error = Some(error);
    }
}

/// Shared per-session import path.
pub async fn run_import(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    entries: Vec<CanonicalEntry>,
    source_filtered_count: usize,
    since_millis: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
) -> CanonicalSessionResult {
    if session.source_id == "opencode" {
        return run_import_streaming(
            client,
            session,
            CanonicalEntryStream::from_vec(entries),
            source_filtered_count,
            since_millis,
            user_id,
            extra_labels,
            ChunkLimits::default(),
        )
        .await;
    }
    run_import_with_event_sink(
        client,
        session,
        entries,
        source_filtered_count,
        since_millis,
        user_id,
        extra_labels,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_import_with_event_sink(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    mut entries: Vec<CanonicalEntry>,
    source_filtered_count: usize,
    since_millis: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    event_sink: Option<&dyn ImportEventSink>,
) -> CanonicalSessionResult {
    let mut result = CanonicalSessionResult {
        session_id: session.session_id.clone(),
        session_key: Some(session.channel.clone()),
        memories_skipped_filtered: source_filtered_count,
        ..Default::default()
    };

    // Stable sort by (import_order, timestamp_ms) per spec §6.2.
    entries.sort_by_key(|e| (e.import_order, e.timestamp_ms));

    // Drop entries the runner-side `--since` filter rejects.
    let total = entries.len();
    let mut importable: Vec<CanonicalEntry> = entries
        .into_iter()
        .filter(|e| is_entry_importable(e, since_millis))
        .collect();
    let since_filtered = total.saturating_sub(importable.len());
    result.memories_skipped_filtered += since_filtered;
    if importable.is_empty() {
        return result;
    }

    for entry in &mut importable {
        namespace_entry_external_ids(entry, &session.source_id, user_id);
    }

    let labels = build_labels(session, extra_labels);
    // The existing-external_id pre-fetch only matters when there is an
    // attachment to (potentially) skip. Pre-fetched external_ids belong to
    // memories that `upsert_by_external_id` will reuse; resolving their
    // attachments would re-upload bytes and leak orphan url-backed
    // media_object rows on every re-import. A failure here only affects
    // media optimisation; namespace resolution has already succeeded.
    let has_attachment = importable.iter().any(|e| e.canonical.attachment.is_some());
    let already_imported: std::collections::HashSet<String> = if has_attachment {
        match fetch_existing_external_ids(
            client,
            importable
                .iter()
                .filter(|entry| entry.canonical.attachment.is_some())
                .map(|entry| entry.external_id.as_str()),
        )
        .await
        {
            Ok(external_ids) => external_ids,
            Err(e) => {
                tracing::warn!(
                    "existing-external_id pre-fetch failed, resolving all \
                     attachments (may re-upload on re-import): {e}"
                );
                std::collections::HashSet::new()
            }
        }
    } else {
        std::collections::HashSet::new()
    };

    // Resolve each NEW entry's `canonical.attachment` into a
    // `media_object` via MediaService before the batch, so the memory
    // carries a first-class `media_object_id` instead of an embedded
    // base64/url JSON blob. Best-effort per entry: a single image
    // failing must not abort the whole session — that entry is still
    // imported with `metadata.attachment` left intact (the offline
    // attachment migration can still convert it later).
    let (media_ids, media_ok, media_failed) =
        resolve_media_objects(client, &importable, &already_imported, &session.source_id).await;
    result.media_objects_linked += media_ok;
    result.media_link_failures += media_failed;

    // Pre-build proto memories so we can size each request precisely
    // and split chunks on either the entry-count cap or the encoded
    // byte budget — long transcripts / attachment payloads can fill
    // the 16 MiB tonic frame well below 500 entries (open-issue A5).
    let pb_entries: Vec<PbBatchMemoryInput> = importable
        .iter()
        .map(|entry| PbBatchMemoryInput {
            memory: Some(build_memory_data(session, entry, user_id, &media_ids)),
            parent_external_ids: entry.parent_external_ids.clone(),
        })
        .collect();
    let chunks: Vec<std::ops::Range<usize>> = split_chunks(&pb_entries);
    let total_chunks = chunks.len();

    let mut current_thread_id: Option<ThreadId> = None;

    // Owner-local source identity for the suppression gate / revival.
    // Empty for plain / non-source sessions (no adapter observation).
    let source_identity = source_identity_to_proto(session, user_id);

    for (chunk_idx0, range) in chunks.iter().enumerate() {
        let chunk_idx = chunk_idx0 + 1;
        let is_last = chunk_idx == total_chunks;
        let chunk = &importable[range.clone()];
        let pb_memories: Vec<PbBatchMemoryInput> = pb_entries[range.clone()].to_vec();

        let thread_target = match current_thread_id {
            Some(tid) => PbThreadTarget::ExistingThreadId(tid),
            None => PbThreadTarget::Upsert(ThreadUpsertByChannel {
                thread_data: Some(build_thread_data(session, user_id)),
            }),
        };

        let request = AddMemoriesBatchRequest {
            thread_target: Some(thread_target),
            memories: pb_memories,
            upsert_by_external_id: true,
            labels: if is_last { labels.clone() } else { Vec::new() },
            source_identity: source_identity.clone(),
            explicit_override: false,
        };

        let response = match client.add_memories_batch(request).await {
            Ok(r) => r,
            Err(e) => {
                result.error = Some(format!(
                    "AddMemoriesBatch (chunk {chunk_idx}/{total_chunks}) failed: {e}"
                ));
                return result;
            }
        };

        // A forbidden deletion marker suppressed the session: report it
        // without counting a created thread.
        if response.suppressed {
            result.skip_reason = Some("suppressed_by_deletion_marker".to_string());
            return result;
        }

        let Some(batch_thread_id) = response.thread_id else {
            result.error = Some("AddMemoriesBatch response missing thread_id".to_string());
            return result;
        };
        if current_thread_id.is_none() {
            result.thread_id = Some(batch_thread_id.value);
            result.thread_created = response.thread_created;
            if response.thread_created
                && let Some(sink) = event_sink
                && let Err(error) = sink.thread_created(&session.channel, batch_thread_id.value)
            {
                result.error = Some(error);
                return result;
            }
        }
        current_thread_id = Some(batch_thread_id);

        if response.outcomes.len() != chunk.len() {
            result.error = Some(format!(
                "AddMemoriesBatch returned {} outcomes for {} inputs (chunk {chunk_idx})",
                response.outcomes.len(),
                chunk.len()
            ));
            return result;
        }

        if let Err(e) = run_rewire_pass(
            client,
            chunk,
            &response.outcomes,
            batch_thread_id,
            &mut result,
        )
        .await
        {
            result.error = Some(e);
            return result;
        }
    }

    finalize_thread_group_observations(client, session, user_id, &mut result).await;

    result
}

/// ThreadGroup observation ingestion (design 8.2). Runs after a successful
/// content write, or after exact duplicate validation for OpenCode-only
/// sessions. Suppressed/error outcomes never reach it. Best-effort relative
/// to the memory import: an RPC failure is surfaced but the memories stay
/// imported (the next run is idempotent and retries the observation). Skipped
/// entirely by the per-run `--no-thread-group-writes` gate.
async fn finalize_thread_group_observations(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    user_id: i64,
    result: &mut CanonicalSessionResult,
) {
    let Some(identity) = canonical_source_identity(session) else {
        return;
    };
    if !thread_group_writes_enabled()
        || result.error.is_some()
        || result.skip_reason.is_some()
        || (session.thread_group_observations.is_empty()
            && identity.identity_scope == ThreadGroupIdentityScope::Unknown)
    {
        return;
    }
    let Some(thread_id) = result.thread_id else {
        return;
    };
    if let Err(error) = record_thread_group_observations(client, session, user_id, thread_id).await
        && result.error.is_none()
    {
        result.error = Some(error);
    }
}

/// Map the adapter's source-independent observations to the gRPC import
/// contract. The typed owner is assigned here; the adapter deliberately
/// leaves it unset.
async fn record_thread_group_observations(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    user_id: i64,
    thread_id: i64,
) -> std::result::Result<(), String> {
    let Some(subject) = canonical_source_identity(session) else {
        return Ok(());
    };
    let subject_proto = endpoint_to_proto(subject, user_id);
    let observations = session
        .thread_group_observations
        .iter()
        .map(|observation| ThreadGroupObservationInput {
            subject: Some(endpoint_to_proto(&observation.subject, user_id)),
            candidate_parent: observation
                .candidate_parent
                .as_ref()
                .map(|parent| endpoint_to_proto(parent, user_id)),
            relation_kind: observation.relation_kind.clone(),
            evidence_kind: evidence_kind_to_proto(observation.evidence_kind),
            polarity: polarity_to_proto(observation.polarity),
            source_confidence: Some(confidence_to_proto(observation.confidence)),
            adapter_version: observation.adapter_version.to_string(),
            source_record_ref: observation.source_record_ref.clone(),
            observed_at: None,
        })
        .collect();
    let request = RecordThreadGroupObservationsRequest {
        subject_thread_id: thread_id,
        subject: Some(subject_proto),
        observations,
        operation_id: None,
        group_owner_user_id: Some(client.group_owner_user_id(user_id)),
    };
    client
        .record_thread_group_observations(request)
        .await
        .map_err(|e| format!("RecordThreadGroupObservations failed: {e}"))?;
    Ok(())
}

fn endpoint_to_proto(identity: &ThreadGroupSourceIdentity, user_id: i64) -> ThreadGroupEndpoint {
    let (identity_scope_known, identity_scope) = match &identity.identity_scope {
        ThreadGroupIdentityScope::Known(value) => (true, value.clone()),
        ThreadGroupIdentityScope::Unknown => (false, String::new()),
    };
    ThreadGroupEndpoint {
        source: identity.source.clone(),
        identity_scope_known,
        identity_scope,
        owner_scope: format!("user:{user_id}"),
        native_id: identity.native_id.clone(),
        user_id: Some(user_id),
    }
}

fn canonical_source_identity(session: &CanonicalSession) -> Option<&ThreadGroupSourceIdentity> {
    session.source_identity.as_ref().or_else(|| {
        session
            .thread_group_observations
            .first()
            .map(|observation| &observation.subject)
    })
}

fn source_identity_to_proto(
    session: &CanonicalSession,
    user_id: i64,
) -> Option<ThreadGroupEndpoint> {
    canonical_source_identity(session).map(|identity| endpoint_to_proto(identity, user_id))
}

fn evidence_kind_to_proto(kind: ThreadGroupEvidenceKind) -> i32 {
    (match kind {
        ThreadGroupEvidenceKind::SourceEvent => ThreadEvidenceKind::SourceEvent,
        ThreadGroupEvidenceKind::SourceField => ThreadEvidenceKind::SourceField,
        ThreadGroupEvidenceKind::NegativeOrConflict => ThreadEvidenceKind::NegativeOrConflict,
    }) as i32
}

fn polarity_to_proto(polarity: ThreadGroupPolarity) -> i32 {
    (match polarity {
        ThreadGroupPolarity::Supports => ThreadObservationPolarity::Supports,
        ThreadGroupPolarity::Negates => ThreadObservationPolarity::Negates,
        ThreadGroupPolarity::Conflicts => ThreadObservationPolarity::Conflicts,
    }) as i32
}

fn confidence_to_proto(confidence: ThreadGroupConfidence) -> i32 {
    (match confidence {
        ThreadGroupConfidence::Exact => ThreadEvidenceConfidence::Exact,
        ThreadGroupConfidence::Strong => ThreadEvidenceConfidence::Strong,
        ThreadGroupConfidence::Heuristic => ThreadEvidenceConfidence::Heuristic,
        ThreadGroupConfidence::Unsupported => ThreadEvidenceConfidence::Unsupported,
    }) as i32
}

/// Split a sequence of pre-built `BatchMemoryInput`s into ranges that
/// each fit within `CHUNK_MAX_ENTRIES` entries and `CHUNK_MAX_BYTES`
/// encoded bytes. A single oversized entry (> CHUNK_MAX_BYTES on its
/// own) is emitted as a one-element chunk so the server is the one
/// that surfaces the size violation as ResourceExhausted, instead of
/// the importer silently dropping it.
fn split_chunks(entries: &[PbBatchMemoryInput]) -> Vec<std::ops::Range<usize>> {
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut acc_bytes = 0usize;
    let mut acc_count = 0usize;
    for (i, entry) in entries.iter().enumerate() {
        let size = entry.encoded_len();
        let exceeds_count = acc_count + 1 > CHUNK_MAX_ENTRIES;
        let exceeds_bytes = acc_count > 0 && acc_bytes + size > CHUNK_MAX_BYTES;
        if exceeds_count || exceeds_bytes {
            chunks.push(start..i);
            start = i;
            acc_bytes = 0;
            acc_count = 0;
        }
        acc_bytes += size;
        acc_count += 1;
    }
    if start < entries.len() {
        chunks.push(start..entries.len());
    }
    chunks
}

fn build_labels(session: &CanonicalSession, extra: &[String]) -> Vec<String> {
    let mut labels: Vec<String> = session
        .source_labels
        .iter()
        .map(|label| strip_nul_bytes(label))
        .collect();
    for l in extra {
        let label = strip_nul_bytes(l);
        if !labels.contains(&label) {
            labels.push(label);
        }
    }
    labels.retain(|l| !l.is_empty());
    labels
}

fn build_thread_data(session: &CanonicalSession, user_id: i64) -> ThreadData {
    ThreadData {
        default_system_memory_id: None,
        user_id: Some(UserId { value: user_id }),
        description: session.description.as_deref().map(strip_nul_bytes),
        channel: Some(session.channel.clone()),
        embedding: None,
        embedding_dim: None,
        // Thread audit timestamps are assigned by the server. Source
        // history timestamps remain on the session metadata and memories.
        created_at: 0,
        updated_at: 0,
        first_message_at: None,
        last_message_at: None,
        labels: Vec::new(),
        metadata: session.thread_metadata.as_ref().map(|value| {
            let mut value = value.clone();
            strip_nul_bytes_in_value(&mut value);
            serde_json::to_string(&value).expect("thread metadata is JSON")
        }),
        memory_kind: MemoryKind::Raw as i32,
    }
}

/// Bounded concurrency for pre-batch media resolution. Mirrors
/// `REWIRE_CONCURRENCY`: independent per-entry RPCs, amortise RTT.
const MEDIA_CONCURRENCY: usize = 8;

/// Resolve every importable entry's `canonical.attachment` into a
/// `media_object` via `MediaService`. Returns `(external_id ->
/// media_object_id, ok_count, fail_count)`. Best-effort per entry: a
/// failed Upload/Register is logged and omitted from the map so
/// `build_memory_data` keeps `metadata.attachment` for that entry.
///
/// Entries whose `external_id` is in `already_imported` are skipped:
/// `AddMemoriesBatch` reuses the existing memory (`upsert_by_external_id`)
/// and never consumes a freshly-created `media_object_id`, so resolving
/// them would only re-upload bytes and leak orphan url-backed
/// `media_object` rows on every re-import.
async fn resolve_media_objects(
    client: &dyn ImportClient,
    importable: &[CanonicalEntry],
    already_imported: &std::collections::HashSet<String>,
    source_id: &str,
) -> (std::collections::HashMap<String, i64>, usize, usize) {
    use crate::common::canonical::{AttachmentImportAction, classify_attachment_for_source};

    // Base64 is kept ENCODED until the worker picks the job up so at
    // most MEDIA_CONCURRENCY decoded images are resident at once, not the
    // whole session's worth (an inline-image-heavy transcript would
    // otherwise spike memory by Σ all decoded images).
    enum Job {
        Upload {
            external_id: String,
            header: crate::client::UploadMediaHeader,
            data_b64: String,
        },
        Register {
            external_id: String,
            params: crate::client::RegisterMediaUrl,
        },
    }

    let mut jobs: Vec<Job> = Vec::new();
    for entry in importable {
        // Skip already-imported entries: their memory is reused, so the
        // media_object_id would be created but never referenced.
        if already_imported.contains(&entry.external_id) {
            continue;
        }
        let Some(att) = entry.canonical.attachment.as_ref() else {
            continue;
        };
        match classify_attachment_for_source(source_id, att) {
            AttachmentImportAction::Upload {
                kind,
                media_type,
                data_b64,
                width,
                height,
                alt,
            } => jobs.push(Job::Upload {
                external_id: entry.external_id.clone(),
                header: crate::client::UploadMediaHeader {
                    kind,
                    media_type,
                    alt,
                    width,
                    height,
                },
                data_b64,
            }),
            AttachmentImportAction::RegisterUrl {
                kind,
                media_type,
                url,
                width,
                height,
                alt,
            } => jobs.push(Job::Register {
                external_id: entry.external_id.clone(),
                params: crate::client::RegisterMediaUrl {
                    kind,
                    media_type,
                    url,
                    alt,
                    width,
                    height,
                },
            }),
            AttachmentImportAction::KeepAsIs => {}
        }
    }

    if jobs.is_empty() {
        return (std::collections::HashMap::new(), 0, 0);
    }

    let results: Vec<(String, Result<i64>)> =
        futures::stream::iter(jobs.into_iter().map(|job| async move {
            match job {
                Job::Upload {
                    external_id,
                    header,
                    data_b64,
                } => {
                    use base64::Engine;
                    let r = match base64::engine::general_purpose::STANDARD
                        .decode(data_b64.as_bytes())
                    {
                        Ok(bytes) => client.upload_media(header, bytes).await,
                        Err(e) => Err(anyhow::anyhow!("attachment base64 decode failed: {e}")),
                    };
                    (external_id, r)
                }
                Job::Register {
                    external_id,
                    params,
                } => {
                    let r = client.register_media_url(params).await;
                    (external_id, r)
                }
            }
        }))
        .buffer_unordered(MEDIA_CONCURRENCY)
        .collect()
        .await;

    let mut map = std::collections::HashMap::new();
    let mut ok = 0usize;
    let mut failed = 0usize;
    for (external_id, r) in results {
        match r {
            Ok(id) => {
                map.insert(external_id, id);
                ok += 1;
            }
            Err(e) => {
                tracing::warn!(
                    external_id = external_id.as_str(),
                    "MediaService resolution failed, keeping metadata.attachment: {e}"
                );
                failed += 1;
            }
        }
    }
    (map, ok, failed)
}

fn build_memory_data(
    session: &CanonicalSession,
    entry: &CanonicalEntry,
    user_id: i64,
    media_ids: &std::collections::HashMap<String, i64>,
) -> MemoryData {
    // Step 1: copy the source-parser free namespace (entry.metadata)
    // and drop any reserved key it accidentally carries. NUL-laundered
    // variants ("source\0", "kind\0", …) must also be dropped because
    // `strip_nul_bytes_in_value` below would otherwise normalize them
    // to plain "source"/"kind"/… and collide with the fixed metadata
    // written in Step 3, defeating run_import's last-writer-wins rule.
    const RESERVED: &[&str] = &["source", "kind", "session", "tool", "attachment", "raw"];
    let mut metadata = entry.metadata.clone();
    metadata.retain(|k, _| {
        let normalized = if k.contains('\u{0000}') {
            std::borrow::Cow::Owned(k.replace('\u{0000}', ""))
        } else {
            std::borrow::Cow::Borrowed(k.as_str())
        };
        if RESERVED.contains(&normalized.as_ref()) {
            debug_assert!(
                false,
                "entry.metadata contained reserved key '{}' (raw '{}'); parser must use \
                 entry.canonical / fixed merge instead (spec §4.2.2.2)",
                normalized, k
            );
            tracing::warn!(
                key = normalized.as_ref(),
                raw_key = k.as_str(),
                external_id = entry.external_id.as_str(),
                "dropping reserved key from entry.metadata"
            );
            false
        } else {
            true
        }
    });

    // Step 2: merge canonical addons (helper-only top-level keys).
    if let Some(t) = entry.canonical.tool.as_ref() {
        metadata.insert("tool".to_string(), t.clone());
    }
    // When the attachment was resolved to a media_object the image is
    // first-class media — do NOT also embed the attachment JSON.
    // Unresolved attachments keep the blob so the offline attachment
    // migration can still convert them.
    let resolved_media_object_id = media_ids.get(&entry.external_id).copied();
    if resolved_media_object_id.is_none()
        && let Some(a) = entry.canonical.attachment.as_ref()
    {
        metadata.insert("attachment".to_string(), a.clone());
    }
    if let Some(r) = entry.canonical.raw.as_ref() {
        metadata.insert("raw".to_string(), serde_json::Value::Object(r.clone()));
    }

    // Step 3: fixed keys are written last so run_import always wins.
    metadata.insert(
        "source".to_string(),
        serde_json::Value::String(session.source_id.clone()),
    );
    metadata.insert(
        "kind".to_string(),
        serde_json::Value::String(entry.kind_tag.to_string()),
    );
    metadata.insert("session".to_string(), session.source_metadata.clone());

    // Strip NULs on the Value tree (see `strip_nul_bytes_in_value`
    // for the SQLSTATE 22021 / serde escape rationale).
    let mut metadata_value = serde_json::Value::Object(metadata);
    crate::common::canonical::strip_nul_bytes_in_value(&mut metadata_value);
    let metadata_str = serde_json::to_string(&metadata_value)
        .expect("Value::Object keys are strings; serialization is infallible");

    MemoryData {
        // parent_ids is intentionally empty; the server resolves
        // parent_external_ids server-side (spec §3.2.2). Sending a
        // non-empty parent_ids returns InvalidArgument from the server.
        parent_ids: Vec::new(),
        user_id: Some(UserId { value: user_id }),
        content: crate::common::canonical::strip_nul_bytes(&entry.content),
        content_type: entry.content_type as i32,
        params: None,
        metadata: Some(metadata_str),
        created_at: entry.timestamp_ms,
        updated_at: entry.timestamp_ms,
        role: entry.role as i32,
        external_id: Some(entry.external_id.clone()),
        media_object_id: resolved_media_object_id
            .map(|value| protobuf::llm_memory::data::MediaObjectId { value }),
        thread_ids: Vec::new(),
        memory_kind: MemoryKind::Raw as i32,
    }
}

pub(crate) fn namespace_external_id(
    source_id: &str,
    creator_user_id: i64,
    external_id: &str,
) -> String {
    let namespace =
        namespace_for_external_id(source_id, external_id).unwrap_or_else(|| source_id.to_string());
    owner_scoped(
        &namespace,
        creator_user_id,
        external_id,
        EXTERNAL_ID_MAX_BYTES,
    )
    .expect("the configured external ID limit must fit a SHA-256 identifier")
}

async fn fetch_existing_external_ids(
    client: &dyn ImportClient,
    external_ids: impl IntoIterator<Item = &str>,
) -> Result<std::collections::HashSet<String>> {
    const EXTERNAL_ID_LOOKUP_CONCURRENCY: usize = 16;
    let requested_ids = external_ids
        .into_iter()
        .map(str::to_owned)
        .collect::<std::collections::HashSet<_>>();
    let found =
        futures::stream::iter(requested_ids.into_iter().map(|external_id| async move {
            client.find_memory_by_external_id(external_id).await
        }))
        .buffer_unordered(EXTERNAL_ID_LOOKUP_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;
    Ok(found
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            entry
                .memory
                .and_then(|memory| memory.data)
                .and_then(|data| data.external_id)
        })
        .collect())
}

fn namespace_entry_external_ids(entry: &mut CanonicalEntry, source_id: &str, creator_user_id: i64) {
    entry.external_id = namespace_external_id(source_id, creator_user_id, &entry.external_id);
    entry.parent_external_ids = entry
        .parent_external_ids
        .iter()
        .map(|external_id| namespace_external_id(source_id, creator_user_id, external_id))
        .collect();
}

/// Iterate every input the source yields and feed each session through
/// the streaming importer. Pass `client = None` for dry-run mode —
/// sessions are parsed and counted but no RPC is issued and the result
/// reflects the "no thread created, no memory written" reality so
/// summary output does not falsely advertise side effects.
///
/// This is the new default entry point: sources that emit `ImportStream`
/// (claude_code, codex, plain) flow directly into `run_import_streaming`
/// without ever materialising the `Vec<CanonicalEntry>`. Callers that
/// still need the prune collector should use
/// `run_all_with_entry_collector` instead.
pub async fn run_all<S: ChatSource>(
    source: &S,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    since_millis_with_margin: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    chunk_limits: ChunkLimits,
) -> Result<Vec<CanonicalSessionResult>> {
    run_all_with_event_sink(
        source,
        client,
        since_millis,
        since_millis_with_margin,
        user_id,
        extra_labels,
        chunk_limits,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_all_with_event_sink<S: ChatSource>(
    source: &S,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    since_millis_with_margin: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    chunk_limits: ChunkLimits,
    event_sink: Option<&dyn ImportEventSink>,
) -> Result<Vec<CanonicalSessionResult>> {
    // OpenCode resolves a thread per session by channel. Snapshot the
    // user's `channel -> thread_id` map once per run so `--all-sessions`
    // does not re-stream every user thread per session. The snapshot is
    // advisory: misses fall back to the per-channel lookup, so a thread
    // created mid-run by this same import is still resolved correctly.
    let thread_channel_snapshot = match client {
        Some(c) => match c.find_thread_channels_by_user_id(user_id).await {
            Ok(map) => map,
            // Snapshot failure must not abort the run; the per-channel
            // lookup remains the authoritative fallback.
            Err(error) => {
                tracing::warn!(
                    user_id,
                    "thread channel snapshot failed, falling back to per-session lookups: {error}"
                );
                HashMap::new()
            }
        },
        None => HashMap::new(),
    };
    let inputs = source.discover()?;
    let entry_since = source.entry_since(since_millis);
    let mut results = Vec::with_capacity(inputs.len());
    for input in &inputs {
        let mut res = run_session_dispatch(
            source,
            input,
            client,
            entry_since,
            since_millis_with_margin,
            user_id,
            extra_labels,
            chunk_limits,
            event_sink,
            &thread_channel_snapshot,
        )
        .await;
        emit_session_completion(&mut res, event_sink);
        results.push(res);
    }
    Ok(results)
}

/// Variant of `run_all` that hands every successful session's
/// `CanonicalEntry` slice to a caller-provided collector before the
/// import call. Phase A's `--prune-missing` uses this to recover the
/// `D_external_id` / `D_path` set without re-walking the filesystem.
///
/// Collector-bearing callers always go through the Vec-materialising
/// path (legacy `run_import`) because the collector contract is "give
/// me every importable entry as a slice"; building the slice from a
/// streaming source still requires draining it first.
pub async fn run_all_with_entry_collector<'a, S: ChatSource>(
    source: &S,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    since_millis_with_margin: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    collect: impl FnMut(&[CanonicalEntry]) + 'a,
) -> Result<Vec<CanonicalSessionResult>> {
    run_all_with_entry_collector_and_event_sink(
        source,
        client,
        since_millis,
        since_millis_with_margin,
        user_id,
        extra_labels,
        collect,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_all_with_entry_collector_and_event_sink<'a, S: ChatSource>(
    source: &S,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    since_millis_with_margin: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    mut collect: impl FnMut(&[CanonicalEntry]) + 'a,
    event_sink: Option<&dyn ImportEventSink>,
) -> Result<Vec<CanonicalSessionResult>> {
    // Note: the collector path still feeds the legacy `run_import`
    // (Vec-based) flow, which has its own hard-coded chunk caps
    // (`CHUNK_MAX_ENTRIES` / `CHUNK_MAX_BYTES`). Plumbing CLI overrides
    // through this path is intentionally deferred — the prune
    // (`--prune-missing`) use case is interactive and unlikely to hit
    // cnpg INSERT contention the way the auto-cron path does.
    let inputs = source.discover()?;
    let entry_since = source.entry_since(since_millis);
    let mut results = Vec::with_capacity(inputs.len());
    for input in &inputs {
        let mut res = run_session_with_collector(
            source,
            input,
            client,
            entry_since,
            since_millis_with_margin,
            user_id,
            extra_labels,
            &mut collect,
            event_sink,
        )
        .await;
        emit_session_completion(&mut res, event_sink);
        results.push(res);
    }
    Ok(results)
}

async fn run_session<S: ChatSource>(
    source: &S,
    input: &S::SessionInput,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    since_millis_with_margin: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
) -> CanonicalSessionResult {
    let entry_since = source.entry_since(since_millis);
    run_session_with_collector(
        source,
        input,
        client,
        entry_since,
        since_millis_with_margin,
        user_id,
        extra_labels,
        &mut |_| {},
        None,
    )
    .await
}

/// Streaming entry point used by collector-less callers (`run_all`).
/// Sources that emit `ImportStream` flow directly into
/// `run_import_streaming` without ever materialising the
/// `Vec<CanonicalEntry>`. Sources still on the `Import` Vec variant
/// are wrapped via `CanonicalEntryStream::from_vec` so the downstream
/// importer code path is the same for both.
/// Shared finish for `Import` / `ImportStream` outcomes: route to the
/// live importer or the matching dry-run counter, then fold the source
/// diagnostics into the session result. `run_import_streaming_with_event_sink`
/// itself routes OpenCode sessions through the duplicate-safe path, so both
/// outcome variants share this single tail.
#[allow(clippy::too_many_arguments)]
async fn finish_session_import(
    client: Option<&dyn ImportClient>,
    session: CanonicalSession,
    entries: crate::source::CanonicalEntryStream,
    source_filtered_count: usize,
    since_millis: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    chunk_limits: ChunkLimits,
    event_sink: Option<&dyn ImportEventSink>,
    thread_channel_snapshot: &HashMap<String, ThreadId>,
    diagnostics: SourceDiagnostics,
) -> CanonicalSessionResult {
    let mut result = match client {
        Some(c) => {
            run_import_streaming_with_event_sink(
                c,
                &session,
                entries,
                source_filtered_count,
                since_millis,
                user_id,
                extra_labels,
                chunk_limits,
                event_sink,
                thread_channel_snapshot,
            )
            .await
        }
        None => dry_run_streaming(&session, entries, source_filtered_count, since_millis),
    };
    apply_source_diagnostics(&mut result, diagnostics);
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_session_dispatch<S: ChatSource>(
    source: &S,
    input: &S::SessionInput,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    since_millis_with_margin: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    chunk_limits: ChunkLimits,
    event_sink: Option<&dyn ImportEventSink>,
    thread_channel_snapshot: &HashMap<String, ThreadId>,
) -> CanonicalSessionResult {
    let outcome = match source.read_session(input, since_millis_with_margin) {
        Ok(o) => o,
        Err(e) => {
            return CanonicalSessionResult {
                session_id: source.input_label(input),
                error: Some(e.to_string()),
                ..Default::default()
            };
        }
    };
    match outcome {
        ReadSessionOutcome::Skipped {
            session_id_hint,
            reason,
            filtered_count,
        } => CanonicalSessionResult {
            session_id: session_id_hint.unwrap_or_else(|| source.input_label(input)),
            memories_skipped_filtered: filtered_count as usize,
            skip_reason: Some(reason),
            ..Default::default()
        },
        ReadSessionOutcome::Import {
            session,
            entries,
            source_filtered_count,
            diagnostics,
        } => {
            // Route through the shared tail so the live/dry-run split and
            // diagnostics folding cannot drift between the two variants.
            finish_session_import(
                client,
                session,
                crate::source::CanonicalEntryStream::from_vec(entries),
                source_filtered_count,
                since_millis,
                user_id,
                extra_labels,
                chunk_limits,
                event_sink,
                thread_channel_snapshot,
                diagnostics,
            )
            .await
        }
        ReadSessionOutcome::ImportStream {
            session,
            entries: stream,
            source_filtered_count_initial,
            diagnostics,
        } => {
            finish_session_import(
                client,
                session,
                stream,
                source_filtered_count_initial,
                since_millis,
                user_id,
                extra_labels,
                chunk_limits,
                event_sink,
                thread_channel_snapshot,
                diagnostics,
            )
            .await
        }
    }
}

fn dry_run_streaming(
    session: &CanonicalSession,
    stream: crate::source::CanonicalEntryStream,
    source_filtered_count_initial: usize,
    since_millis: Option<i64>,
) -> CanonicalSessionResult {
    let mut imported = 0usize;
    let mut filtered = source_filtered_count_initial;
    for item in stream {
        match item {
            StreamItem::Entry(e) => {
                if is_entry_importable(&e, since_millis) {
                    imported += 1;
                } else {
                    filtered += 1;
                }
            }
            StreamItem::Filtered | StreamItem::Warn(_) => filtered += 1,
        }
    }
    CanonicalSessionResult {
        session_id: session.session_id.clone(),
        memories_imported: imported,
        memories_skipped_filtered: filtered,
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_session_with_collector<S: ChatSource>(
    source: &S,
    input: &S::SessionInput,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    since_millis_with_margin: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    collect: &mut dyn FnMut(&[CanonicalEntry]),
    event_sink: Option<&dyn ImportEventSink>,
) -> CanonicalSessionResult {
    let outcome = match source.read_session(input, since_millis_with_margin) {
        Ok(o) => o,
        Err(e) => {
            return CanonicalSessionResult {
                session_id: source.input_label(input),
                error: Some(e.to_string()),
                ..Default::default()
            };
        }
    };

    match outcome {
        ReadSessionOutcome::Import {
            session,
            entries,
            source_filtered_count,
            diagnostics,
        } => {
            run_import_for_vec(
                session,
                entries,
                source_filtered_count,
                diagnostics,
                client,
                since_millis,
                user_id,
                extra_labels,
                collect,
                event_sink,
            )
            .await
        }
        ReadSessionOutcome::ImportStream {
            session,
            entries: stream,
            source_filtered_count_initial,
            diagnostics,
        } => {
            // Collector contract needs `&[CanonicalEntry]`, so the
            // streaming source has to be drained first. Only this
            // (prune) path takes the hit; `run_session_dispatch` —
            // the no-collector route — consumes the stream directly.
            let mut entries: Vec<CanonicalEntry> = Vec::new();
            let mut source_filtered_count = source_filtered_count_initial;
            for item in stream {
                match item {
                    StreamItem::Entry(e) => entries.push(e),
                    StreamItem::Filtered => source_filtered_count += 1,
                    StreamItem::Warn(reason) => {
                        tracing::warn!(
                            session = %source.input_label(input),
                            "source stream warning: {reason}",
                        );
                        source_filtered_count += 1;
                    }
                }
            }
            run_import_for_vec(
                session,
                entries,
                source_filtered_count,
                diagnostics,
                client,
                since_millis,
                user_id,
                extra_labels,
                collect,
                event_sink,
            )
            .await
        }
        ReadSessionOutcome::Skipped {
            session_id_hint,
            reason,
            filtered_count,
        } => CanonicalSessionResult {
            session_id: session_id_hint.unwrap_or_else(|| source.input_label(input)),
            memories_skipped_filtered: filtered_count as usize,
            skip_reason: Some(reason),
            ..Default::default()
        },
    }
}

/// Shared finishing path for the two `ReadSessionOutcome` variants that
/// hand us entries: feed the collector (`--prune-missing` D-set
/// reconstruction) and then call `run_import` (live) or
/// `dry_run_session_result` (dry-run).
#[allow(clippy::too_many_arguments)]
async fn run_import_for_vec(
    session: CanonicalSession,
    entries: Vec<CanonicalEntry>,
    source_filtered_count: usize,
    diagnostics: SourceDiagnostics,
    client: Option<&dyn ImportClient>,
    since_millis: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    collect: &mut dyn FnMut(&[CanonicalEntry]),
    _event_sink: Option<&dyn ImportEventSink>,
) -> CanonicalSessionResult {
    // Collector must only see entries that are actually going to be
    // sent to AddMemoriesBatch. plain's `--prune-missing` keys off
    // `D_external_id` / `D_path` to decide what to delete; if a
    // `--since`-rejected entry leaks into D_path, the prune step will
    // treat the un-imported new version as "same path new content" and
    // wrongly delete the old memory for that file. Mirror
    // `run_import`'s `is_entry_importable` filter.
    if since_millis.is_none() {
        collect(&entries);
    } else {
        let importable: Vec<CanonicalEntry> = entries
            .iter()
            .filter(|e| is_entry_importable(e, since_millis))
            .cloned()
            .collect();
        if !importable.is_empty() {
            collect(&importable);
        }
    }
    // The OpenCode-vs-legacy routing lives in `run_import` so the two
    // entry points cannot drift apart. OpenCode sessions never reach the
    // collector path (plain --prune-missing only), so an empty thread
    // snapshot keeps the per-channel fallback authoritative here.
    let mut result = match client {
        Some(c) => {
            run_import(
                c,
                &session,
                entries,
                source_filtered_count,
                since_millis,
                user_id,
                extra_labels,
            )
            .await
        }
        None => dry_run_session_result(session, entries, source_filtered_count, since_millis),
    };
    apply_source_diagnostics(&mut result, diagnostics);
    result
}

/// Dry-run accounting: count import-eligible entries against the
/// runner-side `--since` filter without contacting the server. Mirrors
/// the legacy direct-DB importer so `--dry-run` summaries report
/// `thread_created = false` / `thread_id = None`.
fn dry_run_session_result(
    session: CanonicalSession,
    entries: Vec<CanonicalEntry>,
    source_filtered_count: usize,
    since_millis: Option<i64>,
) -> CanonicalSessionResult {
    let total = entries.len();
    let importable = entries
        .iter()
        .filter(|e| is_entry_importable(e, since_millis))
        .count();
    let since_filtered = total.saturating_sub(importable);
    CanonicalSessionResult {
        session_id: session.session_id,
        memories_imported: importable,
        memories_skipped_filtered: source_filtered_count + since_filtered,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------
// Streaming import path
//
// Consumes a `CanonicalEntryStream` one entry at a time, building
// `AddMemoriesBatchRequest`s incrementally and flushing each one as
// soon as the configured size cap is reached. Compared with `run_import`
// this avoids holding `Vec<CanonicalEntry>` / `Vec<PbBatchMemoryInput>`
// / per-chunk slice clones simultaneously in memory: the peak working
// set is one in-flight batch plus the entry currently being processed.
//
// Trade-offs vs `run_import`:
//   * `already_imported` is fetched lazily on the first attachment
//     encountered so transcripts without attachments never round-trip
//     `find_memories_by_external_id_prefix`.
//   * Media resolution runs per-entry instead of pre-pass-with-
//     `MEDIA_CONCURRENCY=8` parallelism. The session-wide buffer that
//     held every `data_b64` until upload finished is gone (it could
//     itself reach ~1×S for attachment-heavy sessions).
//   * Ordering: sources are expected to yield entries in the same order
//     they would have appeared after `sort_by_key((import_order,
//     timestamp_ms))`. Each source builds its `import_order` to
//     monotonically follow file position, so the stream order already
//     matches; we re-sort only within the chunk to absorb any tied
//     `import_order` values whose intra-tie order the source can't
//     guarantee (single source code paths assert this elsewhere).

/// Builder that accumulates `(entry, pb_input)` pairs until either the
/// entry-count cap or the encoded-byte cap would be exceeded by the
/// next push, then `take()` moves the contents out to be sent as a
/// single `AddMemoriesBatchRequest`. After `take()` the builder is
/// empty and reusable for the next chunk.
struct ChunkBuilder {
    entries: Vec<CanonicalEntry>,
    inputs: Vec<PbBatchMemoryInput>,
    acc_bytes: usize,
    limits: ChunkLimits,
}

impl ChunkBuilder {
    fn new(limits: ChunkLimits) -> Self {
        Self {
            entries: Vec::new(),
            inputs: Vec::new(),
            acc_bytes: 0,
            limits,
        }
    }

    fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    fn len(&self) -> usize {
        self.inputs.len()
    }

    /// Returns `true` when adding a record of `next_size` encoded bytes
    /// would push the chunk past either cap. A single oversized record
    /// (≥ `max_bytes` alone) still yields `false` so it gets emitted as
    /// a one-element chunk — matching legacy `split_chunks` behaviour,
    /// letting the server surface the size violation as
    /// `ResourceExhausted` rather than silently dropping it.
    fn would_overflow(&self, next_size: usize) -> bool {
        if self.is_empty() {
            return false;
        }
        let exceeds_count = self.inputs.len() + 1 > self.limits.max_entries;
        let exceeds_bytes = self.acc_bytes + next_size > self.limits.max_bytes;
        exceeds_count || exceeds_bytes
    }

    fn push(&mut self, entry: CanonicalEntry, pb: PbBatchMemoryInput, size: usize) {
        self.entries.push(entry);
        self.inputs.push(pb);
        self.acc_bytes += size;
    }

    fn take(&mut self) -> (Vec<CanonicalEntry>, Vec<PbBatchMemoryInput>) {
        self.acc_bytes = 0;
        (
            std::mem::take(&mut self.entries),
            std::mem::take(&mut self.inputs),
        )
    }
}

/// Streaming counterpart of `run_import`. Drains the entry stream into
/// `ChunkBuilder` and sends one `AddMemoriesBatchRequest` per
/// `would_overflow` boundary. Carries the same side effects (label
/// assignment on the last chunk, thread `updated_at` override,
/// `already_imported` set, rewire pass) as `run_import` so callers
/// observe identical results.
#[allow(clippy::too_many_arguments)]
pub async fn run_import_streaming(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    entries_stream: crate::source::CanonicalEntryStream,
    source_filtered_count_initial: usize,
    since_millis: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    chunk_limits: ChunkLimits,
) -> CanonicalSessionResult {
    run_import_streaming_with_event_sink(
        client,
        session,
        entries_stream,
        source_filtered_count_initial,
        since_millis,
        user_id,
        extra_labels,
        chunk_limits,
        None,
        &HashMap::new(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_import_streaming_with_event_sink(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    entries_stream: crate::source::CanonicalEntryStream,
    source_filtered_count_initial: usize,
    since_millis: Option<i64>,
    user_id: i64,
    extra_labels: &[String],
    chunk_limits: ChunkLimits,
    event_sink: Option<&dyn ImportEventSink>,
    thread_channel_snapshot: &HashMap<String, ThreadId>,
) -> CanonicalSessionResult {
    let mut result = CanonicalSessionResult {
        session_id: session.session_id.clone(),
        session_key: Some(session.channel.clone()),
        memories_skipped_filtered: source_filtered_count_initial,
        ..Default::default()
    };

    let labels = build_labels(session, extra_labels);

    // OpenCode is append-only from the importer's perspective. Resolve every
    // external ID before media registration or batch insertion so a replay
    // cannot overwrite an existing memory or re-register its attachment.
    let (entries_stream, opencode_duplicates, opencode_thread) = if session.source_id == "opencode"
    {
        match preflight_opencode_duplicates(
            client,
            session,
            entries_stream,
            user_id,
            thread_channel_snapshot,
        )
        .await
        {
            Ok(preflight) => {
                result.memories_skipped_duplicate += preflight.duplicate_count;
                (
                    preflight.entries_stream,
                    preflight.duplicates,
                    preflight.thread_id,
                )
            }
            Err(error) => {
                result.error = Some(error);
                return result;
            }
        }
    } else {
        (entries_stream, Vec::new(), None)
    };

    // Lazy attachment context: attachment-free transcripts never issue an
    // external-ID lookup. Attachment-bearing entries use exact lookups so a
    // re-import never scans another session's hashed overflow IDs.
    let mut attachment_ctx: Option<AttachmentContext> = None;

    // Streaming state.
    let mut builder = ChunkBuilder::new(chunk_limits);
    let mut current_thread_id: Option<ThreadId> = opencode_thread;
    if let Some(thread_id) = current_thread_id {
        result.thread_id = Some(thread_id.value);
        result.thread_created = false;
    }

    // Source-side warnings are accumulated to `memories_skipped_filtered`
    // — same accounting as `Filtered`, plus a tracing line so operators
    // can audit them.
    for item in entries_stream {
        let entry = match item {
            StreamItem::Entry(e) => e,
            StreamItem::Filtered => {
                result.memories_skipped_filtered += 1;
                continue;
            }
            StreamItem::Warn(reason) => {
                tracing::warn!(
                    session = session.session_id.as_str(),
                    "source stream warning: {reason}"
                );
                result.memories_skipped_filtered += 1;
                continue;
            }
        };

        let mut entry = entry;
        if session.source_id != "opencode" {
            namespace_entry_external_ids(&mut entry, &session.source_id, user_id);
        }

        if !is_entry_importable(&entry, since_millis) {
            result.memories_skipped_filtered += 1;
            continue;
        }

        // Resolve attachment to a `media_object` BEFORE we build the pb
        // input. `build_memory_data` consults the resolved map to keep
        // `metadata.attachment` blob attached only when resolution
        // failed.
        let media_id_for_entry: Option<i64> = if entry.canonical.attachment.is_some() {
            // OpenCode already performed exact lookup for every entry above;
            // repeat lookups would add latency and could turn a transient
            // duplicate-lookup failure into an attachment side effect.
            let already_imported = if session.source_id == "opencode" {
                false
            } else {
                let ctx = attachment_ctx.get_or_insert_default();
                ctx.is_already_imported(client, &entry.external_id).await
            };
            if already_imported {
                None
            } else {
                match resolve_one_media(client, &entry, &session.source_id).await {
                    Some(Ok(id)) => {
                        result.media_objects_linked += 1;
                        Some(id)
                    }
                    Some(Err(e)) => {
                        result.media_link_failures += 1;
                        if session.source_id == "opencode" {
                            result.warnings_by_reason_entry("media_registration_failed");
                        }
                        tracing::warn!(
                            external_id = entry.external_id.as_str(),
                            "MediaService resolution failed, keeping metadata.attachment: {e}"
                        );
                        None
                    }
                    None => None,
                }
            }
        } else {
            None
        };

        let mut media_ids: std::collections::HashMap<String, i64> =
            std::collections::HashMap::with_capacity(1);
        if let Some(id) = media_id_for_entry {
            media_ids.insert(entry.external_id.clone(), id);
        }
        let memory = build_memory_data(session, &entry, user_id, &media_ids);
        let pb = PbBatchMemoryInput {
            memory: Some(memory),
            parent_external_ids: entry.parent_external_ids.clone(),
        };
        let size = pb.encoded_len();

        if builder.would_overflow(size) {
            // Flush the current batch before pushing the new entry.
            // We don't yet know whether this is the last chunk, so
            // labels are deferred until the final request.
            if let Err(err) = flush_chunk(
                client,
                session,
                user_id,
                &mut builder,
                &mut current_thread_id,
                &mut result,
                /*is_last=*/ false,
                0,
                &labels,
                event_sink,
            )
            .await
            {
                result.error = Some(err);
                return result;
            }
            if result.skip_reason.is_some() {
                return result;
            }
        }
        builder.push(entry, pb, size);
    }

    if builder.is_empty() {
        if session.source_id == "opencode"
            && result.memories_skipped_duplicate > 0
            && !thread_group_writes_enabled()
        {
            result.error = Some(
                "ThreadGroup writes are disabled; duplicate-only import cannot check deletion markers"
                    .to_string(),
            );
            return result;
        }
        // Validate/reconcile a duplicate-only source identity before any
        // separately-issued label or parent-rewire side effects.
        finalize_thread_group_observations(client, session, user_id, &mut result).await;
        if result.error.is_some() || result.skip_reason.is_some() {
            return result;
        }

        if session.source_id == "opencode"
            && result.memories_skipped_duplicate > 0
            && let Some(thread_id) = current_thread_id
        {
            if let Err(error) = apply_opencode_labels(client, thread_id, &labels).await {
                result.error = Some(error);
                return result;
            }
            if let Err(error) = rewire_opencode_duplicates(
                client,
                &opencode_duplicates,
                thread_id,
                user_id,
                &mut result,
            )
            .await
            {
                result.error = Some(error);
            }
        }
        return result;
    }

    // The server derives thread extrema from the accepted memories. Source
    // session timestamps are intentionally not sent as thread audit values.
    if let Err(err) = flush_chunk(
        client,
        session,
        user_id,
        &mut builder,
        &mut current_thread_id,
        &mut result,
        /*is_last=*/ true,
        0,
        &labels,
        event_sink,
    )
    .await
    {
        result.error = Some(err);
        return result;
    }
    if result.skip_reason.is_some() {
        return result;
    }
    finalize_thread_group_observations(client, session, user_id, &mut result).await;
    if result.error.is_some() || result.skip_reason.is_some() {
        return result;
    }
    if session.source_id == "opencode"
        && let Some(thread_id) = current_thread_id
        && let Err(error) = rewire_opencode_duplicates(
            client,
            &opencode_duplicates,
            thread_id,
            user_id,
            &mut result,
        )
        .await
    {
        result.error = Some(error);
    }
    result
}

/// Lazy-loaded "this session already has these external_ids on the
/// server" set. Built once per session, on the first attachment-bearing
/// entry.
#[derive(Default)]
struct AttachmentContext {
    already_imported: std::collections::HashSet<String>,
    queried_external_ids: std::collections::HashSet<String>,
}

impl AttachmentContext {
    async fn is_already_imported(&mut self, client: &dyn ImportClient, external_id: &str) -> bool {
        if self.queried_external_ids.insert(external_id.to_string()) {
            match fetch_existing_external_ids(client, [external_id]).await {
                Ok(external_ids) => self.already_imported.extend(external_ids),
                Err(e) => {
                    tracing::warn!(
                        "existing-external_id pre-fetch failed, resolving all \
                         attachments (may re-upload on re-import): {e}"
                    );
                }
            }
        }
        self.already_imported.contains(external_id)
    }
}

/// Per-entry attachment resolution. Returns:
///   - `None` if the attachment is a no-op (`KeepAsIs`)
///   - `Some(Ok(id))` on successful Upload / Register
///   - `Some(Err(_))` on resolution failure (caller keeps
///     `metadata.attachment` and bumps `media_link_failures`).
async fn resolve_one_media(
    client: &dyn ImportClient,
    entry: &CanonicalEntry,
    source_id: &str,
) -> Option<anyhow::Result<i64>> {
    use crate::common::canonical::{AttachmentImportAction, classify_attachment_for_source};
    let att = entry.canonical.attachment.as_ref()?;
    match classify_attachment_for_source(source_id, att) {
        AttachmentImportAction::Upload {
            kind,
            media_type,
            data_b64,
            width,
            height,
            alt,
        } => {
            use base64::Engine;
            let r = match base64::engine::general_purpose::STANDARD.decode(data_b64.as_bytes()) {
                Ok(bytes) => {
                    client
                        .upload_media(
                            crate::client::UploadMediaHeader {
                                kind,
                                media_type,
                                alt,
                                width,
                                height,
                            },
                            bytes,
                        )
                        .await
                }
                Err(e) => Err(anyhow::anyhow!("attachment base64 decode failed: {e}")),
            };
            Some(r)
        }
        AttachmentImportAction::RegisterUrl {
            kind,
            media_type,
            url,
            width,
            height,
            alt,
        } => Some(
            client
                .register_media_url(crate::client::RegisterMediaUrl {
                    kind,
                    media_type,
                    url,
                    alt,
                    width,
                    height,
                })
                .await,
        ),
        AttachmentImportAction::KeepAsIs => None,
    }
}

/// Flush the in-progress chunk to the server, do the rewire pass for
/// any duplicates, and reset the builder. Returns `Err(message)` so the
/// caller can stash the message on `result.error` and stop processing
/// further entries (matching `run_import`'s "first failure stops the
/// session" behaviour).
#[allow(clippy::too_many_arguments)]
async fn flush_chunk(
    client: &dyn ImportClient,
    session: &CanonicalSession,
    user_id: i64,
    builder: &mut ChunkBuilder,
    current_thread_id: &mut Option<ThreadId>,
    result: &mut CanonicalSessionResult,
    is_last: bool,
    _session_updated_at_ms: i64,
    labels: &[String],
    event_sink: Option<&dyn ImportEventSink>,
) -> Result<(), String> {
    let chunk_len = builder.len();
    let (mut chunk_entries, chunk_inputs) = builder.take();

    // Sort within the chunk to absorb any tied `import_order` values
    // whose intra-tie order the source can't guarantee. Sources emit in
    // ascending `(import_order, timestamp_ms)` order across the stream,
    // so this is purely a defence in depth — N is tiny (<= max_entries).
    let mut zipped: Vec<(CanonicalEntry, PbBatchMemoryInput)> =
        chunk_entries.drain(..).zip(chunk_inputs).collect();
    zipped.sort_by_key(|(e, _)| (e.import_order, e.timestamp_ms));
    let (entries_sorted, pb_sorted): (Vec<_>, Vec<_>) = zipped.into_iter().unzip();

    let thread_target = match *current_thread_id {
        Some(tid) => PbThreadTarget::ExistingThreadId(tid),
        None => PbThreadTarget::Upsert(ThreadUpsertByChannel {
            thread_data: Some(build_thread_data(session, user_id)),
        }),
    };

    let source_identity = source_identity_to_proto(session, user_id);
    let request = AddMemoriesBatchRequest {
        thread_target: Some(thread_target),
        memories: pb_sorted,
        upsert_by_external_id: session.source_id != "opencode",
        labels: if is_last || (session.source_id == "opencode" && current_thread_id.is_none()) {
            labels.to_vec()
        } else {
            Vec::new()
        },
        source_identity,
        explicit_override: false,
    };

    let response = client
        .add_memories_batch(request)
        .await
        .map_err(|e| format!("AddMemoriesBatch failed: {e}"))?;

    if response.suppressed {
        result.skip_reason = Some("suppressed_by_deletion_marker".to_string());
        return Ok(());
    }

    let batch_thread_id = response
        .thread_id
        .ok_or_else(|| "AddMemoriesBatch response missing thread_id".to_string())?;
    if current_thread_id.is_none() {
        result.thread_id = Some(batch_thread_id.value);
        result.thread_created = response.thread_created;
        if response.thread_created
            && let Some(sink) = event_sink
        {
            sink.thread_created(&session.channel, batch_thread_id.value)?;
        }
    }
    *current_thread_id = Some(batch_thread_id);

    if response.outcomes.len() != chunk_len {
        return Err(format!(
            "AddMemoriesBatch returned {} outcomes for {} inputs",
            response.outcomes.len(),
            chunk_len
        ));
    }

    run_rewire_pass(
        client,
        &entries_sorted,
        &response.outcomes,
        batch_thread_id,
        result,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{CanonicalAddons, ThreadGroupObservation};

    fn make_entry(ts: i64) -> CanonicalEntry {
        CanonicalEntry {
            external_id: "x".to_string(),
            parent_external_ids: Vec::new(),
            role: protobuf::llm_memory::data::MessageRole::RoleUser,
            content_type: protobuf::llm_memory::data::ContentType::Text,
            content: String::new(),
            metadata: serde_json::Map::new(),
            timestamp_ms: ts,
            import_order: 0,
            kind_tag: "user",
            canonical: CanonicalAddons::default(),
        }
    }

    #[test]
    fn is_entry_importable_no_since() {
        let e = make_entry(100);
        assert!(is_entry_importable(&e, None));
    }

    #[test]
    fn is_entry_importable_within_since() {
        let e = make_entry(100);
        assert!(is_entry_importable(&e, Some(50)));
    }

    #[test]
    fn is_entry_importable_below_since() {
        let e = make_entry(100);
        assert!(!is_entry_importable(&e, Some(200)));
    }

    fn make_session() -> CanonicalSession {
        CanonicalSession {
            source_id: "codex".to_string(),
            session_id: "s1".to_string(),
            channel: "codex:s1".to_string(),
            description: None,
            cwd: None,
            git_branch: None,
            created_at_ms: 0,
            updated_at_ms: 0,
            source_labels: Vec::new(),
            source_metadata: serde_json::json!({"scope": "test"}),
            thread_metadata: None,
            source_identity: None,
            thread_group_observations: Vec::new(),
        }
    }

    #[test]
    fn build_thread_data_leaves_audit_timestamps_to_the_server() {
        let mut session = make_session();
        session.source_id = "opencode".to_string();
        session.created_at_ms = 1_700_000_000_000;
        session.updated_at_ms = 1_700_000_001_000;

        let thread = build_thread_data(&session, 42);

        assert_eq!(thread.created_at, 0);
        assert_eq!(thread.updated_at, 0);
    }

    #[test]
    fn build_thread_data_strips_nul_from_metadata_values_and_keys() {
        let mut session = make_session();
        session.description = Some("title\u{0000}suffix".to_string());
        session.thread_metadata = Some(serde_json::json!({
            "session": {
                "title\u{0000}suffix": "value\u{0000}suffix",
                "nested": ["a\u{0000}b"]
            }
        }));

        let thread = build_thread_data(&session, 42);
        assert_eq!(thread.description.as_deref(), Some("titlesuffix"));
        let metadata = thread.metadata.expect("thread metadata is set");
        assert!(!metadata.contains('\u{0000}'));
        let value: serde_json::Value = serde_json::from_str(&metadata).unwrap();
        assert_eq!(value["session"]["titlesuffix"], "valuesuffix");
        assert_eq!(value["session"]["nested"][0], "ab");
    }

    fn parse_meta(md: &MemoryData) -> serde_json::Map<String, serde_json::Value> {
        let s = md.metadata.as_deref().expect("metadata is set");
        let v: serde_json::Value = serde_json::from_str(s).expect("metadata is JSON");
        v.as_object().expect("top-level object").clone()
    }

    /// Spec §4.2.2.2 contract (a): even if a parser bug leaves
    /// reserved keys in entry.metadata, the final
    /// `MemoryData.metadata.{source,kind,session}` reflect run_import's
    /// values.
    #[test]
    fn build_memory_data_drops_reserved_keys_in_entry_metadata() {
        let session = make_session();
        let mut entry = make_entry(100);
        entry.kind_tag = "tool_call";
        entry.metadata.insert(
            "source".to_string(),
            serde_json::Value::String("ATTACKER".to_string()),
        );
        entry.metadata.insert(
            "kind".to_string(),
            serde_json::Value::String("ATTACKER".to_string()),
        );
        entry
            .metadata
            .insert("tool".to_string(), serde_json::json!({"name": "ATTACKER"}));
        entry.metadata.insert(
            "attachment".to_string(),
            serde_json::json!({"kind": "ATTACKER"}),
        );
        entry
            .metadata
            .insert("raw".to_string(), serde_json::json!({"ATTACKER": true}));
        entry
            .metadata
            .insert("session".to_string(), serde_json::json!("ATTACKER"));

        let no_media = std::collections::HashMap::new();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            build_memory_data(&session, &entry, 1, &no_media)
        }));
        let mut clean_entry = entry.clone();
        for k in ["source", "kind", "session", "tool", "attachment", "raw"] {
            clean_entry.metadata.remove(k);
        }
        let md = build_memory_data(&session, &clean_entry, 1, &no_media);
        let meta = parse_meta(&md);
        assert_eq!(meta["source"].as_str(), Some("codex"));
        assert_eq!(meta["kind"].as_str(), Some("tool_call"));
        assert!(meta["session"].is_object());
        assert!(meta.get("tool").is_none());
        assert!(meta.get("attachment").is_none());
        assert!(meta.get("raw").is_none());
    }

    #[test]
    fn build_memory_data_force_overwrites_kind() {
        let session = make_session();
        let mut entry = make_entry(100);
        entry.kind_tag = "assistant";
        let md = build_memory_data(&session, &entry, 1, &std::collections::HashMap::new());
        let meta = parse_meta(&md);
        assert_eq!(meta["kind"].as_str(), Some("assistant"));
    }

    #[test]
    fn build_memory_data_writes_canonical_tool_attachment_raw_at_top_level() {
        let session = make_session();
        let mut entry = make_entry(100);
        entry.kind_tag = "tool_call";
        entry.canonical.tool = Some(serde_json::json!({"name": "shell"}));
        entry.canonical.attachment = Some(serde_json::json!({"kind": "image"}));
        let mut raw = serde_json::Map::new();
        raw.insert("codex".to_string(), serde_json::json!({"k": 1}));
        entry.canonical.raw = Some(raw);

        let md = build_memory_data(&session, &entry, 1, &std::collections::HashMap::new());
        let meta = parse_meta(&md);

        assert_eq!(meta["tool"], serde_json::json!({"name": "shell"}));
        assert_eq!(meta["attachment"], serde_json::json!({"kind": "image"}));
        assert_eq!(meta["raw"], serde_json::json!({"codex": {"k": 1}}));
    }

    #[test]
    fn build_memory_data_omits_parent_ids() {
        let session = make_session();
        let entry = make_entry(100);
        let md = build_memory_data(&session, &entry, 1, &std::collections::HashMap::new());
        assert!(
            md.parent_ids.is_empty(),
            "parent_ids must always be empty in batch mode"
        );
    }

    #[test]
    fn namespaces_entry_and_parent_external_ids_by_thread_creator() {
        let mut entry = entry_with(
            "codex:session-1:message-1",
            1,
            vec!["codex:session-1:parent-1"],
        );

        namespace_entry_external_ids(&mut entry, "codex", 42);

        assert_eq!(entry.external_id, "codex:42:session-1:message-1");
        assert_eq!(
            entry.parent_external_ids,
            vec!["codex:42:session-1:parent-1"]
        );
    }

    #[test]
    fn creator_scoped_external_id_at_the_database_limit_is_preserved() {
        let source_id = "src";
        let creator_user_id = 42;
        let prefix = format!("{source_id}:{creator_user_id}:");
        let raw_external_id = format!("{source_id}:{}", "x".repeat(512 - prefix.len()));

        let external_id = namespace_external_id(source_id, creator_user_id, &raw_external_id);

        assert_eq!(external_id.len(), 512);
        assert_eq!(
            external_id,
            format!("{prefix}{}", "x".repeat(512 - prefix.len()))
        );
    }

    #[test]
    fn creator_scoped_external_id_uses_the_legacy_normalized_source_namespace() {
        assert_eq!(
            namespace_external_id(
                "news-aggregator",
                42,
                "news_aggregator:https://example.test/article",
            ),
            "news_aggregator:42:https://example.test/article"
        );
    }

    #[test]
    fn creator_scoped_external_id_over_the_database_limit_is_hashed_deterministically() {
        let raw_external_id = format!("codex:{}", "あ".repeat(200));

        let first = namespace_external_id("codex", 42, &raw_external_id);
        let second = namespace_external_id("codex", 42, &raw_external_id);
        let different = namespace_external_id("codex", 42, &(raw_external_id + "x"));

        assert!(first.len() <= 512);
        assert!(first.starts_with("codex:42:~"));
        assert_eq!(first, second);
        assert_ne!(first, different);
    }

    // build_memory_data must strip NULs before they reach the DB;
    // see `strip_nul_bytes_in_value` for why.
    #[test]
    fn build_memory_data_strips_nul_bytes_from_content_and_metadata() {
        let session = make_session();
        let mut entry = make_entry(100);
        entry.content = "good\u{0000}content".to_string();
        entry.metadata.insert(
            "leaked".to_string(),
            serde_json::Value::String("tool\u{0000}stderr".to_string()),
        );

        let md = build_memory_data(&session, &entry, 1, &std::collections::HashMap::new());

        assert_eq!(md.content, "goodcontent");
        assert!(
            !md.content.contains('\u{0000}'),
            "content must not retain NUL bytes"
        );
        let meta_str = md.metadata.as_deref().expect("metadata is set");
        assert!(
            !meta_str.contains("\\u0000"),
            "serialized metadata must not retain escaped NUL sequences (\\u0000)"
        );
        let meta: serde_json::Value = serde_json::from_str(meta_str).expect("metadata is JSON");
        assert_eq!(meta["leaked"].as_str(), Some("toolstderr"));
        assert!(
            !meta["leaked"].as_str().unwrap().contains('\u{0000}'),
            "round-tripped metadata must not restore NUL bytes"
        );
    }

    // Object keys also need stripping: serde escapes them and the
    // escape is undone on the receiving side at JSON parse time.
    #[test]
    fn build_memory_data_strips_nul_bytes_from_metadata_object_keys() {
        let session = make_session();
        let mut entry = make_entry(100);
        entry.metadata.insert(
            "bad\u{0000}key".to_string(),
            serde_json::Value::String("v".to_string()),
        );

        let md = build_memory_data(&session, &entry, 1, &std::collections::HashMap::new());

        let meta_str = md.metadata.as_deref().expect("metadata is set");
        assert!(
            !meta_str.contains("\\u0000"),
            "serialized metadata must not retain escaped NUL sequences from keys: {meta_str}"
        );
        let meta: serde_json::Value = serde_json::from_str(meta_str).expect("metadata is JSON");
        let obj = meta.as_object().expect("top-level object");
        assert!(obj.contains_key("badkey"));
        assert!(!obj.keys().any(|k| k.contains('\u{0000}')));
    }

    // Without NUL-aware reserved-key filtering an attacker could smuggle
    // "source\0" past Step 1's literal-name check, have it normalized to
    // "source" by the NUL strip, and then overwrite the fixed metadata
    // written in Step 3 (run_import's last-writer-wins contract). Step 1
    // must therefore reject reserved-key NUL variants up front.
    #[test]
    fn build_memory_data_rejects_nul_laundered_reserved_keys() {
        let session = make_session();
        let mut entry = make_entry(100);
        entry.kind_tag = "tool_call";
        entry.metadata.insert(
            "source\u{0000}".to_string(),
            serde_json::Value::String("ATTACKER".to_string()),
        );
        entry.metadata.insert(
            "ki\u{0000}nd".to_string(),
            serde_json::Value::String("ATTACKER".to_string()),
        );
        entry.metadata.insert(
            "\u{0000}session".to_string(),
            serde_json::json!({"hijacked": true}),
        );
        // A legitimate non-reserved key with a NUL must still survive
        // (only normalized), to prove the filter does not over-reach.
        entry.metadata.insert(
            "user\u{0000}note".to_string(),
            serde_json::Value::String("kept".to_string()),
        );

        let no_media = std::collections::HashMap::new();
        // debug_assert! fires in debug builds; swallow the panic so we
        // can still inspect the release-build behavior.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            build_memory_data(&session, &entry, 1, &no_media)
        }));

        let mut clean_entry = entry.clone();
        clean_entry.metadata.remove("source\u{0000}");
        clean_entry.metadata.remove("ki\u{0000}nd");
        clean_entry.metadata.remove("\u{0000}session");
        let md = build_memory_data(&session, &clean_entry, 1, &no_media);
        let meta = parse_meta(&md);

        // Fixed keys retain run_import's values, not the attacker's.
        assert_eq!(meta["source"].as_str(), Some("codex"));
        assert_eq!(meta["kind"].as_str(), Some("tool_call"));
        assert!(meta["session"].is_object());
        assert_eq!(meta["session"]["hijacked"], serde_json::Value::Null);
        // Non-reserved NUL-bearing key is preserved (with NUL stripped).
        assert_eq!(meta["usernote"].as_str(), Some("kept"));
    }

    // --- FakeImportClient-based importer behaviour -------------------------
    //
    // Exercises the trait-driven import path without touching the network or
    // the DB. The fake records every RPC and returns synthesised outcomes so
    // the gating logic for `UpdateMemoryParents` can be verified end-to-end.

    use crate::source::CanonicalSession;
    use async_trait::async_trait;
    use protobuf::llm_memory::data::{
        Memory as PbMemory, MemoryData as PbMemoryData, MemoryId as PbMemoryId,
        ThreadId as PbThreadId, UserId as PbUserId,
    };
    use protobuf::llm_memory::service::{
        AddMemoriesBatchRequest, AddMemoriesBatchResponse, AddMemoryOutcome, MemoryListEntry,
        UpdateMemoryParentsRequest, UpdateMemoryParentsResponse,
    };
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    struct FakeImportClient {
        // Per-call canned outcomes for AddMemoriesBatch. When the queue
        // is empty, the fake fabricates a default response.
        canned_batches: Mutex<Vec<AddMemoriesBatchResponse>>,
        // Per-call canned outcomes for UpdateMemoryParents.
        canned_rewires: Mutex<Vec<UpdateMemoryParentsResponse>>,
        recorded_batches: Mutex<Vec<AddMemoriesBatchRequest>>,
        recorded_labels: Mutex<Vec<AddLabelsRequest>>,
        recorded_rewires: Mutex<Vec<UpdateMemoryParentsRequest>>,
        recorded_observations: Mutex<Vec<RecordThreadGroupObservationsRequest>>,
        side_effect_order: Mutex<Vec<&'static str>>,
        fail_observation_record: Mutex<bool>,
        // Media-resolution recordings. `next_media_id` hands out a
        // distinct id per call; a `media_type` listed in
        // `fail_upload_media_types` makes upload_media return an error so
        // the per-entry skip path can be exercised.
        recorded_uploads: Mutex<Vec<crate::client::UploadMediaHeader>>,
        recorded_registers: Mutex<Vec<crate::client::RegisterMediaUrl>>,
        next_media_id: Mutex<i64>,
        fail_upload_media_types: Mutex<Vec<String>>,
        // External_ids the prefix pre-fetch should report as already in
        // the DB (default empty = nothing pre-existing). When
        // `fail_prefix_query` is set the pre-fetch returns an error so
        // the fail-safe (resolve everything) path is exercised.
        existing_external_ids: Mutex<Vec<String>>,
        fail_prefix_query: Mutex<bool>,
        exact_external_id_queries: Mutex<Vec<String>>,
        exact_memories: Mutex<HashMap<String, MemoryListEntry>>,
        opencode_thread: Mutex<Option<PbThreadId>>,
        thread_lookup_calls: Mutex<Vec<(String, i64)>>,
        thread_lookup_error: Mutex<Option<String>>,
        group_owner_user_id: Mutex<Option<i64>>,
        prefix_queries: Mutex<Vec<String>>,
        fail_batch_call: Mutex<Option<usize>>,
    }

    impl FakeImportClient {
        fn push_batch(&self, r: AddMemoriesBatchResponse) {
            self.canned_batches.lock().unwrap().push(r);
        }
        fn push_rewire(&self, r: UpdateMemoryParentsResponse) {
            self.canned_rewires.lock().unwrap().push(r);
        }
        fn rewires(&self) -> Vec<UpdateMemoryParentsRequest> {
            self.recorded_rewires.lock().unwrap().clone()
        }
        fn batches(&self) -> Vec<AddMemoriesBatchRequest> {
            self.recorded_batches.lock().unwrap().clone()
        }
        fn labels(&self) -> Vec<AddLabelsRequest> {
            self.recorded_labels.lock().unwrap().clone()
        }
        fn observations(&self) -> Vec<RecordThreadGroupObservationsRequest> {
            self.recorded_observations.lock().unwrap().clone()
        }
        fn side_effect_order(&self) -> Vec<&'static str> {
            self.side_effect_order.lock().unwrap().clone()
        }
        fn uploads(&self) -> Vec<crate::client::UploadMediaHeader> {
            self.recorded_uploads.lock().unwrap().clone()
        }
        fn registers(&self) -> Vec<crate::client::RegisterMediaUrl> {
            self.recorded_registers.lock().unwrap().clone()
        }
        fn fail_upload_for(&self, media_type: &str) {
            self.fail_upload_media_types
                .lock()
                .unwrap()
                .push(media_type.to_string());
        }
        fn alloc_media_id(&self) -> i64 {
            let mut n = self.next_media_id.lock().unwrap();
            *n += 1;
            7_000 + *n
        }
        fn mark_existing(&self, external_id: &str) {
            self.existing_external_ids
                .lock()
                .unwrap()
                .push(external_id.to_string());
        }
        fn fail_prefix_query(&self) {
            *self.fail_prefix_query.lock().unwrap() = true;
        }
        fn exact_external_id_queries(&self) -> Vec<String> {
            self.exact_external_id_queries.lock().unwrap().clone()
        }
        fn prefix_queries(&self) -> Vec<String> {
            self.prefix_queries.lock().unwrap().clone()
        }
        fn fail_batch_call(&self, call: usize) {
            *self.fail_batch_call.lock().unwrap() = Some(call);
        }
        fn set_opencode_thread(&self, thread_id: Option<i64>) {
            *self.opencode_thread.lock().unwrap() = thread_id.map(|value| PbThreadId { value });
        }
        fn set_group_owner_user_id(&self, user_id: i64) {
            *self.group_owner_user_id.lock().unwrap() = Some(user_id);
        }
        fn thread_lookup_calls(&self) -> Vec<(String, i64)> {
            self.thread_lookup_calls.lock().unwrap().clone()
        }
        fn fail_thread_lookup(&self, error: &str) {
            *self.thread_lookup_error.lock().unwrap() = Some(error.to_string());
        }
        fn set_exact_memory(&self, external_id: &str, entry: MemoryListEntry) {
            self.exact_memories
                .lock()
                .unwrap()
                .insert(external_id.to_string(), entry);
        }
        fn fail_observation_record(&self) {
            *self.fail_observation_record.lock().unwrap() = true;
        }
    }

    #[async_trait]
    impl crate::client::ImportClient for FakeImportClient {
        fn group_owner_user_id(&self, source_user_id: i64) -> i64 {
            self.group_owner_user_id
                .lock()
                .unwrap()
                .unwrap_or(source_user_id)
        }

        async fn add_labels(&self, request: AddLabelsRequest) -> anyhow::Result<()> {
            self.side_effect_order.lock().unwrap().push("labels");
            self.recorded_labels.lock().unwrap().push(request);
            Ok(())
        }

        async fn add_memories_batch(
            &self,
            request: AddMemoriesBatchRequest,
        ) -> anyhow::Result<AddMemoriesBatchResponse> {
            self.recorded_batches.lock().unwrap().push(request.clone());
            let call = self.recorded_batches.lock().unwrap().len();
            if *self.fail_batch_call.lock().unwrap() == Some(call) {
                anyhow::bail!("simulated batch failure at call {call}");
            }
            let mut q = self.canned_batches.lock().unwrap();
            if let Some(r) = (!q.is_empty()).then(|| q.remove(0)) {
                return Ok(r);
            }
            // Default: every memory looks newly created, no resolved parents.
            let outcomes: Vec<AddMemoryOutcome> = request
                .memories
                .iter()
                .enumerate()
                .map(|(i, _)| AddMemoryOutcome {
                    memory_id: Some(PbMemoryId {
                        value: 1_000 + i as i64,
                    }),
                    created: true,
                    position: i as i32,
                    existing_parent_ids_empty: false,
                    resolved_parent_ids: Vec::new(),
                })
                .collect();
            Ok(AddMemoriesBatchResponse {
                thread_id: Some(PbThreadId { value: 9_000 }),
                thread_created: true,
                suppressed: false,
                outcomes,
            })
        }

        async fn update_memory_parents(
            &self,
            request: UpdateMemoryParentsRequest,
        ) -> anyhow::Result<UpdateMemoryParentsResponse> {
            self.side_effect_order.lock().unwrap().push("rewire");
            self.recorded_rewires.lock().unwrap().push(request);
            let mut q = self.canned_rewires.lock().unwrap();
            if let Some(r) = (!q.is_empty()).then(|| q.remove(0)) {
                return Ok(r);
            }
            Ok(UpdateMemoryParentsResponse {
                rewired: true,
                skip_reason: 0,
            })
        }

        async fn upload_media(
            &self,
            header: crate::client::UploadMediaHeader,
            _bytes: Vec<u8>,
        ) -> anyhow::Result<i64> {
            if self
                .fail_upload_media_types
                .lock()
                .unwrap()
                .contains(&header.media_type)
            {
                anyhow::bail!("simulated upload failure for {}", header.media_type);
            }
            self.recorded_uploads.lock().unwrap().push(header);
            Ok(self.alloc_media_id())
        }

        async fn register_media_url(
            &self,
            params: crate::client::RegisterMediaUrl,
        ) -> anyhow::Result<i64> {
            self.recorded_registers.lock().unwrap().push(params);
            Ok(self.alloc_media_id())
        }

        // The four prune-related methods are unused by these importer tests
        // (they exercise add/rewire only). Panicking on accidental use makes
        // a test-time misuse loud rather than silently returning empty.
        async fn find_memories_by_external_id_prefix(
            &self,
            prefix: String,
        ) -> anyhow::Result<Vec<protobuf::llm_memory::service::MemoryListEntry>> {
            self.prefix_queries.lock().unwrap().push(prefix.clone());
            if *self.fail_prefix_query.lock().unwrap() {
                anyhow::bail!("simulated prefix query failure");
            }
            use protobuf::llm_memory::data::{Memory, MemoryData};
            use protobuf::llm_memory::service::MemoryListEntry;
            let out = self
                .existing_external_ids
                .lock()
                .unwrap()
                .iter()
                .filter(|external_id| external_id.starts_with(&prefix))
                .map(|external_id| MemoryListEntry {
                    memory: Some(Memory {
                        data: Some(MemoryData {
                            external_id: Some(external_id.clone()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect();
            Ok(out)
        }
        async fn find_memory_by_external_id(
            &self,
            external_id: String,
        ) -> anyhow::Result<Option<protobuf::llm_memory::service::MemoryListEntry>> {
            self.exact_external_id_queries
                .lock()
                .unwrap()
                .push(external_id.clone());
            use protobuf::llm_memory::data::{Memory, MemoryData};
            use protobuf::llm_memory::service::MemoryListEntry;
            if let Some(entry) = self
                .exact_memories
                .lock()
                .unwrap()
                .get(&external_id)
                .cloned()
            {
                return Ok(Some(entry));
            }
            Ok(self
                .existing_external_ids
                .lock()
                .unwrap()
                .contains(&external_id)
                .then(|| MemoryListEntry {
                    memory: Some(Memory {
                        data: Some(MemoryData {
                            external_id: Some(external_id),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }))
        }
        async fn find_thread_by_channel_and_user_id(
            &self,
            channel: String,
            user_id: i64,
        ) -> anyhow::Result<Option<PbThreadId>> {
            self.thread_lookup_calls
                .lock()
                .unwrap()
                .push((channel, user_id));
            if let Some(error) = self.thread_lookup_error.lock().unwrap().clone() {
                anyhow::bail!("{error}");
            }
            Ok(*self.opencode_thread.lock().unwrap())
        }
        async fn record_thread_group_observations(
            &self,
            request: RecordThreadGroupObservationsRequest,
        ) -> anyhow::Result<protobuf::llm_memory::service::RecordThreadGroupObservationsResponse>
        {
            self.side_effect_order.lock().unwrap().push("record");
            self.recorded_observations.lock().unwrap().push(request);
            if *self.fail_observation_record.lock().unwrap() {
                anyhow::bail!("simulated observation record failure");
            }
            Ok(protobuf::llm_memory::service::RecordThreadGroupObservationsResponse::default())
        }
        async fn delete_memory(&self, _memory_id: PbMemoryId) -> anyhow::Result<()> {
            unimplemented!("FakeImportClient (importer tests) does not stub prune RPCs")
        }
        async fn delete_thread(&self, _thread_id: PbThreadId) -> anyhow::Result<()> {
            unimplemented!("FakeImportClient (importer tests) does not stub prune RPCs")
        }
        async fn count_memories_in_thread(&self, _thread_id: PbThreadId) -> anyhow::Result<i64> {
            unimplemented!("FakeImportClient (importer tests) does not stub prune RPCs")
        }
    }

    /// Test ChatSource that returns one canonical session with caller-
    /// supplied entries. Used to exercise `run_all_with_entry_collector`
    /// end-to-end without touching the filesystem.
    struct FakeSource {
        id: &'static str,
        sessions: Vec<(CanonicalSession, Vec<CanonicalEntry>)>,
        /// When true, return the new `ImportStream` variant instead of
        /// the legacy `Import` variant. Lets one test fixture cover
        /// both code paths.
        use_stream: bool,
    }

    impl FakeSource {
        fn new(id: &'static str, session: CanonicalSession, entries: Vec<CanonicalEntry>) -> Self {
            Self {
                id,
                sessions: vec![(session, entries)],
                use_stream: false,
            }
        }
        fn with_sessions(
            id: &'static str,
            sessions: Vec<(CanonicalSession, Vec<CanonicalEntry>)>,
        ) -> Self {
            Self {
                id,
                sessions,
                use_stream: false,
            }
        }
        fn streaming(mut self) -> Self {
            self.use_stream = true;
            self
        }
    }

    impl crate::source::ChatSource for FakeSource {
        type SessionInput = usize;

        fn id(&self) -> &str {
            self.id
        }
        fn input_label(&self, input: &Self::SessionInput) -> String {
            self.sessions[*input].0.session_id.clone()
        }
        fn discover(&self) -> anyhow::Result<Vec<Self::SessionInput>> {
            Ok((0..self.sessions.len()).collect())
        }
        fn read_session(
            &self,
            input: &Self::SessionInput,
            _since_millis_with_margin: Option<i64>,
        ) -> anyhow::Result<ReadSessionOutcome> {
            let (session, entries) = &self.sessions[*input];
            if self.use_stream {
                Ok(ReadSessionOutcome::ImportStream {
                    session: session.clone(),
                    entries: crate::source::CanonicalEntryStream::from_vec(entries.clone()),
                    source_filtered_count_initial: 0,
                    diagnostics: Default::default(),
                })
            } else {
                Ok(ReadSessionOutcome::Import {
                    session: session.clone(),
                    entries: entries.clone(),
                    source_filtered_count: 0,
                    diagnostics: Default::default(),
                })
            }
        }
    }

    /// Regression for the prune `--since` interaction: entries dropped by
    /// `is_entry_importable` must NOT reach the collector. Letting them
    /// through poisons `D_external_id` / `D_path` and lets `--prune-missing`
    /// delete the old memory for any path whose new version was filtered.
    #[tokio::test]
    async fn collector_skips_entries_filtered_by_since() {
        let source = FakeSource::new(
            "fake",
            fake_session(),
            vec![
                entry_with("a-old", 100, vec![]), // dropped by since=200
                entry_with("b-new", 300, vec![]),
            ],
        );
        let fake = FakeImportClient::default();
        let mut collected: Vec<String> = Vec::new();
        let _ = run_all_with_entry_collector(
            &source,
            Some(&fake as &dyn crate::client::ImportClient),
            Some(200),
            None,
            1,
            &[],
            |entries| {
                for e in entries {
                    collected.push(e.external_id.clone());
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(
            collected,
            vec!["b-new".to_string()],
            "--since-rejected entries must not reach the collector"
        );
    }

    /// Without `--since`, the collector still sees every entry — same path
    /// as the legacy `run_all` (collector is a no-op).
    #[tokio::test]
    async fn collector_passes_all_entries_when_since_unset() {
        let source = FakeSource::new(
            "fake",
            fake_session(),
            vec![entry_with("a", 100, vec![]), entry_with("b", 200, vec![])],
        );
        let fake = FakeImportClient::default();
        let mut collected: Vec<String> = Vec::new();
        let _ = run_all_with_entry_collector(
            &source,
            Some(&fake as &dyn crate::client::ImportClient),
            None,
            None,
            1,
            &[],
            |entries| {
                for e in entries {
                    collected.push(e.external_id.clone());
                }
            },
        )
        .await
        .unwrap();
        collected.sort();
        assert_eq!(collected, vec!["a".to_string(), "b".to_string()]);
    }

    /// `ImportStream` outcomes must reach the importer with the same
    /// observable result as the legacy `Import` Vec — collector is
    /// fed, entries are imported, and the dry-run summary matches.
    #[tokio::test]
    async fn import_stream_variant_drains_into_run_import_path() {
        let source = FakeSource::new(
            "fake",
            fake_session(),
            vec![entry_with("a", 100, vec![]), entry_with("b", 200, vec![])],
        )
        .streaming();
        let mut collected: Vec<String> = Vec::new();
        // Dry-run (client = None) takes the dry_run_session_result branch
        // so the test does not need to stage AddMemoriesBatch responses.
        let results = run_all_with_entry_collector(&source, None, None, None, 1, &[], |entries| {
            for e in entries {
                collected.push(e.external_id.clone());
            }
        })
        .await
        .unwrap();
        collected.sort();
        assert_eq!(collected, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].memories_imported, 2);
        assert_eq!(results[0].memories_skipped_filtered, 0);
        assert!(results[0].error.is_none(), "{:?}", results[0].error);
    }

    #[tokio::test]
    async fn failed_session_does_not_stop_following_sessions() {
        let mut second_session = fake_session();
        second_session.session_id = "second".into();
        second_session.channel = "codex:second".into();
        let source = FakeSource::with_sessions(
            "fake",
            vec![
                (fake_session(), vec![entry_with("first", 100, vec![])]),
                (second_session, vec![entry_with("second", 200, vec![])]),
            ],
        );
        let fake = FakeImportClient::default();
        fake.fail_batch_call(1);

        let results = run_all_with_event_sink(
            &source,
            Some(&fake),
            None,
            None,
            1,
            &[],
            ChunkLimits::default(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].error.is_some());
        assert!(results[1].error.is_none(), "{:?}", results[1].error);
        assert_eq!(results[1].memories_imported, 1);
        assert_eq!(fake.batches().len(), 2);
    }

    fn fake_session() -> CanonicalSession {
        CanonicalSession {
            source_id: "codex".to_string(),
            session_id: "sfake".to_string(),
            channel: "codex:sfake".to_string(),
            description: None,
            cwd: None,
            git_branch: None,
            created_at_ms: 1_000,
            updated_at_ms: 2_000,
            source_labels: Vec::new(),
            source_metadata: serde_json::json!({}),
            thread_metadata: None,
            source_identity: None,
            thread_group_observations: Vec::new(),
        }
    }

    fn entry_with(eid: &str, ts: i64, parents: Vec<&str>) -> CanonicalEntry {
        CanonicalEntry {
            external_id: eid.to_string(),
            parent_external_ids: parents.into_iter().map(|s| s.to_string()).collect(),
            role: protobuf::llm_memory::data::MessageRole::RoleUser,
            content_type: protobuf::llm_memory::data::ContentType::Text,
            content: format!("c-{eid}"),
            metadata: serde_json::Map::new(),
            timestamp_ms: ts,
            import_order: 0,
            kind_tag: "user",
            canonical: CanonicalAddons::default(),
        }
    }

    fn opencode_session() -> CanonicalSession {
        let mut session = fake_session();
        session.source_id = "opencode".to_string();
        session.session_id = "sopencode".to_string();
        session.channel = "opencode:sopencode".to_string();
        session.source_identity = Some(test_source_identity("opencode", "sopencode"));
        session
    }

    fn test_source_identity(source: &str, native_id: &str) -> ThreadGroupSourceIdentity {
        ThreadGroupSourceIdentity {
            source: source.to_string(),
            native_kind: "session",
            owner_scope: None,
            identity_scope: ThreadGroupIdentityScope::Known(String::new()),
            native_id: native_id.to_string(),
        }
    }

    fn exact_memory_entry(external_id: &str, user_id: i64, thread_ids: &[i64]) -> MemoryListEntry {
        MemoryListEntry {
            memory: Some(PbMemory {
                id: Some(PbMemoryId { value: 123 }),
                data: Some(PbMemoryData {
                    external_id: Some(external_id.to_string()),
                    user_id: Some(PbUserId { value: user_id }),
                    thread_ids: thread_ids
                        .iter()
                        .copied()
                        .map(|value| PbThreadId { value })
                        .collect(),
                    ..Default::default()
                }),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn opencode_duplicate_in_another_thread_is_rejected_before_write() {
        let session = opencode_session();
        let entry = entry_with("duplicate", 100, vec![]);
        let scoped_id = namespace_external_id("opencode", 1, &entry.external_id);
        let fake = FakeImportClient::default();
        fake.set_opencode_thread(Some(10));
        fake.set_exact_memory(&scoped_id, exact_memory_entry(&scoped_id, 1, &[20]));

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("different thread"))
        );
        assert!(fake.batches().is_empty());
    }

    #[tokio::test]
    async fn opencode_duplicate_in_target_thread_is_reused() {
        let session = opencode_session();
        let entry = entry_with("duplicate", 100, vec![]);
        let scoped_id = namespace_external_id("opencode", 1, &entry.external_id);
        let fake = FakeImportClient::default();
        fake.set_opencode_thread(Some(10));
        fake.set_exact_memory(&scoped_id, exact_memory_entry(&scoped_id, 1, &[10]));

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.thread_id, Some(10));
        assert_eq!(result.memories_skipped_duplicate, 1);
        assert!(fake.batches().is_empty());
        assert_eq!(
            fake.thread_lookup_calls(),
            vec![(session.channel.clone(), 1)],
            "a snapshot miss must resolve the thread through the per-channel fallback"
        );
    }

    #[tokio::test]
    async fn opencode_duplicate_preflight_uses_thread_channel_snapshot_hit() {
        let session = opencode_session();
        let entry = entry_with("snapshot-duplicate", 100, vec![]);
        let scoped_id = namespace_external_id("opencode", 1, &entry.external_id);
        let fake = FakeImportClient::default();
        // A conflicting fallback result makes using the snapshot observable.
        fake.set_opencode_thread(Some(20));
        fake.set_exact_memory(&scoped_id, exact_memory_entry(&scoped_id, 1, &[10]));
        let snapshot = HashMap::from([(session.channel.clone(), PbThreadId { value: 10 })]);

        let result = run_import_streaming_with_event_sink(
            &fake,
            &session,
            stream_of(vec![entry]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
            None,
            &snapshot,
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.thread_id, Some(10));
        assert_eq!(result.memories_skipped_duplicate, 1);
        assert!(fake.batches().is_empty());
        assert!(fake.thread_lookup_calls().is_empty());
        assert_eq!(fake.exact_external_id_queries(), vec![scoped_id]);
    }

    #[tokio::test]
    async fn opencode_ambiguous_thread_lookup_keeps_error_and_stops_preflight() {
        let session = opencode_session();
        let fake = FakeImportClient::default();
        fake.fail_thread_lookup("ambiguous channel: multiple matching threads");

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert_eq!(
            result.error.as_deref(),
            Some(
                "OpenCode target thread lookup failed: ambiguous channel: multiple matching threads"
            )
        );
        assert_eq!(fake.thread_lookup_calls(), vec![(session.channel, 1)]);
        assert!(fake.exact_external_id_queries().is_empty());
        assert!(fake.batches().is_empty());
    }

    #[tokio::test]
    async fn opencode_root_identity_is_sent_with_empty_observation_record() {
        let session = opencode_session();
        let fake = FakeImportClient::default();
        fake.set_group_owner_user_id(7);

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("root-entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        let batches = fake.batches();
        assert_eq!(batches.len(), 1);
        let batch_identity = batches[0]
            .source_identity
            .as_ref()
            .expect("OpenCode root identity is independent of observations");
        assert_eq!(batch_identity.source, "opencode");
        assert!(batch_identity.identity_scope_known);
        assert_eq!(batch_identity.identity_scope, "");
        assert_eq!(batch_identity.owner_scope, "user:1");
        assert_eq!(batch_identity.native_id, "sopencode");

        let records = fake.observations();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].subject_thread_id, 9_000);
        assert_eq!(records[0].subject, Some(batch_identity.clone()));
        assert_eq!(records[0].group_owner_user_id, Some(7));
        assert!(records[0].observations.is_empty());
    }

    #[tokio::test]
    async fn opencode_duplicate_only_records_before_labels_and_rewires() {
        let session = opencode_session();
        let entry = entry_with("duplicate", 100, vec!["parent"]);
        let duplicate_id = namespace_external_id("opencode", 1, &entry.external_id);
        let parent_id = namespace_external_id("opencode", 1, "parent");
        let fake = FakeImportClient::default();
        fake.set_opencode_thread(Some(10));
        fake.set_exact_memory(&duplicate_id, exact_memory_entry(&duplicate_id, 1, &[10]));
        fake.set_exact_memory(&parent_id, exact_memory_entry(&parent_id, 1, &[10]));

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry]),
            0,
            None,
            1,
            &["extra-label".to_string()],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(
            fake.batches().is_empty(),
            "duplicate-only run must not batch"
        );
        assert_eq!(fake.side_effect_order(), vec!["record", "labels", "rewire"]);
        assert_eq!(fake.observations().len(), 1);
        assert!(fake.observations()[0].observations.is_empty());
        assert_eq!(fake.labels().len(), 1);
        assert_eq!(fake.rewires().len(), 1);
    }

    #[tokio::test]
    async fn opencode_duplicate_preflight_partitions_mixed_entries_in_order() {
        let session = opencode_session();
        let entries = vec![
            entry_with("new-first", 100, vec![]),
            entry_with("duplicate", 200, vec![]),
            entry_with("new-second", 300, vec![]),
        ];
        let scoped_ids = entries
            .iter()
            .map(|entry| namespace_external_id("opencode", 1, &entry.external_id))
            .collect::<Vec<_>>();
        let fake = FakeImportClient::default();
        fake.set_opencode_thread(Some(10));
        fake.set_exact_memory(&scoped_ids[1], exact_memory_entry(&scoped_ids[1], 1, &[10]));

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(entries),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.thread_id, Some(10));
        assert_eq!(result.memories_imported, 2);
        assert_eq!(result.memories_skipped_duplicate, 1);
        assert_eq!(fake.exact_external_id_queries(), scoped_ids);
        assert_eq!(fake.thread_lookup_calls(), vec![(session.channel, 1)]);
        let batches = fake.batches();
        assert_eq!(batches.len(), 1);
        let imported_ids = batches[0]
            .memories
            .iter()
            .map(|input| {
                input
                    .memory
                    .as_ref()
                    .and_then(|memory| memory.external_id.clone())
                    .expect("imported entry has its namespaced external ID")
            })
            .collect::<Vec<_>>();
        assert_eq!(
            imported_ids,
            vec![scoped_ids[0].clone(), scoped_ids[2].clone()]
        );
    }

    #[tokio::test]
    async fn opencode_duplicate_only_record_failure_prevents_labels_and_rewires() {
        let session = opencode_session();
        let entry = entry_with("duplicate", 100, vec!["parent"]);
        let duplicate_id = namespace_external_id("opencode", 1, &entry.external_id);
        let parent_id = namespace_external_id("opencode", 1, "parent");
        let fake = FakeImportClient::default();
        fake.set_opencode_thread(Some(10));
        fake.set_exact_memory(&duplicate_id, exact_memory_entry(&duplicate_id, 1, &[10]));
        fake.set_exact_memory(&parent_id, exact_memory_entry(&parent_id, 1, &[10]));
        fake.fail_observation_record();

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry]),
            0,
            None,
            1,
            &["extra-label".to_string()],
            ChunkLimits::default(),
        )
        .await;

        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("simulated observation record failure"))
        );
        assert_eq!(fake.side_effect_order(), vec!["record"]);
        assert!(fake.labels().is_empty());
        assert!(fake.rewires().is_empty());
    }

    #[tokio::test]
    async fn opencode_suppressed_batch_reports_skip_without_later_side_effects() {
        let session = opencode_session();
        let fake = FakeImportClient::default();
        fake.push_batch(AddMemoriesBatchResponse {
            suppressed: true,
            ..Default::default()
        });

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("root-entry", 100, vec![])]),
            0,
            None,
            1,
            &["extra-label".to_string()],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.skip_reason.is_some());
        assert!(result.error.is_none(), "suppression is a reported skip");
        assert!(fake.observations().is_empty());
        assert!(fake.labels().is_empty());
        assert!(fake.rewires().is_empty());
    }

    #[tokio::test]
    async fn opencode_missing_thread_id_does_not_record_or_apply_labels() {
        let session = opencode_session();
        let fake = FakeImportClient::default();
        fake.push_batch(AddMemoriesBatchResponse {
            outcomes: vec![AddMemoryOutcome {
                memory_id: Some(PbMemoryId { value: 1 }),
                created: true,
                position: 0,
                existing_parent_ids_empty: false,
                resolved_parent_ids: Vec::new(),
            }],
            ..Default::default()
        });

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("root-entry", 100, vec![])]),
            0,
            None,
            1,
            &["extra-label".to_string()],
            ChunkLimits::default(),
        )
        .await;

        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|error| error.contains("missing thread_id"))
        );
        assert!(fake.observations().is_empty());
        assert!(fake.labels().is_empty());
        assert!(fake.rewires().is_empty());
    }

    #[tokio::test]
    async fn no_thread_group_writes_still_sends_identity_on_batch_only() {
        struct RestoreThreadGroupWrites(bool);
        impl Drop for RestoreThreadGroupWrites {
            fn drop(&mut self) {
                set_thread_group_writes_enabled(self.0);
            }
        }

        let restore = RestoreThreadGroupWrites(thread_group_writes_enabled());
        set_thread_group_writes_enabled(false);
        let session = opencode_session();
        let fake = FakeImportClient::default();

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("root-entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(fake.batches()[0].source_identity.is_some());
        assert!(fake.observations().is_empty());
        drop(restore);
    }

    #[tokio::test]
    async fn duplicate_only_with_writes_disabled_does_not_bypass_suppression_gate() {
        struct RestoreThreadGroupWrites(bool);
        impl Drop for RestoreThreadGroupWrites {
            fn drop(&mut self) {
                set_thread_group_writes_enabled(self.0);
            }
        }
        let _restore = RestoreThreadGroupWrites(thread_group_writes_enabled());
        set_thread_group_writes_enabled(false);

        let session = opencode_session();
        let entry = entry_with("duplicate-no-write", 100, vec![]);
        let duplicate_id = namespace_external_id("opencode", 1, &entry.external_id);
        let fake = FakeImportClient::default();
        fake.set_opencode_thread(Some(10));
        fake.set_exact_memory(&duplicate_id, exact_memory_entry(&duplicate_id, 1, &[10]));
        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry]),
            0,
            None,
            1,
            &["extra-label".to_string()],
            ChunkLimits::default(),
        )
        .await;
        assert!(result.error.is_some());
        assert!(fake.labels().is_empty());
        assert!(fake.rewires().is_empty());
    }

    #[tokio::test]
    async fn absent_identity_keeps_plain_import_without_thread_group_writes() {
        let mut session = fake_session();
        session.source_id = "plain".to_string();
        session.channel = "plain:sfake".to_string();
        let fake = FakeImportClient::default();

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("plain-entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(fake.batches()[0].source_identity.is_none());
        assert!(fake.observations().is_empty());
    }

    #[tokio::test]
    async fn existing_codex_observation_still_supplies_batch_identity() {
        let mut session = fake_session();
        let subject = test_source_identity("codex", "codex-thread");
        session
            .thread_group_observations
            .push(ThreadGroupObservation {
                subject: subject.clone(),
                candidate_parent: Some(test_source_identity("codex", "codex-parent")),
                relation_kind: Some("fork".to_string()),
                evidence_kind: ThreadGroupEvidenceKind::SourceField,
                polarity: ThreadGroupPolarity::Supports,
                confidence: ThreadGroupConfidence::Exact,
                source_record_ref: "codex:session_meta:fork".to_string(),
                adapter_version: "codex-adapter@1",
            });
        let fake = FakeImportClient::default();

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("codex-entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        let batches = fake.batches();
        let source_identity = batches[0]
            .source_identity
            .as_ref()
            .expect("existing adapter subject remains the batch identity");
        assert_eq!(source_identity.source, "codex");
        assert_eq!(source_identity.native_id, "codex-thread");
    }

    #[tokio::test]
    async fn source_backed_roots_register_identity_without_parent_evidence() {
        for (source, native_id, scope) in [
            ("codex", "codex-root", ""),
            ("claude_code", "claude-root", "project-key"),
        ] {
            let mut session = fake_session();
            session.source_id = source.to_string();
            session.session_id = native_id.to_string();
            session.channel = format!("{source}:{native_id}");
            let mut identity = test_source_identity(source, native_id);
            identity.identity_scope = ThreadGroupIdentityScope::Known(scope.to_string());
            session.source_identity = Some(identity);
            let fake = FakeImportClient::default();
            let result = run_import_streaming(
                &fake,
                &session,
                stream_of(vec![entry_with("root-entry", 100, vec![])]),
                0,
                None,
                1,
                &[],
                ChunkLimits::default(),
            )
            .await;
            assert!(result.error.is_none(), "{source}: {:?}", result.error);
            let batch_identity = fake.batches()[0].source_identity.clone().unwrap();
            assert_eq!(batch_identity.source, source);
            assert_eq!(batch_identity.identity_scope, scope);
            let records = fake.observations();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].subject, Some(batch_identity));
            assert!(records[0].observations.is_empty());
        }
    }

    #[tokio::test]
    async fn unknown_scope_root_does_not_send_empty_reconciliation_request() {
        let mut session = fake_session();
        session.source_id = "claude_code".into();
        let mut identity = test_source_identity("claude_code", "unknown-root");
        identity.identity_scope = ThreadGroupIdentityScope::Unknown;
        session.source_identity = Some(identity);
        let fake = FakeImportClient::default();
        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("root-entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        assert!(
            !fake.batches()[0]
                .source_identity
                .as_ref()
                .unwrap()
                .identity_scope_known
        );
        assert!(fake.observations().is_empty());
    }

    #[tokio::test]
    async fn unknown_scope_child_preserves_observation_without_root_registration() {
        let mut session = fake_session();
        session.source_id = "claude_code".into();
        let mut identity = test_source_identity("claude_code", "unknown-child");
        identity.identity_scope = ThreadGroupIdentityScope::Unknown;
        session.source_identity = Some(identity.clone());
        let mut parent = test_source_identity("claude_code", "parent");
        parent.identity_scope = ThreadGroupIdentityScope::Unknown;
        session
            .thread_group_observations
            .push(ThreadGroupObservation {
                subject: identity,
                candidate_parent: Some(parent),
                relation_kind: Some("fork".to_string()),
                evidence_kind: ThreadGroupEvidenceKind::SourceField,
                polarity: ThreadGroupPolarity::Supports,
                confidence: ThreadGroupConfidence::Exact,
                source_record_ref: "claude_code:session.fork_metadata".to_string(),
                adapter_version: "claude-code-adapter@1",
            });
        let fake = FakeImportClient::default();
        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("child-entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;
        assert!(result.error.is_none(), "{:?}", result.error);
        let records = fake.observations();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].observations.len(), 1);
        assert!(!records[0].subject.as_ref().unwrap().identity_scope_known);
    }

    #[tokio::test]
    async fn child_session_keeps_its_exact_parent_observation() {
        let mut session = opencode_session();
        session.session_id = "ses-child".to_string();
        session.channel = "opencode:ses-child".to_string();
        let subject = test_source_identity("opencode", "ses-child");
        session.source_identity = Some(subject.clone());
        session
            .thread_group_observations
            .push(ThreadGroupObservation {
                subject,
                candidate_parent: Some(test_source_identity("opencode", "ses-parent")),
                relation_kind: Some("delegated".to_string()),
                evidence_kind: ThreadGroupEvidenceKind::SourceField,
                polarity: ThreadGroupPolarity::Supports,
                confidence: ThreadGroupConfidence::Exact,
                source_record_ref: "opencode:session.parent_id".to_string(),
                adapter_version: "opencode-adapter@1",
            });
        let fake = FakeImportClient::default();

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![entry_with("child-entry", 100, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        let records = fake.observations();
        let record = &records[0];
        assert_eq!(record.observations.len(), 1);
        assert_eq!(
            record.observations[0].relation_kind.as_deref(),
            Some("delegated")
        );
        let parent = record.observations[0]
            .candidate_parent
            .as_ref()
            .expect("the exact source parent is preserved");
        assert_eq!(parent.source, "opencode");
        assert_eq!(parent.native_id, "ses-parent");
        assert!(parent.identity_scope_known);
        assert_eq!(parent.owner_scope, "user:1");
    }

    #[tokio::test]
    async fn run_import_counts_created_and_dispatches_no_rewire_when_empty_resolved() {
        let session = fake_session();
        let entries = vec![entry_with("a", 100, vec![]), entry_with("b", 200, vec![])];
        let fake = FakeImportClient::default();
        // Two outcomes: created=false, but resolved_parent_ids empty
        // → no rewire RPC.
        fake.push_batch(AddMemoriesBatchResponse {
            thread_id: Some(PbThreadId { value: 1 }),
            thread_created: false,
            suppressed: false,
            outcomes: vec![
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 11 }),
                    created: false,
                    position: 0,
                    existing_parent_ids_empty: true,
                    resolved_parent_ids: Vec::new(),
                },
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 12 }),
                    created: false,
                    position: 1,
                    existing_parent_ids_empty: true,
                    resolved_parent_ids: Vec::new(),
                },
            ],
        });

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.memories_imported, 0);
        assert_eq!(res.memories_skipped_duplicate, 2);
        assert_eq!(res.memories_rewired, 0);
        assert!(
            fake.rewires().is_empty(),
            "no rewire when resolved is empty"
        );
    }

    #[tokio::test]
    async fn run_import_triggers_rewire_only_when_gate_satisfied() {
        let session = fake_session();
        let entries = vec![
            entry_with("p", 100, vec![]),
            entry_with("c", 200, vec!["p"]),
        ];
        let fake = FakeImportClient::default();

        // Outcome shape: parent created, child reused with empty parents
        // and a resolved parent id → triggers rewire.
        fake.push_batch(AddMemoriesBatchResponse {
            thread_id: Some(PbThreadId { value: 1 }),
            thread_created: false,
            suppressed: false,
            outcomes: vec![
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 21 }),
                    created: true,
                    position: 0,
                    existing_parent_ids_empty: false,
                    resolved_parent_ids: Vec::new(),
                },
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 22 }),
                    created: false,
                    position: 1,
                    existing_parent_ids_empty: true,
                    resolved_parent_ids: vec![PbMemoryId { value: 21 }],
                },
            ],
        });
        fake.push_rewire(UpdateMemoryParentsResponse {
            rewired: true,
            skip_reason: 0,
        });

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.memories_imported, 1);
        assert_eq!(res.memories_skipped_duplicate, 1);
        assert_eq!(res.memories_rewired, 1);
        let rewires = fake.rewires();
        assert_eq!(rewires.len(), 1);
        assert_eq!(rewires[0].memory_id.unwrap().value, 22);
        assert_eq!(rewires[0].parent_ids[0].value, 21);
        // proto3 default = false on both force flags.
        assert!(!rewires[0].force_overwrite_when_shared);
        assert!(!rewires[0].force_overwrite_when_non_empty);
    }

    #[tokio::test]
    async fn run_import_does_not_rewire_when_existing_parents_not_empty() {
        let session = fake_session();
        let entries = vec![
            entry_with("p", 100, vec![]),
            entry_with("c", 200, vec!["p"]),
        ];
        let fake = FakeImportClient::default();
        fake.push_batch(AddMemoriesBatchResponse {
            thread_id: Some(PbThreadId { value: 1 }),
            thread_created: false,
            suppressed: false,
            outcomes: vec![
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 31 }),
                    created: true,
                    position: 0,
                    existing_parent_ids_empty: false,
                    resolved_parent_ids: Vec::new(),
                },
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 32 }),
                    created: false,
                    position: 1,
                    existing_parent_ids_empty: false, // gate fails
                    resolved_parent_ids: vec![PbMemoryId { value: 31 }],
                },
            ],
        });

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert_eq!(res.memories_rewired, 0);
        assert!(fake.rewires().is_empty());
    }

    #[tokio::test]
    async fn run_import_chunks_large_batches_and_pins_labels_to_last() {
        let session = fake_session();
        let total = CHUNK_MAX_ENTRIES + 5;
        let entries: Vec<CanonicalEntry> = (0..total)
            .map(|i| entry_with(&format!("eid-{i}"), 1_000 + i as i64, vec![]))
            .collect();
        let fake = FakeImportClient::default();
        // No canned responses → fake's default fabricates per-input outcomes.

        let labels = vec!["agent:codex".to_string()];
        let res = run_import(&fake, &session, entries, 0, None, 1, &labels).await;
        assert!(res.error.is_none(), "{:?}", res.error);

        let batches = fake.batches();
        // total=505 with chunk size 500 → 2 chunks.
        assert_eq!(batches.len(), 2);
        // Labels are attached only on the final chunk.
        assert!(batches[0].labels.is_empty());
        assert_eq!(batches[1].labels, labels);
    }

    /// A handful of entries whose individual encoded sizes already
    /// exceed the byte budget must be split on bytes, not on the
    /// 500-entry cap. Each entry is well below `CHUNK_MAX_ENTRIES`
    /// but their cumulative encoded size crosses `CHUNK_MAX_BYTES`.
    #[tokio::test]
    async fn run_import_splits_on_byte_budget_when_entries_are_large() {
        let session = fake_session();
        // 5 MiB content per entry × 4 entries = 20 MiB > 12 MiB budget,
        // forcing a byte-driven split well before the 500 entry cap.
        let big = "a".repeat(5 * 1024 * 1024);
        let entries: Vec<CanonicalEntry> = (0..4)
            .map(|i| {
                let mut e = entry_with(&format!("eid-{i}"), 1_000 + i as i64, vec![]);
                e.content = big.clone();
                e
            })
            .collect();
        let fake = FakeImportClient::default();
        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);

        let batches = fake.batches();
        assert!(
            batches.len() >= 2,
            "expected byte-driven split to produce >=2 chunks, got {}",
            batches.len()
        );
        for b in &batches {
            assert!(
                b.memories.len() <= CHUNK_MAX_ENTRIES,
                "chunk over entry cap: {}",
                b.memories.len()
            );
        }
    }

    /// `split_chunks` boundary: a single oversized entry stays as a
    /// one-element chunk so the server (not the importer) is the one
    /// that surfaces the size violation as ResourceExhausted.
    #[test]
    fn split_chunks_keeps_oversized_entry_as_singleton() {
        let mut e = PbBatchMemoryInput {
            memory: Some(MemoryData {
                content: "x".repeat(CHUNK_MAX_BYTES + 1024),
                ..Default::default()
            }),
            parent_external_ids: Vec::new(),
        };
        // give it a non-empty external_id to make encoding realistic
        e.memory.as_mut().unwrap().external_id = Some("only".to_string());
        let chunks = split_chunks(&[e]);
        assert_eq!(chunks, vec![0..1]);
    }

    // --- pre-batch media resolution ----------------------------------

    fn entry_with_attachment(eid: &str, ts: i64, attachment: serde_json::Value) -> CanonicalEntry {
        let mut e = entry_with(eid, ts, vec![]);
        e.canonical.attachment = Some(attachment);
        e
    }

    /// Decode the metadata JSON the importer sent for the raw (pre-namespacing)
    /// `eid` from the single recorded batch. All callers use `fake_session()`
    /// (source_id "codex") with creator_user_id 1, so the expected id is
    /// derived the same way production code namespaces it.
    fn sent_memory<'a>(
        batch: &'a AddMemoriesBatchRequest,
        eid: &str,
    ) -> &'a protobuf::llm_memory::data::MemoryData {
        let expected = namespace_external_id("codex", 1, eid);
        batch
            .memories
            .iter()
            .map(|m| m.memory.as_ref().unwrap())
            .find(|m| m.external_id.as_deref() == Some(expected.as_str()))
            .expect("memory for eid not in batch")
    }

    #[tokio::test]
    async fn inline_base64_attachment_becomes_media_object_and_drops_blob() {
        let session = fake_session();
        let att = serde_json::json!({
            "storage": "inline_base64",
            "media_type": "image/png",
            "data": "aGVsbG8=",
        });
        let entries = vec![entry_with_attachment("img", 100, att)];
        let fake = FakeImportClient::default();

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.media_objects_linked, 1);
        assert_eq!(res.media_link_failures, 0);
        assert_eq!(fake.uploads().len(), 1, "one Upload RPC");
        assert_eq!(fake.uploads()[0].media_type, "image/png");

        let batches = fake.batches();
        let batch = &batches[0];
        let m = sent_memory(batch, "img");
        assert!(
            m.media_object_id.is_some(),
            "media_object_id must be set from Upload"
        );
        let meta: serde_json::Value = serde_json::from_str(m.metadata.as_deref().unwrap()).unwrap();
        assert!(
            meta.get("attachment").is_none(),
            "metadata.attachment must be dropped once resolved"
        );
    }

    #[tokio::test]
    async fn audio_attachment_preserves_audio_kind_in_media_object() {
        let session = fake_session();
        // codex input_audio produces kind="audio". The media_object must
        // be tagged AUDIO (not IMAGE) so the embedding pipeline does not
        // treat audio bytes as an image and the type is not lost.
        let att = serde_json::json!({
            "kind": "audio",
            "storage": "inline_base64",
            "media_type": "audio/mpeg",
            "data": "YXVkaW8=",
        });
        let entries = vec![entry_with_attachment("aud", 100, att)];
        let fake = FakeImportClient::default();

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.media_objects_linked, 1);
        let uploads = fake.uploads();
        assert_eq!(uploads.len(), 1);
        assert_eq!(
            uploads[0].kind,
            protobuf::llm_memory::data::ContentType::Audio,
            "audio attachment must keep AUDIO kind, not be coerced to IMAGE"
        );

        let batches = fake.batches();
        let m = sent_memory(&batches[0], "aud");
        assert!(m.media_object_id.is_some());
        let meta: serde_json::Value = serde_json::from_str(m.metadata.as_deref().unwrap()).unwrap();
        assert!(meta.get("attachment").is_none());
    }

    #[tokio::test]
    async fn url_attachment_registers_and_drops_blob() {
        let session = fake_session();
        let att = serde_json::json!({
            "storage": "url",
            "media_type": "image/jpeg",
            "url": "https://example.test/cat.jpg",
        });
        let entries = vec![entry_with_attachment("u", 100, att)];
        let fake = FakeImportClient::default();

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.media_objects_linked, 1);
        assert_eq!(fake.registers().len(), 1, "one Register RPC");
        assert_eq!(fake.registers()[0].url, "https://example.test/cat.jpg");
        assert!(fake.uploads().is_empty(), "url path must not Upload");

        let batches = fake.batches();
        let m = sent_memory(&batches[0], "u");
        assert!(m.media_object_id.is_some());
        let meta: serde_json::Value = serde_json::from_str(m.metadata.as_deref().unwrap()).unwrap();
        assert!(meta.get("attachment").is_none());
    }

    #[tokio::test]
    async fn elided_attachment_keeps_blob_no_media_call() {
        let session = fake_session();
        let att = serde_json::json!({
            "storage": "elided",
            "media_type": "image/png",
            "data_sha256": "abc123",
            "size_bytes": 4096,
        });
        let entries = vec![entry_with_attachment("e", 100, att.clone())];
        let fake = FakeImportClient::default();

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.media_objects_linked, 0);
        assert_eq!(res.media_link_failures, 0);
        assert!(fake.uploads().is_empty());
        assert!(fake.registers().is_empty());

        let batches = fake.batches();
        let m = sent_memory(&batches[0], "e");
        assert!(m.media_object_id.is_none());
        let meta: serde_json::Value = serde_json::from_str(m.metadata.as_deref().unwrap()).unwrap();
        assert_eq!(
            meta.get("attachment"),
            Some(&att),
            "elided attachment must be preserved for later migration"
        );
    }

    #[tokio::test]
    async fn upload_failure_keeps_blob_and_continues_batch() {
        let session = fake_session();
        let bad = serde_json::json!({
            "storage": "inline_base64", "media_type": "image/png", "data": "aGVsbG8=",
        });
        let good = serde_json::json!({
            "storage": "url", "media_type": "image/gif", "url": "https://x.test/g.gif",
        });
        let entries = vec![
            entry_with_attachment("bad", 100, bad),
            entry_with_attachment("good", 200, good),
            entry_with("plain", 300, vec![]),
        ];
        let fake = FakeImportClient::default();
        fake.fail_upload_for("image/png");

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "batch must not abort: {:?}", res.error);
        assert_eq!(res.media_objects_linked, 1, "only the url one resolved");
        assert_eq!(res.media_link_failures, 1, "the failed upload counted");
        assert_eq!(res.memories_imported, 3, "all entries still imported");

        let batches = fake.batches();
        let batch = &batches[0];
        let bad_m = sent_memory(batch, "bad");
        assert!(bad_m.media_object_id.is_none());
        let bad_meta: serde_json::Value =
            serde_json::from_str(bad_m.metadata.as_deref().unwrap()).unwrap();
        assert!(
            bad_meta.get("attachment").is_some(),
            "failed-upload attachment must be kept"
        );
        let good_m = sent_memory(batch, "good");
        assert!(good_m.media_object_id.is_some());
    }

    #[tokio::test]
    async fn no_attachment_entry_is_unaffected() {
        let session = fake_session();
        let entries = vec![entry_with("plain", 100, vec![])];
        let fake = FakeImportClient::default();

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.media_objects_linked, 0);
        assert!(fake.uploads().is_empty());
        assert!(fake.registers().is_empty());
        let batches = fake.batches();
        let m = sent_memory(&batches[0], "plain");
        assert!(m.media_object_id.is_none());
    }

    // Re-import dedup: an entry whose external_id is already in the DB
    // (reused by upsert_by_external_id) must NOT have its attachment
    // re-resolved — that would re-upload bytes and leak orphan media.

    #[tokio::test]
    async fn reimport_skips_media_for_already_existing_external_id() {
        let session = fake_session(); // prefix = "codex:1:sfake:"
        let att = serde_json::json!({
            "storage": "url",
            "media_type": "image/png",
            "url": "https://example.test/x.png",
        });
        let entries = vec![entry_with_attachment("codex:sfake:old", 100, att)];
        let fake = FakeImportClient::default();
        // The pre-fetch reports this external_id as already imported.
        fake.mark_existing("codex:1:sfake:old");

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(
            res.media_objects_linked, 0,
            "existing entry's media must NOT be resolved on re-import"
        );
        assert!(
            fake.registers().is_empty() && fake.uploads().is_empty(),
            "no Upload/Register for an already-imported entry"
        );
    }

    #[tokio::test]
    async fn reimport_still_resolves_media_for_new_external_id() {
        let session = fake_session();
        let att = serde_json::json!({
            "storage": "url",
            "media_type": "image/png",
            "url": "https://example.test/new.png",
        });
        let entries = vec![entry_with_attachment("codex:sfake:new", 100, att)];
        let fake = FakeImportClient::default();
        // A *different* external_id is pre-existing; the new one is not.
        fake.mark_existing("codex:sfake:other");

        let res = run_import(&fake, &session, entries, 0, None, 1, &[]).await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.media_objects_linked, 1, "new entry still resolved");
        assert_eq!(fake.registers().len(), 1);
    }

    #[tokio::test]
    async fn reimport_looks_up_only_current_attachment_ids_including_hashed_ids() {
        let session = fake_session();
        let attachment = serde_json::json!({
            "storage": "url",
            "media_type": "image/png",
            "url": "https://example.test/large.png",
        });
        let long_id = format!("codex:sfake:{}", "x".repeat(EXTERNAL_ID_MAX_BYTES));
        let expected = namespace_external_id("codex", 1, &long_id);
        let unrelated = namespace_external_id(
            "codex",
            1,
            &format!("codex:other:{}", "y".repeat(EXTERNAL_ID_MAX_BYTES)),
        );
        let fake = FakeImportClient::default();
        fake.mark_existing(&unrelated);

        let result = run_import(
            &fake,
            &session,
            vec![entry_with_attachment(&long_id, 100, attachment)],
            0,
            None,
            1,
            &[],
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(fake.exact_external_id_queries(), vec![expected]);
        assert!(fake.prefix_queries().is_empty());
        assert_eq!(result.media_objects_linked, 1);
    }

    #[tokio::test]
    async fn streaming_reimport_checks_each_attachment_id_without_namespace_scan() {
        let session = fake_session();
        let first = "codex:sfake:first";
        let second = "codex:sfake:second";
        let attachment = serde_json::json!({
            "storage": "url",
            "media_type": "image/png",
            "url": "https://example.test/reimport.png",
        });
        let fake = FakeImportClient::default();
        let first_scoped = namespace_external_id("codex", 1, first);
        let second_scoped = namespace_external_id("codex", 1, second);
        fake.mark_existing(&first_scoped);
        fake.mark_existing(&second_scoped);

        let result = run_import_streaming(
            &fake,
            &session,
            stream_of(vec![
                entry_with_attachment(first, 100, attachment.clone()),
                entry_with_attachment(second, 101, attachment),
            ]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;

        assert!(result.error.is_none(), "{:?}", result.error);
        assert_eq!(result.media_objects_linked, 0);
        assert!(fake.uploads().is_empty() && fake.registers().is_empty());
        assert_eq!(
            fake.exact_external_id_queries(),
            vec![first_scoped, second_scoped]
        );
    }

    // ---------- streaming importer tests ----------

    fn stream_of(entries: Vec<CanonicalEntry>) -> crate::source::CanonicalEntryStream {
        crate::source::CanonicalEntryStream::from_vec(entries)
    }

    #[derive(Default)]
    struct RecordingEventSink {
        creations: Mutex<Vec<(String, i64)>>,
        completions: Mutex<Vec<(String, i64, usize, bool)>>,
    }

    impl ImportEventSink for RecordingEventSink {
        fn thread_created(&self, session_key: &str, thread_id: i64) -> Result<(), String> {
            self.creations
                .lock()
                .unwrap()
                .push((session_key.to_string(), thread_id));
            Ok(())
        }

        fn session_completed(
            &self,
            session_key: &str,
            thread_id: i64,
            imported_count: usize,
            success: bool,
        ) -> Result<(), String> {
            self.completions.lock().unwrap().push((
                session_key.to_string(),
                thread_id,
                imported_count,
                success,
            ));
            Ok(())
        }
    }

    #[test]
    fn completion_events_cover_existing_retry_failure_and_threadless_results() {
        let sink = RecordingEventSink::default();
        let mut retry = CanonicalSessionResult {
            session_id: "retry".into(),
            session_key: Some("codex:retry".into()),
            thread_id: Some(9_007_199_254_740_993),
            thread_created: false,
            memories_skipped_duplicate: 3,
            ..Default::default()
        };
        let mut failure = CanonicalSessionResult {
            session_id: "failure".into(),
            session_key: Some("codex:failure".into()),
            thread_id: Some(43),
            memories_imported: 1,
            error: Some("later chunk failed".into()),
            ..Default::default()
        };
        let mut threadless = CanonicalSessionResult {
            session_id: "parse-failure".into(),
            error: Some("invalid input".into()),
            ..Default::default()
        };

        emit_session_completion(&mut retry, Some(&sink));
        emit_session_completion(&mut failure, Some(&sink));
        emit_session_completion(&mut threadless, Some(&sink));

        assert_eq!(
            *sink.completions.lock().unwrap(),
            vec![
                ("codex:retry".into(), 9_007_199_254_740_993, 3, true),
                ("codex:failure".into(), 43, 1, false),
            ]
        );
    }

    #[tokio::test]
    async fn creation_event_precedes_a_later_chunk_failure() {
        let fake = FakeImportClient::default();
        fake.push_batch(AddMemoriesBatchResponse {
            thread_id: Some(PbThreadId {
                value: 9_007_199_254_740_993,
            }),
            thread_created: true,
            suppressed: false,
            outcomes: vec![AddMemoryOutcome {
                memory_id: Some(PbMemoryId { value: 1 }),
                created: true,
                position: 0,
                existing_parent_ids_empty: false,
                resolved_parent_ids: Vec::new(),
            }],
        });
        fake.fail_batch_call(2);
        let sink = RecordingEventSink::default();

        let result = run_import_streaming_with_event_sink(
            &fake,
            &fake_session(),
            stream_of(vec![
                entry_with("first", 1, vec![]),
                entry_with("second", 2, vec![]),
            ]),
            0,
            None,
            1,
            &[],
            ChunkLimits {
                max_entries: 1,
                max_bytes: usize::MAX,
            },
            Some(&sink),
            &HashMap::new(),
        )
        .await;

        assert!(
            result
                .error
                .as_deref()
                .is_some_and(|e| e.contains("simulated batch failure"))
        );
        assert_eq!(
            *sink.creations.lock().unwrap(),
            vec![("codex:sfake".to_string(), 9_007_199_254_740_993)]
        );
        assert_eq!(result.cumulative_imported_count(), 1);
    }

    struct FailingEventSink;

    impl ImportEventSink for FailingEventSink {
        fn thread_created(&self, _session_key: &str, _thread_id: i64) -> Result<(), String> {
            Err("event stdout closed".into())
        }

        fn session_completed(
            &self,
            _session_key: &str,
            _thread_id: i64,
            _imported_count: usize,
            _success: bool,
        ) -> Result<(), String> {
            Err("event stdout closed".into())
        }
    }

    #[tokio::test]
    async fn creation_event_output_failure_fails_the_session_immediately() {
        let result = run_import_streaming_with_event_sink(
            &FakeImportClient::default(),
            &fake_session(),
            stream_of(vec![entry_with("first", 1, vec![])]),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
            Some(&FailingEventSink),
            &HashMap::new(),
        )
        .await;

        assert_eq!(result.thread_id, Some(9_000));
        assert_eq!(result.error.as_deref(), Some("event stdout closed"));
        assert_eq!(result.memories_imported, 0);
    }

    #[test]
    fn chunk_builder_would_overflow_count_only() {
        let mut b = ChunkBuilder::new(ChunkLimits {
            max_entries: 2,
            max_bytes: 10_000,
        });
        assert!(!b.would_overflow(100), "empty builder accepts any size");
        b.push(entry_with("a", 1, vec![]), PbBatchMemoryInput::default(), 1);
        assert!(!b.would_overflow(1), "1/2 entries — still room");
        b.push(entry_with("b", 2, vec![]), PbBatchMemoryInput::default(), 1);
        assert!(b.would_overflow(1), "3rd entry would exceed max_entries=2");
    }

    #[test]
    fn chunk_builder_would_overflow_bytes_only() {
        let mut b = ChunkBuilder::new(ChunkLimits {
            max_entries: 100,
            max_bytes: 50,
        });
        b.push(
            entry_with("a", 1, vec![]),
            PbBatchMemoryInput::default(),
            30,
        );
        assert!(
            !b.would_overflow(15),
            "30+15 = 45 ≤ 50, still within byte cap"
        );
        assert!(b.would_overflow(30), "30+30 = 60 > 50, exceeds byte cap");
    }

    #[test]
    fn chunk_builder_take_resets_state() {
        let mut b = ChunkBuilder::new(ChunkLimits::default());
        b.push(entry_with("a", 1, vec![]), PbBatchMemoryInput::default(), 1);
        b.push(entry_with("b", 2, vec![]), PbBatchMemoryInput::default(), 1);
        assert_eq!(b.len(), 2);
        let (entries, inputs) = b.take();
        assert_eq!(entries.len(), 2);
        assert_eq!(inputs.len(), 2);
        assert!(b.is_empty(), "take() empties the builder");
        assert_eq!(b.acc_bytes, 0, "take() resets byte accumulator");
    }

    #[tokio::test]
    async fn run_import_streaming_matches_run_import_for_small_session() {
        // The streaming importer must produce the same observable
        // `CanonicalSessionResult` as the legacy `run_import` when
        // handed the same entries — both paths coexist for collector
        // (`--prune-missing`) callers.
        let session = fake_session();
        let entries = vec![
            entry_with("a", 100, vec![]),
            entry_with("b", 200, vec![]),
            entry_with("c", 300, vec![]),
        ];
        let fake = FakeImportClient::default();
        // One AddMemoriesBatch response covering all 3 (no overflow
        // with default chunk limits).
        fake.push_batch(AddMemoriesBatchResponse {
            thread_id: Some(PbThreadId { value: 42 }),
            thread_created: true,
            suppressed: false,
            outcomes: vec![
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 1 }),
                    created: true,
                    position: 0,
                    existing_parent_ids_empty: true,
                    resolved_parent_ids: Vec::new(),
                },
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 2 }),
                    created: true,
                    position: 1,
                    existing_parent_ids_empty: true,
                    resolved_parent_ids: Vec::new(),
                },
                AddMemoryOutcome {
                    memory_id: Some(PbMemoryId { value: 3 }),
                    created: true,
                    position: 2,
                    existing_parent_ids_empty: true,
                    resolved_parent_ids: Vec::new(),
                },
            ],
        });

        let res = run_import_streaming(
            &fake,
            &session,
            stream_of(entries),
            0,
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.memories_imported, 3);
        assert_eq!(res.memories_skipped_duplicate, 0);
        assert_eq!(res.thread_id, Some(42));
        assert!(res.thread_created);
        // One AddMemoriesBatch call (single chunk).
        assert_eq!(fake.batches().len(), 1);
    }

    #[tokio::test]
    async fn run_import_streaming_flushes_multiple_chunks_under_entry_cap() {
        // With max_entries=2, 5 entries split into 3 chunks (2+2+1).
        // Each chunk must arrive as a separate AddMemoriesBatch call,
        // i.e. the importer is genuinely back-pressuring on chunk
        // boundaries rather than buffering the whole session.
        let session = fake_session();
        let entries: Vec<CanonicalEntry> = (0..5)
            .map(|i| entry_with(&format!("e{i}"), 100 + i as i64, vec![]))
            .collect();

        let fake = FakeImportClient::default();
        // Same thread for all chunks: first batch creates thread 7,
        // subsequent batches re-use it.
        for chunk_size in [2usize, 2, 1] {
            fake.push_batch(AddMemoriesBatchResponse {
                thread_id: Some(PbThreadId { value: 7 }),
                thread_created: false,
                suppressed: false,
                outcomes: (0..chunk_size)
                    .map(|j| AddMemoryOutcome {
                        memory_id: Some(PbMemoryId {
                            value: 100 + j as i64,
                        }),
                        created: true,
                        position: j as i32,
                        existing_parent_ids_empty: true,
                        resolved_parent_ids: Vec::new(),
                    })
                    .collect(),
            });
        }

        let res = run_import_streaming(
            &fake,
            &session,
            stream_of(entries),
            0,
            None,
            1,
            &[],
            ChunkLimits {
                max_entries: 2,
                max_bytes: 1_000_000,
            },
        )
        .await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.memories_imported, 5);
        assert_eq!(fake.batches().len(), 3, "expected 3 chunks");
    }

    #[tokio::test]
    async fn run_import_streaming_counts_filtered_and_warn() {
        // `StreamItem::Filtered` and `StreamItem::Warn` must both bump
        // `memories_skipped_filtered` without reaching the backend.
        let session = fake_session();
        let fake = FakeImportClient::default();
        // 1 importable entry → 1 batch.
        fake.push_batch(AddMemoriesBatchResponse {
            thread_id: Some(PbThreadId { value: 1 }),
            thread_created: true,
            suppressed: false,
            outcomes: vec![AddMemoryOutcome {
                memory_id: Some(PbMemoryId { value: 1 }),
                created: true,
                position: 0,
                existing_parent_ids_empty: true,
                resolved_parent_ids: Vec::new(),
            }],
        });

        let stream = crate::source::CanonicalEntryStream::new(
            vec![
                StreamItem::Filtered,
                StreamItem::Warn("simulated".to_string()),
                StreamItem::Entry(entry_with("ok", 100, vec![])),
                StreamItem::Filtered,
            ]
            .into_iter(),
        );
        let res = run_import_streaming(
            &fake,
            &session,
            stream,
            5, // pre-pass filtered (e.g. source-side --include-types)
            None,
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.memories_imported, 1);
        // 5 pre-pass + 2 Filtered + 1 Warn = 8.
        assert_eq!(res.memories_skipped_filtered, 8);
    }

    #[tokio::test]
    async fn run_import_streaming_skips_since_filtered_entries() {
        // Runner-side --since must drop entries whose timestamp is older
        // than the cutoff, same as `run_import`.
        let session = fake_session();
        let entries = vec![
            entry_with("old", 100, vec![]), // dropped
            entry_with("new", 300, vec![]),
        ];
        let fake = FakeImportClient::default();
        fake.push_batch(AddMemoriesBatchResponse {
            thread_id: Some(PbThreadId { value: 9 }),
            thread_created: true,
            suppressed: false,
            outcomes: vec![AddMemoryOutcome {
                memory_id: Some(PbMemoryId { value: 1 }),
                created: true,
                position: 0,
                existing_parent_ids_empty: true,
                resolved_parent_ids: Vec::new(),
            }],
        });

        let res = run_import_streaming(
            &fake,
            &session,
            stream_of(entries),
            0,
            Some(200),
            1,
            &[],
            ChunkLimits::default(),
        )
        .await;
        assert!(res.error.is_none(), "{:?}", res.error);
        assert_eq!(res.memories_imported, 1);
        assert_eq!(res.memories_skipped_filtered, 1);
    }

    #[test]
    fn thread_group_endpoint_maps_known_and_unknown_scope() {
        let known = ThreadGroupSourceIdentity {
            source: "codex".into(),
            native_kind: "session",
            owner_scope: None,
            identity_scope: ThreadGroupIdentityScope::Known(String::new()),
            native_id: "s1".into(),
        };
        let proto = endpoint_to_proto(&known, 7);
        assert!(proto.identity_scope_known);
        assert_eq!(proto.identity_scope, "");
        assert_eq!(proto.owner_scope, "user:7");
        assert_eq!(proto.user_id, Some(7));
        assert_eq!(proto.native_id, "s1");

        let unknown = ThreadGroupSourceIdentity {
            identity_scope: ThreadGroupIdentityScope::Unknown,
            ..known
        };
        let proto = endpoint_to_proto(&unknown, 7);
        assert!(!proto.identity_scope_known);
        assert_eq!(proto.user_id, Some(7));
    }

    #[test]
    fn thread_group_evidence_mappers_cover_all_variants() {
        assert_eq!(
            evidence_kind_to_proto(ThreadGroupEvidenceKind::SourceEvent),
            ThreadEvidenceKind::SourceEvent as i32
        );
        assert_eq!(
            evidence_kind_to_proto(ThreadGroupEvidenceKind::NegativeOrConflict),
            ThreadEvidenceKind::NegativeOrConflict as i32
        );
        assert_eq!(
            polarity_to_proto(ThreadGroupPolarity::Conflicts),
            ThreadObservationPolarity::Conflicts as i32
        );
        assert_eq!(
            confidence_to_proto(ThreadGroupConfidence::Strong),
            ThreadEvidenceConfidence::Strong as i32
        );
        assert_eq!(
            confidence_to_proto(ThreadGroupConfidence::Unsupported),
            ThreadEvidenceConfidence::Unsupported as i32
        );
    }
}
