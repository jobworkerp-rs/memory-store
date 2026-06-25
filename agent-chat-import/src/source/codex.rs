//! OpenAI Codex CLI rollout source (`~/.codex/sessions/<y>/<m>/<d>/rollout-*.jsonl`).
//!
//! See `docs/agent-chat-import-subcommand-spec.md` §5.2 for the
//! per-type semantics. Entry IDs are composed under §5.2.1
//! (`<kind_prefix>:<line_ordinal>:<sha1[...]>`); call/output linkage
//! is resolved here in a single pass through the rollout (§5.2.2)
//! so that the shared importer never sees raw `call_id`s.

#![allow(dead_code)]
// We deliberately build CanonicalAddons in two stages — set the
// kind-specific field (`tool`/`attachment`) first, then optionally
// merge a `raw` payload via `merge_raw`. Rewriting that as a single
// struct literal would force every emit site into a let-else for
// the JSON serialization, so we accept the Default + reassign
// pattern crate-wide here.
#![allow(clippy::field_reassign_with_default)]

use crate::cli::CodexArgs;
use crate::common::canonical;
use crate::common::ids::{sha1_hex_prefix, sha256_hex_prefix};
use crate::common::path::apply_path_prefix;
use crate::parser::parse_timestamp_millis;
use crate::source::{
    CanonicalAddons, CanonicalEntry, CanonicalSession, ChatSource, ReadSessionOutcome,
    mtime_skip_outcome,
};
use anyhow::{Context, Result};
use protobuf::llm_memory::data::{ContentType, MessageRole};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

const SOURCE_ID: &str = "codex";

/// `ChatSource` implementation backed by `--session-file` /
/// `--day-dir` / `--all-sessions`.
pub struct CodexSource {
    args: CodexArgs,
}

impl CodexSource {
    pub fn new(args: CodexArgs) -> Self {
        Self { args }
    }
}

impl ChatSource for CodexSource {
    type SessionInput = PathBuf;

    fn id(&self) -> &str {
        SOURCE_ID
    }

    fn input_label(&self, input: &Self::SessionInput) -> String {
        input.display().to_string()
    }

    fn discover(&self) -> Result<Vec<Self::SessionInput>> {
        if let Some(ref f) = self.args.session_file {
            return Ok(vec![f.clone()]);
        }
        let codex_dir = self.args.resolved_codex_dir();
        let sessions_dir = codex_dir.join("sessions");
        if let Some(ref day) = self.args.day_dir {
            // Spec §3.4 / CLI help: `--day-dir` is documented as
            // "under `<codex-dir>/sessions/<yyyy>/<mm>/<dd>/`". Treat
            // a relative argument as `sessions/`-relative so users can
            // type `--day-dir 2026/05/02` like the help shows; absolute
            // paths still pass through for the escape-hatch case.
            let resolved = if day.is_absolute() {
                day.clone()
            } else {
                sessions_dir.join(day)
            };
            return collect_rollout_files(&resolved);
        }
        // --all-sessions: <codex-dir>/sessions/<yyyy>/<mm>/<dd>/rollout-*.jsonl
        if !sessions_dir.exists() {
            anyhow::bail!("Sessions directory not found: {}", sessions_dir.display());
        }
        let mut all = Vec::new();
        walk_rollout_tree(&sessions_dir, &mut all)?;
        all.sort();
        Ok(all)
    }

    fn read_session(
        &self,
        input: &Self::SessionInput,
        since_millis_with_margin: Option<i64>,
    ) -> Result<ReadSessionOutcome> {
        if let Some(skip) = mtime_skip_outcome(input.as_path(), since_millis_with_margin) {
            return Ok(skip);
        }
        let include_types = self.args.include_types_set();
        let link_tool_calls = self.args.effective_link_tool_calls();
        // Default: do NOT persist `encrypted_content`. The opt-in flag
        // is kept verbose because the data is sensitive and large; the
        // legacy `--exclude-encrypted-reasoning` flag is accepted as a
        // no-op for backwards compatibility.
        let include_encrypted_reasoning = self.args.include_encrypted_reasoning;
        let strip_prefixes = self.args.path_prefixes();

        // Phase 1: stream the JSONL line-by-line so a multi-MB rollout
        // doesn't materialise the raw text and the parsed Vec at the
        // same time. Phase 2 still needs random access for
        // function_call ↔ function_call_output linkage, so the parsed
        // Vec is retained; only the raw String is gone.
        let (parsed_lines, broken_lines) = read_parsed_lines(input)?;
        if parsed_lines.is_empty() {
            // Even when nothing parsed, a file full of broken lines
            // should not be confused with an empty rollout.
            let reason = if broken_lines > 0 {
                format!("rollout had {broken_lines} unparseable line(s) and no usable records")
            } else {
                "empty rollout".to_string()
            };
            return Ok(ReadSessionOutcome::Skipped {
                session_id_hint: filename_stem(input),
                reason,
                filtered_count: broken_lines as u32,
            });
        }

        let Some((meta_line_idx, meta)) = parsed_lines
            .iter()
            .find(|(_, l)| l.record_type == "session_meta")
        else {
            return Ok(ReadSessionOutcome::Skipped {
                session_id_hint: filename_stem(input),
                reason: "session_meta missing".to_string(),
                filtered_count: parsed_lines.len() as u32,
            });
        };
        let session_id = match meta.payload.get("id").and_then(|v| v.as_str()) {
            Some(id) if !id.is_empty() => id.to_string(),
            _ => {
                return Ok(ReadSessionOutcome::Skipped {
                    session_id_hint: filename_stem(input),
                    reason: "session_meta.payload.id missing".to_string(),
                    filtered_count: parsed_lines.len() as u32,
                });
            }
        };
        let mut canonical_session =
            build_canonical_session(&session_id, &meta.payload, &strip_prefixes);

        // Phase 2: walk lines in order, building canonical entries
        // and resolving function_call -> call_id -> external_id so
        // outputs can attach a tool link via `parent_external_ids`.
        let mut entries = Vec::new();
        // Roll the broken-line count into source_filtered so that the
        // top-level summary attributes the loss to the rollout, not to
        // a "0 imported, 0 errors" silent partial.
        let mut source_filtered = broken_lines;
        let mut call_id_map: HashMap<String, String> = HashMap::new();
        // Resolve `metadata.tool.name` for tool_output / event-derived
        // outputs (`exec_command_end` / `patch_apply_end`). The
        // function_call entry stores `(call_id → name)` here so the
        // output entry can pick the canonical name without re-reading
        // the matching call payload. Spec §4.2.2.6 codex.
        let mut call_id_name_map: HashMap<String, String> = HashMap::new();

        for (line_idx, parsed) in &parsed_lines {
            let line_idx = *line_idx as i64;
            let outcome = match parsed.record_type.as_str() {
                "session_meta" => {
                    if !include_types.contains("system") {
                        Ok(EntryDecision::SourceFiltered)
                    } else if line_idx == *meta_line_idx as i64 {
                        Ok(build_session_meta_entry(
                            &parsed.payload,
                            line_idx,
                            &session_id,
                        ))
                    } else {
                        // duplicate session_meta — treat as system event
                        Ok(EntryDecision::SourceFiltered)
                    }
                }
                "turn_context" => {
                    if include_types.contains("system") {
                        Ok(build_turn_context_entry(
                            &parsed.payload,
                            line_idx,
                            &session_id,
                        ))
                    } else {
                        Ok(EntryDecision::SourceFiltered)
                    }
                }
                "response_item" => build_response_item_entry(
                    &parsed.payload,
                    line_idx,
                    &session_id,
                    &include_types,
                    link_tool_calls,
                    include_encrypted_reasoning,
                    &mut call_id_map,
                    &mut call_id_name_map,
                ),
                "event_msg" => build_event_msg_entry(
                    &parsed.payload,
                    line_idx,
                    &session_id,
                    &include_types,
                    link_tool_calls,
                    &call_id_map,
                    &call_id_name_map,
                ),
                other => {
                    tracing::debug!(
                        record_type = other,
                        line = line_idx,
                        "unknown record type, skipping"
                    );
                    Ok(EntryDecision::SourceFiltered)
                }
            };

            // Backfill the entry's timestamp from the rollout line's
            // top-level `timestamp` so `--since` filters work on user
            // / assistant / tool / reasoning entries (build_*_entry
            // leaves `timestamp_ms = 0` because most payloads carry
            // no own timestamp). session_meta has its own payload
            // timestamp and is set inside build_session_meta_entry;
            // we only overwrite when the line-level fallback would be
            // an upgrade.
            //
            // `kind` is no longer set on entry.metadata: §4.2.2.2
            // makes `kind` a reserved key written by run_import in
            // step 3 of the metadata merge.
            let backfill_ts = |entry: &mut CanonicalEntry| {
                if entry.timestamp_ms == 0
                    && let Some(ref ts) = parsed.timestamp
                    && let Some(ms) = parse_timestamp_millis(ts)
                {
                    entry.timestamp_ms = ms;
                }
            };
            match outcome {
                Ok(EntryDecision::Take(mut entry)) => {
                    backfill_ts(&mut entry);
                    entries.push(*entry);
                }
                Ok(EntryDecision::TakeMany {
                    entries: many,
                    filtered,
                }) => {
                    for mut entry in many {
                        backfill_ts(&mut entry);
                        entries.push(entry);
                    }
                    source_filtered += filtered;
                }
                Ok(EntryDecision::SourceFiltered) => source_filtered += 1,
                Ok(EntryDecision::Drop) => {}
                Err(e) => {
                    tracing::warn!(line = line_idx, "codex entry build failed: {e}");
                    source_filtered += 1;
                }
            }
        }

        // Second pass: re-resolve tool_output -> call links. The main
        // loop only consults `call_id_map` forward, so an output whose
        // `function_call` appeared later in the rollout (re-ordered or
        // edited files) would otherwise stay orphaned with empty
        // `parent_external_ids`. By the end of phase 2 the map holds
        // every call's external_id, so a single sweep is enough.
        // Also resolves `metadata.tool.name` for outputs whose call's
        // name only became visible after the output (call_id_name_map
        // is populated forward like call_id_map).
        if link_tool_calls {
            for entry in entries.iter_mut() {
                if entry.kind_tag != "tool_output" {
                    continue;
                }
                // §4.2.2.2: parsers must not stash call_id on
                // entry.metadata, so read it through the canonical
                // getter instead of walking the JSON value directly.
                let Some(call_id) = entry
                    .canonical
                    .tool
                    .as_ref()
                    .and_then(canonical::tool_call_id)
                    .map(|s| s.to_string())
                else {
                    continue;
                };

                if entry.parent_external_ids.is_empty()
                    && let Some(parent) = call_id_map.get(&call_id)
                {
                    entry.parent_external_ids = vec![parent.clone()];
                }
                if let Some(tool) = entry.canonical.tool.as_mut()
                    && canonical::tool_name(tool).is_none()
                    && let Some(name) = call_id_name_map.get(&call_id)
                {
                    // Re-derive `category` together with `name`: when
                    // build_tool_output ran without a name the category
                    // was frozen to null. Filtering by category="shell_exec"
                    // would otherwise miss reordered rollouts.
                    canonical::backfill_tool_name_and_category(tool, SOURCE_ID, name);
                }
            }
        }

        // Bump session.updated_at_ms to the latest entry timestamp so
        // thread.updated_at reflects the latest activity, not the
        // session start. Without this, --since + --summarize-after-*
        // can fail to surface threads whose newest memory is past the
        // since cutoff. Parity with claude-code's
        // `extract_session_info` (max of entry timestamps).
        if let Some(max_ts) = entries.iter().map(|e| e.timestamp_ms).max()
            && max_ts > canonical_session.updated_at_ms
        {
            canonical_session.updated_at_ms = max_ts;
        }

        // Codex rollouts are typically small enough (≲ 50 MB) that
        // carrying the materialised `Vec<CanonicalEntry>` is acceptable
        // — the second-pass tool_output → call_id backfill above
        // requires random access across the full session. We expose
        // them through the new `ImportStream` variant so the importer
        // takes the streaming code path uniformly across sources.
        Ok(ReadSessionOutcome::ImportStream {
            session: canonical_session,
            entries: crate::source::CanonicalEntryStream::from_vec(entries),
            source_filtered_count_initial: source_filtered,
        })
    }
}

#[derive(Deserialize, Debug)]
struct RawLine {
    #[serde(rename = "type")]
    record_type: String,
    timestamp: Option<String>,
    payload: serde_json::Value,
}

enum EntryDecision {
    // Box the CanonicalEntry so the enum's size stays small relative
    // to the no-data variants. clippy::large_enum_variant fires
    // otherwise once `CanonicalAddons` lands on the entry.
    Take(Box<CanonicalEntry>),
    /// One source line decomposed into multiple canonical entries
    /// (e.g. `message.content[]` block split per §4.2.2.6 codex
    /// table). The entries are emitted in `payload.content[]` order;
    /// each is processed independently by the caller (timestamp
    /// backfill, `entries.push(entry)`). The vec already lives on
    /// the heap so we don't double-box like `Take`.
    ///
    /// `filtered` counts blocks that the source-side `include_types`
    /// filter dropped *within* this line so the importer summary can
    /// surface a faithful `memories_skipped_filtered`. Without this
    /// number, mixed-block lines (e.g. `-t user,assistant` against a
    /// message carrying text + image) silently undercount the skip.
    TakeMany {
        entries: Vec<CanonicalEntry>,
        filtered: usize,
    },
    SourceFiltered,
    /// Unrecoverable parse miss (e.g. function_call without `call_id`):
    /// counted neither as success nor as include-types filtering.
    Drop,
}

fn build_canonical_session(
    session_id: &str,
    payload: &serde_json::Value,
    strip_prefixes: &[String],
) -> CanonicalSession {
    let cwd = payload
        .get("cwd")
        .and_then(|v| v.as_str())
        .map(String::from);
    let git_branch = payload
        .get("git")
        .and_then(|g| g.get("branch"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let model_provider = payload
        .get("model_provider")
        .and_then(|v| v.as_str())
        .map(String::from);
    let originator = payload
        .get("originator")
        .and_then(|v| v.as_str())
        .map(String::from);
    let cli_version = payload
        .get("cli_version")
        .and_then(|v| v.as_str())
        .map(String::from);

    let timestamp_ms = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(parse_timestamp_millis)
        .unwrap_or(0);

    // Default labels per §5.2.3.
    let mut labels: Vec<String> = vec!["coding_agent".to_string(), "agent:codex".to_string()];
    if let Some(ref cwd_str) = cwd {
        let stripped = apply_path_prefix(cwd_str, strip_prefixes);
        labels.push(crate::common::labels::truncate_label_keep_tail(
            "path:", stripped,
        ));
        if let Some(dir_name) = cwd_str.rsplit_once('/').map(|(_, d)| d)
            && !dir_name.is_empty()
        {
            labels.push(crate::common::labels::truncate_label_keep_tail(
                "dir:", dir_name,
            ));
        }
    }
    if let Some(ref branch) = git_branch {
        labels.push(crate::common::labels::truncate_label_keep_head(
            "branch:", branch,
        ));
    }
    if let Some(ref provider) = model_provider {
        labels.push(crate::common::labels::truncate_label_keep_head(
            "provider:",
            provider,
        ));
    }
    labels.retain(|l| !l.is_empty());

    let channel = format!("{SOURCE_ID}:{session_id}");

    let mut session_meta = serde_json::Map::new();
    if let Some(ref c) = cwd {
        session_meta.insert("cwd".to_string(), serde_json::Value::String(c.clone()));
    }
    if let Some(ref b) = git_branch {
        session_meta.insert(
            "git_branch".to_string(),
            serde_json::Value::String(b.clone()),
        );
    }
    if let Some(ref m) = model_provider {
        session_meta.insert(
            "model_provider".to_string(),
            serde_json::Value::String(m.clone()),
        );
    }
    if let Some(ref o) = originator {
        session_meta.insert(
            "originator".to_string(),
            serde_json::Value::String(o.clone()),
        );
    }
    if let Some(ref v) = cli_version {
        session_meta.insert(
            "cli_version".to_string(),
            serde_json::Value::String(v.clone()),
        );
    }

    CanonicalSession {
        source_id: SOURCE_ID.to_string(),
        session_id: session_id.to_string(),
        channel,
        description: cwd
            .as_deref()
            .and_then(|c| c.rsplit('/').next())
            .map(|s| s.to_string()),
        cwd,
        git_branch,
        created_at_ms: timestamp_ms,
        updated_at_ms: timestamp_ms,
        source_labels: labels,
        source_metadata: serde_json::Value::Object(session_meta),
    }
}

fn build_session_meta_entry(
    payload: &serde_json::Value,
    line_idx: i64,
    session_id: &str,
) -> EntryDecision {
    let timestamp_ms = payload
        .get("timestamp")
        .and_then(|v| v.as_str())
        .and_then(parse_timestamp_millis)
        .unwrap_or(0);

    // base_instructions can be either a plain string (legacy field
    // name `instructions`) or an object `{"text": "..."}` (current
    // schema). Take whichever exists; fall back to empty string.
    let content = extract_base_instructions(payload).unwrap_or_default();

    let mut metadata = serde_json::Map::new();
    for key in ["id", "cwd", "originator", "cli_version", "model_provider"] {
        if let Some(v) = payload.get(key) {
            metadata.insert(key.to_string(), v.clone());
        }
    }
    // `payload.source` (e.g. "cli") describes what produced this rollout
    // and would collide with the spec §4.2.1 reserved `source` key that
    // `build_memory_data` always sets to the canonical source_id. The
    // reserved-key check there `debug_assert!`s on collision, so emitting
    // `source` here panics every Codex import in debug builds. Spec
    // §4.2.1 explicitly suggests `session_source` / `session_event` as
    // the rename target for this kind of "entry-level session-related"
    // metadata.
    if let Some(v) = payload.get("source") {
        metadata.insert("session_source".to_string(), v.clone());
    }
    if let Some(g) = payload.get("git") {
        metadata.insert("git".to_string(), g.clone());
    }

    let entry = CanonicalEntry {
        external_id: format!("{SOURCE_ID}:{session_id}:meta:{line_idx}"),
        parent_external_ids: Vec::new(),
        role: MessageRole::RoleMeta,
        content_type: ContentType::Text,
        content,
        metadata,
        timestamp_ms,
        import_order: line_idx,
        kind_tag: "system",
        canonical: CanonicalAddons::default(),
    };
    EntryDecision::Take(Box::new(entry))
}

fn extract_base_instructions(payload: &serde_json::Value) -> Option<String> {
    if let Some(bi) = payload.get("base_instructions") {
        if let Some(s) = bi.as_str() {
            return Some(s.to_string());
        }
        if let Some(text) = bi.get("text").and_then(|v| v.as_str()) {
            return Some(text.to_string());
        }
    }
    if let Some(s) = payload.get("instructions").and_then(|v| v.as_str()) {
        return Some(s.to_string());
    }
    None
}

fn build_turn_context_entry(
    payload: &serde_json::Value,
    line_idx: i64,
    session_id: &str,
) -> EntryDecision {
    let timestamp_ms = 0; // turn_context has no own timestamp; relies on session ordering
    let payload_json = serde_json::to_string(payload).unwrap_or_default();
    let entry_uid = format!(
        "tctx:{line_idx}:{}",
        sha1_hex_prefix(payload_json.as_bytes(), 16)
    );
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "payload_type".to_string(),
        serde_json::Value::String("turn_context".to_string()),
    );

    EntryDecision::Take(Box::new(CanonicalEntry {
        external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
        parent_external_ids: Vec::new(),
        role: MessageRole::RoleMeta,
        content_type: ContentType::Text,
        content: payload_json,
        metadata,
        timestamp_ms,
        import_order: line_idx,
        kind_tag: "system",
        canonical: CanonicalAddons::default(),
    }))
}

/// Spec §4.2.2.6 codex table — split `payload.type="message"`'s
/// `content[]` into one canonical entry per block (input_text /
/// output_text / input_image / future audio). Block 0 acts as the
/// conversational anchor for block 1..N (block chain rule). text /
/// fallback blocks go through `kind=user`/`assistant`; `input_image`
/// becomes a `kind=attachment` entry whose `image_url` is dispatched
/// by scheme via `parse_image_url` (§4.2.2.6.1).
fn build_message_block_entries(
    payload: &serde_json::Value,
    line_idx: i64,
    session_id: &str,
    include_types: &std::collections::HashSet<String>,
) -> Result<EntryDecision> {
    let role_str = payload.get("role").and_then(|v| v.as_str()).unwrap_or("");
    // Default `kind_tag` for text blocks is determined by role.
    // attachment kind comes from the block type and inherits the
    // role for `MemoryData.role`.
    let role = match role_str {
        "user" => MessageRole::RoleUser,
        "assistant" => MessageRole::RoleAssistant,
        _ => MessageRole::RoleMeta,
    };
    let text_kind: &'static str = match role_str {
        "user" => "user",
        "assistant" => "assistant",
        // legacy / system roles fall back to the system bucket so
        // `--include-types` filters can drop them as a group.
        _ => "system",
    };

    // Codex `payload.content` is **always** an array in current
    // rollouts; if a future variant emits a string we degrade to a
    // single block carrying the raw string.
    let blocks: Vec<serde_json::Value> = match payload.get("content") {
        Some(serde_json::Value::Array(arr)) => arr.clone(),
        Some(serde_json::Value::String(s)) => {
            vec![serde_json::json!({"type": "input_text", "text": s})]
        }
        _ => Vec::new(),
    };

    if blocks.is_empty() {
        // Empty message: emit nothing (the line is filtered, not
        // dropped, so the summary attributes the omission to source).
        return Ok(EntryDecision::SourceFiltered);
    }

    let mut out: Vec<CanonicalEntry> = Vec::new();
    let mut block0_eid: Option<String> = None;
    let mut filtered_blocks: usize = 0;

    for (block_idx, block) in blocks.iter().enumerate() {
        let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let block_payload_json = serde_json::to_string(block).unwrap_or_default();

        // Determine the kind tag and entry_uid prefix for this block.
        let (kind_tag, uid_prefix): (&'static str, &str) = match block_type {
            "input_image" | "input_audio" | "input_video" => ("attachment", "att"),
            // input_text / output_text / unknown text fallback all
            // share the role-derived text kind.
            _ => (text_kind, "msg"),
        };

        // Source-side include-types filter at block granularity. The
        // partial-skip count is propagated up via TakeMany.filtered so
        // mixed-block lines (e.g. text + image with `-t user,assistant`)
        // contribute to the importer summary's
        // `memories_skipped_filtered`.
        if !include_types.contains(kind_tag) {
            filtered_blocks += 1;
            continue;
        }

        let entry_uid = format!(
            "{uid_prefix}:{line_idx}:b{block_idx}:{}",
            sha1_hex_prefix(block_payload_json.as_bytes(), 16)
        );
        let external_id = format!("{SOURCE_ID}:{session_id}:{entry_uid}");

        // block chain rule: block 0 has no parent (codex has no
        // parentUuid; we leave the conversational link unset),
        // block 1..N points back at block 0's external_id.
        let parent_external_ids = match block_idx {
            0 => Vec::new(),
            _ => block0_eid
                .as_ref()
                .map(|eid| vec![eid.clone()])
                .unwrap_or_default(),
        };
        if block_idx == 0 {
            block0_eid = Some(external_id.clone());
        }

        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "payload_type".to_string(),
            serde_json::Value::String("message".to_string()),
        );
        metadata.insert(
            "block_type".to_string(),
            serde_json::Value::String(block_type.to_string()),
        );

        // Build the entry per block type.
        let entry: CanonicalEntry = match block_type {
            "input_image" | "input_audio" | "input_video" => {
                let attachment_kind = match block_type {
                    "input_image" => canonical::AttachmentKind::Image,
                    "input_audio" => canonical::AttachmentKind::Audio,
                    _ => canonical::AttachmentKind::Video,
                };
                let url_field = match block_type {
                    "input_image" => "image_url",
                    "input_audio" => "audio_url",
                    _ => "video_url",
                };
                let url_str = block.get(url_field).and_then(|v| v.as_str()).unwrap_or("");
                let variant = canonical::parse_image_url(url_str);
                let (storage, data_b64, url_for_helper, media_type): (
                    canonical::AttachmentStorage,
                    Option<&str>,
                    Option<&str>,
                    Option<&str>,
                ) = match variant {
                    canonical::ImageUrlVariant::Http { url } => {
                        (canonical::AttachmentStorage::Url, None, Some(url), None)
                    }
                    canonical::ImageUrlVariant::File { url } => {
                        (canonical::AttachmentStorage::Ref, None, Some(url), None)
                    }
                    canonical::ImageUrlVariant::DataBase64 {
                        media_type,
                        base64_payload,
                    } => (
                        canonical::AttachmentStorage::InlineBase64,
                        Some(base64_payload),
                        None,
                        media_type,
                    ),
                    canonical::ImageUrlVariant::DataNonBase64 { media_type, .. } => (
                        canonical::AttachmentStorage::Invalid {
                            reason: "data_url_not_base64".to_string(),
                        },
                        None,
                        None,
                        media_type,
                    ),
                    canonical::ImageUrlVariant::Unrecognized { .. } => (
                        canonical::AttachmentStorage::Invalid {
                            reason: "unrecognized_scheme".to_string(),
                        },
                        None,
                        None,
                        None,
                    ),
                };

                let result = canonical::build_attachment(
                    attachment_kind,
                    storage,
                    media_type,
                    data_b64,
                    url_for_helper,
                    None,
                    None,
                    None,
                );

                CanonicalEntry {
                    external_id,
                    parent_external_ids,
                    role,
                    content_type: result.content_type,
                    content: result.content,
                    metadata,
                    timestamp_ms: 0,
                    import_order: line_idx,
                    kind_tag: "attachment",
                    canonical: CanonicalAddons::with_attachment(
                        SOURCE_ID,
                        result.attachment,
                        block,
                    ),
                }
            }
            // text-bearing blocks: input_text / output_text / fallback
            _ => {
                // Unknown block shapes degrade to the raw block JSON
                // so content is never silently lost; the raw payload
                // is also saved under metadata.raw.codex below.
                let text = block
                    .get("text")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| block_payload_json.clone());

                let canonical_addons = if matches!(block_type, "input_text" | "output_text") {
                    CanonicalAddons::default()
                } else {
                    let mut addons = CanonicalAddons::default();
                    canonical::merge_raw(
                        &mut addons.raw,
                        canonical::raw_entry(SOURCE_ID, block.clone()),
                    );
                    addons
                };

                CanonicalEntry {
                    external_id,
                    parent_external_ids,
                    role,
                    content_type: ContentType::Text,
                    content: text,
                    metadata,
                    timestamp_ms: 0,
                    import_order: line_idx,
                    kind_tag,
                    canonical: canonical_addons,
                }
            }
        };
        out.push(entry);
    }

    // Always report `filtered_blocks` at block granularity, even when
    // every block was dropped: a 3-image message read with
    // `-t user,assistant` represents 3 dropped memory candidates, not
    // 1 dropped line. Reporting `SourceFiltered` here would tally a
    // single +1 and contradict the partial-skip path right above —
    // mixing line-level and block-level units in the same summary.
    Ok(EntryDecision::TakeMany {
        entries: out,
        filtered: filtered_blocks,
    })
}

#[allow(clippy::too_many_arguments)]
fn build_response_item_entry(
    payload: &serde_json::Value,
    line_idx: i64,
    session_id: &str,
    include_types: &std::collections::HashSet<String>,
    link_tool_calls: bool,
    include_encrypted_reasoning: bool,
    call_id_map: &mut HashMap<String, String>,
    call_id_name_map: &mut HashMap<String, String>,
) -> Result<EntryDecision> {
    let payload_type = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    match payload_type {
        "message" => build_message_block_entries(payload, line_idx, session_id, include_types),
        "function_call" | "custom_tool_call" => {
            // Resolve call_id and register the call_id → name mapping
            // BEFORE the include_types gate. Otherwise `-t tool_output`
            // (which excludes tool_call) leaves call_id_name_map empty,
            // and downstream function_call_output entries lose
            // `metadata.tool.name` / `category` even when the matching
            // function_call exists in the source. Spec §4.2.2.3
            // category resolution depends on this map.
            let Some(call_id) = payload.get("call_id").and_then(|v| v.as_str()) else {
                tracing::warn!(line = line_idx, "function_call missing call_id");
                return Ok(EntryDecision::Drop);
            };
            let name = payload.get("name").and_then(|v| v.as_str());
            if let Some(n) = name {
                call_id_name_map.insert(call_id.to_string(), n.to_string());
            }

            if !include_types.contains("tool_call") {
                // Skip the entry but keep the name map populated above
                // so tool_output entries can still resolve their tool
                // identity. parent_external_ids is intentionally NOT
                // populated here: with no tool_call entry persisted,
                // there is no parent to point at.
                return Ok(EntryDecision::SourceFiltered);
            }

            let prefix = if payload_type == "function_call" {
                "call"
            } else {
                "ctcall"
            };
            let entry_uid = format!(
                "{prefix}:{line_idx}:{}",
                sha1_hex_prefix(call_id.as_bytes(), 16)
            );
            let external_id = format!("{SOURCE_ID}:{session_id}:{entry_uid}");
            if link_tool_calls {
                call_id_map.insert(call_id.to_string(), external_id.clone());
            }

            // Tool name normalization (§4.2.2.3): function_call carries
            // `arguments` as a string of JSON; custom_tool_call carries
            // `input` instead. We try to parse arguments as JSON for
            // structured storage, falling back to the raw string when
            // the provider sent something not strictly parseable.
            let arguments_value: Option<serde_json::Value> = if payload_type == "function_call" {
                payload.get("arguments").map(|args| {
                    args.as_str()
                        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                        .unwrap_or_else(|| args.clone())
                })
            } else {
                payload.get("input").cloned()
            };

            let tool_result = canonical::build_tool_call(
                SOURCE_ID,
                name,
                Some(call_id),
                arguments_value.as_ref(),
                None,
            );

            // Source-free metadata: keep `payload_type` as a hint for
            // downstream queries that want to distinguish
            // function_call vs custom_tool_call. `kind` / `tool` are
            // run_import / canonical territory and must not be
            // duplicated here (§4.2.2.2 contract (a)).
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );

            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id,
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleAssistant,
                content_type: ContentType::Tool,
                content: tool_result.content,
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "tool_call",
                canonical: CanonicalAddons::with_tool(SOURCE_ID, tool_result.tool, payload),
            })))
        }
        "function_call_output" | "custom_tool_call_output" => {
            if !include_types.contains("tool_output") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let Some(call_id) = payload.get("call_id").and_then(|v| v.as_str()) else {
                tracing::warn!(line = line_idx, "function_call_output missing call_id");
                return Ok(EntryDecision::Drop);
            };
            let prefix = if payload_type == "function_call_output" {
                "output"
            } else {
                "ctout"
            };
            let entry_uid = format!(
                "{prefix}:{line_idx}:{}",
                sha1_hex_prefix(call_id.as_bytes(), 16)
            );
            let parent = if link_tool_calls {
                call_id_map.get(call_id).cloned()
            } else {
                None
            };

            // The function_call_output payload exposes the tool's
            // textual result via `output` (codex CLI emits this as
            // either a JSON object or a plain string). We accept both:
            // strings flow straight through, objects are JSON-stringified
            // so the canonical layer always sees a `&str`.
            let output_text = payload.get("output").map(|v| match v {
                serde_json::Value::String(s) => s.clone(),
                other => serde_json::to_string(other).unwrap_or_default(),
            });
            let resolved_name = call_id_name_map.get(call_id).cloned();
            let tool_result = canonical::build_tool_output(
                SOURCE_ID,
                resolved_name.as_deref(),
                Some(call_id),
                output_text.as_deref(),
                // function_call_output carries no explicit success/error
                // signal — `status` stays null per §4.2.2.3 tri-state.
                None,
                None,
            );

            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );

            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: parent.map_or_else(Vec::new, |p| vec![p]),
                role: MessageRole::RoleTool,
                content_type: ContentType::Tool,
                content: tool_result.content,
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "tool_output",
                canonical: CanonicalAddons::with_tool(SOURCE_ID, tool_result.tool, payload),
            })))
        }
        "reasoning" => {
            if !include_types.contains("reasoning") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let summary_text = extract_summary_text(payload);
            let encrypted = payload
                .get("encrypted_content")
                .and_then(|v| v.as_str())
                .map(String::from);

            // Hash material: prefer summary text, fall back to
            // encrypted content (so two reasoning entries with empty
            // summary still produce distinct entry_uids per §5.2.1).
            let hash_material = if !summary_text.is_empty() {
                summary_text.clone()
            } else {
                encrypted.clone().unwrap_or_default()
            };
            let entry_uid = format!(
                "rsn:{line_idx}:{}",
                sha1_hex_prefix(hash_material.as_bytes(), 16)
            );

            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String("reasoning".to_string()),
            );
            // Default: keep a sha256+size fingerprint so an operator
            // can correlate / detect tampering without persisting the
            // sensitive blob. Opt-in flag retains the full content for
            // workflows that re-render reasoning.
            if let Some(enc) = encrypted {
                metadata.insert(
                    "encrypted_content_sha256".to_string(),
                    serde_json::Value::String(sha256_hex_prefix(enc.as_bytes(), 64)),
                );
                metadata.insert(
                    "encrypted_content_size".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(enc.len() as u64)),
                );
                if include_encrypted_reasoning {
                    metadata.insert(
                        "encrypted_content".to_string(),
                        serde_json::Value::String(enc),
                    );
                }
            }

            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleAssistant,
                content_type: ContentType::Text,
                content: summary_text,
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "reasoning",
                canonical: CanonicalAddons::default(),
            })))
        }
        other => {
            // Forward-compat: unknown response_item subtypes are
            // surfaced as system memories so a future Codex addition
            // doesn't silently drop data. Mirrors the event_msg
            // fallback at the end of `build_event_msg_entry`.
            if !include_types.contains("system") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let payload_json = serde_json::to_string(payload).unwrap_or_default();
            let entry_uid = format!(
                "ri:{line_idx}:{}:{}",
                sha1_hex_prefix(other.as_bytes(), 8),
                sha1_hex_prefix(payload_json.as_bytes(), 16)
            );
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(other.to_string()),
            );
            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleMeta,
                content_type: ContentType::Text,
                content: payload_json,
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "system",
                canonical: CanonicalAddons::default(),
            })))
        }
    }
}

fn build_event_msg_entry(
    payload: &serde_json::Value,
    line_idx: i64,
    session_id: &str,
    include_types: &std::collections::HashSet<String>,
    link_tool_calls: bool,
    call_id_map: &HashMap<String, String>,
    call_id_name_map: &HashMap<String, String>,
) -> Result<EntryDecision> {
    let payload_type = payload
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let payload_json = serde_json::to_string(payload).unwrap_or_default();
    match payload_type {
        "user_message" => {
            if !include_types.contains("user") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let message = payload
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let entry_uid = format!(
                "evt:{line_idx}:{}:{}",
                sha1_hex_prefix(payload_type.as_bytes(), 8),
                sha1_hex_prefix(message.as_bytes(), 16)
            );
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleUser,
                content_type: ContentType::Text,
                content: message.to_string(),
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "user",
                canonical: CanonicalAddons::default(),
            })))
        }
        "agent_message" => {
            if !include_types.contains("assistant") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let message = payload
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let entry_uid = format!(
                "evt:{line_idx}:{}:{}",
                sha1_hex_prefix(payload_type.as_bytes(), 8),
                sha1_hex_prefix(message.as_bytes(), 16)
            );
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleAssistant,
                content_type: ContentType::Text,
                content: message.to_string(),
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "assistant",
                canonical: CanonicalAddons::default(),
            })))
        }
        "agent_reasoning" => {
            if !include_types.contains("reasoning") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let text = payload
                .get("text")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let entry_uid = format!("rsn:{line_idx}:{}", sha1_hex_prefix(text.as_bytes(), 16));
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleAssistant,
                content_type: ContentType::Text,
                content: text.to_string(),
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "reasoning",
                canonical: CanonicalAddons::default(),
            })))
        }
        "token_count" => {
            if !include_types.contains("system") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let entry_uid = format!("tok:{line_idx}");
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleMeta,
                content_type: ContentType::Text,
                content: payload_json,
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "system",
                canonical: CanonicalAddons::default(),
            })))
        }
        // shell exec / patch apply results: normalize as tool_output
        // (§4.2.2.6.2). They live in the `tool_output` include-types
        // bucket and surface `metadata.tool.{name,category,
        // source_event,status}` so consumers can filter / aggregate
        // them alongside response_item.function_call_output entries.
        "exec_command_end" => build_event_tool_output(
            payload,
            line_idx,
            session_id,
            "exec_command",
            "exec_command_end",
            include_types,
            link_tool_calls,
            call_id_map,
            call_id_name_map,
            exec_command_end_status,
            format_exec_command_end_output,
        ),
        "patch_apply_end" => build_event_tool_output(
            payload,
            line_idx,
            session_id,
            "apply_patch",
            "patch_apply_end",
            include_types,
            link_tool_calls,
            call_id_map,
            call_id_name_map,
            patch_apply_end_status,
            format_patch_apply_end_output,
        ),
        // Lifecycle events fall back to the generic `evt:` rule per
        // §5.2 (table). Bundled under the `system` include-types
        // name so `-t user,assistant` excludes them.
        "task_started"
        | "task_complete"
        | "turn_aborted"
        | "entered_review_mode"
        | "exited_review_mode" => {
            if !include_types.contains("system") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let entry_uid = format!(
                "evt:{line_idx}:{}:{}",
                sha1_hex_prefix(payload_type.as_bytes(), 8),
                sha1_hex_prefix(payload_json.as_bytes(), 16)
            );
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleMeta,
                content_type: ContentType::Text,
                content: payload_json,
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "system",
                canonical: CanonicalAddons::default(),
            })))
        }
        other => {
            // Unknown event_msg subtype: still admit it as a system
            // event (forward-compat per §5.2 fallback paragraph).
            if !include_types.contains("system") {
                return Ok(EntryDecision::SourceFiltered);
            }
            let entry_uid = format!(
                "evt:{line_idx}:{}:{}",
                sha1_hex_prefix(other.as_bytes(), 8),
                sha1_hex_prefix(payload_json.as_bytes(), 16)
            );
            let mut metadata = serde_json::Map::new();
            metadata.insert(
                "payload_type".to_string(),
                serde_json::Value::String(other.to_string()),
            );
            Ok(EntryDecision::Take(Box::new(CanonicalEntry {
                external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
                parent_external_ids: Vec::new(),
                role: MessageRole::RoleMeta,
                content_type: ContentType::Text,
                content: payload_json,
                metadata,
                timestamp_ms: 0,
                import_order: line_idx,
                kind_tag: "system",
                canonical: CanonicalAddons::default(),
            })))
        }
    }
}

/// Spec §4.2.2.6.2 — shared helper for `exec_command_end` /
/// `patch_apply_end` events. They become canonical `tool_output`
/// entries so consumers can filter / aggregate them alongside
/// `response_item.function_call_output` entries even though codex
/// emitted them on a different record type.
#[allow(clippy::too_many_arguments)]
fn build_event_tool_output(
    payload: &serde_json::Value,
    line_idx: i64,
    session_id: &str,
    tool_name: &'static str,
    source_event: &'static str,
    include_types: &std::collections::HashSet<String>,
    link_tool_calls: bool,
    call_id_map: &HashMap<String, String>,
    call_id_name_map: &HashMap<String, String>,
    status_fn: fn(&serde_json::Value) -> Option<canonical::ToolStatus>,
    format_fn: fn(&serde_json::Value) -> String,
) -> Result<EntryDecision> {
    if !include_types.contains("tool_output") {
        return Ok(EntryDecision::SourceFiltered);
    }
    let payload_json = serde_json::to_string(payload).unwrap_or_default();
    let entry_uid = format!(
        "evt:{line_idx}:{}:{}",
        sha1_hex_prefix(source_event.as_bytes(), 8),
        sha1_hex_prefix(payload_json.as_bytes(), 16)
    );
    let call_id = payload.get("call_id").and_then(|v| v.as_str());
    // Prefer the call's own name (set by the matching function_call)
    // when one is available; fall back to the canonical fixed name
    // (`exec_command` / `apply_patch`) otherwise.
    let resolved_name = call_id
        .and_then(|c| call_id_name_map.get(c))
        .map(String::as_str)
        .unwrap_or(tool_name);
    let output_text = format_fn(payload);
    let status = status_fn(payload);

    let tool_result = canonical::build_tool_output(
        SOURCE_ID,
        Some(resolved_name),
        call_id,
        Some(&output_text),
        status,
        Some(source_event),
    );

    let parent = if link_tool_calls
        && let Some(c) = call_id
        && let Some(eid) = call_id_map.get(c)
    {
        Some(eid.clone())
    } else {
        None
    };

    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "payload_type".to_string(),
        serde_json::Value::String(source_event.to_string()),
    );

    Ok(EntryDecision::Take(Box::new(CanonicalEntry {
        external_id: format!("{SOURCE_ID}:{session_id}:{entry_uid}"),
        parent_external_ids: parent.map_or_else(Vec::new, |p| vec![p]),
        role: MessageRole::RoleTool,
        content_type: ContentType::Tool,
        content: tool_result.content,
        metadata,
        timestamp_ms: 0,
        import_order: line_idx,
        kind_tag: "tool_output",
        canonical: CanonicalAddons::with_tool(SOURCE_ID, tool_result.tool, payload),
    })))
}

/// `exec_command_end` carries `exit_code` (i64). Status tri-state
/// per §4.2.2.3: 0 → Ok, non-zero integer → Error, missing → null.
/// We deliberately do not treat a missing field as Error so a CLI
/// version that omits exit_code does not get spuriously flagged.
fn exec_command_end_status(payload: &serde_json::Value) -> Option<canonical::ToolStatus> {
    payload
        .get("exit_code")
        .and_then(|v| v.as_i64())
        .map(|c| canonical::ToolStatus::from_success(c == 0))
}

/// Render exec_command_end into a human-readable single string so
/// `metadata.tool.output` is consumable directly. Unavailable
/// fields are skipped — we never invent values for missing data.
fn format_exec_command_end_output(payload: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    if let Some(c) = payload.get("exit_code").and_then(|v| v.as_i64()) {
        parts.push(format!("exit_code: {c}"));
    }
    if let Some(d) = payload.get("duration_seconds").and_then(|v| v.as_f64()) {
        parts.push(format!("duration: {d}s"));
    }
    if let Some(s) = payload.get("stdout").and_then(|v| v.as_str())
        && !s.is_empty()
    {
        parts.push(format!("stdout:\n{s}"));
    }
    if let Some(s) = payload.get("stderr").and_then(|v| v.as_str())
        && !s.is_empty()
    {
        parts.push(format!("stderr:\n{s}"));
    }
    if let Some(s) = payload.get("aggregated_output").and_then(|v| v.as_str())
        && !s.is_empty()
        // Only fall back to aggregated_output when neither stdout nor
        // stderr was usable, otherwise we'd duplicate content.
        && parts.iter().all(|p| !p.starts_with("stdout:") && !p.starts_with("stderr:"))
    {
        parts.push(format!("output:\n{s}"));
    }
    parts.join("\n")
}

/// `patch_apply_end` carries `success` (bool) per §4.2.2.6.2.
/// Tri-state: true → Ok, false → Error, missing → null.
fn patch_apply_end_status(payload: &serde_json::Value) -> Option<canonical::ToolStatus> {
    payload
        .get("success")
        .and_then(|v| v.as_bool())
        .map(canonical::ToolStatus::from_success)
}

fn format_patch_apply_end_output(payload: &serde_json::Value) -> String {
    let mut parts = Vec::new();
    if let Some(s) = payload.get("success").and_then(|v| v.as_bool()) {
        parts.push(format!("success: {s}"));
    }
    if let Some(s) = payload.get("stdout").and_then(|v| v.as_str())
        && !s.is_empty()
    {
        parts.push(format!("stdout:\n{s}"));
    }
    if let Some(s) = payload.get("stderr").and_then(|v| v.as_str())
        && !s.is_empty()
    {
        parts.push(format!("stderr:\n{s}"));
    }
    parts.join("\n")
}

/// Returns the rendered text body and, for mixed-content arrays, the
/// untouched parts list so non-text elements (images, file refs,
/// future part types) survive in `metadata.content_parts` instead of
/// being silently dropped. The body remains text-only because
/// `MemoryData.content` is plain text downstream.
fn extract_message_text(payload: &serde_json::Value) -> (String, Option<serde_json::Value>) {
    let Some(content) = payload.get("content") else {
        return (String::new(), None);
    };
    if let Some(s) = content.as_str() {
        return (s.to_string(), None);
    }
    let Some(arr) = content.as_array() else {
        return (content.to_string(), None);
    };
    let mut text_parts = Vec::new();
    let mut has_non_text = false;
    for item in arr {
        // Schemas observed: {"type":"input_text","text":"..."},
        // {"type":"output_text","text":"..."}, plus opaque non-text
        // types like input_image / input_file. Anything without a
        // string `text` field is non-text.
        match item.get("text").and_then(|t| t.as_str()) {
            Some(t) => text_parts.push(t.to_string()),
            None => has_non_text = true,
        }
    }
    let body = if text_parts.is_empty() {
        serde_json::to_string(arr).unwrap_or_default()
    } else {
        text_parts.join("\n")
    };
    // Preserve the full array when a non-text element exists so a
    // later importer revision can rehydrate images/files without
    // needing the original rollout. The clone is intentionally deep:
    // the input `payload` is borrowed (parser keeps it for downstream
    // record types) and image parts may carry sizeable base64 inline,
    // so the cost is real but bounded to mixed-content messages.
    let preserved = has_non_text.then(|| serde_json::Value::Array(arr.clone()));
    (body, preserved)
}

fn extract_summary_text(payload: &serde_json::Value) -> String {
    let Some(arr) = payload.get("summary").and_then(|v| v.as_array()) else {
        return String::new();
    };
    let parts: Vec<&str> = arr
        .iter()
        .filter_map(|item| item.get("text").and_then(|t| t.as_str()))
        .collect();
    parts.join("\n")
}

/// Stream a rollout JSONL file line by line, returning the parsed
/// records and a count of lines that failed (IO error or non-JSON
/// content) so the caller can surface partial-import damage in the
/// summary instead of warn-and-forget. Empty lines are not counted.
/// Failed lines still consume an ordinal so `entry_uid` line numbers
/// stay stable across re-imports per spec §5.2.1.
fn read_parsed_lines(input: &Path) -> Result<(Vec<(usize, RawLine)>, usize)> {
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(input).with_context(|| format!("read {}", input.display()))?;
    let reader = BufReader::new(file);
    let mut out = Vec::new();
    let mut broken = 0usize;
    for (idx, line_res) in reader.lines().enumerate() {
        let line = match line_res {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(
                    line = idx,
                    path = %input.display(),
                    "rollout read error: {e}"
                );
                broken += 1;
                continue;
            }
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<RawLine>(trimmed) {
            Ok(p) => out.push((idx, p)),
            Err(e) => {
                tracing::warn!(
                    line = idx,
                    path = %input.display(),
                    "skipping unparseable rollout line: {e}"
                );
                broken += 1;
            }
        }
    }
    Ok((out, broken))
}

fn collect_rollout_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("jsonl")
            && path.is_file()
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("rollout-"))
        {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn walk_rollout_tree(dir: &Path, out: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            walk_rollout_tree(&path, out)?;
        } else if path.extension().and_then(|e| e.to_str()) == Some("jsonl")
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("rollout-"))
        {
            out.push(path);
        }
    }
    Ok(())
}

fn filename_stem(p: &Path) -> Option<String> {
    p.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::test_support::{entries_from_outcome, unpack_outcome};
    use std::io::Write;

    fn args(file: PathBuf) -> CodexArgs {
        CodexArgs {
            session_file: Some(file),
            day_dir: None,
            all_sessions: false,
            codex_dir: PathBuf::from("~/.codex"),
            include_types: "user,assistant,tool_call,tool_output,system,reasoning,attachment"
                .to_string(),
            strip_path_prefix: None,
            include_encrypted_reasoning: false,
            exclude_encrypted_reasoning: false,
            link_tool_calls: true,
            no_link_tool_calls: false,
        }
    }

    fn write_lines(lines: &[&str]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rollout-test.jsonl");
        let mut f = std::fs::File::create(&p).unwrap();
        for l in lines {
            writeln!(f, "{l}").unwrap();
        }
        (dir, p)
    }

    fn meta_line() -> &'static str {
        r#"{"timestamp":"2026-04-20T01:00:00.000Z","type":"session_meta","payload":{"id":"019de647-f162-7bb2-bf70-6c6bd4f21171","timestamp":"2026-04-20T01:00:00.000Z","cwd":"/home/me/proj","originator":"codex_cli","cli_version":"1.0","source":"cli","model_provider":"openai","base_instructions":{"text":"system prompt body"},"git":{"commit_hash":"abc","branch":"main","repository_url":"https://example.com"}}}"#
    }

    #[test]
    fn day_dir_relative_path_resolves_under_codex_sessions() {
        // Spec §3.4: `--day-dir 2026/05/02` should walk
        // `<codex-dir>/sessions/2026/05/02/`, not `./2026/05/02/`.
        let dir = tempfile::tempdir().unwrap();
        let codex_dir = dir.path().to_path_buf();
        let day_path = codex_dir.join("sessions/2026/05/02");
        std::fs::create_dir_all(&day_path).unwrap();
        let rollout = day_path.join("rollout-test.jsonl");
        std::fs::File::create(&rollout).unwrap();

        let mut a = args(PathBuf::from("/dev/null"));
        a.session_file = None;
        a.day_dir = Some(PathBuf::from("2026/05/02"));
        a.codex_dir = codex_dir;
        let s = CodexSource::new(a);
        let inputs = s.discover().unwrap();
        assert_eq!(inputs, vec![rollout]);
    }

    #[test]
    fn day_dir_absolute_path_is_used_as_is() {
        // Absolute paths bypass the `sessions/` join so users can still
        // point at a directory outside the configured codex-dir.
        let dir = tempfile::tempdir().unwrap();
        let day_path = dir.path().join("custom/day");
        std::fs::create_dir_all(&day_path).unwrap();
        let rollout = day_path.join("rollout-x.jsonl");
        std::fs::File::create(&rollout).unwrap();

        let mut a = args(PathBuf::from("/dev/null"));
        a.session_file = None;
        a.day_dir = Some(day_path.clone());
        a.codex_dir = PathBuf::from("/should/be/ignored");
        let s = CodexSource::new(a);
        let inputs = s.discover().unwrap();
        assert_eq!(inputs, vec![rollout]);
    }

    #[test]
    fn broken_jsonl_lines_are_counted_in_source_filtered() {
        // Mixed valid + invalid lines: the invalid ones must show up
        // in source_filtered_count instead of being warn-and-forget.
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"this is not json"#,
            r#"{"type":"event_msg","payload":{"type":"user_message","message":"hi"}}"#,
            r#"{ "broken: }"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let outcome = s.read_session(&p, None).unwrap();
        let filtered = unpack_outcome(outcome).2;
        assert!(
            filtered >= 2,
            "two broken lines must surface as source_filtered, got {filtered}"
        );
    }

    #[test]
    fn rollout_with_only_broken_lines_returns_skipped_with_count() {
        // No parsable record at all: the session should be `Skipped`
        // and the operator should see the broken line count, not the
        // generic "empty rollout" message.
        let (_d, p) = write_lines(&[r#"this is not json"#, r#"{ broken }"#]);
        let s = CodexSource::new(args(p.clone()));
        let outcome = s.read_session(&p, None).unwrap();
        match outcome {
            ReadSessionOutcome::Skipped {
                reason,
                filtered_count,
                ..
            } => {
                assert_eq!(filtered_count, 2);
                assert!(
                    reason.contains("unparseable"),
                    "reason should mention parse failure, got {reason:?}"
                );
            }
            _ => panic!("expected Skipped"),
        }
    }

    #[test]
    fn skips_when_session_meta_missing() {
        let (_d, p) = write_lines(&[
            r#"{"timestamp":"2026-04-20T01:00:00.000Z","type":"event_msg","payload":{"type":"user_message","message":"hi"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let outcome = s.read_session(&p, None).unwrap();
        assert!(matches!(outcome, ReadSessionOutcome::Skipped { .. }));
    }

    #[test]
    fn session_meta_alone_is_taken() {
        let (_d, p) = write_lines(&[meta_line()]);
        let s = CodexSource::new(args(p.clone()));
        let outcome = s.read_session(&p, None).unwrap();
        let (session, entries, filtered) = unpack_outcome(outcome);
        assert_eq!(session.source_id, "codex");
        assert_eq!(session.session_id, "019de647-f162-7bb2-bf70-6c6bd4f21171");
        assert_eq!(
            session.channel,
            "codex:019de647-f162-7bb2-bf70-6c6bd4f21171"
        );
        assert!(session.source_labels.iter().any(|l| l == "agent:codex"));
        assert!(session.source_labels.iter().any(|l| l.starts_with("path:")));
        assert!(session.source_labels.iter().any(|l| l == "branch:main"));
        assert!(session.source_labels.iter().any(|l| l == "provider:openai"));

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind_tag, "system");
        assert_eq!(entries[0].role, MessageRole::RoleMeta);
        assert_eq!(entries[0].content, "system prompt body");
        assert_eq!(filtered, 0);
    }

    #[test]
    fn function_call_then_output_links_via_call_id_map() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{}","call_id":"call_abc123"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_abc123","output":"done"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let call = entries.iter().find(|e| e.kind_tag == "tool_call").unwrap();
        let output = entries
            .iter()
            .find(|e| e.kind_tag == "tool_output")
            .unwrap();
        assert_ne!(call.external_id, output.external_id);
        assert!(call.external_id.contains(":call:"));
        assert!(output.external_id.contains(":output:"));
        assert_eq!(output.parent_external_ids, vec![call.external_id.clone()]);
        // Both should fit in 512 bytes
        assert!(call.external_id.len() <= 512);
        assert!(output.external_id.len() <= 512);
    }

    #[test]
    fn output_before_call_is_linked_after_post_loop_rewire() {
        // Reordered rollout: function_call_output appears *before* the
        // matching function_call. The forward 1-pass map can't resolve
        // it; the post-loop rewire must close the link.
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_late","output":"done"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{}","call_id":"call_late"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let call = entries.iter().find(|e| e.kind_tag == "tool_call").unwrap();
        let output = entries
            .iter()
            .find(|e| e.kind_tag == "tool_output")
            .unwrap();
        assert_eq!(
            output.parent_external_ids,
            vec![call.external_id.clone()],
            "output must be linked to the late-arriving call after post-loop rewire"
        );

        // Both `name` and `category` must be backfilled in the 2nd pass:
        // build_tool_output ran without a name (call hadn't been seen),
        // so `category` was frozen as null. The post-loop rewire has to
        // re-derive it; otherwise filtering by category="shell_exec" would
        // silently miss reordered rollouts.
        let tool = output
            .canonical
            .tool
            .as_ref()
            .expect("output entry must have canonical.tool");
        assert_eq!(tool["name"].as_str(), Some("shell"));
        assert_eq!(tool["category"].as_str(), Some("shell_exec"));
    }

    #[test]
    fn no_link_tool_calls_leaves_output_parent_none() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{}","call_id":"call_abc123"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call_abc123","output":"done"}}"#,
        ]);
        let mut a = args(p.clone());
        a.no_link_tool_calls = true;
        let s = CodexSource::new(a);
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let output = entries
            .iter()
            .find(|e| e.kind_tag == "tool_output")
            .unwrap();
        assert!(output.parent_external_ids.is_empty());
    }

    #[test]
    fn custom_tool_call_uses_ctcall_prefix() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"custom_tool_call","name":"web","input":"{}","call_id":"cct_001"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"response_item","payload":{"type":"custom_tool_call_output","call_id":"cct_001","output":"ok"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let call = entries.iter().find(|e| e.kind_tag == "tool_call").unwrap();
        let output = entries
            .iter()
            .find(|e| e.kind_tag == "tool_output")
            .unwrap();
        assert!(call.external_id.contains(":ctcall:"));
        assert!(output.external_id.contains(":ctout:"));
        assert_eq!(output.parent_external_ids, vec![call.external_id.clone()]);
    }

    /// Spec §4.2.2.6 codex: text + image is split into two entries
    /// (text → kind=user, image → kind=attachment) connected by the
    /// block chain rule (block 1 points at block 0 via
    /// `parent_external_ids`).
    #[test]
    fn message_content_mixed_text_and_image_splits_blocks_with_chain() {
        let line = r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"see attached"},{"type":"input_image","image_url":"https://example.com/cat.png"}]}}"#;
        let (_d, p) = write_lines(&[meta_line(), line]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let user = entries.iter().find(|e| e.kind_tag == "user").unwrap();
        let attachment = entries
            .iter()
            .find(|e| e.kind_tag == "attachment")
            .expect("input_image must be split into a separate attachment entry");
        assert_eq!(user.content, "see attached");
        // block chain: attachment (block 1) is anchored on the text block (block 0)
        assert_eq!(
            attachment.parent_external_ids,
            vec![user.external_id.clone()],
            "block 1+ must link back to block 0 of the same message"
        );
        // Image storage is dispatched by scheme: https → url
        let storage = attachment
            .canonical
            .attachment
            .as_ref()
            .and_then(|a| a.get("storage"))
            .and_then(|v| v.as_str());
        assert_eq!(storage, Some("url"));
    }

    /// `-t user,assistant` against a mixed-block message (text +
    /// image) must surface the image-block skip in
    /// `source_filtered_count` so the importer summary's
    /// `memories_skipped_filtered` reflects the real per-block drop.
    /// Without this, partial-skip lines silently undercount.
    #[test]
    fn mixed_block_partial_filter_increments_source_filtered() {
        let line = r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"see attached"},{"type":"input_image","image_url":"https://example.com/cat.png"},{"type":"input_image","image_url":"https://example.com/dog.png"}]}}"#;
        let (_d, p) = write_lines(&[meta_line(), line]);
        let mut a = args(p.clone());
        // Drop attachment from include set: only text blocks should
        // be admitted.
        a.include_types = "user,assistant,tool_call,tool_output,system,reasoning".to_string();
        let s = CodexSource::new(a);
        let (_session, entries, source_filtered) =
            unpack_outcome(s.read_session(&p, None).unwrap());
        // 1 user-text entry survives.
        assert_eq!(
            entries.iter().filter(|e| e.kind_tag == "user").count(),
            1,
            "the text block must still be imported"
        );
        // No attachment entries.
        assert!(
            entries.iter().all(|e| e.kind_tag != "attachment"),
            "image blocks must be filtered out by include_types"
        );
        // Two image blocks were skipped — both must be reflected in
        // source_filtered (line itself is NOT counted because the
        // line still emitted at least one entry).
        assert_eq!(
            source_filtered, 2,
            "both filtered image blocks must be counted in source_filtered_count"
        );
    }

    /// When **every** block in a message is filtered out (e.g. a
    /// 3-image-only message read with `-t user,assistant`), the
    /// summary must count each dropped block, not "1 dropped line".
    /// Otherwise the line-level fallback would silently understate
    /// the skip and contradict the per-block counting used by the
    /// partial-skip path right above.
    #[test]
    fn all_blocks_filtered_increments_source_filtered_per_block() {
        let line = r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_image","image_url":"https://example.com/a.png"},{"type":"input_image","image_url":"https://example.com/b.png"},{"type":"input_image","image_url":"https://example.com/c.png"}]}}"#;
        let (_d, p) = write_lines(&[meta_line(), line]);
        let mut a = args(p.clone());
        // Drop attachment from include set: every image block is
        // rejected.
        a.include_types = "user,assistant,tool_call,tool_output,system,reasoning".to_string();
        let s = CodexSource::new(a);
        let (_session, entries, source_filtered) =
            unpack_outcome(s.read_session(&p, None).unwrap());
        // No attachment entries survive.
        assert!(
            entries.iter().all(|e| e.kind_tag != "attachment"),
            "all image blocks must be filtered out"
        );
        // Three blocks were filtered — each must be counted, even
        // though the entire line is gone.
        assert_eq!(
            source_filtered, 3,
            "every dropped block must be counted at block granularity, not 1 per line"
        );
    }

    /// Pure-text messages now consist of a single block-0 entry per
    /// §4.2.2.6 (block-split is uniform). The legacy `content_parts`
    /// metadata field is no longer emitted.
    #[test]
    fn message_content_pure_text_array_emits_single_block_entry() {
        let line = r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}"#;
        let (_d, p) = write_lines(&[meta_line(), line]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let user = entries.iter().find(|e| e.kind_tag == "user").unwrap();
        assert_eq!(user.content, "hi");
        // Legacy content_parts is gone.
        assert!(!user.metadata.contains_key("content_parts"));
        // Block 0 has no parent (codex has no parentUuid; conversational
        // links are not synthesized at this layer).
        assert!(user.parent_external_ids.is_empty());
    }

    #[test]
    fn long_call_id_does_not_blow_external_id_budget() {
        let long_call_id = "call_".to_string() + &"x".repeat(2048);
        let line = format!(
            r#"{{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{{"type":"function_call","name":"shell","arguments":"{{}}","call_id":"{long_call_id}"}}}}"#
        );
        let (_d, p) = write_lines(&[meta_line(), &line]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let call = entries.iter().find(|e| e.kind_tag == "tool_call").unwrap();
        assert!(
            call.external_id.len() <= 512,
            "external_id should be bounded, got {}",
            call.external_id.len()
        );
    }

    #[test]
    fn duplicate_text_messages_get_distinct_external_ids() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"hello"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"event_msg","payload":{"type":"user_message","message":"hello"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let ids: Vec<&str> = entries
            .iter()
            .filter(|e| e.kind_tag == "user")
            .map(|e| e.external_id.as_str())
            .collect();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[0], ids[1]);
    }

    #[test]
    fn include_types_filter_drops_reasoning() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"event_msg","payload":{"type":"user_message","message":"hi"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"response_item","payload":{"type":"reasoning","summary":[{"text":"thinking"}]}}"#,
        ]);
        let mut a = args(p.clone());
        a.include_types = "user,assistant".to_string();
        let s = CodexSource::new(a);
        let (_session, entries, filtered) = unpack_outcome(s.read_session(&p, None).unwrap());
        assert!(entries.iter().all(|e| e.kind_tag != "reasoning"));
        // session_meta + reasoning are filtered (system / reasoning),
        // user_message is kept.
        assert!(filtered >= 2);
    }

    #[test]
    fn encrypted_reasoning_is_not_persisted_by_default() {
        // Default policy: keep only sha256+size so the operator can
        // detect that reasoning was redacted without storing the
        // sensitive blob in DB / dumps / backups.
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"reasoning","summary":[{"text":"thinking"}],"encrypted_content":"SECRET"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let r = entries.iter().find(|e| e.kind_tag == "reasoning").unwrap();
        assert!(!r.metadata.contains_key("encrypted_content"));
        assert!(r.metadata.contains_key("encrypted_content_sha256"));
        assert_eq!(
            r.metadata
                .get("encrypted_content_size")
                .and_then(|v| v.as_u64()),
            Some(b"SECRET".len() as u64)
        );
    }

    #[test]
    fn include_encrypted_reasoning_opt_in_persists_full_content() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"reasoning","summary":[{"text":"thinking"}],"encrypted_content":"SECRET"}}"#,
        ]);
        let mut a = args(p.clone());
        a.include_encrypted_reasoning = true;
        let s = CodexSource::new(a);
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let r = entries.iter().find(|e| e.kind_tag == "reasoning").unwrap();
        assert_eq!(
            r.metadata.get("encrypted_content").and_then(|v| v.as_str()),
            Some("SECRET")
        );
        // Fingerprint stays even with opt-in so downstream consumers
        // have a single canonical hash to key off.
        assert!(r.metadata.contains_key("encrypted_content_sha256"));
    }

    #[test]
    fn unknown_response_item_subtype_is_kept_as_system_event() {
        // Forward-compat parity: unknown event_msg subtypes already
        // get retained, so unknown response_item subtypes must do the
        // same. Otherwise a future Codex schema addition silently
        // drops data without showing up in source_filtered_count
        // either.
        let line = r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"future_subtype","blob":"x"}}"#;
        let (_d, p) = write_lines(&[meta_line(), line]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let unknown = entries
            .iter()
            .find(|e| {
                e.kind_tag == "system"
                    && e.metadata.get("payload_type").and_then(|v| v.as_str())
                        == Some("future_subtype")
            })
            .expect("unknown response_item subtype must surface as a system memory");
        assert!(unknown.content.contains("future_subtype"));
    }

    #[test]
    fn unknown_response_item_subtype_respects_include_types_system() {
        // When the user excludes system, unknown response_items still
        // count as filtered (parity with the existing event_msg
        // fallback). The entry must not appear in `entries`.
        let line = r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"future_subtype","blob":"x"}}"#;
        let (_d, p) = write_lines(&[meta_line(), line]);
        let mut a = args(p.clone());
        a.include_types = "user,assistant".to_string();
        let s = CodexSource::new(a);
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        assert!(
            entries
                .iter()
                .all(|e| e.metadata.get("payload_type").and_then(|v| v.as_str())
                    != Some("future_subtype"))
        );
    }

    /// Spec §4.2.2.6.2: `exec_command_end` and `patch_apply_end` are
    /// **tool_output** kinds, not system. Lifecycle events
    /// (`task_started`, `turn_aborted`, etc.) stay as system.
    #[test]
    fn task_lifecycle_events_split_system_and_tool_output() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"event_msg","payload":{"type":"task_started","turn_id":"t1"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"event_msg","payload":{"type":"exec_command_end","exit_code":0,"stdout":"ok"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let systems = entries.iter().filter(|e| e.kind_tag == "system").count();
        // session_meta + task_started both kind=system
        assert_eq!(systems, 2);
        let outputs = entries
            .iter()
            .filter(|e| e.kind_tag == "tool_output")
            .count();
        // exec_command_end normalized as a tool_output
        assert_eq!(outputs, 1);
    }

    #[test]
    fn rollout_top_level_timestamp_populates_entry_ts() {
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:05.000Z","type":"event_msg","payload":{"type":"user_message","message":"hi"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:10.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}}"#,
            r#"{"timestamp":"2026-04-20T01:00:15.000Z","type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"{}","call_id":"call_a"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let user = entries.iter().find(|e| e.kind_tag == "user").unwrap();
        let assistant = entries.iter().find(|e| e.kind_tag == "assistant").unwrap();
        let tool_call = entries.iter().find(|e| e.kind_tag == "tool_call").unwrap();
        let expect = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_millis()
        };
        assert_eq!(user.timestamp_ms, expect("2026-04-20T01:00:05.000Z"));
        assert_eq!(assistant.timestamp_ms, expect("2026-04-20T01:00:10.000Z"));
        assert_eq!(tool_call.timestamp_ms, expect("2026-04-20T01:00:15.000Z"));
    }

    #[test]
    fn session_meta_payload_source_is_renamed_to_session_source() {
        // Spec §4.2.1: `source` is reserved by run_import. Emitting it
        // here causes a debug_assert panic during live import, which
        // bites every Codex run because rollouts ship `source: "cli"`
        // by default. The parser must rename it to a non-reserved key.
        let (_d, p) = write_lines(&[meta_line()]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let meta = entries.iter().find(|e| e.kind_tag == "system").unwrap();
        assert!(
            !meta.metadata.contains_key("source"),
            "entry metadata must not collide with the reserved `source` key, got {:?}",
            meta.metadata.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            meta.metadata.get("session_source").and_then(|v| v.as_str()),
            Some("cli"),
            "payload.source must be preserved under the renamed key"
        );
    }

    #[test]
    fn session_updated_at_advances_to_latest_entry_timestamp() {
        // session_meta is at 01:00:00; the newest event is at 01:00:30.
        // session.updated_at_ms must reflect 01:00:30 so that
        // thread.updated_at (set from session.updated_at_ms on a fresh
        // import) places the thread inside an updated_after_ms window
        // that starts after session_meta but before the latest entry.
        let (_d, p) = write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:10.000Z","type":"event_msg","payload":{"type":"user_message","message":"hi"}}"#,
            r#"{"timestamp":"2026-04-20T01:00:30.000Z","type":"event_msg","payload":{"type":"agent_message","message":"hello"}}"#,
        ]);
        let s = CodexSource::new(args(p.clone()));
        let session = unpack_outcome(s.read_session(&p, None).unwrap()).0;
        let expect = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .timestamp_millis()
        };
        assert_eq!(session.created_at_ms, expect("2026-04-20T01:00:00.000Z"));
        assert_eq!(session.updated_at_ms, expect("2026-04-20T01:00:30.000Z"));
    }

    #[test]
    fn session_updated_at_keeps_meta_when_entries_are_only_session_meta() {
        // No event/response_item lines: updated_at falls back to the
        // session_meta timestamp (which is also session_meta entry's
        // timestamp_ms).
        let (_d, p) = write_lines(&[meta_line()]);
        let s = CodexSource::new(args(p.clone()));
        let session = unpack_outcome(s.read_session(&p, None).unwrap()).0;
        let meta_ts = chrono::DateTime::parse_from_rfc3339("2026-04-20T01:00:00.000Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(session.updated_at_ms, meta_ts);
    }

    #[test]
    fn strip_path_prefix_relativizes_path_label() {
        let line = r#"{"timestamp":"2026-04-20T01:00:00.000Z","type":"session_meta","payload":{"id":"019de647-f162-7bb2-bf70-6c6bd4f21171","timestamp":"2026-04-20T01:00:00.000Z","cwd":"/home/me/work/foo","model_provider":"openai","git":{"branch":"main"}}}"#;
        let (_d, p) = write_lines(&[line]);
        let mut a = args(p.clone());
        a.strip_path_prefix = Some("/home/me".to_string());
        let s = CodexSource::new(a);
        let session = unpack_outcome(s.read_session(&p, None).unwrap()).0;
        assert!(
            session.source_labels.contains(&"path:work/foo".to_string()),
            "labels: {:?}",
            session.source_labels
        );
    }

    /// Regression: when `-t tool_output` is selected without
    /// `tool_call`, the tool_call entries are filtered out — but the
    /// `(call_id, name)` mapping must still be populated so that
    /// downstream function_call_output entries can resolve
    /// `metadata.tool.name` / `category`. Without this, queries
    /// filtering by `category="shell_exec"` silently miss every
    /// shell output. Spec §4.2.2.3.
    #[test]
    fn tool_output_resolves_name_when_tool_call_filtered_out() {
        let call = r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"function_call","call_id":"call-1","name":"shell","arguments":"{}"}}"#;
        let out = r#"{"timestamp":"2026-04-20T01:00:02.000Z","type":"response_item","payload":{"type":"function_call_output","call_id":"call-1","output":"hello"}}"#;
        let (_d, p) = write_lines(&[meta_line(), call, out]);
        // Exclude tool_call but keep tool_output. Pre-fix, the
        // function_call_output below would lose name + category.
        let mut a = args(p.clone());
        a.include_types = "tool_output".to_string();
        let s = CodexSource::new(a);
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let outputs: Vec<_> = entries
            .iter()
            .filter(|e| e.kind_tag == "tool_output")
            .collect();
        assert_eq!(outputs.len(), 1, "tool_output should be imported");
        let tool = outputs[0]
            .canonical
            .tool
            .as_ref()
            .expect("tool_output has metadata.tool");
        assert_eq!(
            tool.get("name").and_then(|v| v.as_str()),
            Some("shell"),
            "name must come from the prior function_call even when tool_call is filtered"
        );
        assert_eq!(
            tool.get("category").and_then(|v| v.as_str()),
            Some("shell_exec"),
            "category must resolve via the populated name map"
        );
        // tool_call entries must be absent (verifying the filter still
        // applies — only the name map survives the gate).
        assert!(
            !entries.iter().any(|e| e.kind_tag == "tool_call"),
            "tool_call should remain filtered"
        );
    }

    /// Regression: a function_call_output with multi-MB stdout used to
    /// land verbatim in `metadata.raw.codex.output` because
    /// `tool_addons` cloned the payload as-is. Verify the same
    /// `tool_output_full` budget that the canonical layer applies to
    /// `metadata.tool.output` also bounds the raw copy. Without this
    /// guard a single shell call can blow up one memory row past the
    /// DB's practical row-size limit.
    #[test]
    fn tool_addons_sanitizes_oversized_output_in_raw() {
        // 200 KB stdout — well above the default tool_output_full=64KiB.
        let big = "x".repeat(200_000);
        let line = format!(
            r#"{{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{{"type":"function_call_output","call_id":"call-1","output":{}}}}}"#,
            serde_json::Value::String(big.clone())
        );
        let meta = meta_line();
        let (_d, p) = write_lines(&[meta, &line]);
        let s = CodexSource::new(args(p.clone()));
        let entries = entries_from_outcome(s.read_session(&p, None).unwrap());
        let tool_entry = entries
            .iter()
            .find(|e| e.kind_tag == "tool_output")
            .expect("tool_output entry should be emitted");
        let raw_codex = tool_entry
            .canonical
            .raw
            .as_ref()
            .and_then(|m| m.get("codex"))
            .expect("raw.codex should be populated");
        let raw_output = raw_codex
            .get("output")
            .expect("payload.output is preserved as a key");
        assert_eq!(
            raw_output.get("elided"),
            Some(&serde_json::Value::Bool(true)),
            "oversized output must be elided in raw.codex (got: {raw_output})"
        );
        assert_eq!(
            raw_output.get("size_bytes").and_then(|v| v.as_u64()),
            Some(big.len() as u64),
            "size_bytes should record the original byte length"
        );
        // The serialized raw object must be small enough that a single
        // memory row stays well under DB practical limits. 4 KB is a
        // generous ceiling; the elided summary is < 200 bytes.
        let serialized = serde_json::to_string(raw_codex).unwrap();
        assert!(
            serialized.len() < 4096,
            "raw.codex stayed bloated after sanitization: {} bytes",
            serialized.len()
        );
    }

    use crate::source::test_support::set_file_mtime_ms;

    fn write_minimal_rollout() -> (tempfile::TempDir, PathBuf) {
        write_lines(&[
            meta_line(),
            r#"{"timestamp":"2026-04-20T01:00:01.000Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}}"#,
        ])
    }

    #[test]
    fn mtime_filter_skips_old_session_when_since_set() {
        let (_d, p) = write_minimal_rollout();
        set_file_mtime_ms(&p, 1_000_000);
        let s = CodexSource::new(args(p.clone()));
        let outcome = s.read_session(&p, Some(2_000_000)).unwrap();
        match outcome {
            ReadSessionOutcome::Skipped { reason, .. } => {
                assert!(reason.starts_with("unchanged since"), "got {reason}");
            }
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. } => {
                panic!("expected Skipped");
            }
        }
    }

    #[test]
    fn mtime_filter_parses_recent_session() {
        let (_d, p) = write_minimal_rollout();
        set_file_mtime_ms(&p, 9_000_000);
        let s = CodexSource::new(args(p.clone()));
        let outcome = s.read_session(&p, Some(2_000_000)).unwrap();
        assert!(matches!(
            outcome,
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
        ));
    }

    #[test]
    fn mtime_filter_disabled_when_since_unset() {
        let (_d, p) = write_minimal_rollout();
        set_file_mtime_ms(&p, 1_000);
        let s = CodexSource::new(args(p.clone()));
        let outcome = s.read_session(&p, None).unwrap();
        assert!(matches!(
            outcome,
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
        ));
    }
}
