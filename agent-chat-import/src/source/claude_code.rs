//! Claude Code source — canonical-block implementation.
//!
//! Each Anthropic JSONL line is decomposed into 1..N
//! `CanonicalEntry`s along the `message.content[]` boundary
//! (`tool_use`, `tool_result`, `image`, `text`). `type=attachment`
//! events are also routed through this source under a subtype
//! whitelist, so events like `task_reminder` and `diagnostics` land
//! as canonical attachment memories. Spec §4.2.2.6 claude-code table.
//!
//! Migrated from the legacy single-entry mapping (`crate::importer::
//! import_session` + `crate::converter::convert_entry_to_memory_data`)
//! per docs/agent-chat-import-open-issues.md A3 / A4.

use crate::cli::ClaudeCodeArgs;
use crate::common::canonical;
use crate::common::ids::{
    ID_SHA256_THRESHOLD, sha1_hex_prefix, sha256_hex_prefix, truncate_id_for_external,
};
use crate::common::labels::{truncate_label_keep_head, truncate_label_keep_tail};
use crate::common::path::apply_path_prefix;
use crate::parser::{
    self as raw_parser, RawEntry, SessionInfo, parse_jsonl_file, parse_timestamp_millis,
};
use crate::source::{
    CanonicalAddons, CanonicalEntry, CanonicalSession, ChatSource, ReadSessionOutcome, StreamItem,
    mtime_skip_outcome,
};
use anyhow::{Context, Result};
use protobuf::llm_memory::data::{ContentType, MessageRole};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

const SOURCE_ID: &str = "claude_code";

/// Cap on the canonical `MemoryData.content` for attachment events.
/// Subtypes such as `task_reminder` can carry multi-MB transcripts;
/// without this guard one row balloons past sensible DB limits. The
/// raw payload is still preserved verbatim under `canonical.raw` so
/// nothing is lost — the bound applies only to the searchable body.
const ATTACHMENT_CONTENT_MAX_BYTES: usize = 64 * 1024;

/// Safety-net cap for a single top-level scalar transcribed into
/// `metadata.claude_code.*`. Anything serializing larger is skipped so
/// the per-entry metadata stays a lean attribute bag — the body and
/// large payloads already live in `MemoryData.content` / `canonical.*`.
const CLAUDE_CODE_META_MAX_BYTES: usize = 2048;

/// Top-level JSONL keys NOT transcribed into `metadata.claude_code.*`.
/// Two groups: keys already promoted to top-level metadata (avoids
/// double-writing), and keys carrying the body or large payloads that
/// `MemoryData.content` / the canonical tool/attachment/block paths
/// already own. Everything else flows through generically so new
/// upstream scalar fields are captured without code changes.
const CLAUDE_CODE_META_EXCLUDED: &[&str] = &[
    // Promoted to top-level metadata by `insert_common_entry_metadata`.
    "uuid",
    "parentUuid",
    "type",
    "isSidechain",
    "requestId",
    // Promoted to top-level metadata by the attachment emit path
    // (`metadata.subtype`); excluding it here upholds the documented
    // "no duplicate under claude_code.*" contract. (`block_type` is
    // synthesized from the block payload, never a raw top-level key.)
    "subtype",
    // Body — preserved verbatim in `MemoryData.content`.
    "message",
    "content",
    "title",
    // Large payloads consumed by canonical tool/attachment/block paths
    // or attachment subtype extractors (snippet/files/addedBlocks/…).
    "toolUseResult",
    "data",
    "snapshot",
    "attachment",
    "files",
    "addedBlocks",
    "snippet",
    "stdout",
    "stderr",
    "thinkingMetadata",
    "compactMetadata",
    "todos",
    "planContent",
    "hookInfos",
    "hookErrors",
];

/// Subtypes of `type=attachment` JSONL events that the default policy
/// admits. Restricted to the bodies that carry conversation-relevant
/// text (todo lists, diagnostics, file edits, nested CLAUDE.md
/// notes). The remaining ~10 subtypes are noisy or telemetry-only and
/// require an explicit `--attachment-subtypes all` opt-in.
const HIGH_VALUE_ATTACHMENT_SUBTYPES: &[&str] = &[
    "task_reminder",
    "diagnostics",
    "edited_text_file",
    "nested_memory",
];

/// Subtype-aware policy for `type=attachment` events. Built from the
/// CLI string by `from_cli`; the source-side filter consults `includes`
/// per event after the canonical kind filter has already cleared the
/// `attachment` bucket.
#[derive(Debug, Clone)]
pub enum AttachmentSubtypePolicy {
    Default,
    All,
    None,
    Whitelist(HashSet<String>),
}

impl AttachmentSubtypePolicy {
    pub fn from_cli(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        match trimmed.to_ascii_lowercase().as_str() {
            "default" => return Ok(AttachmentSubtypePolicy::Default),
            "all" => return Ok(AttachmentSubtypePolicy::All),
            "none" => return Ok(AttachmentSubtypePolicy::None),
            _ => {}
        }
        // Treat anything else as a comma-separated whitelist; validate
        // that every token is non-empty so a stray comma surfaces as
        // a CLI error rather than silently broadening the filter.
        let mut set = HashSet::new();
        for part in trimmed.split(',') {
            let token = part.trim();
            if token.is_empty() {
                return Err(format!(
                    "--attachment-subtypes: empty token in whitelist '{raw}'"
                ));
            }
            set.insert(token.to_lowercase());
        }
        if set.is_empty() {
            return Err(format!(
                "--attachment-subtypes: whitelist '{raw}' resolved to no subtypes"
            ));
        }
        Ok(AttachmentSubtypePolicy::Whitelist(set))
    }

    pub fn includes(&self, subtype: &str) -> bool {
        match self {
            AttachmentSubtypePolicy::Default => HIGH_VALUE_ATTACHMENT_SUBTYPES
                .iter()
                .any(|s| s.eq_ignore_ascii_case(subtype)),
            AttachmentSubtypePolicy::All => true,
            AttachmentSubtypePolicy::None => false,
            AttachmentSubtypePolicy::Whitelist(set) => set.contains(&subtype.to_lowercase()),
        }
    }
}

/// `ChatSource` implementation backed by `--session-file` /
/// `--project-dir` / `--all-projects`. Owns the resolved
/// `AttachmentSubtypePolicy` so the per-line dispatcher does not have
/// to re-parse the CLI string for every entry.
pub struct ClaudeCodeSource {
    args: ClaudeCodeArgs,
    attachment_policy: AttachmentSubtypePolicy,
}

impl ClaudeCodeSource {
    /// Construct a source from CLI args. Panics if the
    /// `--attachment-subtypes` policy fails to parse — by the time we
    /// reach this constructor the CLI layer has already validated the
    /// string via `attachment_subtypes_policy()` from `main.rs`.
    pub fn new(args: ClaudeCodeArgs) -> Self {
        let policy = args
            .attachment_subtypes_policy()
            .expect("attachment_subtypes_policy must be validated before ClaudeCodeSource::new");
        Self {
            args,
            attachment_policy: policy,
        }
    }
}

impl ChatSource for ClaudeCodeSource {
    type SessionInput = PathBuf;

    fn id(&self) -> &str {
        SOURCE_ID
    }

    fn input_label(&self, input: &Self::SessionInput) -> String {
        input.display().to_string()
    }

    fn discover(&self) -> Result<Vec<Self::SessionInput>> {
        if let Some(ref file) = self.args.session_file {
            return Ok(vec![file.clone()]);
        }
        if let Some(ref dir) = self.args.project_dir {
            return collect_jsonl_files(dir);
        }
        let projects_dir = self.args.resolved_claude_dir().join("projects");
        let mut all = Vec::new();
        let mut subdirs: Vec<PathBuf> = std::fs::read_dir(&projects_dir)
            .with_context(|| format!("Projects directory not found: {}", projects_dir.display()))?
            .filter_map(|e| e.ok().map(|d| d.path()))
            .filter(|p| p.is_dir())
            .collect();
        subdirs.sort();
        for sub in &subdirs {
            all.extend(collect_jsonl_files(sub)?);
        }
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
        let entries =
            parse_jsonl_file(input).with_context(|| format!("parse {}", input.display()))?;
        if entries.is_empty() {
            return Ok(ReadSessionOutcome::Skipped {
                session_id_hint: input
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(|s| s.to_string()),
                reason: "empty JSONL".to_string(),
                filtered_count: 0,
            });
        }

        let info = match raw_parser::extract_session_info(&entries, input) {
            Ok(i) => i,
            Err(e) => {
                return Ok(ReadSessionOutcome::Skipped {
                    session_id_hint: input
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .map(|s| s.to_string()),
                    reason: format!("{e}"),
                    filtered_count: entries.len() as u32,
                });
            }
        };

        let session = build_canonical_session(&info, &self.args);
        let include_types = self.args.include_types_set();
        let strip_prefixes = self.args.path_prefixes();

        // Pre-pass: build the uuid → block-0 external_id map so block
        // 1..N children and `parentUuid`-linked children can resolve
        // their parent without a second filesystem read. Spec §6.2
        // expects deterministic eid_map lookup; doing this lazily would
        // race with the canonical importer's parent rewire.
        let anchor_map = build_block0_anchor_map(&entries, &info);

        // Hand the entries Vec to a streaming iterator instead of
        // materialising a `Vec<CanonicalEntry>` up-front. Filtered
        // counts that the legacy path would have folded into
        // `source_filtered_count` now arrive as `StreamItem::Filtered`
        // values that the importer accumulates the same way.
        let iter = ClaudeCodeEntryIter::new(
            entries,
            info,
            include_types,
            strip_prefixes,
            self.attachment_policy.clone(),
            anchor_map,
        );

        Ok(ReadSessionOutcome::ImportStream {
            session,
            entries: crate::source::CanonicalEntryStream::new(iter),
            source_filtered_count_initial: 0,
        })
    }
}

/// Streaming `RawEntry → CanonicalEntry` converter.
///
/// Owns the parsed `Vec<RawEntry>` plus the lookup tables the per-line
/// dispatchers consume, and yields one `StreamItem` per `next()` call.
/// A single transcript line may emit multiple canonical blocks
/// (`message.content[]` of length > 1, attachment events with nested
/// payloads); the iterator buffers them in `pending` and drains that
/// buffer before advancing to the next line.
struct ClaudeCodeEntryIter {
    entries: Vec<RawEntry>,
    idx: usize,
    info: SessionInfo,
    include_types: HashSet<String>,
    strip_prefixes: Vec<String>,
    attachment_policy: AttachmentSubtypePolicy,
    anchor_map: HashMap<String, String>,
    /// `tool_use_id → tool_call entry's external_id`. Populated as we
    /// emit `tool_use` blocks so subsequent `tool_result` blocks attach
    /// the canonical tool_call → tool_output link (spec §4.2.2.6).
    tool_call_id_map: HashMap<String, String>,
    /// Buffered items the next `next()` call(s) will drain before
    /// advancing to the next entry. Always strictly in FIFO order.
    pending: std::collections::VecDeque<StreamItem>,
}

impl ClaudeCodeEntryIter {
    fn new(
        entries: Vec<RawEntry>,
        info: SessionInfo,
        include_types: HashSet<String>,
        strip_prefixes: Vec<String>,
        attachment_policy: AttachmentSubtypePolicy,
        anchor_map: HashMap<String, String>,
    ) -> Self {
        Self {
            entries,
            idx: 0,
            info,
            include_types,
            strip_prefixes,
            attachment_policy,
            anchor_map,
            tool_call_id_map: HashMap::new(),
            pending: std::collections::VecDeque::new(),
        }
    }
}

impl Iterator for ClaudeCodeEntryIter {
    type Item = StreamItem;

    fn next(&mut self) -> Option<StreamItem> {
        loop {
            if let Some(item) = self.pending.pop_front() {
                return Some(item);
            }
            if self.idx >= self.entries.len() {
                return None;
            }
            let line_idx = self.idx;
            // Borrow the current RawEntry; we don't move it because the
            // dispatchers only need a `&RawEntry`.
            let entry = &self.entries[line_idx];

            // Reject entry types that aren't conversational events at
            // all (custom-title, agent-name, permission-mode, …). The
            // legacy path counted these into `source_filtered_count`;
            // we emit one `Filtered` so the importer keeps the same
            // running tally.
            if !raw_parser::is_canonical_event_type(&entry.entry_type) {
                self.idx += 1;
                self.pending.push_back(StreamItem::Filtered);
                continue;
            }

            let emit = if entry.entry_type == "attachment" {
                build_attachment_event_entries(
                    entry,
                    &self.info,
                    line_idx as i64,
                    &self.include_types,
                    &self.attachment_policy,
                    &self.anchor_map,
                )
            } else {
                build_message_block_entries(
                    entry,
                    &self.info,
                    line_idx as i64,
                    &self.include_types,
                    &self.anchor_map,
                    &self.strip_prefixes,
                    &mut self.tool_call_id_map,
                )
            };
            self.idx += 1;

            // Push `emit.filtered` synthetic Filtered markers so the
            // importer's `memories_skipped_filtered` lines up with the
            // legacy path's `source_filtered_count` accumulation.
            for _ in 0..emit.filtered {
                self.pending.push_back(StreamItem::Filtered);
            }
            for e in emit.entries {
                self.pending.push_back(StreamItem::Entry(e));
            }
            // Loop continues so `pending.pop_front()` runs and we
            // return the first buffered item without recursion.
        }
    }
}

/// Output of a per-entry block dispatcher. `filtered` is the count of
/// blocks dropped by the source-side `--include-types` /
/// `--attachment-subtypes` filters, propagated up to the importer
/// summary so mixed-block lines (e.g. text + image) report partial
/// skips faithfully (parity with codex's `EntryDecision::TakeMany`).
#[derive(Debug, Default)]
pub struct BlockEmit {
    pub entries: Vec<CanonicalEntry>,
    pub filtered: usize,
}

/// Locator for a block within a line. `Top(idx)` is a direct
/// `message.content[idx]` block; `Sub(outer, inner)` lives inside
/// `tool_result.content[]` for nested image / non-text payloads.
#[derive(Debug, Clone, Copy)]
pub enum BlockIdx {
    Top(usize),
    Sub(usize, usize),
}

impl BlockIdx {
    fn token(self) -> String {
        match self {
            BlockIdx::Top(i) => format!("b{i}"),
            BlockIdx::Sub(o, i) => format!("b{o}_sub{i}"),
        }
    }

    /// `import_order` keys must stay strictly monotonic across all
    /// blocks of a single line so the canonical importer's
    /// `(import_order, timestamp_ms)` sort doesn't reshuffle blocks
    /// that share `timestamp_ms`.
    ///
    /// Bit layout (lower 32 bits of `import_order`):
    ///   bits 31..16: outer index (Top index, or Sub's outer Top)
    ///   bit 15     : SUB_FLAG — 0 for Top, 1 for Sub (Sub always sorts
    ///                immediately after its outer Top, before the next
    ///                Top)
    ///   bits 14..0 : Sub's inner index (0 for Top)
    ///
    /// Collisions are now mathematically impossible for any
    /// `outer < 2^16 = 65536` and `inner < 2^15 = 32768`, which far
    /// exceeds anything seen in real `content[]` / `tool_result.content[]`
    /// arrays.
    fn order_offset(self) -> i64 {
        const SUB_FLAG: i64 = 1 << 15;
        match self {
            BlockIdx::Top(i) => (i as i64) << 16,
            BlockIdx::Sub(o, i) => ((o as i64) << 16) | SUB_FLAG | (i as i64),
        }
    }
}

/// Build the `external_id` for a single canonical block:
/// `claude_code:<session>:<msg_uuid>:<block_idx_token>:<sha1_16>`
/// where the sha1 fingerprint is derived from the block's JSON
/// payload so the same JSONL line always emits the same id, regardless
/// of source-tree layout. Spec §5.1 / §6.3.
fn build_block_external_id(
    session_id: &str,
    msg_uuid: &str,
    idx: BlockIdx,
    payload_json: &str,
) -> String {
    let token = idx.token();
    let sha = sha1_hex_prefix(payload_json.as_bytes(), 16);
    format!("{SOURCE_ID}:{session_id}:{msg_uuid}:{token}:{sha}")
}

/// Pre-compute every line's block-0 external_id so child blocks /
/// `parentUuid`-linked entries can resolve their parent. The map is
/// keyed by the entry's own `uuid`.
///
/// 1-pass walk: for each importable entry compute the same payload
/// fingerprint that `build_block_external_id` will use. We don't
/// emit the entries here — that happens in the second pass — but we
/// do mirror its block-0 selection rule (first content array element)
/// so the resolved id matches whatever the dispatcher will hash.
fn build_block0_anchor_map(entries: &[RawEntry], info: &SessionInfo) -> HashMap<String, String> {
    let mut out: HashMap<String, String> = HashMap::new();
    for entry in entries {
        let Some(uuid) = entry.uuid.as_deref() else {
            continue;
        };
        // type=attachment events live in their own block-0-only
        // namespace. The hash material must mirror what
        // `build_attachment_event_entries` will feed into
        // `build_block_external_id`; otherwise children that name this
        // entry as `parentUuid` would resolve to a different anchor than
        // the importer ever sees, and parent rewire would silently fail.
        if entry.entry_type == "attachment" {
            let payload = entry
                .raw
                .get("content")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            let payload_json = serde_json::to_string(&payload).unwrap_or_default();
            let eid =
                build_block_external_id(&info.session_id, uuid, BlockIdx::Top(0), &payload_json);
            out.insert(uuid.to_string(), eid);
            continue;
        }

        // user/assistant: anchor sits at message.content[0]; system:
        // anchor sits at the entry-level `content`.
        let payload_json = if entry.entry_type == "system" {
            entry
                .content
                .as_ref()
                .map(|v| serde_json::to_string(v).unwrap_or_default())
                .unwrap_or_default()
        } else {
            block0_payload_for_message(entry).unwrap_or_default()
        };
        let eid = build_block_external_id(&info.session_id, uuid, BlockIdx::Top(0), &payload_json);
        out.insert(uuid.to_string(), eid);
    }
    out
}

/// Return the JSON serialization of `message.content[0]` for a
/// user/assistant entry, falling back to an empty string when the
/// content array is missing or empty (matches the dispatcher's
/// degraded-block behaviour).
fn block0_payload_for_message(entry: &RawEntry) -> Option<String> {
    let msg = entry.message.as_ref()?;
    let content = msg.get("content")?;
    match content {
        serde_json::Value::Array(arr) => arr
            .first()
            .map(|b| serde_json::to_string(b).unwrap_or_default()),
        serde_json::Value::String(s) => {
            // Promote a bare string to a synthetic text block so the
            // anchor map and the dispatcher agree on the hash material.
            let synthetic = serde_json::json!({ "type": "text", "text": s });
            Some(serde_json::to_string(&synthetic).unwrap_or_default())
        }
        _ => None,
    }
}

/// Dispatch a single user/assistant/system entry into 1..N canonical
/// entries. Spec §4.2.2.6 claude-code row. `tool_call_id_map` is
/// populated as we emit `tool_use` blocks and consulted from
/// `tool_result` blocks so the canonical tool_call → tool_output
/// link survives even though the two blocks live on different JSONL
/// lines.
#[allow(clippy::too_many_arguments)]
fn build_message_block_entries(
    entry: &RawEntry,
    info: &SessionInfo,
    line_idx: i64,
    include_types: &HashSet<String>,
    anchor_map: &HashMap<String, String>,
    _strip_prefixes: &[String],
    tool_call_id_map: &mut HashMap<String, String>,
) -> BlockEmit {
    let mut out = BlockEmit::default();

    let Some(uuid) = entry.uuid.as_deref() else {
        out.filtered += 1;
        return out;
    };
    let Some(timestamp_ms) = entry.timestamp.as_deref().and_then(parse_timestamp_millis) else {
        out.filtered += 1;
        return out;
    };

    // The conversational anchor is parentUuid (or empty for thread
    // roots). Block 0's parent_external_ids points at it via the
    // anchor map; block 1..N points at block 0's external_id.
    let parent_anchor: Vec<String> = entry
        .parent_uuid
        .as_deref()
        .and_then(|p| anchor_map.get(p).cloned())
        .map(|eid| vec![eid])
        .unwrap_or_default();

    let entry_role = match entry.entry_type.as_str() {
        "user" => MessageRole::RoleUser,
        "assistant" => MessageRole::RoleAssistant,
        "system" => MessageRole::RoleMeta,
        _ => MessageRole::RoleUnspecified,
    };

    // System entries live entirely in entry.content (no block array).
    if entry.entry_type == "system" {
        let kind_tag = "system";
        if !include_types.contains(kind_tag) {
            out.filtered += 1;
            return out;
        }
        // Drop system entries whose `content` is missing or null —
        // legacy `is_entry_importable` rejected them outright, and
        // emitting a `"null"` body here would only generate noise
        // that pollutes search results without conveying anything.
        let Some(content_value) = entry.content.as_ref().filter(|v| !v.is_null()).cloned() else {
            out.filtered += 1;
            return out;
        };
        let payload_json = serde_json::to_string(&content_value).unwrap_or_default();
        let content_str = match &content_value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };

        let eid = build_block_external_id(&info.session_id, uuid, BlockIdx::Top(0), &payload_json);

        let mut metadata = serde_json::Map::new();
        insert_common_entry_metadata(&mut metadata, entry);

        out.entries.push(CanonicalEntry {
            external_id: eid,
            parent_external_ids: parent_anchor,
            role: entry_role,
            content_type: ContentType::Text,
            content: content_str,
            metadata,
            timestamp_ms,
            import_order: (line_idx << 32) | BlockIdx::Top(0).order_offset(),
            kind_tag,
            canonical: CanonicalAddons::default(),
        });
        return out;
    }

    // user / assistant: walk the message.content[] array.
    let blocks: Vec<serde_json::Value> = match entry
        .message
        .as_ref()
        .and_then(|m| m.get("content").cloned())
    {
        Some(serde_json::Value::Array(arr)) => arr,
        Some(serde_json::Value::String(s)) => vec![serde_json::json!({"type": "text", "text": s})],
        _ => Vec::new(),
    };

    if blocks.is_empty() {
        out.filtered += 1;
        return out;
    }

    let ctx = BlockCtx {
        entry,
        info,
        uuid,
        line_idx,
        include_types,
        parent_anchor: &parent_anchor,
        timestamp_ms,
        entry_role,
    };
    let mut block0_eid: Option<String> = None;

    for (block_idx, block) in blocks.iter().enumerate() {
        let block_type = block.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let payload_json = serde_json::to_string(block).unwrap_or_default();

        match block_type {
            "tool_use" => emit_tool_use_block(
                &mut out,
                &ctx,
                block_idx,
                block,
                &payload_json,
                &mut block0_eid,
                tool_call_id_map,
            ),
            "tool_result" => emit_tool_result_blocks(
                &mut out,
                &ctx,
                block_idx,
                block,
                &payload_json,
                &mut block0_eid,
                tool_call_id_map,
            ),
            "image" => emit_image_block(
                &mut out,
                &ctx,
                block_idx,
                block,
                &payload_json,
                &mut block0_eid,
            ),
            "thinking" | "redacted_thinking" => emit_reasoning_block(
                &mut out,
                &ctx,
                block_idx,
                block,
                block_type,
                &payload_json,
                &mut block0_eid,
            ),
            // text + unknown text-shaped blocks fall back to the
            // role-derived kind.
            _ => emit_text_block(
                &mut out,
                &ctx,
                block_idx,
                block,
                block_type,
                &payload_json,
                &mut block0_eid,
            ),
        }
    }

    out
}

/// Per-message dispatch context. Hoisted out of every `emit_*_block`
/// signature so each emit call needs only the per-block state
/// (`block_idx`, `block`, `payload_json`, `block0_eid`) on top of this
/// shared bundle. Without this struct each emit had ~13 parameters
/// and a `#[allow(clippy::too_many_arguments)]` escape hatch.
struct BlockCtx<'a> {
    entry: &'a RawEntry,
    info: &'a SessionInfo,
    uuid: &'a str,
    line_idx: i64,
    include_types: &'a HashSet<String>,
    parent_anchor: &'a [String],
    timestamp_ms: i64,
    entry_role: MessageRole,
}

impl BlockCtx<'_> {
    fn import_order(&self, idx: BlockIdx) -> i64 {
        // Reserve the lower 32 bits for `order_offset` (see
        // `BlockIdx::order_offset` for the bit layout). With i64 and a
        // 32-bit shift, `line_idx` has up to 2^31 ≈ 2.1B headroom —
        // ample for any realistic JSONL transcript.
        (self.line_idx << 32) | idx.order_offset()
    }
}

/// Resolve the parent_external_ids for a block while seeding the
/// shared block-0 anchor. Block 0 always seeds the anchor (even when
/// the block itself is filtered out by `--include-types`) so block
/// 1..N siblings can still attach themselves; without this, dropping
/// a block-0 text by filter would orphan every later tool_use in the
/// same message.
fn anchor_and_parents(
    ctx: &BlockCtx,
    block_idx: usize,
    eid: &str,
    block0_eid: &mut Option<String>,
) -> Vec<String> {
    if block_idx == 0 {
        *block0_eid = Some(eid.to_string());
        ctx.parent_anchor.to_vec()
    } else {
        block0_eid
            .as_ref()
            .map(|p| vec![p.clone()])
            .unwrap_or_default()
    }
}

/// Common metadata applied to every canonical claude_code entry.
fn insert_common_entry_metadata(
    metadata: &mut serde_json::Map<String, serde_json::Value>,
    entry: &RawEntry,
) {
    if let Some(ref uuid) = entry.uuid {
        metadata.insert("uuid".to_string(), serde_json::Value::String(uuid.clone()));
    }
    if let Some(ref puuid) = entry.parent_uuid {
        metadata.insert(
            "parent_uuid".to_string(),
            serde_json::Value::String(puuid.clone()),
        );
    }
    metadata.insert(
        "entry_type".to_string(),
        serde_json::Value::String(entry.entry_type.clone()),
    );
    if entry.is_sidechain {
        metadata.insert("is_sidechain".to_string(), serde_json::Value::Bool(true));
    }
    if let Some(ref req_id) = entry.request_id {
        metadata.insert(
            "request_id".to_string(),
            serde_json::Value::String(req_id.clone()),
        );
    }

    // Forward-compat capture: every remaining top-level scalar from the
    // raw JSONL line lands under `metadata.claude_code.*` so display
    // consumers (e.g. agent-app) can read provider attributes such as
    // `is_meta` / `user_type` / `entrypoint` without parsing the body.
    let claude_code = collect_claude_code_metadata(entry);
    if !claude_code.is_empty() {
        metadata.insert(
            "claude_code".to_string(),
            serde_json::Value::Object(claude_code),
        );
    }
}

/// Build the `metadata.claude_code` object: every top-level scalar from
/// the raw JSONL line that is neither already promoted to top-level
/// metadata nor part of the body / large-payload set. Oversized and
/// `null` values are skipped so the attribute bag stays lean.
fn collect_claude_code_metadata(entry: &RawEntry) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    let Some(obj) = entry.as_raw_value().as_object() else {
        return out;
    };
    for (key, value) in obj {
        if value.is_null() || CLAUDE_CODE_META_EXCLUDED.contains(&key.as_str()) {
            continue;
        }
        if exceeds_meta_size(value) {
            continue;
        }
        out.insert(snake_case_key(key), value.clone());
    }
    out
}

/// True if a single transcribed value exceeds the metadata size cap.
/// Plain scalars take the cheap `String` length check; only arrays and
/// objects — which the exclusion set already keeps rare — pay the
/// serialization cost.
fn exceeds_meta_size(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(s) => s.len() > CLAUDE_CODE_META_MAX_BYTES,
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => serde_json::to_string(value)
            .map(|s| s.len() > CLAUDE_CODE_META_MAX_BYTES)
            .unwrap_or(true),
        _ => false,
    }
}

/// Map a raw Claude Code JSONL key to its stable `claude_code.*`
/// contract name. Keys whose acronym runs (`ID`, `UUID`) a naive
/// splitter handles inconsistently — or whose contract name diverges
/// from the literal transform (`version` → `claude_version`) — are
/// pinned in an explicit table; everything else uses a generic
/// camelCase → snake_case transform.
fn snake_case_key(raw: &str) -> String {
    match raw {
        "version" => return "claude_version".to_string(),
        "toolUseID" => return "tool_use_id".to_string(),
        "parentToolUseID" => return "parent_tool_use_id".to_string(),
        "sourceToolAssistantUUID" => return "source_tool_assistant_uuid".to_string(),
        "promptId" => return "prompt_id".to_string(),
        _ => {}
    }
    let mut out = String::with_capacity(raw.len() + 4);
    for (i, ch) in raw.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn emit_text_block(
    out: &mut BlockEmit,
    ctx: &BlockCtx,
    block_idx: usize,
    block: &serde_json::Value,
    block_type: &str,
    payload_json: &str,
    block0_eid: &mut Option<String>,
) {
    let kind_tag: &'static str = match ctx.entry.entry_type.as_str() {
        "user" => "user",
        "assistant" => "assistant",
        _ => "system",
    };
    let eid = build_block_external_id(
        &ctx.info.session_id,
        ctx.uuid,
        BlockIdx::Top(block_idx),
        payload_json,
    );
    let parent_external_ids = anchor_and_parents(ctx, block_idx, &eid, block0_eid);

    if !ctx.include_types.contains(kind_tag) {
        out.filtered += 1;
        return;
    }

    let content_str = match block.get("text").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        // Unknown text-shaped block: degrade to the raw block JSON so
        // nothing is silently lost; the raw payload is also captured
        // under canonical.raw below.
        None => payload_json.to_string(),
    };

    let mut metadata = serde_json::Map::new();
    insert_common_entry_metadata(&mut metadata, ctx.entry);
    metadata.insert(
        "block_type".to_string(),
        serde_json::Value::String(block_type.to_string()),
    );

    // Unknown block shapes get their full payload captured as raw so
    // operators can audit drift without re-reading the JSONL file.
    let canonical_addons = if matches!(block_type, "text" | "") {
        CanonicalAddons::default()
    } else {
        let mut addons = CanonicalAddons::default();
        canonical::merge_raw(
            &mut addons.raw,
            canonical::raw_entry(SOURCE_ID, canonical::sanitize_raw_payload(block)),
        );
        addons
    };

    out.entries.push(CanonicalEntry {
        external_id: eid,
        parent_external_ids,
        role: ctx.entry_role,
        content_type: ContentType::Text,
        content: content_str,
        metadata,
        timestamp_ms: ctx.timestamp_ms,
        import_order: ctx.import_order(BlockIdx::Top(block_idx)),
        kind_tag,
        canonical: canonical_addons,
    });
}

fn emit_tool_use_block(
    out: &mut BlockEmit,
    ctx: &BlockCtx,
    block_idx: usize,
    block: &serde_json::Value,
    payload_json: &str,
    block0_eid: &mut Option<String>,
    tool_call_id_map: &mut HashMap<String, String>,
) {
    let kind_tag = "tool_call";
    let eid = build_block_external_id(
        &ctx.info.session_id,
        ctx.uuid,
        BlockIdx::Top(block_idx),
        payload_json,
    );
    let parent_external_ids = anchor_and_parents(ctx, block_idx, &eid, block0_eid);
    let tool_id = block.get("id").and_then(|v| v.as_str());
    // Register the call_id → external_id mapping even if the
    // `tool_call` kind is filtered out via `--include-types`. A later
    // `tool_result` may still be in scope, and it should be able to
    // resolve its parent against the call's would-have-been external_id
    // (the canonical importer's `eid_map` will skip the unresolvable
    // entry without breaking other parent links).
    if let Some(tid) = tool_id {
        tool_call_id_map.insert(tid.to_string(), eid.clone());
    }
    if !ctx.include_types.contains(kind_tag) {
        out.filtered += 1;
        return;
    }

    let name = block.get("name").and_then(|v| v.as_str());
    let input = block.get("input");
    let result = canonical::build_tool_call(SOURCE_ID, name, tool_id, input, None);

    let mut metadata = serde_json::Map::new();
    insert_common_entry_metadata(&mut metadata, ctx.entry);
    metadata.insert(
        "block_type".to_string(),
        serde_json::Value::String("tool_use".to_string()),
    );

    out.entries.push(CanonicalEntry {
        external_id: eid,
        parent_external_ids,
        role: MessageRole::RoleAssistant,
        content_type: ContentType::Tool,
        content: result.content,
        metadata,
        timestamp_ms: ctx.timestamp_ms,
        import_order: ctx.import_order(BlockIdx::Top(block_idx)),
        kind_tag,
        canonical: CanonicalAddons::with_tool(SOURCE_ID, result.tool, block),
    });
}

fn emit_tool_result_blocks(
    out: &mut BlockEmit,
    ctx: &BlockCtx,
    block_idx: usize,
    block: &serde_json::Value,
    payload_json: &str,
    block0_eid: &mut Option<String>,
    tool_call_id_map: &HashMap<String, String>,
) {
    let tool_output_eid = build_block_external_id(
        &ctx.info.session_id,
        ctx.uuid,
        BlockIdx::Top(block_idx),
        payload_json,
    );
    let conversation_anchor = anchor_and_parents(ctx, block_idx, &tool_output_eid, block0_eid);

    let tool_use_id = block.get("tool_use_id").and_then(|v| v.as_str());

    // Spec §4.2 / §4.2.2.6: a tool_result entry can carry up to two
    // parent links — the matching tool_call (the more semantically
    // important relation, placed at index 0) and the conversational
    // anchor (parentUuid → previous turn). Build both: the canonical
    // importer's order-preserving dedup keeps them in this priority.
    let tool_call_eid = tool_use_id.and_then(|tid| tool_call_id_map.get(tid).cloned());
    let parent_external_ids_for_tool_output: Vec<String> = match tool_call_eid {
        Some(call_eid) => {
            let mut v = vec![call_eid];
            for anchor in &conversation_anchor {
                if !v.contains(anchor) {
                    v.push(anchor.clone());
                }
            }
            v
        }
        None => conversation_anchor.clone(),
    };
    let status = block
        .get("is_error")
        .and_then(|v| v.as_bool())
        .map(|err| canonical::ToolStatus::from_success(!err));
    let output_text = flatten_tool_result_content(block.get("content"));

    let tool_output_kind = "tool_output";
    let tool_output_emitted = ctx.include_types.contains(tool_output_kind);
    if tool_output_emitted {
        let result = canonical::build_tool_output(
            SOURCE_ID,
            None,
            tool_use_id,
            output_text.as_deref(),
            status,
            None,
        );
        let mut metadata = serde_json::Map::new();
        insert_common_entry_metadata(&mut metadata, ctx.entry);
        metadata.insert(
            "block_type".to_string(),
            serde_json::Value::String("tool_result".to_string()),
        );

        out.entries.push(CanonicalEntry {
            external_id: tool_output_eid.clone(),
            parent_external_ids: parent_external_ids_for_tool_output,
            role: MessageRole::RoleTool,
            content_type: ContentType::Tool,
            content: result.content,
            metadata,
            timestamp_ms: ctx.timestamp_ms,
            import_order: ctx.import_order(BlockIdx::Top(block_idx)),
            kind_tag: tool_output_kind,
            canonical: CanonicalAddons::with_tool(SOURCE_ID, result.tool, block),
        });
    } else {
        out.filtered += 1;
    }

    // Tool result content can carry image sub-blocks. They get their
    // own canonical attachment entries, parented to the tool_output
    // (when emitted) or the conversational anchor otherwise.
    if let Some(serde_json::Value::Array(inner)) = block.get("content") {
        for (sub_idx, sub) in inner.iter().enumerate() {
            let sub_type = sub.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if sub_type != "image" {
                continue;
            }
            if !ctx.include_types.contains("attachment") {
                out.filtered += 1;
                continue;
            }
            let sub_payload = serde_json::to_string(sub).unwrap_or_default();
            let sub_eid = build_block_external_id(
                &ctx.info.session_id,
                ctx.uuid,
                BlockIdx::Sub(block_idx, sub_idx),
                &sub_payload,
            );
            // When the tool_output was emitted, the image hangs off it.
            // When tool_output is filtered out we still need a meaningful
            // parent: `conversation_anchor` already encodes the right
            // choice (block-0 eid for block_idx>=1, parentUuid anchor for
            // block_idx==0), so reuse it instead of falling back to
            // `parent_anchor`, which would otherwise misroute children of
            // a block-1+ tool_result onto the previous turn.
            let parent = if tool_output_emitted {
                vec![tool_output_eid.clone()]
            } else {
                conversation_anchor.clone()
            };
            let img = canonicalize_image_block(sub);
            let mut metadata = serde_json::Map::new();
            insert_common_entry_metadata(&mut metadata, ctx.entry);
            metadata.insert(
                "block_type".to_string(),
                serde_json::Value::String("tool_result.image".to_string()),
            );

            out.entries.push(CanonicalEntry {
                external_id: sub_eid,
                parent_external_ids: parent,
                role: MessageRole::RoleTool,
                content_type: img.content_type,
                content: img.content,
                metadata,
                timestamp_ms: ctx.timestamp_ms,
                import_order: ctx.import_order(BlockIdx::Sub(block_idx, sub_idx)),
                kind_tag: "attachment",
                canonical: CanonicalAddons::with_attachment(SOURCE_ID, img.attachment, sub),
            });
        }
    }
}

fn emit_image_block(
    out: &mut BlockEmit,
    ctx: &BlockCtx,
    block_idx: usize,
    block: &serde_json::Value,
    payload_json: &str,
    block0_eid: &mut Option<String>,
) {
    let kind_tag = "attachment";
    let eid = build_block_external_id(
        &ctx.info.session_id,
        ctx.uuid,
        BlockIdx::Top(block_idx),
        payload_json,
    );
    let parent_external_ids = anchor_and_parents(ctx, block_idx, &eid, block0_eid);
    if !ctx.include_types.contains(kind_tag) {
        out.filtered += 1;
        return;
    }

    let img = canonicalize_image_block(block);
    let mut metadata = serde_json::Map::new();
    insert_common_entry_metadata(&mut metadata, ctx.entry);
    metadata.insert(
        "block_type".to_string(),
        serde_json::Value::String("image".to_string()),
    );

    out.entries.push(CanonicalEntry {
        external_id: eid,
        parent_external_ids,
        role: ctx.entry_role,
        content_type: img.content_type,
        content: img.content,
        metadata,
        timestamp_ms: ctx.timestamp_ms,
        import_order: ctx.import_order(BlockIdx::Top(block_idx)),
        kind_tag,
        canonical: CanonicalAddons::with_attachment(SOURCE_ID, img.attachment, block),
    });
}

/// Anthropic `thinking` / `redacted_thinking` content block →
/// canonical `kind=reasoning` entry. Spec §4.2.2.6 claude-code table:
/// the assistant's chain-of-thought always lands as `RoleAssistant /
/// ContentType::Text` so downstream that filters by `kind=reasoning`
/// (workflows, summary, embedding policy) can identify it without
/// knowing whether the source was Anthropic or Codex.
///
/// `redacted_thinking` carries an opaque encrypted blob in `block.data`
/// that the client cannot decrypt. We mirror codex's policy: by default
/// we record only `metadata.encrypted_content_sha256` /
/// `encrypted_content_size` (so an operator can correlate / detect
/// tampering) and leave `MemoryData.content` empty, since persisting
/// the ciphertext provides zero search value and adds DB / dump bloat.
fn emit_reasoning_block(
    out: &mut BlockEmit,
    ctx: &BlockCtx,
    block_idx: usize,
    block: &serde_json::Value,
    block_type: &str,
    payload_json: &str,
    block0_eid: &mut Option<String>,
) {
    let kind_tag = "reasoning";
    let eid = build_block_external_id(
        &ctx.info.session_id,
        ctx.uuid,
        BlockIdx::Top(block_idx),
        payload_json,
    );
    // Seed the anchor BEFORE the include_types filter so a filtered
    // reasoning block at index 0 does not orphan its block-1+ siblings,
    // matching the policy already used by `emit_tool_use_block`.
    let parent_external_ids = anchor_and_parents(ctx, block_idx, &eid, block0_eid);
    if !ctx.include_types.contains(kind_tag) {
        out.filtered += 1;
        return;
    }

    let mut metadata = serde_json::Map::new();
    insert_common_entry_metadata(&mut metadata, ctx.entry);
    metadata.insert(
        "block_type".to_string(),
        serde_json::Value::String(block_type.to_string()),
    );

    let content = match block_type {
        "thinking" => block
            .get("thinking")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_default(),
        "redacted_thinking" => {
            // Anthropic ships the encrypted blob under `block.data`.
            // Record fingerprint + length only (see header comment).
            if let Some(enc) = block.get("data").and_then(|v| v.as_str()) {
                let bytes = enc.as_bytes();
                metadata.insert(
                    "encrypted_content_sha256".to_string(),
                    serde_json::Value::String(sha256_hex_prefix(bytes, 64)),
                );
                metadata.insert(
                    "encrypted_content_size".to_string(),
                    serde_json::Value::Number(serde_json::Number::from(bytes.len() as u64)),
                );
            }
            String::new()
        }
        _ => String::new(),
    };

    out.entries.push(CanonicalEntry {
        external_id: eid,
        parent_external_ids,
        // Reasoning is always assistant-authored (spec §4.2.2.6); pin
        // it explicitly rather than inheriting `ctx.entry_role`, so a
        // hypothetical thinking block on a non-assistant message still
        // lands with the correct role.
        role: MessageRole::RoleAssistant,
        content_type: ContentType::Text,
        content,
        metadata,
        timestamp_ms: ctx.timestamp_ms,
        import_order: ctx.import_order(BlockIdx::Top(block_idx)),
        kind_tag,
        canonical: CanonicalAddons::default(),
    });
}

/// Flatten `tool_result.content` into a string for the canonical
/// `tool_output` body. Non-text sub-blocks (e.g. image) are skipped
/// here — they emit their own `kind=attachment` entries via
/// `emit_tool_result_blocks`.
fn flatten_tool_result_content(content: Option<&serde_json::Value>) -> Option<String> {
    let content = content?;
    if let Some(s) = content.as_str() {
        return Some(s.to_string());
    }
    if let Some(arr) = content.as_array() {
        let mut parts = Vec::new();
        for item in arr {
            let ty = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if ty == "text"
                && let Some(t) = item.get("text").and_then(|v| v.as_str())
            {
                parts.push(t.to_string());
            }
        }
        if !parts.is_empty() {
            return Some(parts.join("\n"));
        }
    }
    None
}

/// Convert an Anthropic `{"type":"image"}` block into the canonical
/// attachment shape. `source.type` ∈ {base64, url}.
fn canonicalize_image_block(block: &serde_json::Value) -> canonical::BuildAttachmentResult {
    let source = block.get("source");
    let source_type = source
        .and_then(|s| s.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let media_type = source
        .and_then(|s| s.get("media_type"))
        .and_then(|v| v.as_str());
    let (storage, data, url): (canonical::AttachmentStorage, Option<&str>, Option<&str>) =
        match source_type {
            "base64" => {
                let data_b64 = source.and_then(|s| s.get("data")).and_then(|v| v.as_str());
                (canonical::AttachmentStorage::InlineBase64, data_b64, None)
            }
            "url" => {
                let url = source.and_then(|s| s.get("url")).and_then(|v| v.as_str());
                (canonical::AttachmentStorage::Url, None, url)
            }
            _ => (
                canonical::AttachmentStorage::Invalid {
                    reason: "unknown_image_source_type".to_string(),
                },
                None,
                None,
            ),
        };
    canonical::build_attachment(
        canonical::AttachmentKind::Image,
        storage,
        media_type,
        data,
        url,
        None,
        None,
        None,
    )
}

/// Dispatch a `type=attachment` JSONL event to a single canonical
/// attachment entry. Spec §4.2.2.4 + Claude Code subtype catalog.
fn build_attachment_event_entries(
    entry: &RawEntry,
    info: &SessionInfo,
    line_idx: i64,
    include_types: &HashSet<String>,
    policy: &AttachmentSubtypePolicy,
    anchor_map: &HashMap<String, String>,
) -> BlockEmit {
    let mut out = BlockEmit::default();

    if !include_types.contains("attachment") {
        out.filtered += 1;
        return out;
    }
    let subtype = entry.subtype.as_deref().unwrap_or("");
    if !policy.includes(subtype) {
        out.filtered += 1;
        return out;
    }
    let Some(uuid) = entry.uuid.as_deref() else {
        out.filtered += 1;
        return out;
    };
    // Reject attachments without a parseable timestamp for parity
    // with the user/assistant/system path. Falling back to
    // `info.created_at` would let `--since` filtering silently include
    // events whose original timestamp is corrupted, and would also
    // misrepresent the chronological position of the saved memory.
    let Some(timestamp_ms) = entry.timestamp.as_deref().and_then(parse_timestamp_millis) else {
        out.filtered += 1;
        return out;
    };

    let payload = entry
        .raw
        .get("content")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let payload_json = serde_json::to_string(&payload).unwrap_or_default();
    let eid = build_block_external_id(&info.session_id, uuid, BlockIdx::Top(0), &payload_json);

    // Resolve the parent through the anchor map so the
    // `parent_external_ids` we emit always matches what the canonical
    // importer's `eid_map` will lookup. Building a 3-segment
    // `claude_code:<sess>:<uuid>` directly here would never match any
    // imported memory's external_id (which is always 5-segment), and
    // the importer's parent rewire would silently skip the link.
    let parent_anchor: Vec<String> = entry
        .parent_uuid
        .as_deref()
        .filter(|p| !p.is_empty())
        .and_then(|p| anchor_map.get(p).cloned())
        .map(|eid| vec![eid])
        .unwrap_or_default();

    let extracted_text = extract_attachment_text(entry, subtype).unwrap_or_default();
    let truncated = extracted_text.len() > ATTACHMENT_CONTENT_MAX_BYTES;
    let content_str = if truncated {
        let cut = canonical::safe_truncate_at_char_boundary(
            extracted_text.as_str(),
            ATTACHMENT_CONTENT_MAX_BYTES,
        );
        format!("{cut}\n…(truncated)")
    } else {
        extracted_text
    };

    // Build a synthetic `metadata.attachment` describing this as a
    // text-shaped attachment with the subtype as media_type. Inline
    // size guard does not apply (no base64 payload), so storage is
    // always `inline_base64=false / url=null` — represented as
    // `storage=ref` for forensic recovery. The CanonicalEntry's
    // `content_type` is forced to Text below so the extracted body
    // remains highlightable / embeddable; the helper's default
    // `ContentType::Url` for `kind=ref` would otherwise route this row
    // through the URL-content paths in `dispatcher::is_embeddable` and
    // `compute_memory_highlights`, masking the body we just extracted.
    let kind = canonical::AttachmentKind::Ref;
    let attachment = canonical::build_attachment(
        kind,
        canonical::AttachmentStorage::Ref,
        Some(subtype),
        None,
        Some(&format!("attachment://{subtype}/{uuid}")),
        None,
        None,
        None,
    );
    let entry_content_type = ContentType::Text;

    let mut metadata = serde_json::Map::new();
    insert_common_entry_metadata(&mut metadata, entry);
    metadata.insert(
        "subtype".to_string(),
        serde_json::Value::String(subtype.to_string()),
    );
    if truncated {
        metadata.insert(
            "content_truncated".to_string(),
            serde_json::Value::Bool(true),
        );
    }

    out.entries.push(CanonicalEntry {
        external_id: eid,
        parent_external_ids: parent_anchor,
        role: MessageRole::RoleMeta,
        content_type: entry_content_type,
        // The canonical helper's placeholder string isn't useful for
        // a text attachment; emit the extracted body directly.
        content: content_str,
        metadata,
        timestamp_ms,
        import_order: (line_idx << 32) | BlockIdx::Top(0).order_offset(),
        kind_tag: "attachment",
        canonical: CanonicalAddons::with_attachment(
            SOURCE_ID,
            attachment.attachment,
            entry.as_raw_value(),
        ),
    });

    out
}

/// Pull a usable text body out of a `type=attachment` event by
/// subtype. Falls back to the verbatim JSON line so unknown subtypes
/// still surface something. Spec §4.2.2.4.
fn extract_attachment_text(entry: &RawEntry, subtype: &str) -> Option<String> {
    match subtype {
        "task_reminder" | "skill_listing" | "command_permissions" | "queued_command" => entry
            .content
            .as_ref()
            .and_then(|v| v.as_str())
            .map(String::from),
        "nested_memory" => entry
            .content
            .as_ref()
            .and_then(|v| v.get("content"))
            .and_then(|v| v.as_str())
            .map(String::from),
        "edited_text_file" => entry
            .extra("snippet")
            .and_then(|v| v.as_str())
            .map(String::from),
        "diagnostics" => format_diagnostics(entry),
        "mcp_instructions_delta" => entry
            .extra("addedBlocks")
            .map(|v| serde_json::to_string(v).unwrap_or_default()),
        "hook_non_blocking_error" => Some(join_hook_streams(entry)),
        // Unknown subtype: fall back to whichever payload the new
        // subtype happens to use. Prefer the entry-level `content`
        // when present (the most common shape), and otherwise
        // serialize the entire JSONL line so a future subtype that
        // puts its body on a top-level field (e.g. `addedBlocks`,
        // `stdout`/`stderr`-style siblings) still surfaces something
        // searchable / embeddable. The ATTACHMENT_CONTENT_MAX_BYTES
        // truncation upstream keeps a pathological line bounded.
        _ => match entry.content.as_ref() {
            Some(serde_json::Value::String(s)) => Some(s.clone()),
            Some(other) => Some(serde_json::to_string(other).unwrap_or_default()),
            None => {
                let verbatim = serde_json::to_string(entry.as_raw_value()).unwrap_or_default();
                if verbatim.is_empty() {
                    None
                } else {
                    Some(verbatim)
                }
            }
        },
    }
}

fn format_diagnostics(entry: &RawEntry) -> Option<String> {
    let arr = entry.extra("files")?.as_array()?;
    let mut parts = Vec::new();
    for f in arr {
        let path = f.get("path").and_then(|v| v.as_str()).unwrap_or("?");
        let Some(diags) = f.get("diagnostics").and_then(|v| v.as_array()) else {
            continue;
        };
        for d in diags {
            let msg = d.get("message").and_then(|v| v.as_str()).unwrap_or("");
            let sev = d.get("severity").and_then(|v| v.as_str()).unwrap_or("");
            parts.push(format!("[{sev}] {path}: {msg}"));
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

fn join_hook_streams(entry: &RawEntry) -> String {
    let stdout = entry.extra("stdout").and_then(|v| v.as_str());
    let stderr = entry.extra("stderr").and_then(|v| v.as_str());
    match (stdout, stderr) {
        (Some(o), Some(e)) => format!("{o}\n---\n{e}"),
        (Some(o), None) => o.to_string(),
        (None, Some(e)) => e.to_string(),
        (None, None) => String::new(),
    }
}

fn build_canonical_session(info: &SessionInfo, args: &ClaudeCodeArgs) -> CanonicalSession {
    let safe_session_id =
        truncate_id_for_external(&info.session_id, ID_SHA256_THRESHOLD).into_owned();
    let channel = format!("{SOURCE_ID}:{safe_session_id}");

    let description = info.custom_title.clone().or_else(|| {
        let project_name = info
            .cwd
            .as_deref()
            .and_then(|p| p.rsplit('/').next())
            .unwrap_or("unknown");
        let branch = info.git_branch.as_deref().unwrap_or("unknown");
        Some(format!("{project_name} ({branch})"))
    });

    // Default labels per §5.2.3 (mirrors codex).
    let strip_prefixes = args.path_prefixes();
    let mut labels: Vec<String> = vec!["coding_agent".to_string(), format!("agent:{SOURCE_ID}")];
    if let Some(ref cwd) = info.cwd {
        let stripped = apply_path_prefix(cwd, &strip_prefixes);
        labels.push(truncate_label_keep_tail("path:", stripped));
        if let Some((_, dir_name)) = cwd.rsplit_once('/')
            && !dir_name.is_empty()
        {
            labels.push(truncate_label_keep_tail("dir:", dir_name));
        }
    }
    if let Some(ref branch) = info.git_branch {
        labels.push(truncate_label_keep_head("branch:", branch));
    }
    labels.retain(|l| !l.is_empty());

    let mut session_meta = serde_json::Map::new();
    if let Some(ref cwd) = info.cwd {
        session_meta.insert("cwd".to_string(), serde_json::Value::String(cwd.clone()));
    }
    if let Some(ref branch) = info.git_branch {
        session_meta.insert(
            "git_branch".to_string(),
            serde_json::Value::String(branch.clone()),
        );
    }
    if let Some(ref slug) = info.slug {
        session_meta.insert("slug".to_string(), serde_json::Value::String(slug.clone()));
    }
    if let Some(ref hash) = info.project_hash {
        session_meta.insert(
            "project_hash".to_string(),
            serde_json::Value::String(hash.clone()),
        );
    }

    CanonicalSession {
        source_id: SOURCE_ID.to_string(),
        session_id: safe_session_id,
        channel,
        description,
        cwd: info.cwd.clone(),
        git_branch: info.git_branch.clone(),
        created_at_ms: info.created_at,
        updated_at_ms: info.updated_at,
        source_labels: labels,
        source_metadata: serde_json::Value::Object(session_meta),
    }
}

fn collect_jsonl_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("jsonl") && path.is_file() {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::test_support::{entries_from_outcome, unpack_outcome};
    use std::io::Write;

    fn args_with_file(p: PathBuf) -> ClaudeCodeArgs {
        ClaudeCodeArgs {
            session_file: Some(p),
            project_dir: None,
            all_projects: false,
            claude_dir: PathBuf::from("~/.claude"),
            include_types: "user,assistant,tool_call,tool_output,system,reasoning,attachment"
                .to_string(),
            strip_path_prefix: None,
            attachment_subtypes: "default".to_string(),
        }
    }

    fn write_jsonl(lines: &[&str]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("session.jsonl");
        let mut f = std::fs::File::create(&file_path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
        (dir, file_path)
    }

    #[test]
    fn discover_yields_session_file() {
        let (_d, path) = write_jsonl(&[]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let inputs = src.discover().unwrap();
        assert_eq!(inputs, vec![path]);
    }

    #[test]
    fn simple_user_text_emits_one_canonical_entry() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"hello"}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.kind_tag, "user");
        assert_eq!(e.role, MessageRole::RoleUser);
        assert_eq!(e.content, "hello");
        assert!(e.external_id.starts_with("claude_code:s1:u1:b0:"));
        assert!(e.parent_external_ids.is_empty());
    }

    #[test]
    fn assistant_text_plus_two_tool_uses_decompose_with_chain() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:01:00.000Z","sessionId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"let me look"},{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"/x"}},{"type":"tool_use","id":"t2","name":"Bash","input":{"command":"ls"}}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].kind_tag, "assistant");
        assert_eq!(entries[1].kind_tag, "tool_call");
        assert_eq!(entries[2].kind_tag, "tool_call");
        // Block 0 is parent-less; blocks 1/2 chain back to block 0.
        assert!(entries[0].parent_external_ids.is_empty());
        assert_eq!(
            entries[1].parent_external_ids,
            vec![entries[0].external_id.clone()]
        );
        assert_eq!(
            entries[2].parent_external_ids,
            vec![entries[0].external_id.clone()]
        );
        // import_order is monotonic across the three blocks
        assert!(entries[0].import_order < entries[1].import_order);
        assert!(entries[1].import_order < entries[2].import_order);
    }

    #[test]
    fn user_tool_result_emits_tool_output_entry() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u2","timestamp":"2026-04-17T10:02:00.000Z","sessionId":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"file contents","is_error":false}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.kind_tag, "tool_output");
        assert_eq!(e.role, MessageRole::RoleTool);
        assert_eq!(e.content_type, ContentType::Tool);
        assert!(e.canonical.tool.is_some());
    }

    #[test]
    fn user_image_block_becomes_attachment() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u3","timestamp":"2026-04-17T10:03:00.000Z","sessionId":"s1","message":{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.kind_tag, "attachment");
        assert!(e.canonical.attachment.is_some());
        let attachment = e.canonical.attachment.as_ref().unwrap();
        assert_eq!(
            attachment.get("storage").and_then(|v| v.as_str()),
            Some("inline_base64")
        );
    }

    #[test]
    fn tool_result_links_back_to_matching_tool_use() {
        // Realistic Claude Code transcript shape: assistant emits text
        // + tool_use; the next user line carries the matching
        // tool_result. Spec §4.2.2.6 expects the tool_output's
        // `parent_external_ids` to point at the tool_call (semantic
        // link, index 0) plus the conversational anchor (parentUuid).
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"please read a file"}}"#,
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:01:00.000Z","parentUuid":"u1","sessionId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"on it"},{"type":"tool_use","id":"toolu_01","name":"Read","input":{"file_path":"/x"}}]}}"#,
            r#"{"type":"user","uuid":"u2","timestamp":"2026-04-17T10:02:00.000Z","parentUuid":"a1","sessionId":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"ok","is_error":false}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        // 4 entries: user, assistant.text, assistant.tool_use,
        // user.tool_result
        assert_eq!(entries.len(), 4);
        let tool_call = entries
            .iter()
            .find(|e| e.kind_tag == "tool_call")
            .expect("tool_call");
        let tool_output = entries
            .iter()
            .find(|e| e.kind_tag == "tool_output")
            .expect("tool_output");
        // parent[0] is the tool_call (semantic link); parent[1] is
        // the conversational anchor (assistant.text block 0).
        let parents = &tool_output.parent_external_ids;
        assert!(
            parents.len() >= 2,
            "expected tool link + conversation anchor, got {parents:?}"
        );
        assert_eq!(parents[0], tool_call.external_id);
        // The second parent must be the assistant's block-0 anchor
        // (text), not the tool_call (no duplicates).
        let assistant_text = entries
            .iter()
            .find(|e| e.kind_tag == "assistant")
            .expect("assistant text");
        assert_eq!(parents[1], assistant_text.external_id);
    }

    #[test]
    fn tool_result_resolves_correct_tool_use_when_multiple_in_flight() {
        // Two parallel tool_use blocks, then two tool_result lines
        // (potentially in any order). Each tool_result must link to
        // the *matching* tool_use_id, not just block 0.
        let (_d, path) = write_jsonl(&[
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"assistant","content":[{"type":"tool_use","id":"toolu_a","name":"Read","input":{}},{"type":"tool_use","id":"toolu_b","name":"Bash","input":{}}]}}"#,
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:01:00.000Z","parentUuid":"a1","sessionId":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_b","content":"b done"}]}}"#,
            r#"{"type":"user","uuid":"u2","timestamp":"2026-04-17T10:02:00.000Z","parentUuid":"a1","sessionId":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_a","content":"a done"}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        let calls: Vec<_> = entries
            .iter()
            .filter(|e| e.kind_tag == "tool_call")
            .collect();
        let outputs: Vec<_> = entries
            .iter()
            .filter(|e| e.kind_tag == "tool_output")
            .collect();
        assert_eq!(calls.len(), 2);
        assert_eq!(outputs.len(), 2);
        // Find each output by its content body and verify the parent
        // is the corresponding tool_call by tool_use_id.
        let out_a = outputs
            .iter()
            .find(|e| e.content.contains("a done"))
            .expect("output for toolu_a");
        let out_b = outputs
            .iter()
            .find(|e| e.content.contains("b done"))
            .expect("output for toolu_b");
        let call_a = calls
            .iter()
            .find(|e| {
                e.canonical
                    .tool
                    .as_ref()
                    .and_then(|t| t.get("call_id"))
                    .and_then(|v| v.as_str())
                    == Some("toolu_a")
            })
            .expect("call for toolu_a");
        let call_b = calls
            .iter()
            .find(|e| {
                e.canonical
                    .tool
                    .as_ref()
                    .and_then(|t| t.get("call_id"))
                    .and_then(|v| v.as_str())
                    == Some("toolu_b")
            })
            .expect("call for toolu_b");
        assert_eq!(out_a.parent_external_ids[0], call_a.external_id);
        assert_eq!(out_b.parent_external_ids[0], call_b.external_id);
    }

    #[test]
    fn tool_result_with_inner_image_emits_two_entries() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u4","timestamp":"2026-04-17T10:04:00.000Z","sessionId":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"ok"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind_tag, "tool_output");
        assert_eq!(entries[1].kind_tag, "attachment");
        // The image attachment hangs off the tool_output entry.
        assert_eq!(
            entries[1].parent_external_ids,
            vec![entries[0].external_id.clone()]
        );
        // Sub-block external_id uses the b<outer>_sub<inner> token.
        assert!(entries[1].external_id.contains(":b0_sub1:"));
    }

    #[test]
    fn include_types_filter_drops_blocks() {
        let mut args = args_with_file(PathBuf::from("/tmp/dummy.jsonl"));
        args.include_types = "user,assistant".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"assistant","uuid":"a2","timestamp":"2026-04-17T10:05:00.000Z","sessionId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"hi"},{"type":"tool_use","id":"t1","name":"Read","input":{}}]}}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let (_session, entries, source_filtered_count) = unpack_outcome(outcome);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind_tag, "assistant");
        assert_eq!(source_filtered_count, 1);
    }

    #[test]
    fn attachment_subtype_default_admits_high_value_only() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"attachment","uuid":"x1","timestamp":"2026-04-17T10:10:00.000Z","sessionId":"s1","subtype":"task_reminder","content":"todo: fix bug"}"#,
            r#"{"type":"attachment","uuid":"x2","timestamp":"2026-04-17T10:11:00.000Z","sessionId":"s1","subtype":"mcp_instructions_delta","addedBlocks":[]}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let (_session, entries, source_filtered_count) = unpack_outcome(outcome);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind_tag, "attachment");
        assert!(entries[0].content.contains("todo: fix bug"));
        assert_eq!(source_filtered_count, 1);
    }

    #[test]
    fn attachment_subtype_all_admits_every_subtype() {
        let mut args = args_with_file(PathBuf::from("/tmp/x.jsonl"));
        args.attachment_subtypes = "all".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"attachment","uuid":"x1","timestamp":"2026-04-17T10:10:00.000Z","sessionId":"s1","subtype":"task_reminder","content":"todo"}"#,
            r#"{"type":"attachment","uuid":"x2","timestamp":"2026-04-17T10:11:00.000Z","sessionId":"s1","subtype":"hook_non_blocking_error","stdout":"ok","stderr":"err"}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 2);
        assert!(entries[1].content.contains("ok"));
        assert!(entries[1].content.contains("err"));
    }

    #[test]
    fn attachment_subtype_none_drops_all() {
        let mut args = args_with_file(PathBuf::from("/tmp/x.jsonl"));
        args.attachment_subtypes = "none".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"attachment","uuid":"x1","timestamp":"2026-04-17T10:10:00.000Z","sessionId":"s1","subtype":"task_reminder","content":"todo"}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let (_session, entries, source_filtered_count) = unpack_outcome(outcome);
        assert_eq!(entries.len(), 0);
        assert_eq!(source_filtered_count, 1);
    }

    #[test]
    fn attachment_diagnostics_extracts_message_lines() {
        let mut args = args_with_file(PathBuf::from("/tmp/x.jsonl"));
        args.attachment_subtypes = "all".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"attachment","uuid":"d1","timestamp":"2026-04-17T10:10:00.000Z","sessionId":"s1","subtype":"diagnostics","files":[{"path":"a.rs","diagnostics":[{"severity":"error","message":"bang"}]}]}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].content.contains("[error] a.rs: bang"));
    }

    #[test]
    fn external_id_is_deterministic_across_reads() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"hello"}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let a = entries_from_outcome(src.read_session(&path, None).unwrap())[0]
            .external_id
            .clone();
        let b = entries_from_outcome(src.read_session(&path, None).unwrap())[0]
            .external_id
            .clone();
        assert_eq!(a, b);
    }

    #[test]
    fn parent_anchor_resolves_via_anchor_map() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"hi"}}"#,
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:01:00.000Z","parentUuid":"u1","sessionId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"yo"}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 2);
        let user_eid = entries[0].external_id.clone();
        assert_eq!(entries[1].parent_external_ids, vec![user_eid]);
    }

    #[test]
    fn attachment_with_invalid_timestamp_is_filtered() {
        // Parity with the user/assistant/system path: a corrupted
        // `timestamp` must NOT silently fall back to `info.created_at`
        // because that would let `--since` filtering admit the broken
        // record and misplace it at the session start.
        let mut args = args_with_file(PathBuf::from("/tmp/x.jsonl"));
        args.attachment_subtypes = "all".to_string();
        let (_d, path) = write_jsonl(&[
            // baseline entry to anchor session timestamps
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"hi"}}"#,
            // attachment with non-RFC3339 timestamp
            r#"{"type":"attachment","uuid":"x1","timestamp":"not-a-date","sessionId":"s1","subtype":"task_reminder","content":"todo"}"#,
            // attachment with missing timestamp
            r#"{"type":"attachment","uuid":"x2","sessionId":"s1","subtype":"task_reminder","content":"todo"}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let (_session, entries, source_filtered_count) = unpack_outcome(outcome);
        // Only the user line survives; both attachments are dropped.
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind_tag, "user");
        assert_eq!(source_filtered_count, 2);
    }

    #[test]
    fn system_entry_without_content_is_filtered() {
        // Legacy `is_entry_importable` rejected system entries whose
        // `content` was missing; the canonical path must keep the same
        // behaviour rather than emit a synthetic "null" memory body.
        let (_d, path) = write_jsonl(&[
            // system without content -> filtered
            r#"{"type":"system","uuid":"s1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","subtype":"compact_boundary"}"#,
            // system with explicit JSON null content -> filtered (same noise reasoning)
            r#"{"type":"system","uuid":"s2","timestamp":"2026-04-17T10:01:00.000Z","sessionId":"s1","subtype":"compact_boundary","content":null}"#,
            // system with real content -> emitted
            r#"{"type":"system","uuid":"s3","timestamp":"2026-04-17T10:02:00.000Z","sessionId":"s1","subtype":"compact_boundary","content":"boundary"}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let (_session, entries, source_filtered_count) = unpack_outcome(outcome);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind_tag, "system");
        assert_eq!(entries[0].content, "boundary");
        assert_eq!(source_filtered_count, 2);
    }

    #[test]
    fn attachment_event_parent_resolves_via_anchor_map() {
        // The attachment event's parentUuid points at a regular user
        // entry; anchor_map must resolve it to the 5-segment block-0
        // external_id, not a 3-segment placeholder. Without the
        // anchor_map lookup, the parent_external_ids would never match
        // any imported memory.
        let mut args = args_with_file(PathBuf::from("/tmp/x.jsonl"));
        args.attachment_subtypes = "all".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"hi"}}"#,
            r#"{"type":"attachment","uuid":"x1","timestamp":"2026-04-17T10:01:00.000Z","parentUuid":"u1","sessionId":"s1","subtype":"task_reminder","content":"todo"}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 2);
        let user_eid = entries[0].external_id.clone();
        // The user's external_id must be the 5-segment canonical form.
        assert!(
            user_eid.starts_with("claude_code:s1:u1:b0:"),
            "user external_id format: {user_eid}"
        );
        // The attachment's parent_external_ids points at the same
        // 5-segment canonical id (NOT a 3-segment claude_code:s1:u1).
        assert_eq!(entries[1].parent_external_ids, vec![user_eid]);
    }

    #[test]
    fn tool_result_with_inner_image_has_distinct_import_orders() {
        // Regression: BlockIdx::Top(0) and BlockIdx::Sub(0, 0) used
        // to share order_offset()=0, leaving the canonical importer's
        // (import_order, timestamp_ms) sort unstable for a tool_result
        // that lives at content[0] and carries an inner image at
        // content[0].content[0]. We require strict monotonicity here.
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u4","timestamp":"2026-04-17T10:04:00.000Z","sessionId":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"ok"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind_tag, "tool_output");
        assert_eq!(entries[1].kind_tag, "attachment");
        assert_ne!(
            entries[0].import_order, entries[1].import_order,
            "Top(0) and Sub(0,0) must not share an import_order"
        );
        assert!(entries[0].import_order < entries[1].import_order);
    }

    // Regression: a tool_result that lives at content[1] (block_idx>=1)
    // with `--include-types` excluding `tool_output` must still parent
    // its inner image at the same message's block-0, not at the previous
    // turn (parent_anchor). conversation_anchor is the right value.
    #[test]
    fn tool_result_image_parents_block0_when_tool_output_filtered() {
        let mut args = args_with_file(PathBuf::from("/tmp/dummy.jsonl"));
        args.include_types = "user,assistant,attachment".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"please run"}}"#,
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:01:00.000Z","parentUuid":"u1","sessionId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"on it"},{"type":"tool_use","id":"t1","name":"Read","input":{}}]}}"#,
            r#"{"type":"user","uuid":"u2","timestamp":"2026-04-17T10:02:00.000Z","parentUuid":"a1","sessionId":"s1","message":{"role":"user","content":[{"type":"text","text":"see attachment"},{"type":"tool_result","tool_use_id":"t1","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        // Expected: user(u1), assistant.text(a1.b0), user.text(u2.b0),
        // attachment(u2.b1.sub0). The assistant.tool_use is filtered by
        // include_types not containing tool_call, and the tool_result is
        // filtered by include_types not containing tool_output.
        let attachment = entries
            .iter()
            .find(|e| e.kind_tag == "attachment")
            .expect("attachment entry");
        let user_block0 = entries
            .iter()
            .find(|e| e.kind_tag == "user" && e.external_id.contains(":u2:b0:"))
            .expect("user block 0 anchor for u2");
        assert_eq!(
            attachment.parent_external_ids,
            vec![user_block0.external_id.clone()],
            "attachment must hang off u2's block 0, not a previous turn"
        );
    }

    // When tool_result lives at block 0, the conversational anchor IS the
    // parent_anchor (parentUuid → previous turn). Ensure the image still
    // hangs off that anchor when tool_output is filtered out.
    #[test]
    fn tool_result_image_parents_conversation_anchor_at_block0() {
        let mut args = args_with_file(PathBuf::from("/tmp/dummy.jsonl"));
        args.include_types = "user,assistant,attachment".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Read","input":{}}]}}"#,
            r#"{"type":"user","uuid":"u2","timestamp":"2026-04-17T10:01:00.000Z","parentUuid":"a1","sessionId":"s1","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AAAA"}}]}]}}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        let attachment = entries
            .iter()
            .find(|e| e.kind_tag == "attachment")
            .expect("attachment entry");
        // a1's tool_use is filtered out (tool_call not in include_types),
        // but its block-0 anchor still seeds the anchor_map since
        // build_block0_anchor_map runs before the include_types filter.
        // u2's tool_result is at block 0, so conversation_anchor =
        // parent_anchor = a1's block-0 eid.
        let parents = &attachment.parent_external_ids;
        assert_eq!(parents.len(), 1, "expected single anchor, got {parents:?}");
        assert!(
            parents[0].contains(":a1:b0:"),
            "attachment must hang off a1's block-0 anchor (the conversational \
             parent), got {}",
            parents[0]
        );
    }

    // type=attachment events extract subtype-aware text bodies (e.g.
    // `task_reminder` content). The CanonicalEntry's content_type must
    // be Text, not Url, so the body is eligible for highlights and
    // auto-embedding. metadata.attachment.kind="ref" is retained for
    // forensic recovery.
    #[test]
    fn attachment_event_uses_text_content_type() {
        let mut args = args_with_file(PathBuf::from("/tmp/x.jsonl"));
        args.attachment_subtypes = "default".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"attachment","uuid":"x1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","subtype":"task_reminder","content":"todo: fix bug"}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(
            e.content_type,
            ContentType::Text,
            "attachment event with text body must surface as Text so it stays \
             highlightable and embeddable"
        );
        let attachment = e
            .canonical
            .attachment
            .as_ref()
            .expect("attachment metadata present");
        assert_eq!(
            attachment.get("kind").and_then(|v| v.as_str()),
            Some("ref"),
            "metadata.attachment.kind must remain 'ref' for forensic recovery"
        );
    }

    // BlockIdx::order_offset must encode Top and Sub in disjoint regions,
    // even for outer/inner indices that previously collided under the
    // legacy SUB_OFFSET_BASE=512 scheme.
    #[test]
    fn block_idx_order_offset_no_collision_top_vs_sub() {
        // Pre-fix collisions: Top(520) == Sub(0,8) == 520, Top(544) ==
        // Sub(1,0) == 544.
        assert_ne!(
            BlockIdx::Top(520).order_offset(),
            BlockIdx::Sub(0, 8).order_offset()
        );
        assert_ne!(
            BlockIdx::Top(544).order_offset(),
            BlockIdx::Sub(1, 0).order_offset()
        );
        // Spot-check additional pairs around the legacy boundary.
        assert_ne!(
            BlockIdx::Top(513).order_offset(),
            BlockIdx::Sub(0, 1).order_offset()
        );
    }

    // Sub blocks must sort immediately after their outer Top and strictly
    // before the next Top, for any outer index.
    #[test]
    fn block_idx_order_offset_preserves_top_then_sub_order() {
        for o in [0_usize, 1, 5, 100, 1000] {
            let top_o = BlockIdx::Top(o).order_offset();
            let sub_o_0 = BlockIdx::Sub(o, 0).order_offset();
            let top_next = BlockIdx::Top(o + 1).order_offset();
            assert!(
                top_o < sub_o_0,
                "Top({o}) must sort before Sub({o},0): {top_o} vs {sub_o_0}"
            );
            assert!(
                sub_o_0 < top_next,
                "Sub({o},0) must sort before Top({}): {sub_o_0} vs {top_next}",
                o + 1
            );
        }
    }

    // End-to-end: a content[] that exceeds the legacy 512 boundary and a
    // tool_result with >32 inner blocks must still produce strictly
    // monotonic import_order across every emitted entry. Pre-fix this
    // test would fail because Top(520) and Sub(0,8) collided.
    #[test]
    fn import_order_monotonic_with_large_content_array() {
        // Build an assistant message with 600 text blocks followed by a
        // tool_use (so the content array straddles the legacy 512 cap),
        // then a user line carrying a tool_result whose inner content
        // has 40 elements (>32, also tripping the legacy outer*32+inner
        // collision).
        let mut blocks: Vec<String> = Vec::with_capacity(601);
        for i in 0..600 {
            blocks.push(format!(r#"{{"type":"text","text":"t{i}"}}"#));
        }
        blocks.push(r#"{"type":"tool_use","id":"t1","name":"Read","input":{}}"#.to_string());
        let assistant_line = format!(
            r#"{{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{{"role":"assistant","content":[{}]}}}}"#,
            blocks.join(",")
        );

        let mut inner: Vec<String> = Vec::with_capacity(40);
        for i in 0..40 {
            inner.push(format!(
                r#"{{"type":"image","source":{{"type":"base64","media_type":"image/png","data":"a{i}"}}}}"#
            ));
        }
        let user_line = format!(
            r#"{{"type":"user","uuid":"u2","timestamp":"2026-04-17T10:01:00.000Z","parentUuid":"a1","sessionId":"s1","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"t1","content":[{}]}}]}}}}"#,
            inner.join(",")
        );

        let (_d, path) = write_jsonl(&[assistant_line.as_str(), user_line.as_str()]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        // Verify strict monotonicity of import_order across emitted entries.
        let mut prev: Option<i64> = None;
        for (i, e) in entries.iter().enumerate() {
            if let Some(p) = prev {
                assert!(
                    e.import_order > p,
                    "import_order regression at idx {i}: prev={p}, cur={}, kind={}",
                    e.import_order,
                    e.kind_tag
                );
            }
            prev = Some(e.import_order);
        }
    }

    // Forward-compat: a future attachment subtype that puts its body on
    // a top-level field (no entry-level `content`) must still surface
    // *something* searchable. The verbatim JSON line is the floor.
    #[test]
    fn attachment_unknown_subtype_falls_back_to_verbatim_line() {
        let mut args = args_with_file(PathBuf::from("/tmp/x.jsonl"));
        args.attachment_subtypes = "all".to_string();
        let (_d, path) = write_jsonl(&[
            // Hypothetical new subtype with body on a top-level field
            // and no `content` key at all.
            r#"{"type":"attachment","uuid":"x1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","subtype":"future_kind","payload":{"note":"new shape body"}}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.kind_tag, "attachment");
        assert!(
            !e.content.is_empty(),
            "unknown subtype must surface verbatim JSON, got empty content"
        );
        assert!(
            e.content.contains("new shape body"),
            "fallback content should include the top-level body, got: {}",
            e.content
        );
    }

    // Spec §4.2.2.6: Anthropic `thinking` block → kind=reasoning,
    // role=Assistant, content_type=Text.
    #[test]
    fn assistant_thinking_block_emits_reasoning_kind() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"hi"},{"type":"thinking","thinking":"step 1: read the file"}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 2);
        let reasoning = entries
            .iter()
            .find(|e| e.kind_tag == "reasoning")
            .expect("reasoning entry must exist");
        assert_eq!(reasoning.role, MessageRole::RoleAssistant);
        assert_eq!(reasoning.content_type, ContentType::Text);
        assert_eq!(reasoning.content, "step 1: read the file");
        // Block 1 chains back to block 0 (the assistant text), not the
        // previous turn.
        let assistant_text = entries
            .iter()
            .find(|e| e.kind_tag == "assistant")
            .expect("assistant text entry");
        assert_eq!(
            reasoning.parent_external_ids,
            vec![assistant_text.external_id.clone()]
        );
    }

    // include_types must gate reasoning emission, mirroring codex.
    #[test]
    fn assistant_thinking_block_filtered_when_reasoning_excluded() {
        let mut args = args_with_file(PathBuf::from("/tmp/dummy.jsonl"));
        args.include_types = "user,assistant".to_string();
        let (_d, path) = write_jsonl(&[
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"assistant","content":[{"type":"text","text":"hi"},{"type":"thinking","thinking":"private"}]}}"#,
        ]);
        args.session_file = Some(path.clone());
        let src = ClaudeCodeSource::new(args);
        let outcome = src.read_session(&path, None).unwrap();
        let (_session, entries, source_filtered_count) = unpack_outcome(outcome);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].kind_tag, "assistant");
        assert!(
            entries.iter().all(|e| e.kind_tag != "reasoning"),
            "thinking must not leak through when reasoning is excluded"
        );
        assert_eq!(source_filtered_count, 1);
    }

    // `redacted_thinking` carries an opaque encrypted blob in
    // `block.data`. We must NOT persist the blob in MemoryData.content
    // (zero search value, sensitive), but downstream operators need a
    // fingerprint to detect tampering / correlate.
    #[test]
    fn assistant_redacted_thinking_block_records_sha_and_size() {
        let (_d, path) = write_jsonl(&[
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"assistant","content":[{"type":"redacted_thinking","data":"AAAAENCRYPTEDBLOB=="}]}}"#,
        ]);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        let entries = entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        let e = &entries[0];
        assert_eq!(e.kind_tag, "reasoning");
        assert_eq!(e.role, MessageRole::RoleAssistant);
        assert_eq!(e.content_type, ContentType::Text);
        assert!(
            e.content.is_empty(),
            "redacted_thinking must not leak ciphertext into content, got: {:?}",
            e.content
        );
        let sha = e
            .metadata
            .get("encrypted_content_sha256")
            .and_then(|v| v.as_str())
            .expect("encrypted_content_sha256 must be set");
        assert_eq!(sha.len(), 64, "sha256 hex must be 64 chars, got {sha}");
        let size = e
            .metadata
            .get("encrypted_content_size")
            .and_then(|v| v.as_u64())
            .expect("encrypted_content_size must be set");
        assert_eq!(size, "AAAAENCRYPTEDBLOB==".len() as u64);
        assert_eq!(
            e.metadata.get("block_type").and_then(|v| v.as_str()),
            Some("redacted_thinking")
        );
    }

    #[test]
    fn attachment_subtypes_policy_parses() {
        assert!(matches!(
            AttachmentSubtypePolicy::from_cli("default").unwrap(),
            AttachmentSubtypePolicy::Default
        ));
        assert!(matches!(
            AttachmentSubtypePolicy::from_cli("all").unwrap(),
            AttachmentSubtypePolicy::All
        ));
        assert!(matches!(
            AttachmentSubtypePolicy::from_cli("none").unwrap(),
            AttachmentSubtypePolicy::None
        ));
        let wl = AttachmentSubtypePolicy::from_cli("task_reminder, diagnostics").unwrap();
        assert!(wl.includes("task_reminder"));
        assert!(wl.includes("DIAGNOSTICS"));
        assert!(!wl.includes("hook_non_blocking_error"));
        assert!(AttachmentSubtypePolicy::from_cli(",,,").is_err());
    }

    use crate::source::test_support::set_file_mtime_ms;

    /// Minimal valid claude_code transcript with two messages, useful
    /// for mtime tests that only care about Import vs. Skipped (the
    /// canonical content is exercised elsewhere).
    fn write_simple_transcript() -> (tempfile::TempDir, PathBuf) {
        write_jsonl(&[
            r#"{"type":"user","uuid":"u1","sessionId":"s","timestamp":"2026-05-08T10:00:00Z","message":{"role":"user","content":"hello"}}"#,
            r#"{"type":"assistant","uuid":"a1","parentUuid":"u1","sessionId":"s","timestamp":"2026-05-08T10:00:01Z","message":{"role":"assistant","content":"hi"}}"#,
        ])
    }

    #[test]
    fn mtime_filter_skips_old_session_when_since_set() {
        let (_dir, path) = write_simple_transcript();
        // mtime well before since-margin → must skip without parsing.
        set_file_mtime_ms(&path, 1_000_000);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, Some(2_000_000)).unwrap();
        match outcome {
            ReadSessionOutcome::Skipped { reason, .. } => {
                assert!(
                    reason.starts_with("unchanged since"),
                    "unexpected reason: {reason}"
                );
            }
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. } => {
                panic!("expected Skipped, got Import");
            }
        }
    }

    #[test]
    fn mtime_filter_parses_recent_session() {
        let (_dir, path) = write_simple_transcript();
        // mtime well after the threshold → must parse.
        set_file_mtime_ms(&path, 9_000_000);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, Some(2_000_000)).unwrap();
        assert!(matches!(
            outcome,
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
        ));
    }

    #[test]
    fn mtime_filter_disabled_when_since_unset() {
        let (_dir, path) = write_simple_transcript();
        // Even an ancient mtime must not cause a skip when since is None.
        set_file_mtime_ms(&path, 1_000);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, None).unwrap();
        assert!(matches!(
            outcome,
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
        ));
    }

    #[test]
    fn mtime_filter_parses_when_threshold_equals_mtime() {
        // The filter is "strictly older than threshold"; equality
        // (mtime == threshold) must fall through to a full parse so
        // a file written exactly at the boundary is never lost.
        let (_dir, path) = write_simple_transcript();
        set_file_mtime_ms(&path, 5_000_000);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        let outcome = src.read_session(&path, Some(5_000_000)).unwrap();
        assert!(matches!(
            outcome,
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
        ));
    }

    // --- metadata.claude_code.* transcription -------------------------

    /// Parse the given JSONL lines through the source and return the
    /// canonical entries. Collapses the write/read/unpack boilerplate
    /// shared by the transcription tests below.
    fn read_entries(lines: &[&str]) -> Vec<CanonicalEntry> {
        let (_d, path) = write_jsonl(lines);
        let src = ClaudeCodeSource::new(args_with_file(path.clone()));
        entries_from_outcome(src.read_session(&path, None).unwrap())
    }

    /// Read the nested `metadata.claude_code` object, asserting it
    /// exists. Helper for the transcription tests below.
    fn claude_code_meta(e: &CanonicalEntry) -> &serde_json::Map<String, serde_json::Value> {
        e.metadata
            .get("claude_code")
            .and_then(|v| v.as_object())
            .expect("entry must carry metadata.claude_code")
    }

    #[test]
    fn snake_case_key_pins_acronym_and_versions() {
        assert_eq!(snake_case_key("version"), "claude_version");
        assert_eq!(snake_case_key("toolUseID"), "tool_use_id");
        assert_eq!(snake_case_key("parentToolUseID"), "parent_tool_use_id");
        assert_eq!(
            snake_case_key("sourceToolAssistantUUID"),
            "source_tool_assistant_uuid"
        );
        assert_eq!(snake_case_key("promptId"), "prompt_id");
        // Generic fallback.
        assert_eq!(snake_case_key("userType"), "user_type");
        assert_eq!(snake_case_key("isMeta"), "is_meta");
        assert_eq!(snake_case_key("entrypoint"), "entrypoint");
        assert_eq!(snake_case_key("slug"), "slug");
    }

    #[test]
    fn is_meta_true_user_text_retains_claude_code_metadata() {
        // A slash-command expansion prompt: isMeta + external. The body
        // must still emit, and the flags must survive in metadata so a
        // display consumer can fold it without reading the body.
        let entries = read_entries(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","isMeta":true,"userType":"external","message":{"role":"user","content":"expanded prompt"}}"#,
        ]);
        assert_eq!(entries.len(), 1);
        let cc = claude_code_meta(&entries[0]);
        assert_eq!(cc.get("is_meta").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            cc.get("user_type").and_then(|v| v.as_str()),
            Some("external")
        );
    }

    #[test]
    fn normal_user_message_not_misclassified() {
        // A plain user message carries no isMeta/userType — those flags
        // must be absent so a consumer never folds real user input.
        let entries = read_entries(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","message":{"role":"user","content":"hello"}}"#,
        ]);
        assert_eq!(entries[0].kind_tag, "user");
        assert_eq!(entries[0].content, "hello");
        let cc = entries[0]
            .metadata
            .get("claude_code")
            .and_then(|v| v.as_object());
        if let Some(cc) = cc {
            assert!(!cc.contains_key("is_meta"));
            assert!(!cc.contains_key("user_type"));
        }
    }

    #[test]
    fn required_scalar_fields_all_transcribed() {
        let entries = read_entries(&[
            r#"{"type":"assistant","uuid":"a1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","userType":"external","isMeta":false,"entrypoint":"cli","version":"2.1.85","slug":"some-slug","promptId":"p1","toolUseID":"tu1","sourceToolAssistantUUID":"sa1","message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}"#,
        ]);
        let cc = claude_code_meta(&entries[0]);
        assert_eq!(
            cc.get("user_type").and_then(|v| v.as_str()),
            Some("external")
        );
        assert_eq!(cc.get("is_meta").and_then(|v| v.as_bool()), Some(false));
        assert_eq!(cc.get("entrypoint").and_then(|v| v.as_str()), Some("cli"));
        // `version` is pinned to the stable contract name.
        assert_eq!(
            cc.get("claude_version").and_then(|v| v.as_str()),
            Some("2.1.85")
        );
        assert_eq!(cc.get("slug").and_then(|v| v.as_str()), Some("some-slug"));
        assert_eq!(cc.get("prompt_id").and_then(|v| v.as_str()), Some("p1"));
        assert_eq!(cc.get("tool_use_id").and_then(|v| v.as_str()), Some("tu1"));
        assert_eq!(
            cc.get("source_tool_assistant_uuid")
                .and_then(|v| v.as_str()),
            Some("sa1")
        );
    }

    #[test]
    fn body_fields_excluded_from_claude_code_metadata() {
        // Body and large-payload keys must never be copied into the
        // attribute bag — they already live in content / canonical.*.
        let entries = read_entries(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","userType":"external","toolUseResult":{"big":"payload"},"data":{"x":1},"snapshot":{"y":2},"todos":[1,2,3],"planContent":"a plan","message":{"role":"user","content":"hi"}}"#,
        ]);
        let cc = claude_code_meta(&entries[0]);
        for k in [
            "message",
            "content",
            "tool_use_result",
            "toolUseResult",
            "data",
            "snapshot",
            "todos",
            "plan_content",
            "planContent",
        ] {
            assert!(!cc.contains_key(k), "claude_code must not contain {k}");
        }
        // A benign sibling scalar still made it through.
        assert_eq!(
            cc.get("user_type").and_then(|v| v.as_str()),
            Some("external")
        );
    }

    #[test]
    fn oversized_scalar_value_skipped() {
        let big = "x".repeat(CLAUDE_CODE_META_MAX_BYTES + 1);
        let line = format!(
            r#"{{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","userType":"external","customField":"{big}","message":{{"role":"user","content":"hi"}}}}"#
        );
        let entries = read_entries(&[line.as_str()]);
        let cc = claude_code_meta(&entries[0]);
        // The oversized value is dropped, but the small sibling stays —
        // skipping is per-value, not per-entry.
        assert!(!cc.contains_key("custom_field"));
        assert_eq!(
            cc.get("user_type").and_then(|v| v.as_str()),
            Some("external")
        );
    }

    #[test]
    fn attachment_and_system_events_keep_claude_code_metadata() {
        // Confirms the single chokepoint covers non-text emit paths:
        // a `task_reminder` attachment and a system entry both carry an
        // extra top-level scalar through to metadata.claude_code.*.
        let entries = read_entries(&[
            r#"{"type":"attachment","uuid":"d1","timestamp":"2026-04-17T10:10:00.000Z","sessionId":"s1","subtype":"task_reminder","entrypoint":"cli","content":"remember to test"}"#,
            r#"{"type":"system","uuid":"y1","timestamp":"2026-04-17T10:11:00.000Z","sessionId":"s1","entrypoint":"cli","content":"system note"}"#,
        ]);
        assert!(entries.len() >= 2);
        for e in &entries {
            let cc = claude_code_meta(e);
            assert_eq!(cc.get("entrypoint").and_then(|v| v.as_str()), Some("cli"));
        }
    }

    #[test]
    fn attachment_subtype_not_duplicated_under_claude_code() {
        // The attachment path writes `metadata.subtype` directly; the
        // raw `subtype` top-level key must therefore stay out of
        // claude_code.* so the display layer sees it exactly once.
        let entries = read_entries(&[
            r#"{"type":"attachment","uuid":"d1","timestamp":"2026-04-17T10:10:00.000Z","sessionId":"s1","subtype":"task_reminder","entrypoint":"cli","content":"todo"}"#,
        ]);
        let e = &entries[0];
        assert_eq!(
            e.metadata.get("subtype").and_then(|v| v.as_str()),
            Some("task_reminder")
        );
        assert!(!claude_code_meta(e).contains_key("subtype"));
    }

    #[test]
    fn no_double_write_of_promoted_keys() {
        let entries = read_entries(&[
            r#"{"type":"user","uuid":"u1","parentUuid":"p0","requestId":"r1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","userType":"external","message":{"role":"user","content":"hi"}}"#,
        ]);
        let e = &entries[0];
        // Promoted keys stay top-level.
        assert_eq!(e.metadata.get("uuid").and_then(|v| v.as_str()), Some("u1"));
        assert_eq!(
            e.metadata.get("parent_uuid").and_then(|v| v.as_str()),
            Some("p0")
        );
        assert_eq!(
            e.metadata.get("request_id").and_then(|v| v.as_str()),
            Some("r1")
        );
        // …and are not duplicated under claude_code.*.
        let cc = claude_code_meta(e);
        assert!(!cc.contains_key("uuid"));
        assert!(!cc.contains_key("parent_uuid"));
        assert!(!cc.contains_key("request_id"));
        assert!(!cc.contains_key("type"));
    }

    #[test]
    fn null_top_level_field_skipped() {
        let entries = read_entries(&[
            r#"{"type":"user","uuid":"u1","timestamp":"2026-04-17T10:00:00.000Z","sessionId":"s1","userType":"external","customNull":null,"message":{"role":"user","content":"hi"}}"#,
        ]);
        let cc = claude_code_meta(&entries[0]);
        assert!(!cc.contains_key("custom_null"));
        assert_eq!(
            cc.get("user_type").and_then(|v| v.as_str()),
            Some("external")
        );
    }
}
