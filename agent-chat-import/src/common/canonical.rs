//! Canonical schema helpers for tool / attachment / raw layers
//! (spec §4.2.2). Source parsers call these helpers and assign the
//! returned values into `CanonicalEntry.canonical.{tool, attachment, raw}`.
//! `run_import::build_memory_data` then merges those into the
//! `MemoryData.metadata` top-level reserved keys (`tool` / `attachment`
//! / `raw`). Direct mutation of these keys via `entry.metadata` is
//! contractually forbidden — the run_import drops any reserved key it
//! finds there (§4.2.2.2 contract (a)).

#![allow(dead_code)]

use crate::common::ids::sha256_hex_prefix;
use base64::Engine;
use protobuf::llm_memory::data::ContentType;
use serde_json::{Map, Value, json};
use std::sync::OnceLock;

// ---------------------------------------------------------------------
// Size config (env-driven, read once)
// ---------------------------------------------------------------------

/// All four byte budgets that gate tool / attachment normalization.
/// Source parsers never read this directly — they call the build_*
/// helpers, which apply the correct guard for their kind.
#[derive(Debug, Clone, Copy)]
pub struct SizeConfig {
    /// `MEMORY_TOOL_ARG_PREVIEW_BYTES` — max bytes of the JSON-serialized
    /// arguments shown in the tool_call `MemoryData.content` summary.
    pub tool_arg_preview: usize,
    /// `MEMORY_TOOL_OUTPUT_PREVIEW_BYTES` — max bytes of the
    /// `MemoryData.content` shown for tool_output.
    pub tool_output_preview: usize,
    /// `MEMORY_TOOL_OUTPUT_FULL_BYTES` — max bytes of
    /// `metadata.tool.output`. Anything over is replaced by a
    /// truncated copy + sha256/full_bytes for verification.
    pub tool_output_full: usize,
    /// `MEMORY_ATTACHMENT_INLINE_MAX_BYTES` — max raw byte size of
    /// `metadata.attachment.data` (after base64 decode). Larger
    /// payloads switch to `storage="elided"` with sha256.
    pub attachment_inline_max: usize,
}

impl SizeConfig {
    pub const DEFAULT: SizeConfig = SizeConfig {
        tool_arg_preview: 512,
        tool_output_preview: 4096,
        tool_output_full: 65536,
        attachment_inline_max: 1_048_576,
    };
}

static SIZE_CONFIG: OnceLock<SizeConfig> = OnceLock::new();

/// Process-wide size config. Read once at first call from env vars
/// listed in `SizeConfig`. Tests should use the `_with_config`
/// variants below to exercise boundary cases without depending on
/// process env state.
pub fn size_config() -> &'static SizeConfig {
    SIZE_CONFIG.get_or_init(|| SizeConfig {
        tool_arg_preview: env_usize(
            "MEMORY_TOOL_ARG_PREVIEW_BYTES",
            SizeConfig::DEFAULT.tool_arg_preview,
        ),
        tool_output_preview: env_usize(
            "MEMORY_TOOL_OUTPUT_PREVIEW_BYTES",
            SizeConfig::DEFAULT.tool_output_preview,
        ),
        tool_output_full: env_usize(
            "MEMORY_TOOL_OUTPUT_FULL_BYTES",
            SizeConfig::DEFAULT.tool_output_full,
        ),
        attachment_inline_max: env_usize(
            "MEMORY_ATTACHMENT_INLINE_MAX_BYTES",
            SizeConfig::DEFAULT.attachment_inline_max,
        ),
    })
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------
// UTF-8 safe truncation (§4.2.2.3.2)
// ---------------------------------------------------------------------

/// Return the longest prefix of `s` whose byte length is <= `max_bytes`,
/// stopping at a UTF-8 char boundary so the result is a valid `&str`.
/// `&s[..n]` is unsafe to use for arbitrary `n` because cutting through
/// a multibyte sequence triggers a panic; this helper is the only
/// sanctioned way for canonical helpers to truncate text.
pub fn safe_truncate_at_char_boundary(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    // Walk back from `max_bytes` until a char boundary is found.
    // `is_char_boundary(0)` is always true so the loop terminates.
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

// ---------------------------------------------------------------------
// Tool name → category mapping (§4.2.2.3.0)
// ---------------------------------------------------------------------

/// Categorize a `(source_id, tool_name)` pair into a stable cross-source
/// bucket so consumers can filter "all shell commands" without caring
/// which agent emitted them. Returns `None` for any pair not in the
/// initial table — adding new entries is backward-compatible because
/// callers must already handle `null`.
pub fn tool_category(source_id: &str, name: &str) -> Option<&'static str> {
    match (source_id, name) {
        ("claude_code", "Bash") | ("codex", "shell") | ("codex", "exec_command") => {
            Some("shell_exec")
        }
        ("claude_code", "Read") => Some("file_read"),
        ("claude_code", "Write") | ("claude_code", "Edit") | ("codex", "apply_patch") => {
            Some("file_write")
        }
        ("claude_code", "Grep") | ("claude_code", "Glob") => Some("file_search"),
        ("claude_code", "WebSearch") => Some("web_search"),
        ("claude_code", "WebFetch") => Some("web_fetch"),
        _ => None,
    }
}

// ---------------------------------------------------------------------
// Tool helpers (§4.2.2.3)
// ---------------------------------------------------------------------

/// Tool execution status (§4.2.2.3 / §4.2.2.6.2). Tri-state: `None`
/// means "no signal" — we explicitly do not treat a missing field as
/// `Error`, since CLI version differences can drop fields silently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolStatus {
    Ok,
    Error,
}

impl ToolStatus {
    fn as_str(self) -> &'static str {
        match self {
            ToolStatus::Ok => "ok",
            ToolStatus::Error => "error",
        }
    }

    /// Map a success-flavored bool (`true` = success) to the
    /// canonical status. Centralized so callers don't independently
    /// invert `is_error`-style flags and risk drift.
    pub fn from_success(success: bool) -> Self {
        if success {
            ToolStatus::Ok
        } else {
            ToolStatus::Error
        }
    }
}

/// Read the `call_id` field from a `metadata.tool` JSON object built
/// by `build_tool_call` / `build_tool_output`. Centralizes the key
/// name so callers don't directly walk the canonical layer.
pub fn tool_call_id(tool: &Value) -> Option<&str> {
    tool.get("call_id").and_then(|v| v.as_str())
}

/// Read the `name` field from a `metadata.tool` JSON object. Returns
/// `None` when the field is missing or null (e.g. the matching call
/// hasn't been seen yet).
pub fn tool_name(tool: &Value) -> Option<&str> {
    tool.get("name").and_then(|v| v.as_str())
}

/// Set `metadata.tool.name` on a previously-built tool object.
/// Used by 2nd-pass rewire when the call's name only became visible
/// after the output entry was emitted.
pub fn set_tool_name(tool: &mut Value, name: &str) {
    if let Some(obj) = tool.as_object_mut() {
        obj.insert("name".to_string(), Value::String(name.to_string()));
    }
}

/// Backfill both `metadata.tool.name` and `metadata.tool.category` on
/// a previously-built tool object. Used by 2nd-pass rewire: at the
/// time `build_tool_output` ran the matching call wasn't visible yet,
/// so `category` was frozen as `null`. Once the name is known we must
/// re-derive category through `tool_category(source_id, name)` —
/// otherwise consumers filtering by `category="shell_exec"` etc. silently
/// miss reordered rollouts.
pub fn backfill_tool_name_and_category(tool: &mut Value, source_id: &str, name: &str) {
    if let Some(obj) = tool.as_object_mut() {
        obj.insert("name".to_string(), Value::String(name.to_string()));
        let cat = match tool_category(source_id, name) {
            Some(c) => Value::String(c.to_string()),
            None => Value::Null,
        };
        obj.insert("category".to_string(), cat);
    }
}

/// Return value of `build_tool_call` / `build_tool_output`. Source
/// parsers assign `tool` to `entry.canonical.tool` and use `content`
/// as `MemoryData.content`.
#[derive(Debug, Clone)]
pub struct BuildToolResult {
    pub tool: Value,
    pub content: String,
}

/// Build a `metadata.tool` object for a tool_call entry plus the
/// 1-line summary used as `MemoryData.content`. `arguments` is
/// stringified via `serde_json::to_string` and truncated to
/// `MEMORY_TOOL_ARG_PREVIEW_BYTES` for the summary, while the full
/// JSON value is preserved in `metadata.tool.arguments`.
pub fn build_tool_call(
    source_id: &str,
    name: Option<&str>,
    call_id: Option<&str>,
    arguments: Option<&Value>,
    source_event: Option<&str>,
) -> BuildToolResult {
    build_tool_call_with_config(
        source_id,
        name,
        call_id,
        arguments,
        source_event,
        size_config(),
    )
}

pub(crate) fn build_tool_call_with_config(
    source_id: &str,
    name: Option<&str>,
    call_id: Option<&str>,
    arguments: Option<&Value>,
    source_event: Option<&str>,
    cfg: &SizeConfig,
) -> BuildToolResult {
    let category = name.and_then(|n| tool_category(source_id, n));
    let args_text = arguments
        .map(|a| serde_json::to_string(a).unwrap_or_default())
        .unwrap_or_default();
    let preview = safe_truncate_at_char_boundary(&args_text, cfg.tool_arg_preview);
    let truncated = preview.len() < args_text.len();
    let display_name = name.unwrap_or("(unknown)");
    let content = if truncated {
        format!("{display_name}({preview} …)")
    } else {
        format!("{display_name}({preview})")
    };

    let tool = json!({
        "kind": "call",
        "name": name,
        "source": source_id,
        "category": category,
        "call_id": call_id,
        "arguments": arguments,
        "output": Value::Null,
        "output_truncated": Value::Null,
        "output_full_bytes": Value::Null,
        "output_full_sha256": Value::Null,
        "status": Value::Null,
        "source_event": source_event,
    });

    BuildToolResult { tool, content }
}

/// Build a `metadata.tool` object for a tool_output entry plus the
/// preview text used as `MemoryData.content`. The full output is
/// truncated to `MEMORY_TOOL_OUTPUT_FULL_BYTES` (with sha256 + full
/// byte count when over), and the content preview shows the first
/// `MEMORY_TOOL_OUTPUT_PREVIEW_BYTES` of that stored value.
pub fn build_tool_output(
    source_id: &str,
    name: Option<&str>,
    call_id: Option<&str>,
    output: Option<&str>,
    status: Option<ToolStatus>,
    source_event: Option<&str>,
) -> BuildToolResult {
    build_tool_output_with_config(
        source_id,
        name,
        call_id,
        output,
        status,
        source_event,
        size_config(),
    )
}

pub(crate) fn build_tool_output_with_config(
    source_id: &str,
    name: Option<&str>,
    call_id: Option<&str>,
    output: Option<&str>,
    status: Option<ToolStatus>,
    source_event: Option<&str>,
    cfg: &SizeConfig,
) -> BuildToolResult {
    let category = name.and_then(|n| tool_category(source_id, n));

    // metadata.tool.output: full-budget guard
    let (
        stored_output,
        output_truncated,
        output_full_bytes,
        output_full_sha256,
        full_bytes_for_preview,
    ) = match output {
        None => (Value::Null, Value::Null, Value::Null, Value::Null, 0usize),
        Some(s) => {
            let full_len = s.len();
            if full_len <= cfg.tool_output_full {
                (
                    Value::String(s.to_string()),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                    full_len,
                )
            } else {
                let truncated = safe_truncate_at_char_boundary(s, cfg.tool_output_full);
                let sha = sha256_hex_prefix(s.as_bytes(), 64);
                let stored = format!("{truncated}\n…(truncated, total {full_len} bytes)");
                (
                    Value::String(stored),
                    Value::Bool(true),
                    Value::Number(serde_json::Number::from(full_len as u64)),
                    Value::String(sha),
                    full_len,
                )
            }
        }
    };

    // MemoryData.content: preview-budget guard on the stored output
    let content = match stored_output.as_str() {
        None => String::new(),
        Some(s) => {
            let preview = safe_truncate_at_char_boundary(s, cfg.tool_output_preview);
            if preview.len() < s.len() {
                format!("{preview}\n…(truncated, total {full_bytes_for_preview} bytes)")
            } else {
                preview.to_string()
            }
        }
    };

    let tool = json!({
        "kind": "output",
        "name": name,
        "source": source_id,
        "category": category,
        "call_id": call_id,
        "arguments": Value::Null,
        "output": stored_output,
        "output_truncated": output_truncated,
        "output_full_bytes": output_full_bytes,
        "output_full_sha256": output_full_sha256,
        "status": status.map(|s| Value::String(s.as_str().to_string())).unwrap_or(Value::Null),
        "source_event": source_event,
    });

    BuildToolResult { tool, content }
}

// ---------------------------------------------------------------------
// Attachment helpers (§4.2.2.4)
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachmentKind {
    Image,
    Audio,
    Video,
    Url,
    Ref,
}

impl AttachmentKind {
    fn as_str(self) -> &'static str {
        match self {
            AttachmentKind::Image => "image",
            AttachmentKind::Audio => "audio",
            AttachmentKind::Video => "video",
            AttachmentKind::Url => "url",
            AttachmentKind::Ref => "ref",
        }
    }

    /// Parse the `attachment.kind` discriminant written by
    /// `build_attachment`. Unknown / missing → `None`.
    fn from_attachment_str(s: &str) -> Option<Self> {
        match s {
            "image" => Some(AttachmentKind::Image),
            "audio" => Some(AttachmentKind::Audio),
            "video" => Some(AttachmentKind::Video),
            "url" => Some(AttachmentKind::Url),
            "ref" => Some(AttachmentKind::Ref),
            _ => None,
        }
    }

    /// kind→ContentType mapping for the *memory's* `content_type`:
    /// a url/ref attachment is a URL memory.
    fn content_type(self) -> ContentType {
        match self {
            AttachmentKind::Image => ContentType::Image,
            AttachmentKind::Audio => ContentType::Audio,
            AttachmentKind::Video => ContentType::Video,
            AttachmentKind::Url | AttachmentKind::Ref => ContentType::Url,
        }
    }

    /// kind→ContentType for the *media_object's* `kind` column. Distinct
    /// from `content_type`: audio/video must be preserved (so the
    /// embedding pipeline does not treat them as images and the original
    /// type is not lost), while url/ref attachments are synthetic image
    /// references registered as IMAGE (the offline migration's
    /// convention).
    pub fn media_object_kind(self) -> ContentType {
        match self {
            AttachmentKind::Audio => ContentType::Audio,
            AttachmentKind::Video => ContentType::Video,
            AttachmentKind::Image | AttachmentKind::Url | AttachmentKind::Ref => ContentType::Image,
        }
    }
}

#[derive(Debug, Clone)]
pub enum AttachmentStorage {
    InlineBase64,
    Url,
    Ref,
    /// Set automatically by `build_attachment` when inline base64
    /// data exceeds `MEMORY_ATTACHMENT_INLINE_MAX_BYTES`.
    Elided,
    /// Dispatch failed (unrecognized URL scheme, non-base64 data: URL,
    /// base64 decode failure). The entry is still emitted (§4.2.2.6.1)
    /// so the conversation flow stays intact; `invalid_reason` carries
    /// the classification. Open string for forward-compat with future
    /// reasons.
    Invalid {
        reason: String,
    },
}

impl AttachmentStorage {
    fn as_str(&self) -> &str {
        match self {
            AttachmentStorage::InlineBase64 => "inline_base64",
            AttachmentStorage::Url => "url",
            AttachmentStorage::Ref => "ref",
            AttachmentStorage::Elided => "elided",
            AttachmentStorage::Invalid { .. } => "invalid",
        }
    }
}

#[derive(Debug, Clone)]
pub struct BuildAttachmentResult {
    pub attachment: Value,
    pub content: String,
    pub content_type: ContentType,
}

/// Build the `metadata.attachment` object plus the placeholder content
/// string and the `MemoryData.content_type`. Inline base64 payloads
/// over the size guard are automatically switched to `storage="elided"`
/// with sha256 / size_bytes; the original `data` field is dropped to
/// keep DB rows bounded.
#[allow(clippy::too_many_arguments)]
pub fn build_attachment(
    kind: AttachmentKind,
    storage: AttachmentStorage,
    media_type: Option<&str>,
    data_b64: Option<&str>,
    url: Option<&str>,
    width: Option<u32>,
    height: Option<u32>,
    alt: Option<&str>,
) -> BuildAttachmentResult {
    build_attachment_with_config(
        kind,
        storage,
        media_type,
        data_b64,
        url,
        width,
        height,
        alt,
        size_config(),
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_attachment_with_config(
    kind: AttachmentKind,
    storage: AttachmentStorage,
    media_type: Option<&str>,
    data_b64: Option<&str>,
    url: Option<&str>,
    width: Option<u32>,
    height: Option<u32>,
    alt: Option<&str>,
    cfg: &SizeConfig,
) -> BuildAttachmentResult {
    let content_type = kind.content_type();

    // Apply size guard for inline_base64.
    let (final_storage, final_data, data_truncated, data_sha256, computed_size_bytes): (
        AttachmentStorage,
        Option<String>,
        Option<bool>,
        Option<String>,
        Option<u64>,
    ) = match (&storage, data_b64) {
        (AttachmentStorage::InlineBase64, Some(b64)) => {
            // Decode to measure raw bytes; on decode failure switch to invalid.
            match base64::engine::general_purpose::STANDARD.decode(b64) {
                Ok(raw) => {
                    let raw_len = raw.len();
                    if raw_len > cfg.attachment_inline_max {
                        let sha = sha256_hex_prefix(&raw, 64);
                        (
                            AttachmentStorage::Elided,
                            None,
                            Some(true),
                            Some(sha),
                            Some(raw_len as u64),
                        )
                    } else {
                        (
                            AttachmentStorage::InlineBase64,
                            Some(b64.to_string()),
                            None,
                            None,
                            Some(raw_len as u64),
                        )
                    }
                }
                Err(_) => (
                    AttachmentStorage::Invalid {
                        reason: "base64_decode_failed".to_string(),
                    },
                    None,
                    None,
                    None,
                    None,
                ),
            }
        }
        // Pass-through for non-inline storages.
        _ => (storage, None, None, None, None),
    };

    let invalid_reason = if let AttachmentStorage::Invalid { ref reason } = final_storage {
        Some(reason.clone())
    } else {
        None
    };

    // For Url / Ref, the url field carries the source. data is null.
    // For Invalid, both data and url are dropped (we keep media_type
    // and alt for caller-provided hints, plus the raw payload via
    // raw_entry).
    let final_url = match &final_storage {
        AttachmentStorage::Url | AttachmentStorage::Ref => url.map(|s| s.to_string()),
        _ => None,
    };

    let attachment = json!({
        "kind": kind.as_str(),
        "storage": final_storage.as_str(),
        "invalid_reason": invalid_reason,
        "media_type": media_type,
        "data": final_data,
        "url": final_url,
        "size_bytes": computed_size_bytes,
        "width": width,
        "height": height,
        "data_truncated": data_truncated,
        "data_sha256": data_sha256,
        "alt": alt,
    });

    let content = format_attachment_content(
        kind,
        &final_storage,
        media_type,
        computed_size_bytes,
        final_url.as_deref(),
        invalid_reason.as_deref(),
        data_sha256.as_deref(),
        width,
        height,
    );

    BuildAttachmentResult {
        attachment,
        content,
        content_type,
    }
}

#[allow(clippy::too_many_arguments)]
fn format_attachment_content(
    kind: AttachmentKind,
    storage: &AttachmentStorage,
    media_type: Option<&str>,
    size_bytes: Option<u64>,
    url: Option<&str>,
    invalid_reason: Option<&str>,
    data_sha256: Option<&str>,
    width: Option<u32>,
    height: Option<u32>,
) -> String {
    let kind_str = kind.as_str();
    match storage {
        AttachmentStorage::InlineBase64 => {
            let mt = media_type.unwrap_or("");
            let dim = match (width, height) {
                (Some(w), Some(h)) => format!(" {w}x{h}"),
                _ => String::new(),
            };
            let size = size_bytes
                .map(|b| format!(" ({b} bytes)"))
                .unwrap_or_default();
            format!("[{kind_str} {mt}{dim}{size}]").replace("  ", " ")
        }
        AttachmentStorage::Url => {
            let mt = media_type.unwrap_or("");
            let url_str = url.unwrap_or("");
            format!("[{kind_str} {mt} url={url_str}]")
        }
        AttachmentStorage::Ref => {
            let url_str = url.unwrap_or("");
            format!("[{kind_str} {url_str}]")
        }
        AttachmentStorage::Elided => {
            let mt = media_type.unwrap_or("");
            let bytes = size_bytes
                .map(|b| format!("{b} bytes"))
                .unwrap_or_else(|| "unknown bytes".to_string());
            let sha_hint = data_sha256
                .map(|s| {
                    let head = safe_truncate_at_char_boundary(s, 8);
                    format!(", sha256={head}…")
                })
                .unwrap_or_default();
            format!("[{kind_str} {mt} {bytes} (data elided{sha_hint})]")
        }
        AttachmentStorage::Invalid { .. } => {
            let reason = invalid_reason.unwrap_or("unknown");
            format!("[{kind_str} (invalid: {reason})]")
        }
    }
}

// ---------------------------------------------------------------------
// image_url scheme dispatch (§4.2.2.6.1)
// ---------------------------------------------------------------------

/// Result of `parse_image_url`. The codex parser uses this to build
/// the right `(AttachmentKind, AttachmentStorage)` pair for
/// `build_attachment`. invalid_reason mapping:
///   - DataBase64    → normal path (`storage="inline_base64"` / `"elided"`)
///   - File          → normal path (`storage="ref"`)
///   - Http          → normal path (`storage="url"`)
///   - DataNonBase64 → `storage="invalid"`, `invalid_reason="data_url_not_base64"`
///   - Unrecognized  → `storage="invalid"`, `invalid_reason="unrecognized_scheme"`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImageUrlVariant<'a> {
    DataBase64 {
        media_type: Option<&'a str>,
        base64_payload: &'a str,
    },
    DataNonBase64 {
        media_type: Option<&'a str>,
        raw: &'a str,
    },
    File {
        url: &'a str,
    },
    Http {
        url: &'a str,
    },
    Unrecognized {
        raw: &'a str,
    },
}

/// Dispatch an `image_url` (or `audio_url` etc.) string by URL scheme.
/// Spec §4.2.2.6.1: the codex `input_image.image_url` field is an
/// arbitrary string; this helper centralizes the parsing so all
/// callers get the same classification.
pub fn parse_image_url(s: &str) -> ImageUrlVariant<'_> {
    if let Some(rest) = s.strip_prefix("data:") {
        let Some(comma) = rest.find(',') else {
            return ImageUrlVariant::Unrecognized { raw: s };
        };
        let header = &rest[..comma];
        let payload = &rest[comma + 1..];
        // Split off ";base64" suffix and the optional media type.
        let (media_type, is_base64) = if let Some(mt) = header.strip_suffix(";base64") {
            (mt, true)
        } else if header == "base64" {
            // Unusual but legal: "data:base64,..." (= no media type)
            ("", true)
        } else {
            (header, false)
        };
        let media_type = if media_type.is_empty() {
            None
        } else {
            Some(media_type)
        };
        if is_base64 {
            ImageUrlVariant::DataBase64 {
                media_type,
                base64_payload: payload,
            }
        } else {
            ImageUrlVariant::DataNonBase64 {
                media_type,
                raw: payload,
            }
        }
    } else if let Some(rest) = s.strip_prefix("file://") {
        // We keep the whole `file://...` URL in the variant's url field
        // for symmetry with Http, so the source parser can pass it
        // straight to `build_attachment` without reconstruction.
        let _ = rest;
        ImageUrlVariant::File { url: s }
    } else if s.starts_with("http://") || s.starts_with("https://") {
        ImageUrlVariant::Http { url: s }
    } else {
        ImageUrlVariant::Unrecognized { raw: s }
    }
}

// ---------------------------------------------------------------------
// raw helpers (§4.2.2.5)
// ---------------------------------------------------------------------

/// Build a `(source_id, payload)` tuple ready to be merged into
/// `entry.canonical.raw` via `merge_raw`. This is the only sanctioned
/// way for source parsers to populate the `raw` reserved key — the
/// run_import drops `entry.metadata.insert("raw", ...)`.
pub fn raw_entry(source_id: &str, payload: Value) -> (String, Value) {
    (source_id.to_string(), payload)
}

/// Names of string fields whose value is a free-form tool output blob
/// and is therefore subject to the `tool_output_full` size guard when
/// landing inside `metadata.raw.<source>`. Spec §4.2.2.5: raw keeps the
/// original payload for forensic recovery, but only with the same byte
/// budgets the canonical layer applies — otherwise a single MCP /
/// shell call can dump multi-MB output verbatim into one memory row.
const RAW_LARGE_TEXT_FIELDS: &[&str] = &["output", "stdout", "stderr", "text"];

/// Strip / summarize fields known to carry unbounded payloads inside
/// a source-side raw blob, so the resulting Value never exceeds the
/// budgets enforced on the canonical layer (`tool_output_full`,
/// `attachment_inline_max`).
///
/// Truncated text fields are replaced with a JSON object recording
/// `{ "elided": true, "size_bytes": N, "sha256": "<hex>", "preview": "..." }`
/// so an operator can correlate against external logs without having
/// to keep the original blob in DB.
///
/// Inline `data:<media>;base64,<payload>` URLs (the codex / Anthropic
/// `image_url` shape) are similarly collapsed to `{ "elided": true,
/// "media_type": ..., "size_bytes": ..., "sha256": ... }` once their
/// raw byte length exceeds `attachment_inline_max`.
///
/// Returns a fresh `Value`; the caller's `payload` is borrowed only.
pub fn sanitize_raw_payload(payload: &Value) -> Value {
    sanitize_raw_payload_with_config(payload, size_config())
}

pub(crate) fn sanitize_raw_payload_with_config(payload: &Value, cfg: &SizeConfig) -> Value {
    sanitize_value(payload, cfg, /*field_hint=*/ None)
}

fn sanitize_value(value: &Value, cfg: &SizeConfig, field_hint: Option<&str>) -> Value {
    match value {
        Value::Object(map) => {
            let mut out = Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(k.clone(), sanitize_value(v, cfg, Some(k.as_str())));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|v| sanitize_value(v, cfg, field_hint))
                .collect(),
        ),
        Value::String(s) => sanitize_string(s, cfg, field_hint),
        other => other.clone(),
    }
}

fn sanitize_string(s: &str, cfg: &SizeConfig, field_hint: Option<&str>) -> Value {
    // image_url-style data URLs are routed through the attachment guard
    // regardless of which key carries them, because codex / Anthropic
    // both nest them under different field names (image_url, url, data).
    if s.starts_with("data:")
        && let Some(elided) = elide_data_url(s, cfg)
    {
        return elided;
    }

    if let Some(name) = field_hint
        && RAW_LARGE_TEXT_FIELDS.contains(&name)
        && s.len() > cfg.tool_output_full
    {
        return elide_text(s, cfg.tool_output_full);
    }

    Value::String(strip_nul_bytes(s))
}

/// Drop U+0000 (NUL) from a string. PostgreSQL's `text` / `varchar`
/// reject NULs as SQLSTATE 22021; Rust's `String` does not, so text
/// reaching the DB must be scrubbed first.
pub fn strip_nul_bytes(s: &str) -> String {
    if s.contains('\u{0000}') {
        s.replace('\u{0000}', "")
    } else {
        s.to_string()
    }
}

/// Recursively drop NUL bytes from every string node in a JSON value,
/// including object keys. `serde_json::to_string` escapes NULs as the
/// six-character sequence `\u0000`, so a NUL hidden inside an object
/// value *or* key survives JSON round-tripping and reaches the DB
/// column intact — string-level stripping on the serialized form
/// would be a no-op. Callers building `MemoryData.metadata` from a
/// `serde_json::Value` should pass through this helper before
/// serialization.
pub fn strip_nul_bytes_in_value(value: &mut Value) {
    match value {
        Value::String(s) if s.contains('\u{0000}') => {
            *s = s.replace('\u{0000}', "");
        }
        Value::Array(arr) => {
            for v in arr {
                strip_nul_bytes_in_value(v);
            }
        }
        Value::Object(map) => {
            // `Map` exposes keys as `&String` through its iterators, so
            // we can't rewrite contaminated keys in place. The common
            // case (no NUL in any key) stays allocation-free; only when
            // at least one key carries a NUL do we rebuild the map.
            let needs_key_rewrite = map.keys().any(|k| k.contains('\u{0000}'));
            if needs_key_rewrite {
                let original = std::mem::take(map);
                for (k, mut v) in original {
                    strip_nul_bytes_in_value(&mut v);
                    let clean_k = if k.contains('\u{0000}') {
                        k.replace('\u{0000}', "")
                    } else {
                        k
                    };
                    map.insert(clean_k, v);
                }
            } else {
                for v in map.values_mut() {
                    strip_nul_bytes_in_value(v);
                }
            }
        }
        _ => {}
    }
}

fn elide_text(s: &str, full_budget: usize) -> Value {
    let preview_budget = full_budget.min(256);
    let preview = safe_truncate_at_char_boundary(s, preview_budget);
    let sha = sha256_hex_prefix(s.as_bytes(), 64);
    json!({
        "elided": true,
        "size_bytes": s.len() as u64,
        "sha256": sha,
        "preview": preview,
    })
}

fn elide_data_url(s: &str, cfg: &SizeConfig) -> Option<Value> {
    let variant = parse_image_url(s);
    let (media_type, base64_payload) = match variant {
        ImageUrlVariant::DataBase64 {
            media_type,
            base64_payload,
        } => (media_type, base64_payload),
        // Non-base64 data URLs and unrecognized schemes get the same
        // `tool_output_full` guard so a giant `data:text/plain,...`
        // blob still gets summarized.
        _ => return None,
    };
    let raw_len = base64::engine::general_purpose::STANDARD
        .decode(base64_payload)
        .map(|b| b.len())
        .unwrap_or(base64_payload.len());
    if raw_len <= cfg.attachment_inline_max {
        return None;
    }
    let sha = sha256_hex_prefix(base64_payload.as_bytes(), 64);
    Some(json!({
        "elided": true,
        "kind": "data_url",
        "media_type": media_type,
        "size_bytes": raw_len as u64,
        "sha256": sha,
    }))
}

/// Merge a `(source_id, payload)` tuple into an `Option<Map>`,
/// initializing the map on first call. Multiple sources can coexist
/// under the same `metadata.raw` object via repeated calls with
/// different `source_id`s.
pub fn merge_raw(base: &mut Option<Map<String, Value>>, entry: (String, Value)) {
    let map = base.get_or_insert_with(Map::new);
    map.insert(entry.0, entry.1);
}

// ---------------------------------------------------------------------
// Attachment → media_object classification for the import path
// ---------------------------------------------------------------------

/// What the importer should do with one `metadata.attachment` object to
/// turn it into a `media_object` reference:
///
/// - `inline_base64` (decodable `data`) → `Upload` (bytes streamed)
/// - `url` / `ref` (non-empty `url`)    → `RegisterUrl` (no fetch)
/// - everything else                    → `KeepAsIs`
///
/// `KeepAsIs` covers `elided` / `invalid` / unknown storage / any
/// required-field-missing row: no gRPC backend exists to ingest those
/// (in particular `MediaService.Register` has no `unresolvable`
/// backend), so the attachment blob is left in place. That is safe and
/// idempotent — the offline `metadata.attachment` migration can still
/// convert such rows later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttachmentImportAction {
    Upload {
        kind: ContentType,
        media_type: String,
        data_b64: String,
        width: Option<u32>,
        height: Option<u32>,
        alt: Option<String>,
    },
    RegisterUrl {
        kind: ContentType,
        media_type: String,
        url: String,
        width: Option<u32>,
        height: Option<u32>,
        alt: Option<String>,
    },
    KeepAsIs,
}

fn attachment_str(att: &Value, key: &str) -> Option<String> {
    att.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
}

fn attachment_u32(att: &Value, key: &str) -> Option<u32> {
    att.get(key).and_then(|v| v.as_u64()).map(|n| n as u32)
}

/// Classify one `metadata.attachment` object for the import path. Pure —
/// the importer performs the gRPC I/O the variant describes. A
/// missing/blank required field downgrades to `KeepAsIs` so a malformed
/// attachment never aborts the batch (parity with the offline migration
/// which downgrades the same shapes to `Skip`).
pub fn classify_attachment_for_import(att: &Value) -> AttachmentImportAction {
    let storage = att.get("storage").and_then(|s| s.as_str()).unwrap_or("");
    let media_type =
        attachment_str(att, "media_type").unwrap_or_else(|| "application/octet-stream".to_string());
    let width = attachment_u32(att, "width");
    let height = attachment_u32(att, "height");
    let alt = attachment_str(att, "alt");
    // The media_object's kind must reflect the attachment's kind so an
    // audio/video attachment is not persisted as an image (which would
    // feed the image embedding pipeline and lose the original type). An
    // unrecognized/absent kind falls back to IMAGE — the only kind with
    // an embedding pipeline; a malformed kind should not silently route
    // bytes to audio/video.
    let kind = att
        .get("kind")
        .and_then(|k| k.as_str())
        .and_then(AttachmentKind::from_attachment_str)
        .map(AttachmentKind::media_object_kind)
        .unwrap_or(ContentType::Image);
    match storage {
        "inline_base64" => match attachment_str(att, "data") {
            Some(data_b64) if !data_b64.is_empty() => AttachmentImportAction::Upload {
                kind,
                media_type,
                data_b64,
                width,
                height,
                alt,
            },
            _ => AttachmentImportAction::KeepAsIs,
        },
        "url" | "ref" => match attachment_str(att, "url") {
            Some(url) if !url.is_empty() => AttachmentImportAction::RegisterUrl {
                kind,
                media_type,
                url,
                width,
                height,
                alt,
            },
            _ => AttachmentImportAction::KeepAsIs,
        },
        // elided / invalid / unknown: no gRPC media_object path.
        _ => AttachmentImportAction::KeepAsIs,
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn small_cfg() -> SizeConfig {
        SizeConfig {
            tool_arg_preview: 10,
            tool_output_preview: 8,
            tool_output_full: 16,
            attachment_inline_max: 6,
        }
    }

    // safe_truncate_at_char_boundary -------------------------------------

    #[test]
    fn safe_truncate_returns_full_when_within_budget() {
        assert_eq!(safe_truncate_at_char_boundary("hello", 100), "hello");
        assert_eq!(safe_truncate_at_char_boundary("hello", 5), "hello");
    }

    #[test]
    fn safe_truncate_at_zero_is_empty() {
        assert_eq!(safe_truncate_at_char_boundary("hello", 0), "");
    }

    /// Multibyte UTF-8 must never panic and must stop at a char boundary.
    /// "あいう" は各 3 byte。max_bytes=4 のとき "あ" (3 byte) で停止すべき。
    #[test]
    fn safe_truncate_respects_char_boundary_japanese() {
        let s = "あいう"; // 9 bytes
        assert_eq!(safe_truncate_at_char_boundary(s, 9), "あいう");
        assert_eq!(safe_truncate_at_char_boundary(s, 8), "あい");
        assert_eq!(safe_truncate_at_char_boundary(s, 7), "あい");
        assert_eq!(safe_truncate_at_char_boundary(s, 6), "あい");
        assert_eq!(safe_truncate_at_char_boundary(s, 5), "あ");
        assert_eq!(safe_truncate_at_char_boundary(s, 4), "あ");
        assert_eq!(safe_truncate_at_char_boundary(s, 3), "あ");
        assert_eq!(safe_truncate_at_char_boundary(s, 2), "");
        assert_eq!(safe_truncate_at_char_boundary(s, 1), "");
        assert_eq!(safe_truncate_at_char_boundary(s, 0), "");
    }

    // tool_category ------------------------------------------------------

    #[test]
    fn tool_category_table_spot_checks() {
        assert_eq!(tool_category("claude_code", "Bash"), Some("shell_exec"));
        assert_eq!(tool_category("codex", "shell"), Some("shell_exec"));
        assert_eq!(tool_category("codex", "exec_command"), Some("shell_exec"));
        assert_eq!(tool_category("claude_code", "Read"), Some("file_read"));
        assert_eq!(tool_category("claude_code", "Write"), Some("file_write"));
        assert_eq!(tool_category("codex", "apply_patch"), Some("file_write"));
        assert_eq!(tool_category("claude_code", "Glob"), Some("file_search"));
        assert_eq!(
            tool_category("claude_code", "WebSearch"),
            Some("web_search")
        );
        assert_eq!(tool_category("claude_code", "WebFetch"), Some("web_fetch"));
        // Unknown pairs return None — adding new tools is forward-compatible
        assert_eq!(tool_category("claude_code", "UnknownTool"), None);
        assert_eq!(tool_category("hypothetical_source", "Bash"), None);
    }

    // build_tool_call ----------------------------------------------------

    #[test]
    fn build_tool_call_fills_canonical_fields() {
        let cfg = SizeConfig::DEFAULT;
        let args = json!({"cmd": ["ls"]});
        let r = build_tool_call_with_config(
            "codex",
            Some("shell"),
            Some("call_xyz"),
            Some(&args),
            None,
            &cfg,
        );
        let t = r.tool.as_object().unwrap();
        assert_eq!(t["kind"].as_str(), Some("call"));
        assert_eq!(t["name"].as_str(), Some("shell"));
        assert_eq!(t["source"].as_str(), Some("codex"));
        assert_eq!(t["category"].as_str(), Some("shell_exec"));
        assert_eq!(t["call_id"].as_str(), Some("call_xyz"));
        assert!(r.content.starts_with("shell({"));
    }

    #[test]
    fn build_tool_call_truncates_long_arguments() {
        let cfg = small_cfg(); // tool_arg_preview = 10
        let args = json!("a very long argument string that won't fit");
        let r = build_tool_call_with_config("codex", Some("shell"), None, Some(&args), None, &cfg);
        // Full JSON is preserved in metadata.tool.arguments
        assert_eq!(r.tool["arguments"], args);
        // But content is truncated and ends with " …)"
        assert!(r.content.ends_with(" …)"), "got: {}", r.content);
    }

    // build_tool_output --------------------------------------------------

    #[test]
    fn build_tool_output_short_output_is_stored_verbatim() {
        let cfg = SizeConfig::DEFAULT;
        let r = build_tool_output_with_config(
            "codex",
            Some("shell"),
            Some("call_x"),
            Some("ok"),
            Some(ToolStatus::Ok),
            None,
            &cfg,
        );
        let t = r.tool.as_object().unwrap();
        assert_eq!(t["output"].as_str(), Some("ok"));
        assert!(t["output_truncated"].is_null());
        assert!(t["output_full_bytes"].is_null());
        assert!(t["output_full_sha256"].is_null());
        assert_eq!(t["status"].as_str(), Some("ok"));
        assert_eq!(r.content, "ok");
    }

    /// Spec §4.2.2.3.1: long output must set truncated=true,
    /// output_full_bytes=<orig>, output_full_sha256=<hex>, and
    /// MemoryData.content's footer must reference the **original**
    /// total byte count, not the preview length.
    #[test]
    fn build_tool_output_truncates_long_output_and_preserves_full_bytes() {
        let cfg = small_cfg(); // tool_output_full=16, tool_output_preview=8
        let long = "a".repeat(100);
        let r = build_tool_output_with_config(
            "codex",
            Some("shell"),
            Some("call_x"),
            Some(&long),
            Some(ToolStatus::Error),
            Some("exec_command_end"),
            &cfg,
        );
        let t = r.tool.as_object().unwrap();
        assert_eq!(t["output_truncated"].as_bool(), Some(true));
        assert_eq!(t["output_full_bytes"].as_u64(), Some(100));
        let sha = t["output_full_sha256"].as_str().unwrap();
        assert_eq!(sha.len(), 64); // sha256 16進
        assert_eq!(t["status"].as_str(), Some("error"));
        assert_eq!(t["source_event"].as_str(), Some("exec_command_end"));
        // content footer references full original byte count (100), not preview budget
        assert!(
            r.content.contains("total 100 bytes"),
            "content footer must reference original byte count: {}",
            r.content
        );
    }

    #[test]
    fn build_tool_output_status_tri_state_null_when_none() {
        let cfg = SizeConfig::DEFAULT;
        let r = build_tool_output_with_config(
            "codex",
            Some("apply_patch"),
            None,
            Some("ok"),
            None, // status absent → metadata.tool.status stays null
            None,
            &cfg,
        );
        assert!(r.tool["status"].is_null());
    }

    // backfill_tool_name_and_category -----------------------------------

    #[test]
    fn backfill_tool_name_and_category_recomputes_category_for_known_name() {
        // Simulate 2nd-pass rewire path: build_tool_output ran without
        // a name (reordered rollout), so category was frozen to null.
        let cfg = SizeConfig::DEFAULT;
        let mut r = build_tool_output_with_config(
            "codex",
            None,
            Some("call_late"),
            Some("done"),
            None,
            None,
            &cfg,
        );
        assert!(r.tool["name"].is_null());
        assert!(r.tool["category"].is_null());

        backfill_tool_name_and_category(&mut r.tool, "codex", "shell");
        assert_eq!(r.tool["name"].as_str(), Some("shell"));
        assert_eq!(r.tool["category"].as_str(), Some("shell_exec"));
    }

    #[test]
    fn backfill_tool_name_and_category_sets_null_for_unknown_name() {
        let cfg = SizeConfig::DEFAULT;
        let mut r = build_tool_output_with_config(
            "codex",
            None,
            Some("call_x"),
            Some("done"),
            None,
            None,
            &cfg,
        );
        backfill_tool_name_and_category(&mut r.tool, "codex", "unknown_tool");
        assert_eq!(r.tool["name"].as_str(), Some("unknown_tool"));
        assert!(r.tool["category"].is_null());
    }

    // build_attachment ---------------------------------------------------

    #[test]
    fn build_attachment_inline_within_budget_keeps_data() {
        let cfg = small_cfg(); // attachment_inline_max=6
        // "abcd" (4 bytes raw)
        let b64 = base64::engine::general_purpose::STANDARD.encode("abcd");
        let r = build_attachment_with_config(
            AttachmentKind::Image,
            AttachmentStorage::InlineBase64,
            Some("image/png"),
            Some(&b64),
            None,
            None,
            None,
            None,
            &cfg,
        );
        assert_eq!(r.tool_storage(), Some("inline_base64"));
        assert_eq!(r.attachment["data"].as_str(), Some(b64.as_str()));
        assert!(r.attachment["data_truncated"].is_null());
        assert_eq!(r.attachment["size_bytes"].as_u64(), Some(4));
        assert_eq!(r.content_type, ContentType::Image);
    }

    /// Spec §4.2.2.4.1: oversized inline base64 must elide and surface
    /// data_sha256 + size_bytes for verification, with the data field
    /// dropped to keep DB rows bounded.
    #[test]
    fn build_attachment_elides_oversized_inline_base64() {
        let cfg = small_cfg(); // attachment_inline_max=6
        let raw = "abcdefghijklmnop"; // 16 bytes > 6
        let b64 = base64::engine::general_purpose::STANDARD.encode(raw);
        let r = build_attachment_with_config(
            AttachmentKind::Image,
            AttachmentStorage::InlineBase64,
            Some("image/png"),
            Some(&b64),
            None,
            None,
            None,
            None,
            &cfg,
        );
        assert_eq!(r.tool_storage(), Some("elided"));
        assert!(r.attachment["data"].is_null());
        assert_eq!(r.attachment["data_truncated"].as_bool(), Some(true));
        assert_eq!(r.attachment["size_bytes"].as_u64(), Some(16));
        let sha = r.attachment["data_sha256"].as_str().unwrap();
        assert_eq!(sha.len(), 64);
        // content_type is still IMAGE — kind, not storage, dictates it
        assert_eq!(r.content_type, ContentType::Image);
        assert!(
            r.content.contains("data elided"),
            "content placeholder must mention elision: {}",
            r.content
        );
    }

    #[test]
    fn build_attachment_invalid_carries_reason_and_drops_data() {
        let cfg = SizeConfig::DEFAULT;
        let r = build_attachment_with_config(
            AttachmentKind::Image,
            AttachmentStorage::Invalid {
                reason: "unrecognized_scheme".to_string(),
            },
            None,
            None,
            None,
            None,
            None,
            None,
            &cfg,
        );
        assert_eq!(r.tool_storage(), Some("invalid"));
        assert_eq!(
            r.attachment["invalid_reason"].as_str(),
            Some("unrecognized_scheme")
        );
        assert!(r.attachment["data"].is_null());
        assert!(r.attachment["url"].is_null());
        // Even invalid: content_type is still IMAGE (kind-driven)
        assert_eq!(r.content_type, ContentType::Image);
        assert!(
            r.content.contains("invalid: unrecognized_scheme"),
            "content placeholder must mention invalid reason: {}",
            r.content
        );
    }

    #[test]
    fn build_attachment_url_storage_keeps_url_only() {
        let cfg = SizeConfig::DEFAULT;
        let r = build_attachment_with_config(
            AttachmentKind::Image,
            AttachmentStorage::Url,
            None,
            None,
            Some("https://example.com/cat.png"),
            None,
            None,
            None,
            &cfg,
        );
        assert_eq!(r.tool_storage(), Some("url"));
        assert_eq!(
            r.attachment["url"].as_str(),
            Some("https://example.com/cat.png")
        );
        assert!(r.attachment["data"].is_null());
        assert_eq!(r.content_type, ContentType::Image);
    }

    /// Ref kind resolves to ContentType::URL (proto has no FILE).
    #[test]
    fn build_attachment_ref_maps_to_url_content_type() {
        let cfg = SizeConfig::DEFAULT;
        let r = build_attachment_with_config(
            AttachmentKind::Ref,
            AttachmentStorage::Ref,
            None,
            None,
            Some("file:///home/u/notes/foo.md"),
            None,
            None,
            None,
            &cfg,
        );
        assert_eq!(r.content_type, ContentType::Url);
        assert_eq!(
            r.attachment["url"].as_str(),
            Some("file:///home/u/notes/foo.md")
        );
    }

    impl BuildAttachmentResult {
        fn tool_storage(&self) -> Option<&str> {
            self.attachment.get("storage").and_then(|v| v.as_str())
        }
    }

    // parse_image_url ----------------------------------------------------

    #[test]
    fn parse_image_url_dispatches_http() {
        match parse_image_url("https://example.com/img.png") {
            ImageUrlVariant::Http { url } => assert_eq!(url, "https://example.com/img.png"),
            other => panic!("expected Http, got {other:?}"),
        }
        match parse_image_url("http://example.com/img.png") {
            ImageUrlVariant::Http { .. } => {}
            other => panic!("expected Http, got {other:?}"),
        }
    }

    #[test]
    fn parse_image_url_dispatches_file() {
        match parse_image_url("file:///home/u/img.png") {
            ImageUrlVariant::File { url } => assert_eq!(url, "file:///home/u/img.png"),
            other => panic!("expected File, got {other:?}"),
        }
    }

    #[test]
    fn parse_image_url_dispatches_data_base64() {
        match parse_image_url("data:image/png;base64,iVBORw0KGgo=") {
            ImageUrlVariant::DataBase64 {
                media_type,
                base64_payload,
            } => {
                assert_eq!(media_type, Some("image/png"));
                assert_eq!(base64_payload, "iVBORw0KGgo=");
            }
            other => panic!("expected DataBase64, got {other:?}"),
        }
    }

    #[test]
    fn parse_image_url_dispatches_data_non_base64() {
        // RFC 2397 allows "data:<media>,<rawtext>" without ;base64
        match parse_image_url("data:text/plain,hello") {
            ImageUrlVariant::DataNonBase64 { media_type, raw } => {
                assert_eq!(media_type, Some("text/plain"));
                assert_eq!(raw, "hello");
            }
            other => panic!("expected DataNonBase64, got {other:?}"),
        }
    }

    #[test]
    fn parse_image_url_unrecognized_scheme() {
        for s in ["./relative/path.png", "ftp://example.com/x", "image.png"] {
            match parse_image_url(s) {
                ImageUrlVariant::Unrecognized { raw } => assert_eq!(raw, s),
                other => panic!("expected Unrecognized for {s}, got {other:?}"),
            }
        }
    }

    #[test]
    fn parse_image_url_data_without_comma_is_unrecognized() {
        // "data:foo" は comma が無いので不正。Unrecognized で fallback。
        match parse_image_url("data:image/png;base64") {
            ImageUrlVariant::Unrecognized { .. } => {}
            other => panic!("expected Unrecognized for malformed data: URL, got {other:?}"),
        }
    }

    // raw helpers --------------------------------------------------------

    #[test]
    fn merge_raw_initializes_and_inserts() {
        let mut base: Option<Map<String, Value>> = None;
        merge_raw(&mut base, raw_entry("codex", json!({"a": 1})));
        let m = base.as_ref().unwrap();
        assert_eq!(m.len(), 1);
        assert_eq!(m["codex"], json!({"a": 1}));

        // Adding a second source coexists under the same map
        merge_raw(&mut base, raw_entry("claude_code", json!([1, 2])));
        let m = base.as_ref().unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m["claude_code"], json!([1, 2]));
    }

    // sanitize_raw_payload ----------------------------------------------

    #[test]
    fn sanitize_raw_keeps_short_output_verbatim() {
        let cfg = small_cfg(); // tool_output_full=16
        let payload = json!({"output": "short"});
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        assert_eq!(sanitized, payload);
    }

    #[test]
    fn sanitize_raw_elides_large_output_field() {
        let cfg = small_cfg(); // tool_output_full=16
        let big = "x".repeat(200);
        let payload = json!({"output": big});
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        let elided = &sanitized["output"];
        assert_eq!(elided["elided"], json!(true));
        assert_eq!(elided["size_bytes"].as_u64(), Some(200));
        assert_eq!(elided["sha256"].as_str().map(str::len), Some(64));
        // Preview retains a UTF-8 safe head of the original.
        assert!(
            elided["preview"]
                .as_str()
                .is_some_and(|s| !s.is_empty() && s.len() <= cfg.tool_output_full)
        );
    }

    #[test]
    fn sanitize_raw_elides_large_stdout_and_stderr() {
        let cfg = small_cfg();
        let payload = json!({
            "stdout": "y".repeat(200),
            "stderr": "z".repeat(200),
            "exit_code": 1,
        });
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        assert_eq!(sanitized["stdout"]["elided"], json!(true));
        assert_eq!(sanitized["stderr"]["elided"], json!(true));
        // Non-text scalars survive untouched.
        assert_eq!(sanitized["exit_code"], json!(1));
    }

    #[test]
    fn sanitize_raw_recurses_into_arrays_and_objects() {
        let cfg = small_cfg();
        let payload = json!({
            "calls": [
                {"output": "a".repeat(200)},
                {"output": "ok"},
            ],
            "nested": {"output": "b".repeat(200)}
        });
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        assert_eq!(sanitized["calls"][0]["output"]["elided"], json!(true));
        assert_eq!(sanitized["calls"][1]["output"], json!("ok"));
        assert_eq!(sanitized["nested"]["output"]["elided"], json!(true));
    }

    #[test]
    fn sanitize_raw_elides_large_data_url_anywhere() {
        let cfg = small_cfg(); // attachment_inline_max=6
        // 9 raw bytes after base64 decode.
        let big_b64 = base64::engine::general_purpose::STANDARD.encode(b"123456789");
        let url = format!("data:image/png;base64,{big_b64}");
        let payload = json!({
            "image_url": {"url": url.clone()},
            "fallback": url,
        });
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        let nested = &sanitized["image_url"]["url"];
        assert_eq!(nested["elided"], json!(true));
        assert_eq!(nested["kind"], json!("data_url"));
        assert_eq!(nested["media_type"], json!("image/png"));
        assert_eq!(nested["size_bytes"].as_u64(), Some(9));
        // Same payload reached through a different field is also elided.
        assert_eq!(sanitized["fallback"]["elided"], json!(true));
    }

    #[test]
    fn sanitize_raw_keeps_small_data_url_inline() {
        let cfg = SizeConfig::DEFAULT;
        let small = base64::engine::general_purpose::STANDARD.encode(b"hi");
        let url = format!("data:image/png;base64,{small}");
        let payload = json!({"image_url": {"url": url.clone()}});
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        // Under the budget → kept as-is so forensic recovery still
        // sees the original blob.
        assert_eq!(sanitized["image_url"]["url"], json!(url));
    }

    #[test]
    fn sanitize_raw_keeps_non_text_field_untouched() {
        // A long string that is NOT in RAW_LARGE_TEXT_FIELDS must pass
        // through verbatim — sanitization is opt-in by field name to
        // avoid losing forensic detail on unrelated keys.
        let cfg = small_cfg();
        let payload = json!({"name": "x".repeat(200)});
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        assert_eq!(sanitized, payload);
    }

    // --- classify_attachment_for_import ------------------------------
    //
    // The offline `metadata.attachment` migration bin classifies the
    // same JSON shapes with the same logic but lives in a separate crate
    // and cannot share code. These tests use inputs identical to that
    // bin's so the two classifiers stay behaviour-identical by test
    // contract (the import path must not diverge or it would leave
    // attachments the migration would have converted).

    #[test]
    fn classify_import_inline_base64_with_data_is_upload() {
        let att = json!({
            "storage": "inline_base64",
            "media_type": "image/png",
            "data": "aGVsbG8=",
            "width": 10, "height": 20, "alt": "hi"
        });
        assert_eq!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::Upload {
                kind: ContentType::Image,
                media_type: "image/png".into(),
                data_b64: "aGVsbG8=".into(),
                width: Some(10),
                height: Some(20),
                alt: Some("hi".into()),
            }
        );
    }

    #[test]
    fn classify_import_audio_inline_preserves_audio_kind() {
        let att = json!({
            "kind": "audio",
            "storage": "inline_base64",
            "media_type": "audio/mpeg",
            "data": "YXVkaW8=",
        });
        assert_eq!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::Upload {
                kind: ContentType::Audio,
                media_type: "audio/mpeg".into(),
                data_b64: "YXVkaW8=".into(),
                width: None,
                height: None,
                alt: None,
            }
        );
    }

    #[test]
    fn classify_import_video_url_preserves_video_kind() {
        let att = json!({
            "kind": "video",
            "storage": "url",
            "media_type": "video/mp4",
            "url": "https://example.com/clip.mp4",
        });
        assert_eq!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::RegisterUrl {
                kind: ContentType::Video,
                media_type: "video/mp4".into(),
                url: "https://example.com/clip.mp4".into(),
                width: None,
                height: None,
                alt: None,
            }
        );
    }

    #[test]
    fn classify_import_image_kind_maps_to_image() {
        let att = json!({
            "kind": "image",
            "storage": "inline_base64",
            "media_type": "image/png",
            "data": "aW1n",
        });
        assert!(matches!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::Upload {
                kind: ContentType::Image,
                ..
            }
        ));
    }

    #[test]
    fn classify_import_inline_base64_without_data_is_keep() {
        let att = json!({ "storage": "inline_base64", "media_type": "image/png" });
        assert_eq!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::KeepAsIs
        );
    }

    #[test]
    fn classify_import_url_and_ref_register_external() {
        for storage in ["url", "ref"] {
            let att = json!({
                "storage": storage,
                "media_type": "image/jpeg",
                "url": "https://example.com/a.jpg"
            });
            assert_eq!(
                classify_attachment_for_import(&att),
                AttachmentImportAction::RegisterUrl {
                    kind: ContentType::Image,
                    media_type: "image/jpeg".into(),
                    url: "https://example.com/a.jpg".into(),
                    width: None,
                    height: None,
                    alt: None,
                }
            );
        }
    }

    #[test]
    fn classify_import_url_without_url_field_is_keep() {
        let att = json!({ "storage": "url", "media_type": "image/jpeg" });
        assert_eq!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::KeepAsIs
        );
    }

    #[test]
    fn classify_import_elided_is_keep_as_is() {
        // elided has no bytes and no gRPC backend can ingest it
        // (Register has no `unresolvable` backend), so the attachment is
        // left intact rather than guessed at.
        let att = json!({
            "storage": "elided",
            "media_type": "image/png",
            "data_sha256": "abc123",
            "size_bytes": 4096
        });
        assert_eq!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::KeepAsIs
        );
    }

    #[test]
    fn classify_import_invalid_and_unknown_are_keep() {
        for storage in ["invalid", "something_new"] {
            let att = json!({ "storage": storage, "media_type": "image/png" });
            assert_eq!(
                classify_attachment_for_import(&att),
                AttachmentImportAction::KeepAsIs
            );
        }
    }

    #[test]
    fn classify_import_missing_media_type_defaults_octet_stream() {
        let att = json!({ "storage": "url", "url": "https://x.test/a" });
        assert_eq!(
            classify_attachment_for_import(&att),
            AttachmentImportAction::RegisterUrl {
                kind: ContentType::Image,
                media_type: "application/octet-stream".into(),
                url: "https://x.test/a".into(),
                width: None,
                height: None,
                alt: None,
            }
        );
    }

    // strip_nul_bytes ----------------------------------------------------

    #[test]
    fn strip_nul_bytes_removes_embedded_nul() {
        let input = "hello\u{0000}world\u{0000}";
        assert_eq!(strip_nul_bytes(input), "helloworld");
    }

    #[test]
    fn strip_nul_bytes_passes_through_clean_string() {
        let clean = "no nul here — 日本語も通る";
        assert_eq!(strip_nul_bytes(clean), clean);
    }

    #[test]
    fn sanitize_string_drops_nul_from_metadata_field() {
        let cfg = SizeConfig::DEFAULT;
        let payload = json!({"output": "ok\u{0000}done"});
        let sanitized = sanitize_raw_payload_with_config(&payload, &cfg);
        assert_eq!(sanitized["output"], json!("okdone"));
    }

    #[test]
    fn strip_nul_bytes_in_value_recurses_into_arrays_and_nested_objects() {
        let mut v = json!({
            "list": ["a\u{0000}b", {"inner": "x\u{0000}y"}],
            "nested": {"deeper": "p\u{0000}q"},
            "clean": "untouched",
            "num": 42,
        });
        strip_nul_bytes_in_value(&mut v);
        assert_eq!(v["list"][0], json!("ab"));
        assert_eq!(v["list"][1]["inner"], json!("xy"));
        assert_eq!(v["nested"]["deeper"], json!("pq"));
        assert_eq!(v["clean"], json!("untouched"));
        assert_eq!(v["num"], json!(42));
    }

    // Without key-level sanitization the import boundary's "no NUL in
    // metadata" guarantee would be silently broken: serde_json escapes
    // a key NUL into the literal six-char `\u0000` sequence, which JSON
    // parse restores back to a real NUL on the receiving side.
    #[test]
    fn strip_nul_bytes_in_value_strips_nul_from_object_keys() {
        let mut map = serde_json::Map::new();
        map.insert("bad\u{0000}key".to_string(), json!("val"));
        map.insert("ok".to_string(), json!("v2"));
        // Nested object whose key also contains a NUL.
        let mut inner = serde_json::Map::new();
        inner.insert("inner\u{0000}k".to_string(), json!("iv"));
        map.insert("nest".to_string(), Value::Object(inner));
        let mut v = Value::Object(map);

        strip_nul_bytes_in_value(&mut v);

        let obj = v.as_object().expect("object");
        assert!(
            obj.contains_key("badkey"),
            "NUL stripped from top-level key"
        );
        assert!(!obj.keys().any(|k| k.contains('\u{0000}')));
        assert_eq!(obj["badkey"], json!("val"));
        assert_eq!(obj["ok"], json!("v2"));
        let nested = obj["nest"].as_object().expect("nested object");
        assert!(nested.contains_key("innerk"));
        assert!(!nested.keys().any(|k| k.contains('\u{0000}')));

        // Round-trip through serde_json to confirm no `\u0000` escape
        // sequence leaks into the serialized form (which would otherwise
        // be parsed back into a real NUL by the consumer).
        let s = serde_json::to_string(&v).unwrap();
        assert!(
            !s.contains("\\u0000"),
            "serialized form must not contain \\u0000 escapes: {s}"
        );
    }
}
