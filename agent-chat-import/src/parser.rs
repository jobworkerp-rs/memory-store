use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

/// Raw JSONL entry from Claude Code chat history.
/// Uses `#[serde(default)]` for forward compatibility with new fields.
///
/// `raw` carries the verbatim parsed JSON Value so canonical-side
/// subtype extractors (e.g. attachment.snippet, attachment.files[],
/// attachment.addedBlocks) can read fields that are not part of the
/// typed shape without re-parsing the line.
#[derive(Debug, Clone)]
pub struct RawEntry {
    pub entry_type: String,
    pub uuid: Option<String>,
    pub timestamp: Option<String>,
    pub parent_uuid: Option<String>,
    pub is_sidechain: bool,
    pub session_id: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    #[allow(dead_code)]
    pub version: Option<String>,
    pub slug: Option<String>,
    /// For user/assistant entries
    pub message: Option<serde_json::Value>,
    /// For system / attachment entries (top-level `content` field)
    pub content: Option<serde_json::Value>,
    pub subtype: Option<String>,
    #[allow(dead_code)]
    pub level: Option<String>,
    /// For custom-title entries
    pub title: Option<String>,
    /// For agent-name entries
    #[allow(dead_code)]
    pub agent_name: Option<String>,
    /// Permission mode
    #[allow(dead_code)]
    pub permission_mode: Option<String>,
    /// Request ID (assistant entries)
    pub request_id: Option<String>,
    /// Verbatim JSON line for forward-compat field access. Read it
    /// through `extra()` rather than reaching for the field directly so
    /// new call sites don't have to know whether the value lives on a
    /// typed member or in the original JSON tree.
    pub(crate) raw: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct RawEntryShape {
    #[serde(rename = "type")]
    entry_type: String,
    uuid: Option<String>,
    timestamp: Option<String>,
    #[serde(rename = "parentUuid")]
    parent_uuid: Option<String>,
    #[serde(rename = "isSidechain", default)]
    is_sidechain: bool,
    #[serde(rename = "sessionId")]
    session_id: Option<String>,
    cwd: Option<String>,
    #[serde(rename = "gitBranch")]
    git_branch: Option<String>,
    version: Option<String>,
    slug: Option<String>,
    message: Option<serde_json::Value>,
    content: Option<serde_json::Value>,
    subtype: Option<String>,
    #[serde(default)]
    level: Option<String>,
    title: Option<String>,
    #[serde(rename = "agentName")]
    agent_name: Option<String>,
    #[serde(rename = "permissionMode")]
    permission_mode: Option<String>,
    #[serde(rename = "requestId")]
    request_id: Option<String>,
}

impl RawEntry {
    /// Parse one JSONL line into both the typed `RawEntryShape` and the
    /// verbatim raw `serde_json::Value`. The line is parsed twice — once
    /// into the typed shape, once into a raw `Value` — so the raw side
    /// is owned outright instead of being a deep-clone of the typed
    /// parse's intermediate Value. For long Claude Code sessions this
    /// removes a per-line peak that doubled overall parse memory.
    fn from_str(line: &str) -> Result<Self, serde_json::Error> {
        let shape: RawEntryShape = serde_json::from_str(line)?;
        let raw: serde_json::Value = serde_json::from_str(line)?;
        Ok(Self::from_parts(shape, raw))
    }

    fn from_parts(shape: RawEntryShape, raw: serde_json::Value) -> Self {
        Self {
            entry_type: shape.entry_type,
            uuid: shape.uuid,
            timestamp: shape.timestamp,
            parent_uuid: shape.parent_uuid,
            is_sidechain: shape.is_sidechain,
            session_id: shape.session_id,
            cwd: shape.cwd,
            git_branch: shape.git_branch,
            version: shape.version,
            slug: shape.slug,
            message: shape.message,
            content: shape.content,
            subtype: shape.subtype,
            level: shape.level,
            title: shape.title,
            agent_name: shape.agent_name,
            permission_mode: shape.permission_mode,
            request_id: shape.request_id,
            raw,
        }
    }

    /// Read a top-level field from the verbatim JSON line. Used by
    /// `type=attachment` subtype extractors that need fields not part
    /// of the typed `RawEntryShape` (e.g. `snippet`, `files`,
    /// `addedBlocks`, `stdout`, `stderr`).
    pub fn extra(&self, key: &str) -> Option<&serde_json::Value> {
        self.raw.get(key)
    }

    /// Borrow the verbatim JSON line as-is. Use this only when a helper
    /// needs the entire raw payload (e.g. `canonical::sanitize_raw_payload`
    /// or unknown-subtype fallback serialization); per-key reads should
    /// go through `extra()` instead.
    pub fn as_raw_value(&self) -> &serde_json::Value {
        &self.raw
    }
}

/// Extracted session metadata from all entries (pre-filter).
#[derive(Debug, Clone)]
pub struct SessionInfo {
    pub session_id: String,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub custom_title: Option<String>,
    pub project_hash: Option<String>,
    #[allow(dead_code)]
    pub slug: Option<String>,
    /// First entry timestamp in millis (pre-filter, all entries)
    pub created_at: i64,
    /// Last entry timestamp in millis (pre-filter, all entries)
    pub updated_at: i64,
}

/// Outcome of one line during JSONL streaming.
///
/// The streamer never short-circuits on a per-line error: parser/source
/// callers need each session to make as much progress as possible even
/// when a few records are malformed. Callers count `Broken` as a
/// "source-filtered" record to keep the summary accurate.
//
// `Entry` is the hot variant (every importable line) and is ~464 bytes;
// `Broken` is rare. Boxing `Entry` per clippy's `large_enum_variant`
// suggestion would add a heap allocation per import-bound line just to
// shrink the rare error variant — net pessimisation.
#[allow(clippy::large_enum_variant)]
pub enum ParseItem {
    Entry(RawEntry),
    /// Line was unparseable. The diagnostic message is emitted via
    /// `tracing::warn!` at the parse site (file path + line number
    /// already included); callers only need to bump a counter. The
    /// field is read by tests asserting which line was rejected.
    Broken {
        #[allow(dead_code)]
        line_num: usize,
    },
}

/// Streaming JSONL reader. Holds an open `File` + `BufReader` so the
/// caller can iterate line-by-line without ever materializing the whole
/// file in memory. This is the new entry point — `parse_jsonl_file`
/// remains as a `collect()` shim for callers that still want a `Vec`.
pub struct JsonlStream {
    iter: Box<dyn Iterator<Item = ParseItem> + Send>,
}

impl Iterator for JsonlStream {
    type Item = ParseItem;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Open `path` for line-by-line streaming. Errors only on open / first
/// line failure; per-line parse failures are reported as
/// `ParseItem::Broken` so the import can keep making forward progress.
pub fn stream_jsonl_file(path: &Path) -> Result<JsonlStream> {
    let file = File::open(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let path_str = path.display().to_string();
    let reader = BufReader::new(file);
    // `BufReader::lines` allocates one `String` per line and returns
    // ownership immediately. We trim/parse inline and emit one
    // `ParseItem` per non-empty line; blank lines are silently skipped.
    let iter = reader.lines().enumerate().filter_map(move |(idx, line)| {
        let line_num = idx + 1;
        let raw_line = match line {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("{}:{}: read error: {}", path_str, line_num, e);
                return Some(ParseItem::Broken { line_num });
            }
        };
        let trimmed = raw_line.trim();
        if trimmed.is_empty() {
            return None;
        }
        match RawEntry::from_str(trimmed) {
            Ok(entry) => Some(ParseItem::Entry(entry)),
            Err(e) => {
                tracing::warn!(
                    "{}:{}: failed to interpret JSONL line as RawEntry: {}",
                    path_str,
                    line_num,
                    e
                );
                Some(ParseItem::Broken { line_num })
            }
        }
    });
    Ok(JsonlStream {
        iter: Box::new(iter),
    })
}

/// Parse a JSONL file into a vector of raw entries.
/// Invalid lines are skipped with a warning.
///
/// Now a thin wrapper around `stream_jsonl_file` that drains the
/// stream into a `Vec`. Kept so existing callers (claude_code,
/// codex sources) keep compiling unchanged; new callers that can
/// process entries lazily should use `stream_jsonl_file` directly to
/// avoid holding the whole session in memory.
pub fn parse_jsonl_file(path: &Path) -> Result<Vec<RawEntry>> {
    let stream = stream_jsonl_file(path)?;
    let mut entries = Vec::new();
    for item in stream {
        if let ParseItem::Entry(e) = item {
            entries.push(e);
        }
    }
    Ok(entries)
}

/// Parse ISO 8601 timestamp to milliseconds epoch.
pub fn parse_timestamp_millis(ts: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// Extract session metadata from all entries (before filtering).
pub fn extract_session_info(entries: &[RawEntry], file_path: &Path) -> Result<SessionInfo> {
    // Find session_id from first entry that has one
    let session_id = entries
        .iter()
        .find_map(|e| e.session_id.clone())
        .unwrap_or_else(|| {
            // Fallback: use filename stem as session_id
            file_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string()
        });

    let cwd = entries.iter().find_map(|e| e.cwd.clone());
    let git_branch = entries.iter().find_map(|e| e.git_branch.clone());
    let slug = entries.iter().find_map(|e| e.slug.clone());

    // Find custom-title
    let custom_title = entries
        .iter()
        .rfind(|e| e.entry_type == "custom-title")
        .and_then(|e| e.title.clone());

    // Extract project_hash from file path
    let project_hash = file_path
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|s| s.to_str())
        .map(|s| s.to_string());

    // Compute timestamps from ALL entries (pre-filter)
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for entry in entries {
        if let Some(ref ts_str) = entry.timestamp
            && let Some(ts) = parse_timestamp_millis(ts_str)
        {
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
        }
    }

    if min_ts == i64::MAX {
        // No top-level timestamp on any entry. Modern Claude Code records
        // emit metadata-only sessions (e.g. `file-history-snapshot` whose
        // timestamp lives at `snapshot.timestamp`) that contain no
        // conversational entries — there is nothing worth importing, so
        // these are intentionally skipped rather than rescued. Surface the
        // dominant entry type so the operator can tell at a glance that
        // the file is not corrupted.
        let mut counts: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
        for e in entries {
            *counts.entry(e.entry_type.as_str()).or_default() += 1;
        }
        let summary = if counts.is_empty() {
            "<empty>".to_string()
        } else {
            let mut pairs: Vec<(&&str, &usize)> = counts.iter().collect();
            pairs.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
            pairs
                .into_iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(", ")
        };
        anyhow::bail!(
            "metadata-only session (no entries with a top-level timestamp; \
             types observed: {summary}) — nothing to import in {}",
            file_path.display()
        );
    }

    Ok(SessionInfo {
        session_id,
        cwd,
        git_branch,
        custom_title,
        project_hash,
        slug,
        created_at: min_ts,
        updated_at: max_ts,
    })
}

/// Parse a single JSON line into a `RawEntry`. Test-only because the
/// production JSONL reader (`stream_jsonl_file`) wraps the same logic
/// while also handling per-line error recovery; callers outside tests
/// should reach for that instead.
#[cfg(test)]
pub fn parse_raw_entry_str(json: &str) -> Result<RawEntry> {
    RawEntry::from_str(json).context("interpret JSON as RawEntry")
}

/// Coarse classification of which entry types should reach the
/// canonical block dispatcher. Drops orchestration-only events
/// (custom-title, agent-name, permission-mode, …) but retains
/// `attachment` so the canonical layer can decide per-subtype
/// whether to materialize it.
pub fn is_canonical_event_type(entry_type: &str) -> bool {
    matches!(entry_type, "user" | "assistant" | "system" | "attachment")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_user_entry() {
        let json = r#"{"type":"user","uuid":"abc-123","timestamp":"2026-04-17T11:28:13.321Z","sessionId":"sess-1","message":{"role":"user","content":"hello"}}"#;
        let entry = parse_raw_entry_str(json).unwrap();
        assert_eq!(entry.entry_type, "user");
        assert_eq!(entry.uuid.as_deref(), Some("abc-123"));
        assert!(entry.message.is_some());
    }

    #[test]
    fn test_parse_assistant_entry() {
        let json = r#"{"type":"assistant","uuid":"def-456","timestamp":"2026-04-17T11:28:14.000Z","message":{"role":"assistant","content":[{"type":"text","text":"hi"}]}}"#;
        let entry = parse_raw_entry_str(json).unwrap();
        assert_eq!(entry.entry_type, "assistant");
    }

    #[test]
    fn test_parse_system_entry() {
        let json = r#"{"type":"system","uuid":"ghi-789","timestamp":"2026-04-17T11:28:15.000Z","subtype":"compact_boundary","content":"boundary"}"#;
        let entry = parse_raw_entry_str(json).unwrap();
        assert_eq!(entry.entry_type, "system");
        assert_eq!(entry.subtype.as_deref(), Some("compact_boundary"));
    }

    #[test]
    fn test_parse_unknown_type() {
        let json = r#"{"type":"future-type","uuid":"xyz","timestamp":"2026-04-17T11:28:16.000Z","newField":"value"}"#;
        let entry = parse_raw_entry_str(json).unwrap();
        assert_eq!(entry.entry_type, "future-type");
    }

    #[test]
    fn test_parse_timestamp_millis() {
        let ts = parse_timestamp_millis("2026-04-17T11:28:13.321Z");
        assert!(ts.is_some());
        assert!(ts.unwrap() > 0);
    }

    #[test]
    fn test_is_canonical_event_type() {
        // Canonical path widens the historical user|assistant|system
        // gate to also admit `attachment`. Orchestration-only events
        // remain rejected at this layer; the canonical layer applies a
        // separate `--include-types` check downstream.
        assert!(is_canonical_event_type("user"));
        assert!(is_canonical_event_type("assistant"));
        assert!(is_canonical_event_type("system"));
        assert!(is_canonical_event_type("attachment"));
        assert!(!is_canonical_event_type("permission-mode"));
        assert!(!is_canonical_event_type("custom-title"));
    }

    /// Regression for the `raw.clone()` removal in `RawEntry::from_str`.
    /// The new path parses the input line twice (typed shape + raw
    /// value) so the typed-side fields must still match what the verbatim
    /// `raw` Value carries — otherwise downstream `extra()` /
    /// `as_raw_value()` consumers would see a desync.
    #[test]
    fn from_str_keeps_shape_and_raw_consistent() {
        let json = r#"{"type":"user","uuid":"u-1","timestamp":"2026-04-17T11:28:13.321Z","sessionId":"sess-1","message":{"role":"user","content":"hello"},"extraField":"forward-compat"}"#;
        let entry = RawEntry::from_str(json).unwrap();
        assert_eq!(entry.entry_type, "user");
        assert_eq!(entry.uuid.as_deref(), Some("u-1"));
        // Raw Value still carries unknown forward-compat fields the
        // typed shape does not name.
        assert_eq!(
            entry.extra("extraField"),
            Some(&serde_json::Value::String("forward-compat".to_string()))
        );
        // And the typed `message` matches the raw subtree byte-for-byte.
        let msg_raw = entry.as_raw_value().get("message").cloned().unwrap();
        assert_eq!(entry.message.as_ref(), Some(&msg_raw));
    }

    fn write_tmp_jsonl(lines: &[&str]) -> tempfile::NamedTempFile {
        use std::io::Write;
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        for line in lines {
            writeln!(tmp, "{line}").unwrap();
        }
        tmp.flush().unwrap();
        tmp
    }

    #[test]
    fn stream_jsonl_file_yields_in_order() {
        let tmp = write_tmp_jsonl(&[
            r#"{"type":"user","uuid":"u-1","timestamp":"2026-04-17T11:28:13.321Z"}"#,
            r#"{"type":"assistant","uuid":"a-1","timestamp":"2026-04-17T11:28:14.000Z"}"#,
            r#"{"type":"system","uuid":"s-1","timestamp":"2026-04-17T11:28:15.000Z","subtype":"x"}"#,
        ]);
        let entries: Vec<ParseItem> = stream_jsonl_file(tmp.path()).unwrap().collect();
        assert_eq!(entries.len(), 3);
        let uuids: Vec<_> = entries
            .iter()
            .map(|item| match item {
                ParseItem::Entry(e) => e.uuid.as_deref(),
                _ => panic!("expected Entry"),
            })
            .collect();
        assert_eq!(uuids, vec![Some("u-1"), Some("a-1"), Some("s-1")]);
    }

    #[test]
    fn stream_jsonl_file_handles_broken_and_blank_lines() {
        let tmp = write_tmp_jsonl(&[
            r#"{"type":"user","uuid":"u-1","timestamp":"2026-04-17T11:28:13.321Z"}"#,
            "this is not json at all",
            "",
            r#"{"type":"assistant","uuid":"a-1","timestamp":"2026-04-17T11:28:14.000Z"}"#,
        ]);
        let items: Vec<ParseItem> = stream_jsonl_file(tmp.path()).unwrap().collect();
        // Blank lines silently skipped; broken line surfaces as Broken.
        assert_eq!(items.len(), 3);
        assert!(matches!(items[0], ParseItem::Entry(_)));
        assert!(matches!(items[1], ParseItem::Broken { line_num: 2, .. }));
        assert!(matches!(items[2], ParseItem::Entry(_)));
    }

    #[test]
    fn parse_jsonl_file_matches_stream_collect() {
        // The shim must produce the same Entries as the stream (minus
        // Broken/blank). This guarantees existing source callers
        // continue to see identical input after the refactor.
        let tmp = write_tmp_jsonl(&[
            r#"{"type":"user","uuid":"u-1","timestamp":"2026-04-17T11:28:13.321Z"}"#,
            "garbage",
            r#"{"type":"assistant","uuid":"a-1","timestamp":"2026-04-17T11:28:14.000Z"}"#,
        ]);
        let via_shim = parse_jsonl_file(tmp.path()).unwrap();
        let via_stream: Vec<RawEntry> = stream_jsonl_file(tmp.path())
            .unwrap()
            .filter_map(|item| match item {
                ParseItem::Entry(e) => Some(e),
                ParseItem::Broken { .. } => None,
            })
            .collect();
        assert_eq!(via_shim.len(), 2);
        assert_eq!(via_shim.len(), via_stream.len());
        for (a, b) in via_shim.iter().zip(via_stream.iter()) {
            assert_eq!(a.entry_type, b.entry_type);
            assert_eq!(a.uuid, b.uuid);
        }
    }
}
