//! Common abstraction for source-specific importers.
//!
//! Each source (claude-code, codex, plain) implements `ChatSource` to
//! discover sessions and parse them into a uniform canonical
//! representation. The shared importer (`crate::common::importer`)
//! consumes `CanonicalSession` + `Vec<CanonicalEntry>` (legacy) or a
//! `CanonicalEntryStream` and writes them to the gRPC `ThreadApp`.
//! Spec §4.2 / §4.3.
//!
//! The crate-level `#![allow(dead_code)]` keeps the canonical type
//! definitions usable by unit tests even when individual field
//! accessors are exercised by only one source.
#![allow(dead_code)]

pub mod claude_code;
pub mod codex;
pub mod plain;

use protobuf::llm_memory::data::{ContentType, MessageRole};

/// Per-session metadata produced by a source. Shared by all entries
/// in the same session and used to build `ThreadData` + the default
/// label set. Spec §4.2.
#[derive(Debug, Clone)]
pub struct CanonicalSession {
    /// Source identity (`"claude_code"` / `"codex"` / `"obsidian"`
    /// etc.). Owned `String` because plain takes its source id from
    /// `--source-name` at runtime.
    pub source_id: String,
    /// Source-internal session identifier — the human-readable suffix
    /// used in `external_id` and `channel`.
    pub session_id: String,
    /// Fully-qualified channel string (`<source_id>:<...>`); the
    /// source assembles it because the layout differs per source
    /// (claude-code uses `claude_code:<sess>`, plain uses
    /// `<source>:file:<sha256>` etc., spec §5.3.4).
    pub channel: String,
    pub description: Option<String>,
    pub cwd: Option<String>,
    pub git_branch: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    /// Default label set computed by the source (e.g. `agent:codex`,
    /// `path:<cwd>` after `--strip-path-prefix`). Already truncated to
    /// 512 bytes via `common::labels` if applicable.
    pub source_labels: Vec<String>,
    /// Source-specific metadata, embedded verbatim under
    /// `metadata.session` per spec §4.2.1.
    pub source_metadata: serde_json::Value,
}

/// A single message-level record produced by a source. The shared
/// importer consumes these in `(import_order, timestamp_ms)` order
/// and converts them into `MemoryData`. Spec §4.2.
#[derive(Debug, Clone)]
pub struct CanonicalEntry {
    /// Source-local external ID. The source has already applied any prefix /
    /// hash / line_ordinal composition; the shared importer adds the thread
    /// creator namespace before writing it to the database.
    pub external_id: String,
    /// 0..N parent `external_id` pointers. Spec §4.2 / §4.2.2.6:
    /// each entry can carry **multiple** parents (e.g. tool_output
    /// links to both the corresponding tool_call and the previous
    /// conversation block). The shared importer resolves each entry
    /// via the eid_map and applies `MemoryId.value`-based order-
    /// preserving dedup before writing `MemoryData.parent_ids`. An
    /// empty `Vec` means thread-root.
    ///
    /// Convention: when an entry has both a tool link and a
    /// conversation link, the **tool link** goes at index 0 and the
    /// conversation link at index 1+ so the most semantically
    /// important relation is the first parent.
    pub parent_external_ids: Vec<String>,
    pub role: MessageRole,
    pub content_type: ContentType,
    pub content: String,
    /// Per-entry metadata (top-level keys). `source` / `kind` /
    /// `session` are reserved (spec §4.2.1).
    pub metadata: serde_json::Map<String, serde_json::Value>,
    pub timestamp_ms: i64,
    /// Source-decided ordering primary key, used for stable sorts
    /// before the import loop. Spec §4.2.
    pub import_order: i64,
    /// Coarse classification used for `--include-types` filtering and
    /// `metadata.kind` fields. e.g. `"user"`, `"assistant"`,
    /// `"tool_call"`, `"tool_output"`, `"reasoning"`, `"system"`.
    pub kind_tag: &'static str,
    /// Canonical reserved layer: helper-only top-level keys (spec
    /// §4.2.2.2). `run_import::build_memory_data` merges
    /// `canonical.tool` / `canonical.attachment` / `canonical.raw`
    /// into `MemoryData.metadata` after entry.metadata and before
    /// the fixed `source` / `kind` / `session` keys (3-stage
    /// merge per §4.2.1 / §4.2.2.2). Source parsers must not
    /// write these keys to `entry.metadata` directly; the run_import
    /// drops any reserved key found there.
    pub canonical: CanonicalAddons,
}

/// Canonical reserved layer (`metadata.tool` / `metadata.attachment` /
/// `metadata.raw`). Helper functions in `common/canonical` are the
/// only sanctioned source of values for these fields. `Default::default`
/// returns all `None`. Spec §4.2.2.2.
#[derive(Debug, Clone, Default)]
pub struct CanonicalAddons {
    /// `metadata.tool` for tool_call / tool_output entries (§4.2.2.3).
    /// `Some` only when `kind_tag in {"tool_call", "tool_output"}`.
    pub tool: Option<serde_json::Value>,
    /// `metadata.attachment` for attachment entries (§4.2.2.4).
    /// `Some` only when `kind_tag = "attachment"`.
    pub attachment: Option<serde_json::Value>,
    /// `metadata.raw.<source_id>` lossless escape hatch (§4.2.2.5).
    /// Optional for any kind. Helpers `raw_entry` / `merge_raw`
    /// build / merge this map; source parsers do not assemble it
    /// directly.
    pub raw: Option<serde_json::Map<String, serde_json::Value>>,
}

impl CanonicalAddons {
    /// Bundle a freshly-built canonical `tool` value with the original
    /// source payload (saved under `raw.<source_id>` for forensic
    /// recovery). Shared by codex and claude_code so the "set tool,
    /// then merge sanitized raw" pattern stays a one-liner without
    /// drifting between sources.
    pub fn with_tool(
        source_id: &str,
        tool: serde_json::Value,
        payload: &serde_json::Value,
    ) -> Self {
        let mut addons = Self {
            tool: Some(tool),
            ..Default::default()
        };
        crate::common::canonical::merge_raw(
            &mut addons.raw,
            crate::common::canonical::raw_entry(
                source_id,
                crate::common::canonical::sanitize_raw_payload(payload),
            ),
        );
        addons
    }

    /// Same as `with_tool` but for attachment entries.
    pub fn with_attachment(
        source_id: &str,
        attachment: serde_json::Value,
        payload: &serde_json::Value,
    ) -> Self {
        let mut addons = Self {
            attachment: Some(attachment),
            ..Default::default()
        };
        crate::common::canonical::merge_raw(
            &mut addons.raw,
            crate::common::canonical::raw_entry(
                source_id,
                crate::common::canonical::sanitize_raw_payload(payload),
            ),
        );
        addons
    }
}

/// Outcome of `ChatSource::read_session`. Lets the runner distinguish
/// "specification-incompatible session, skip without error" from "I/O
/// or parser failure, surface as session-level error".
pub enum ReadSessionOutcome {
    /// Normal path. The session is ready for the importer.
    ///
    /// Eager variant: the source produced the full entry list as a
    /// `Vec<CanonicalEntry>`. The runner wraps it into a
    /// `CanonicalEntryStream` before forwarding to the streaming
    /// importer, so the downstream code path stays uniform. No
    /// in-tree source emits this variant today; it is preserved for
    /// future sources that can't yield incrementally.
    Import {
        session: CanonicalSession,
        entries: Vec<CanonicalEntry>,
        /// Number of records the source filtered out via
        /// `--include-types` or input-shape rejections. Combined with
        /// since-filter results in the importer to populate
        /// `SessionResult.memories_skipped_filtered`. Spec §4.3.
        source_filtered_count: usize,
    },
    /// New variant. Sources that can yield entries incrementally use
    /// this so the importer never holds the full session in memory.
    /// `source_filtered_count_initial` carries the source-side filter
    /// total that the implementation already knows at the time of
    /// stream construction (e.g. type=permission-mode entries dropped
    /// before the canonical pass). Further filter decisions made while
    /// the stream is consumed surface as `StreamItem::Filtered`.
    ImportStream {
        session: CanonicalSession,
        entries: CanonicalEntryStream,
        source_filtered_count_initial: usize,
    },
    /// Skip without error (e.g. codex rollout missing session_meta,
    /// claude session with all-invalid timestamps). Counts as a
    /// processed session but does not increment `errors`.
    Skipped {
        session_id_hint: Option<String>,
        reason: String,
        filtered_count: u32,
    },
}

/// Owned iterator over canonical entries for a single session.
///
/// `Box<dyn Iterator>` rather than an async-stream because every
/// existing source parses synchronously (`BufReader::lines`) and the
/// importer's back-pressure boundary is the per-chunk `await` on
/// `add_memories_batch`, not the per-entry parse.
pub struct CanonicalEntryStream {
    iter: Box<dyn Iterator<Item = StreamItem> + Send>,
}

impl CanonicalEntryStream {
    pub fn new<I>(iter: I) -> Self
    where
        I: Iterator<Item = StreamItem> + Send + 'static,
    {
        Self {
            iter: Box::new(iter),
        }
    }

    /// Wrap an already-built `Vec<CanonicalEntry>` as a stream. Used by
    /// the runner-side shim that lifts the legacy `Import` variant onto
    /// the new pipeline without touching each source yet.
    pub fn from_vec(entries: Vec<CanonicalEntry>) -> Self {
        Self::new(entries.into_iter().map(StreamItem::Entry))
    }
}

impl Iterator for CanonicalEntryStream {
    type Item = StreamItem;
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// One item emitted by `CanonicalEntryStream`.
///
/// `Filtered` lets a source say "I dropped this record, count it
/// against `memories_skipped_filtered`" without forcing the importer to
/// touch source-specific logic. `Warn` is the same accounting-wise but
/// also surfaces a human-readable reason that the importer logs.
//
// `Entry` carries the canonical record (~hundreds of bytes including
// metadata Value); the diagnostic variants are tiny. We accept the size
// imbalance: this is the hot variant on every importable line and
// boxing it would add an allocation per record just to satisfy clippy.
#[allow(clippy::large_enum_variant)]
pub enum StreamItem {
    Entry(CanonicalEntry),
    /// Source-side filter dropped this record (e.g. `--include-types`
    /// mismatch). Importer adds to `memories_skipped_filtered` and
    /// stays silent.
    Filtered,
    /// Source-level non-fatal error (e.g. unreadable file in a
    /// per-dir plain session). Importer logs the reason and counts it
    /// as filtered.
    Warn(String),
}

/// Returns `true` only when `path` has a readable mtime strictly older
/// than `threshold_ms`. Any failure to read mtime — `metadata()` error,
/// pre-epoch timestamp, overflow — must fall through to the full-parse
/// path; otherwise a flaky stat could silently drop a session that
/// genuinely contains new entries (spec §1.2 R2).
pub fn file_mtime_strictly_older_than(path: &std::path::Path, threshold_ms: i64) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    let Ok(modified) = meta.modified() else {
        return false;
    };
    let Ok(dur) = modified.duration_since(std::time::UNIX_EPOCH) else {
        return false;
    };
    let ms = dur.as_millis();
    if ms > i64::MAX as u128 {
        return false;
    }
    (ms as i64) < threshold_ms
}

/// Build a `ReadSessionOutcome::Skipped` when the file's mtime is
/// strictly older than the `since - margin` threshold. The
/// `session_id_hint` falls back to the file's stem so the importer
/// summary attributes the skip to a recognisable session id.
pub fn mtime_skip_outcome(
    path: &std::path::Path,
    since_millis_with_margin: Option<i64>,
) -> Option<ReadSessionOutcome> {
    let threshold = since_millis_with_margin?;
    if !file_mtime_strictly_older_than(path, threshold) {
        return None;
    }
    Some(ReadSessionOutcome::Skipped {
        session_id_hint: path
            .file_stem()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string()),
        reason: format!("unchanged since {threshold}"),
        filtered_count: 0,
    })
}

/// Test helpers shared by every `ChatSource` implementation. Centralised
/// here because `set_file_mtime_ms` would otherwise live three times
/// over (claude_code / codex / plain) with identical bodies.
#[cfg(test)]
pub(crate) mod test_support {
    use super::{CanonicalEntry, CanonicalSession, ReadSessionOutcome, StreamItem};
    use std::path::Path;

    pub fn set_file_mtime_ms(path: &Path, mtime_ms: i64) {
        let dur = std::time::Duration::from_millis(mtime_ms.max(0) as u64);
        let target = std::time::UNIX_EPOCH + dur;
        let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.set_modified(target).unwrap();
    }

    /// Reduce a `read_session` outcome into the
    /// `(session, entries, source_filtered_count)` triple the source
    /// unit tests destructure. Both the legacy `Import` Vec variant and
    /// the streaming `ImportStream` variant fold into the same shape;
    /// `Filtered` / `Warn` items from the stream variant collapse into
    /// the same counter the `Import` variant carried as one integer.
    pub fn unpack_outcome(
        outcome: ReadSessionOutcome,
    ) -> (CanonicalSession, Vec<CanonicalEntry>, usize) {
        match outcome {
            ReadSessionOutcome::Import {
                session,
                entries,
                source_filtered_count,
            } => (session, entries, source_filtered_count),
            ReadSessionOutcome::ImportStream {
                session,
                entries,
                source_filtered_count_initial,
            } => {
                let mut filtered = source_filtered_count_initial;
                let drained: Vec<CanonicalEntry> = entries
                    .filter_map(|item| match item {
                        StreamItem::Entry(e) => Some(e),
                        StreamItem::Filtered | StreamItem::Warn(_) => {
                            filtered += 1;
                            None
                        }
                    })
                    .collect();
                (session, drained, filtered)
            }
            ReadSessionOutcome::Skipped { reason, .. } => {
                panic!("expected Import/ImportStream, got Skipped: {reason}")
            }
        }
    }

    pub fn entries_from_outcome(outcome: ReadSessionOutcome) -> Vec<CanonicalEntry> {
        unpack_outcome(outcome).1
    }

    pub fn session_from_outcome(outcome: ReadSessionOutcome) -> CanonicalSession {
        unpack_outcome(outcome).0
    }
}

/// A chat-history source.
///
/// Implementations are constructed once per CLI invocation
/// (subcommand args are owned internally) and `discover()` is called
/// once. The runner then iterates `read_session` over the discovered
/// inputs.
///
/// `Box<dyn ChatSource>` is **not** supported because of the
/// associated `SessionInput` type — the runner instead dispatches
/// generically per subcommand. Spec §4.3.
pub trait ChatSource {
    /// Owned input handle for one session — typically a `PathBuf` for
    /// JSONL-backed sources. Must not borrow from the source's
    /// internal args (no lifetime parameter on `discover`).
    type SessionInput;

    /// Stable source name (e.g. `"claude_code"`). Used by the runner
    /// only for log messages; the canonical session's `source_id` is
    /// the authoritative value written to memory metadata.
    fn id(&self) -> &str;

    /// Short label used to identify a session input in summary
    /// output / error logs (path basename, etc.).
    fn input_label(&self, input: &Self::SessionInput) -> String;

    /// Enumerate session inputs for the configured subcommand.
    fn discover(&self) -> anyhow::Result<Vec<Self::SessionInput>>;

    /// Parse one session input into canonical form.
    ///
    /// Source-side filtering (`--include-types`, malformed-record
    /// rejection) MUST be applied here — the runner only enforces
    /// `--since`. See `ReadSessionOutcome::Import.source_filtered_count`.
    ///
    /// `since_millis_with_margin` is the optional `since - margin`
    /// threshold for the session-level mtime filter (spec
    /// `agent-chat-import-incremental-spec.md` §1). Implementations MAY
    /// short-circuit with `ReadSessionOutcome::Skipped` when the
    /// underlying file's mtime is strictly older than this value, so
    /// callers avoid the cost of fully parsing an unchanged transcript.
    /// `None` (no `--since` given, or `--no-mtime-filter`) means the
    /// session MUST be fully parsed. Implementations that cannot read
    /// mtime — `metadata()` failure, missing file, in-memory inputs —
    /// MUST fall back to a full parse rather than skip silently.
    fn read_session(
        &self,
        input: &Self::SessionInput,
        since_millis_with_margin: Option<i64>,
    ) -> anyhow::Result<ReadSessionOutcome>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn missing_file_never_marks_strictly_older() {
        // Stat failure must not cause a skip, regardless of how
        // aggressive the caller's threshold is — guarantees the
        // parse-anyway fallback in spec §1.2 R2.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("nope.txt");
        assert!(!file_mtime_strictly_older_than(&missing, i64::MAX));
    }

    #[test]
    fn mtime_skip_outcome_inactive_when_threshold_none() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.txt");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "x").unwrap();
        assert!(mtime_skip_outcome(&path, None).is_none());
    }
}
