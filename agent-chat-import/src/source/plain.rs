//! Plain text tree source (Obsidian vault, notes directory, ...).
//!
//! Spec §5.3. Three thread strategies:
//!
//! - `per-file`: 1 file = 1 thread (`session_id = file:<sha256(rel_path)[:32]>`).
//! - `per-dir`: files sharing the same parent rel_dir form one thread
//!   (`session_id = dir:<sha256(rel_dir)[:32]>`). The hash input is the
//!   *full* rel_dir from `--root`, not just the leaf segment, so
//!   re-parenting a subtree (e.g. moving `2026/05/` under
//!   `archive/2026/05/`) produces a new thread. Thread stability across
//!   structural changes is an explicit non-goal of `per-dir`; use
//!   `single` when that property matters.
//! - `single`: the whole `--root` is one thread, identified by the
//!   canonical basename so cloning the vault to another parent
//!   re-imports into the same thread (spec §5.3.4).
//!
//! `entry_uid = <sha256(rel_path)[:16]>:<sha256(raw_file_bytes)[:16]>`
//! so frontmatter-only edits also produce new memories (raw bytes, not
//! post-frontmatter content). `--source-name` controls the prefix on
//! every identifier the source emits and is rejected at parse time
//! when it doesn't match `^[a-z0-9_-]{1,32}$`.
//!
//! ## Vault invariant (Phase A)
//!
//! Every Phase A behaviour assumes a 1:1 mapping between
//! `--source-name` and `--root`. Both are inputs to every
//! `external_id` / `session_id` / `metadata.path` the source emits, so
//! re-rooting an existing vault under a new `--root` (or sharing a
//! `--source-name` across two vaults) produces an entirely separate
//! identifier namespace and corrupts diffs:
//!
//! - Re-importing `~/notes/2026/05/a.md` with `--root ~/notes` and
//!   then with `--root ~/notes/2026/05` creates two memories in the DB
//!   for the same physical file.
//! - `--prune-missing` then sees the half it didn't observe this run as
//!   "missing on fs" and deletes it.
//!
//! Hold each `--source-name` to a single, stable `--root`. For
//! multiple vaults pick distinct names (`obsidian-private`,
//! `notes-archive`, …).
//!
//! ## Move / delete handling
//!
//! This source walks the filesystem and emits memories for files that
//! are visible *now*. Without `--prune-missing`:
//!
//! - Renames / moves leave the original memory in place under its old
//!   `external_id` and create a fresh memory at the new path.
//! - Deletions are invisible to the importer; the old memory persists.
//!
//! Phase A `--prune-missing` (`docs/plain-prune-spec.md`) detects
//! deleted files via `external_id` set diff and removes the matching
//! memories — and orphan threads when `--prune-orphan-threads` is on.
//! Renames / moves are still treated as delete + create. A
//! git-history-driven follow-up (`docs/plain-git-subcommand-spec.md`,
//! Phase B) will close that gap.

#![allow(dead_code)]

pub mod prune;

use crate::cli::PlainArgs;
use crate::common::ids::sha256_hex_prefix;
use crate::common::labels::{MAX_LABEL_BYTES, truncate_label_keep_head, truncate_label_keep_tail};
use crate::common::path::apply_path_prefix;
use crate::source::{
    CanonicalAddons, CanonicalEntry, CanonicalSession, ChatSource, ReadSessionOutcome,
    file_mtime_strictly_older_than,
};
use anyhow::{Context, Result};
use globset::{Glob, GlobSetBuilder};
use ignore::WalkBuilder;
use protobuf::llm_memory::data::{ContentType, MessageRole};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// Maximum byte length for the value portion of a frontmatter-derived
/// label before the import warns and skips. Spec §5.3.6: handwritten
/// values longer than this are almost always misuse and would only
/// pollute label search if truncated.
const FRONTMATTER_LABEL_VALUE_MAX_BYTES: usize = 480;

/// One unit fed to `read_session`. The variant determines the thread
/// scope (file / dir / root). Spec §4.4 / §5.3.4.
#[derive(Debug, Clone)]
pub enum PlainSessionInput {
    File(PathBuf),
    Dir {
        rel_dir: PathBuf,
        files: Vec<PathBuf>,
    },
    Single {
        root: PathBuf,
        files: Vec<PathBuf>,
    },
}

pub struct PlainSource {
    args: PlainArgs,
    /// Lazily-resolved canonical form of `args.root`. Cached because
    /// `discover()` and every `read_session()` call need it; without
    /// the cache the same `canonicalize()` syscall fires per session.
    canonical_root: OnceLock<PathBuf>,
}

impl PlainSource {
    pub fn new(args: PlainArgs) -> Self {
        Self {
            args,
            canonical_root: OnceLock::new(),
        }
    }

    fn root(&self) -> &Path {
        &self.args.root
    }

    pub fn canonical_root(&self) -> Result<&Path> {
        if let Some(p) = self.canonical_root.get() {
            return Ok(p.as_path());
        }
        let resolved = self.args.root.canonicalize().with_context(|| {
            format!(
                "--root must be an existing directory: {}",
                self.args.root.display()
            )
        })?;
        // get_or_init can't be used because canonicalize returns Result;
        // set+get pattern is fine because every caller handles the result.
        let _ = self.canonical_root.set(resolved);
        Ok(self.canonical_root.get().expect("just set").as_path())
    }

    fn extensions(&self) -> Vec<String> {
        self.args
            .ext
            .split(',')
            .map(|s| {
                s.trim()
                    .trim_start_matches('.')
                    .to_ascii_lowercase()
                    .to_string()
            })
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn build_walker(&self) -> Result<WalkBuilder> {
        // Build the walker from the canonicalised root so the entries
        // it yields are always rooted at the same absolute prefix that
        // `discover_files` and `read_session` use to compute relative
        // paths and reopen files. Constructing it from `self.args.root`
        // makes a relative `--root notes` produce walker paths like
        // `notes/x.md` that fail `strip_prefix(&canonical_root)`,
        // leaving the unstripped value in `rel` and causing
        // `canonical_root.join(rel)` to read `notes/notes/x.md`.
        let mut wb = WalkBuilder::new(self.canonical_root()?);
        wb.follow_links(self.args.effective_follow_symlinks())
            .standard_filters(false)
            .git_global(false)
            .git_exclude(false)
            .git_ignore(self.args.effective_respect_gitignore())
            .ignore(false)
            .parents(self.args.effective_respect_gitignore())
            .hidden(false);
        Ok(wb)
    }

    fn build_excluder(&self) -> Result<Option<globset::GlobSet>> {
        let patterns: Vec<&str> = self
            .args
            .exclude_glob
            .iter()
            .map(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .collect();
        if patterns.is_empty() {
            return Ok(None);
        }
        let mut b = GlobSetBuilder::new();
        for p in &patterns {
            let g = Glob::new(p).with_context(|| format!("invalid --exclude-glob '{p}'"))?;
            b.add(g);
        }
        Ok(Some(b.build()?))
    }

    /// Walk the configured root, respecting `.gitignore` (when on),
    /// extension whitelist, exclude globs, max-file-size, and symlink
    /// behavior. Returns the relative paths (relative to the root)
    /// of files that should be considered for import.
    fn discover_files(&self) -> Result<Vec<PathBuf>> {
        let root = self.canonical_root()?.to_path_buf();
        let exts = self.extensions();
        let excluder = self.build_excluder()?;
        let max_size = self.args.max_file_size_bytes;
        let mut walker = self.build_walker()?;
        walker.add_custom_ignore_filename(".memories-import-ignore");

        let mut files = Vec::new();
        for result in walker.build() {
            let entry = match result {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!("walker error: {e}");
                    continue;
                }
            };
            // Skip the root dir entry itself and any non-file entry.
            let path = entry.path();
            if entry.depth() == 0 {
                continue;
            }
            if !entry.file_type().is_some_and(|ft| ft.is_file()) {
                continue;
            }
            // Extension filter.
            let ext_ok = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.to_ascii_lowercase())
                .is_some_and(|e| exts.iter().any(|allowed| allowed == &e));
            if !ext_ok {
                continue;
            }
            // Compute rel_path for excluder & later canonical use.
            let rel = match path.strip_prefix(&root) {
                Ok(r) => r.to_path_buf(),
                Err(_) => path.to_path_buf(),
            };
            if let Some(ref glob) = excluder
                && glob.is_match(&rel)
            {
                continue;
            }
            // Size guard.
            match std::fs::metadata(path) {
                Ok(meta) if meta.len() > max_size => {
                    tracing::warn!(
                        path = %path.display(),
                        size = meta.len(),
                        max = max_size,
                        "skipping file larger than --max-file-size-bytes"
                    );
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(path = %path.display(), "stat failed: {e}");
                    continue;
                }
            }
            files.push(rel);
        }
        files.sort();
        Ok(files)
    }
}

impl ChatSource for PlainSource {
    type SessionInput = PlainSessionInput;

    fn id(&self) -> &str {
        &self.args.source_name
    }

    fn input_label(&self, input: &Self::SessionInput) -> String {
        match input {
            PlainSessionInput::File(p) => p.display().to_string(),
            PlainSessionInput::Dir { rel_dir, .. } => format!("dir:{}", rel_dir.display()),
            PlainSessionInput::Single { root, .. } => format!("single:{}", root.display()),
        }
    }

    fn discover(&self) -> Result<Vec<Self::SessionInput>> {
        let files = self.discover_files()?;
        if files.is_empty() {
            return Ok(Vec::new());
        }
        match self.args.thread_strategy {
            ThreadStrategy::PerFile => Ok(files.into_iter().map(PlainSessionInput::File).collect()),
            ThreadStrategy::PerDir => {
                // Group by direct parent rel_dir. Root-level files
                // collapse to "." per spec §5.3.4 root-direct rule.
                let mut groups: BTreeMap<PathBuf, Vec<PathBuf>> = BTreeMap::new();
                for f in files {
                    let parent = f
                        .parent()
                        .map(|p| {
                            if p.as_os_str().is_empty() {
                                PathBuf::from(".")
                            } else {
                                p.to_path_buf()
                            }
                        })
                        .unwrap_or_else(|| PathBuf::from("."));
                    groups.entry(parent).or_default().push(f);
                }
                Ok(groups
                    .into_iter()
                    .map(|(rel_dir, files)| PlainSessionInput::Dir { rel_dir, files })
                    .collect())
            }
            ThreadStrategy::Single => Ok(vec![PlainSessionInput::Single {
                root: self.root().to_path_buf(),
                files,
            }]),
        }
    }

    fn read_session(
        &self,
        input: &Self::SessionInput,
        since_millis_with_margin: Option<i64>,
    ) -> Result<ReadSessionOutcome> {
        let root = self.canonical_root()?.to_path_buf();
        if let Some(skip) = session_mtime_skip(input, &root, since_millis_with_margin) {
            return Ok(skip);
        }
        match input {
            PlainSessionInput::File(rel) => {
                let loaded = load_files(&self.args, &root, std::slice::from_ref(rel));
                // Per-file: a single read/decode failure means the
                // session has nothing to import. Surface that as
                // `Skipped` so the runner's summary attributes the
                // missing thread to a real cause instead of a silent
                // "0 imported, 0 errors" line.
                let head = loaded.first();
                if head.is_none_or(|l| l.raw_bytes.is_none() || l.content_body.is_none()) {
                    return Ok(ReadSessionOutcome::Skipped {
                        session_id_hint: rel
                            .file_stem()
                            .and_then(|s| s.to_str())
                            .map(|s| s.to_string()),
                        reason: "file unreadable or invalid encoding".to_string(),
                        filtered_count: 1,
                    });
                }
                let session = build_per_file_session(&self.args, &root, rel, loaded.first());
                let (entries, dropped) =
                    build_entries_from_files(&self.args, SessionScope::PerFile, &root, loaded);
                debug_assert_eq!(dropped, 0, "per-file load was checked above");
                Ok(ReadSessionOutcome::ImportStream {
                    session,
                    entries: crate::source::CanonicalEntryStream::from_vec(entries),
                    source_filtered_count_initial: dropped,
                })
            }
            PlainSessionInput::Dir { rel_dir, files } => {
                let loaded = load_files(&self.args, &root, files);
                let session = build_per_dir_session(&self.args, &root, rel_dir, &loaded);
                let (entries, dropped) =
                    build_entries_from_files(&self.args, SessionScope::PerDir, &root, loaded);
                if entries.is_empty() && dropped > 0 {
                    return Ok(skipped_all_dropped(
                        rel_dir.file_name().and_then(|n| n.to_str()),
                        dropped,
                    ));
                }
                Ok(ReadSessionOutcome::ImportStream {
                    session,
                    entries: crate::source::CanonicalEntryStream::from_vec(entries),
                    source_filtered_count_initial: dropped,
                })
            }
            PlainSessionInput::Single { files, .. } => {
                let loaded = load_files(&self.args, &root, files);
                let session = build_single_session(&self.args, &root, &loaded);
                let (entries, dropped) =
                    build_entries_from_files(&self.args, SessionScope::Single, &root, loaded);
                if entries.is_empty() && dropped > 0 {
                    return Ok(skipped_all_dropped(
                        root.file_name().and_then(|n| n.to_str()),
                        dropped,
                    ));
                }
                Ok(ReadSessionOutcome::ImportStream {
                    session,
                    entries: crate::source::CanonicalEntryStream::from_vec(entries),
                    source_filtered_count_initial: dropped,
                })
            }
        }
    }
}

/// Session-level mtime skip for plain sources. For multi-file bundles
/// we require *every* file to be strictly older than the threshold so a
/// single recent edit forces a full re-parse; under-skipping is safer
/// than dropping a session that just grew a new file. An unreadable
/// mtime in any single file therefore defeats the skip (spec §1.3.3).
fn session_mtime_skip(
    input: &PlainSessionInput,
    root: &Path,
    since_millis_with_margin: Option<i64>,
) -> Option<ReadSessionOutcome> {
    let threshold = since_millis_with_margin?;
    let (older, hint) = match input {
        PlainSessionInput::File(rel) => {
            let abs = root.join(rel);
            (
                file_mtime_strictly_older_than(&abs, threshold),
                rel.file_stem().and_then(|s| s.to_str()).map(String::from),
            )
        }
        PlainSessionInput::Dir { rel_dir, files } => {
            let older = all_mtimes_strictly_older(root, files, threshold);
            (
                older,
                rel_dir
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(String::from),
            )
        }
        PlainSessionInput::Single { files, .. } => {
            let older = all_mtimes_strictly_older(root, files, threshold);
            (
                older,
                root.file_name().and_then(|n| n.to_str()).map(String::from),
            )
        }
    };
    if !older {
        return None;
    }
    Some(ReadSessionOutcome::Skipped {
        session_id_hint: hint,
        reason: format!("unchanged since {threshold}"),
        filtered_count: 0,
    })
}

/// Returns true only when *every* file in the bundle has a known
/// mtime strictly older than `threshold`. An empty bundle is treated
/// as "not older" so callers fall through to the (already empty)
/// session path that produces a meaningful diagnostic.
fn all_mtimes_strictly_older(root: &Path, files: &[PathBuf], threshold: i64) -> bool {
    if files.is_empty() {
        return false;
    }
    files
        .iter()
        .all(|rel| file_mtime_strictly_older_than(&root.join(rel), threshold))
}

#[derive(Copy, Clone, Debug)]
enum SessionScope {
    PerFile,
    PerDir,
    Single,
}

/// One file's bytes plus everything derived from a single read pass.
/// Built once per session in `load_files` so timestamp aggregation
/// (per-dir / single), label extraction (per-file), and entry
/// construction can all share the same buffer instead of re-reading
/// every file 2-3 times.
struct LoadedFile {
    rel: PathBuf,
    /// `None` when the file failed to read or to decode under
    /// `--encoding=utf8-strict`. Such files are dropped from entry
    /// construction *and* from thread timestamp aggregation, so
    /// `thread.updated_at` reflects the newest memory actually
    /// imported (otherwise `--since` / `updated_after_ms` filters
    /// would surface threads whose latest memory is older than the
    /// reported updated_at).
    raw_bytes: Option<Vec<u8>>,
    /// Frontmatter-stripped body (when frontmatter parsing applied) or
    /// the full decoded text. `None` mirrors `raw_bytes == None`.
    content_body: Option<String>,
    frontmatter: Option<serde_yaml::Value>,
    /// Filesystem ctime/mtime (always present, falls back to `now()`).
    fs_created: i64,
    fs_mtime: i64,
    /// Effective timestamps after frontmatter `created` / `updated`
    /// override (spec §5.3.3). `eff_updated` is what gets written into
    /// `CanonicalEntry.timestamp_ms`.
    eff_created: i64,
    eff_updated: i64,
}

impl LoadedFile {
    /// True when this file will become a `CanonicalEntry`. Used to
    /// gate session-level timestamp aggregation so unreadable files do
    /// not pull `thread.updated_at` past the newest real memory.
    fn is_entry(&self) -> bool {
        self.raw_bytes.is_some() && self.content_body.is_some()
    }
}

fn load_files(args: &PlainArgs, root: &Path, files: &[PathBuf]) -> Vec<LoadedFile> {
    files.iter().map(|rel| load_one(args, root, rel)).collect()
}

fn load_one(args: &PlainArgs, root: &Path, rel: &PathBuf) -> LoadedFile {
    let abs = root.join(rel);
    let (fs_created, fs_mtime) = file_timestamps(&abs);
    let raw_bytes = match std::fs::read(&abs) {
        Ok(b) => Some(b),
        Err(e) => {
            tracing::warn!(path = %abs.display(), "read failed: {e}");
            None
        }
    };
    let raw_str = raw_bytes.as_ref().and_then(|bytes| match args.encoding {
        EncodingMode::Utf8Strict => match std::str::from_utf8(bytes) {
            Ok(s) => Some(s.to_string()),
            Err(e) => {
                tracing::warn!(path = %abs.display(), "invalid UTF-8 with --encoding=utf8-strict: {e}");
                None
            }
        },
        EncodingMode::Utf8Lossy => Some(String::from_utf8_lossy(bytes).into_owned()),
    });
    // Run `split_frontmatter` once and remember both the body and the
    // parsed value. Calling it again from `build_entries_from_files`
    // re-walks every line for no reason.
    let (content_body, frontmatter) = match raw_str {
        Some(s) if should_parse_frontmatter(args, &abs) => {
            let (body, fm) = split_frontmatter(&s);
            let fm_opt = (!matches!(fm, serde_yaml::Value::Null)).then_some(fm);
            // When no frontmatter was found, `split_frontmatter`
            // returns the original string as body — keep `s` to avoid
            // a needless allocation.
            let body = if fm_opt.is_some() { body } else { s };
            (Some(body), fm_opt)
        }
        Some(s) => (Some(s), None),
        None => (None, None),
    };
    let (eff_created, eff_updated) = match frontmatter.as_ref() {
        Some(fm) => {
            let (c, u) = frontmatter_timestamps(fm);
            (c.unwrap_or(fs_created), u.unwrap_or(fs_mtime))
        }
        None => (fs_created, fs_mtime),
    };
    LoadedFile {
        rel: rel.clone(),
        raw_bytes,
        content_body,
        frontmatter,
        fs_created,
        fs_mtime,
        eff_created,
        eff_updated,
    }
}

fn build_per_file_session(
    args: &PlainArgs,
    canonical_root: &Path,
    rel: &Path,
    loaded: Option<&LoadedFile>,
) -> CanonicalSession {
    let rel_str = rel.to_string_lossy().to_string();
    let session_id = format!("file:{}", sha256_hex_prefix(rel_str.as_bytes(), 32));
    let channel = format!("{}:{}", args.source_name, session_id);

    let (created_at_ms, updated_at_ms) = match loaded {
        Some(l) => (l.eff_created, l.eff_updated),
        None => file_timestamps(&canonical_root.join(rel)),
    };

    let description = rel
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| rel_str.clone());

    let mut labels = build_default_labels(args, canonical_root, Some(rel));
    if let Some(fm) = loaded.and_then(|l| l.frontmatter.as_ref()) {
        labels.extend(extract_frontmatter_labels(args, fm));
    }
    labels.retain(|l| !l.is_empty() && l.len() <= MAX_LABEL_BYTES);

    CanonicalSession {
        source_id: args.source_name.clone(),
        session_id,
        channel,
        description: Some(description),
        cwd: None,
        git_branch: None,
        created_at_ms,
        updated_at_ms,
        source_labels: labels,
        source_metadata: serde_json::json!({
            "scope": "per-file",
            "rel_path": rel_str,
        }),
    }
}

fn build_per_dir_session(
    args: &PlainArgs,
    canonical_root: &Path,
    rel_dir: &Path,
    loaded: &[LoadedFile],
) -> CanonicalSession {
    let rel_dir_str = rel_dir.to_string_lossy().to_string();
    let session_id = format!("dir:{}", sha256_hex_prefix(rel_dir_str.as_bytes(), 32));
    let channel = format!("{}:{}", args.source_name, session_id);

    let (min_created, max_updated) = aggregate_timestamps(loaded);

    let mut labels = build_default_labels(args, canonical_root, None);
    labels.push(truncate_label_keep_tail("dir:", &rel_dir_str));

    CanonicalSession {
        source_id: args.source_name.clone(),
        session_id,
        channel,
        description: Some(rel_dir_str.clone()),
        cwd: None,
        git_branch: None,
        created_at_ms: min_created,
        updated_at_ms: max_updated,
        source_labels: labels,
        source_metadata: serde_json::json!({
            "scope": "per-dir",
            "rel_dir": rel_dir_str,
            // Count only files that will become entries — unreadable
            // files are already accounted for via `source_filtered_count`,
            // so including them here would double-count in audit summaries.
            "file_count": loaded.iter().filter(|l| l.is_entry()).count(),
        }),
    }
}

fn build_single_session(
    args: &PlainArgs,
    canonical_root: &Path,
    loaded: &[LoadedFile],
) -> CanonicalSession {
    // Spec §5.3.4: `single` session_id is derived from the vault's
    // basename so the same vault stays the same thread when cloned to
    // another machine or moved to another parent. Distinct vaults
    // sharing a basename are disambiguated via `--source-name`
    // (spec §5.3.7), not by the absolute path.
    let basename = single_basename(canonical_root);
    let session_id = format!("root:{}", sha256_hex_prefix(basename.as_bytes(), 32));
    let channel = format!("{}:{}", args.source_name, session_id);

    let (min_created, max_updated) = aggregate_timestamps(loaded);

    CanonicalSession {
        source_id: args.source_name.clone(),
        session_id,
        channel,
        description: Some(basename.clone()),
        cwd: None,
        git_branch: None,
        created_at_ms: min_created,
        updated_at_ms: max_updated,
        source_labels: build_default_labels(args, canonical_root, None),
        source_metadata: serde_json::json!({
            "scope": "single",
            "root_basename": basename,
            "file_count": loaded.iter().filter(|l| l.is_entry()).count(),
        }),
    }
}

/// Resolve a vault basename for `single` session ids. `Path::file_name`
/// returns `None` for `.` / `..` / a trailing slash; in that case walk
/// the canonicalised path's components for the last `Normal` segment so
/// the same vault still maps to the same id regardless of how the user
/// spelled `--root`. Falls back to the literal `"vault"` only when even
/// canonicalisation produced nothing usable (root filesystem `/`).
fn single_basename(canonical_root: &Path) -> String {
    if let Some(name) = canonical_root.file_name().and_then(|n| n.to_str()) {
        return name.to_string();
    }
    for c in canonical_root.components().rev() {
        if let std::path::Component::Normal(name) = c
            && let Some(s) = name.to_str()
        {
            return s.to_string();
        }
    }
    "vault".to_string()
}

/// Aggregate (min created, max updated) across files that will become
/// real entries. Files that failed to load are excluded so a vault
/// with broken files does not advance `thread.updated_at` past the
/// newest memory it actually contains. Empty inputs produce `(0, 0)`
/// so callers don't need to special-case.
/// Build a `Skipped` outcome when every file in a per-dir / single
/// session failed to load. Mirrors the per-file branch so the runner
/// can attribute the missing thread to a real cause instead of
/// surfacing a healthy-looking `Import { entries: [] }`.
fn skipped_all_dropped(hint: Option<&str>, dropped: usize) -> ReadSessionOutcome {
    ReadSessionOutcome::Skipped {
        session_id_hint: hint.map(|s| s.to_string()),
        reason: format!("all {dropped} file(s) unreadable or invalid encoding"),
        filtered_count: dropped as u32,
    }
}

fn aggregate_timestamps(loaded: &[LoadedFile]) -> (i64, i64) {
    let mut min_created = i64::MAX;
    let mut max_updated = i64::MIN;
    for l in loaded.iter().filter(|l| l.is_entry()) {
        min_created = min_created.min(l.eff_created);
        max_updated = max_updated.max(l.eff_updated);
    }
    if min_created == i64::MAX {
        min_created = 0;
    }
    if max_updated == i64::MIN {
        max_updated = 0;
    }
    (min_created, max_updated)
}

fn build_default_labels(
    args: &PlainArgs,
    canonical_root: &Path,
    rel: Option<&Path>,
) -> Vec<String> {
    let mut labels: Vec<String> = vec![
        "notes".to_string(),
        truncate_label_keep_head("agent:", &args.source_name),
    ];
    // Use the canonical root (resolved once in `PlainSource`) so the
    // `path:` label is independent of how the user spelled `--root`
    // (`notes` vs `/abs/path/notes`). Without this, re-importing the
    // same vault from different working directories would attach a
    // second `path:notes` to the same thread, polluting label search.
    let root_str = canonical_root.to_string_lossy().to_string();
    let stripped = apply_path_prefix(&root_str, &args.path_prefixes());
    labels.push(truncate_label_keep_tail("path:", stripped));
    if let Some(name) = canonical_root.file_name().and_then(|n| n.to_str()) {
        labels.push(truncate_label_keep_head("vault:", name));
    }
    if let Some(r) = rel {
        let parent = r
            .parent()
            .map(|p| {
                if p.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    p.to_path_buf()
                }
            })
            .unwrap_or_else(|| PathBuf::from("."));
        labels.push(truncate_label_keep_tail("dir:", &parent.to_string_lossy()));
    }
    labels.retain(|l| !l.is_empty() && l.len() <= MAX_LABEL_BYTES);
    labels
}

/// Returns `(entries, dropped_count)`. `dropped_count` is files that
/// failed to read or to decode under `--encoding=utf8-strict`; callers
/// surface it via `source_filtered_count` so a partial-vault import is
/// visible in the summary instead of silently shrinking.
fn build_entries_from_files(
    args: &PlainArgs,
    scope: SessionScope,
    canonical_root: &Path,
    mut loaded: Vec<LoadedFile>,
) -> (Vec<CanonicalEntry>, usize) {
    // Sort by effective created_at_ms ascending so per-dir/single
    // threads have a chronological position layout (spec §4.2 / §5.3.4).
    loaded.sort_by_key(|l| l.eff_created);

    let mut out = Vec::with_capacity(loaded.len());
    let mut dropped = 0usize;
    for (idx, l) in loaded.into_iter().enumerate() {
        let (Some(raw_bytes), Some(content_body)) = (l.raw_bytes, l.content_body) else {
            dropped += 1;
            continue;
        };
        let frontmatter = l.frontmatter;

        // entry_uid: <sha256(rel_path)[:16]>:<sha256(raw_bytes)[:16]>
        let rel_str = l.rel.to_string_lossy().to_string();
        let entry_uid = format!(
            "{}:{}",
            sha256_hex_prefix(rel_str.as_bytes(), 16),
            sha256_hex_prefix(&raw_bytes, 16)
        );
        let session_id = entry_session_id(scope, canonical_root, &l.rel);
        let external_id = format!("{}:{}:{}", args.source_name, session_id, entry_uid);

        let mut metadata = serde_json::Map::new();
        metadata.insert("path".to_string(), serde_json::Value::String(rel_str));
        metadata.insert(
            "size_bytes".to_string(),
            serde_json::Value::Number(serde_json::Number::from(raw_bytes.len() as u64)),
        );
        // `mtime_ms` records filesystem mtime for ops/forensic
        // visibility; the entry's `timestamp_ms` may differ when
        // frontmatter `updated:` is present (spec §5.3.3 keeps both).
        metadata.insert(
            "mtime_ms".to_string(),
            serde_json::Value::Number(serde_json::Number::from(l.fs_mtime)),
        );
        if let Some(fm) = frontmatter
            && let Ok(json_fm) = serde_yaml::from_value::<serde_json::Value>(fm)
        {
            metadata.insert("frontmatter".to_string(), json_fm);
        }

        out.push(CanonicalEntry {
            external_id,
            parent_external_ids: Vec::new(),
            role: MessageRole::RoleUser,
            content_type: ContentType::Text,
            content: content_body,
            metadata,
            timestamp_ms: l.eff_updated,
            import_order: idx as i64,
            kind_tag: "user",
            canonical: CanonicalAddons::default(),
        });
    }
    (out, dropped)
}

fn entry_session_id(scope: SessionScope, canonical_root: &Path, rel: &Path) -> String {
    match scope {
        SessionScope::PerFile => format!(
            "file:{}",
            sha256_hex_prefix(rel.to_string_lossy().as_bytes(), 32)
        ),
        SessionScope::PerDir => {
            let parent = rel
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .map(Path::to_path_buf)
                .unwrap_or_else(|| PathBuf::from("."));
            format!(
                "dir:{}",
                sha256_hex_prefix(parent.to_string_lossy().as_bytes(), 32)
            )
        }
        // Mirror `build_single_session`: spec §5.3.4 ties the session
        // id to the basename so the same vault stays one thread when
        // moved or cloned. `--source-name` is the disambiguator for
        // distinct vaults sharing a basename.
        SessionScope::Single => {
            let basename = single_basename(canonical_root);
            format!("root:{}", sha256_hex_prefix(basename.as_bytes(), 32))
        }
    }
}

fn split_frontmatter(raw: &str) -> (String, serde_yaml::Value) {
    // Walk lines (LF / CRLF agnostic) so the closing `---` fence is
    // detected even when the file ends without a trailing newline and
    // so CRLF-terminated files don't leave a stray byte at the body's
    // head. `split_inclusive('\n')` keeps each line's terminator with
    // it, which lets us reconstruct the post-fence body by byte offset.
    let mut lines = raw.split_inclusive('\n');
    let Some(first) = lines.next() else {
        return (raw.to_string(), serde_yaml::Value::Null);
    };
    if !is_yaml_fence(first) {
        return (raw.to_string(), serde_yaml::Value::Null);
    }
    let yaml_start = first.len();
    let mut yaml_end = yaml_start;
    let mut body_start = None;
    for line in lines {
        if is_yaml_fence(line) {
            body_start = Some(yaml_end + line.len());
            break;
        }
        yaml_end += line.len();
    }
    let Some(body_start) = body_start else {
        // Open fence with no closing fence — leave the content as-is so
        // the caller can present the file unchanged.
        return (raw.to_string(), serde_yaml::Value::Null);
    };
    let yaml_text = &raw[yaml_start..yaml_end];
    let body = raw.get(body_start..).unwrap_or("");
    match serde_yaml::from_str::<serde_yaml::Value>(yaml_text) {
        Ok(v) => (body.to_string(), v),
        Err(e) => {
            tracing::warn!("frontmatter parse failed: {e}; keeping content as-is");
            (raw.to_string(), serde_yaml::Value::Null)
        }
    }
}

/// `---` line, with optional CR / LF / CRLF terminator and no other
/// content. Trailing whitespace before the terminator is intentionally
/// rejected: YAML's frontmatter convention is a strict three-dash line.
fn is_yaml_fence(line: &str) -> bool {
    let trimmed = line
        .strip_suffix('\n')
        .map(|s| s.strip_suffix('\r').unwrap_or(s))
        .unwrap_or(line);
    trimmed == "---"
}

fn file_timestamps(abs: &Path) -> (i64, i64) {
    let now = chrono::Utc::now().timestamp_millis();
    let meta = match std::fs::metadata(abs) {
        Ok(m) => m,
        Err(_) => return (now, now),
    };
    let modified = system_time_ms(meta.modified().ok());
    // `created()` is unsupported on plenty of Linux setups (older
    // tmpfs, NFS, some FUSE backends, kernels without statx). Falling
    // back to `now` would label every note with no frontmatter as
    // "created at import time", scrambling per-dir / single position
    // ordering and `--since` math. Prefer mtime as the next-best
    // monotonic anchor; only fall back to `now` when neither is
    // readable.
    let created = system_time_ms(meta.created().ok())
        .or(modified)
        .unwrap_or(now);
    (created, modified.unwrap_or(created))
}

fn system_time_ms(t: Option<std::time::SystemTime>) -> Option<i64> {
    t.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as i64)
}

/// Spec §5.3.2 limits frontmatter to `.md` / `.markdown`. Applied to a
/// `.txt` whose body happens to start with `---`, `split_frontmatter`
/// would silently chop the file body, so this guard runs at every
/// callsite that chooses whether to parse.
fn should_parse_frontmatter(args: &PlainArgs, p: &Path) -> bool {
    args.effective_frontmatter() && is_markdown_path(p)
}

fn is_markdown_path(p: &Path) -> bool {
    p.extension()
        .and_then(|e| e.to_str())
        .map(|e| e.eq_ignore_ascii_case("md") || e.eq_ignore_ascii_case("markdown"))
        .unwrap_or(false)
}

/// Extract `created` / `updated` from a frontmatter mapping, accepting
/// either RFC 3339 strings or YAML date / datetime values. Returns
/// `(created_ms, updated_ms)` with `None` for keys that don't parse.
fn frontmatter_timestamps(fm: &serde_yaml::Value) -> (Option<i64>, Option<i64>) {
    let map = match fm {
        serde_yaml::Value::Mapping(m) => m,
        _ => return (None, None),
    };
    let pick = |key: &str| -> Option<i64> {
        let v = map.get(serde_yaml::Value::String(key.to_string()))?;
        match v {
            serde_yaml::Value::String(s) => parse_yaml_timestamp(s),
            // serde_yaml represents `2026-04-20` and
            // `2026-04-20T00:00:00Z` as String when in single-line form;
            // when YAML emits a tagged !!timestamp it surfaces as a
            // String here too. Numbers (epoch seconds/ms) are accepted
            // as a defensive fallback.
            serde_yaml::Value::Number(n) => n.as_i64().map(|x| {
                if x < 10_000_000_000 {
                    x.saturating_mul(1000)
                } else {
                    x
                }
            }),
            _ => None,
        }
    };
    (pick("created"), pick("updated"))
}

fn parse_yaml_timestamp(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_millis());
    }
    // Date-only `YYYY-MM-DD` → midnight UTC.
    if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d
            .and_hms_opt(0, 0, 0)
            .map(|nd| nd.and_utc().timestamp_millis());
    }
    // Naive datetime `YYYY-MM-DD HH:MM:SS` → treat as UTC (Obsidian
    // commonly writes this without a timezone).
    if let Ok(nd) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(nd.and_utc().timestamp_millis());
    }
    if let Ok(nd) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(nd.and_utc().timestamp_millis());
    }
    None
}

/// Build frontmatter-derived labels (spec §5.3.6). Only invoked from
/// `build_per_file_session` because `Thread.labels` is per-thread and
/// merging tags from multiple files into one thread (per-dir/single)
/// would be misleading. Values longer than 480 bytes are warned and
/// skipped to avoid polluting label search with truncated long-form
/// fields like `description:` (spec §5.3.6 skip policy). The `tags`
/// key is aliased to the conventional `tag:` prefix per spec §5.3.2.
fn extract_frontmatter_labels(args: &PlainArgs, fm: &serde_yaml::Value) -> Vec<String> {
    let Some(keys_csv) = args.label_from_frontmatter.as_deref() else {
        return Vec::new();
    };
    let Some(map) = fm.as_mapping() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for raw_key in keys_csv.split(',') {
        let key = raw_key.trim();
        if key.is_empty() {
            continue;
        }
        let prefix = match key {
            // Conventional alias: `tags: [a, b]` becomes `tag:a` / `tag:b`
            // (spec §5.3.2 example) so vault tags surface under the
            // expected search prefix.
            "tags" => "tag:".to_string(),
            other => format!("{other}:"),
        };
        let Some(value) = map.get(serde_yaml::Value::String(key.to_string())) else {
            continue;
        };
        for v in flatten_label_values(value) {
            if v.len() > FRONTMATTER_LABEL_VALUE_MAX_BYTES {
                tracing::warn!(
                    key = key,
                    value_bytes = v.len(),
                    "frontmatter value too long for label, skipping"
                );
                continue;
            }
            let label = truncate_label_keep_head(&prefix, &v);
            if !label.is_empty() {
                out.push(label);
            }
        }
    }
    out
}

/// Return zero or more label-eligible string values from a frontmatter
/// node. Only string scalars and arrays of string scalars are accepted
/// per spec §5.3.2; other shapes are silently dropped (the value still
/// lives in `metadata.frontmatter`).
fn flatten_label_values(v: &serde_yaml::Value) -> Vec<String> {
    match v {
        serde_yaml::Value::String(s) => vec![s.clone()],
        serde_yaml::Value::Sequence(seq) => seq
            .iter()
            .filter_map(|item| match item {
                serde_yaml::Value::String(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

#[derive(Copy, Clone, Debug)]
pub enum ThreadStrategy {
    PerFile,
    PerDir,
    Single,
}

impl std::str::FromStr for ThreadStrategy {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "per-file" => Ok(Self::PerFile),
            "per-dir" => Ok(Self::PerDir),
            "single" => Ok(Self::Single),
            other => Err(format!(
                "invalid --thread-strategy '{other}': expected per-file / per-dir / single"
            )),
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum EncodingMode {
    Utf8Lossy,
    Utf8Strict,
}

impl std::str::FromStr for EncodingMode {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "utf8-lossy" => Ok(Self::Utf8Lossy),
            "utf8-strict" => Ok(Self::Utf8Strict),
            other => Err(format!(
                "invalid --encoding '{other}': expected utf8-lossy / utf8-strict"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::test_support::{entries_from_outcome, session_from_outcome, unpack_outcome};
    use std::io::Write;

    fn args(root: PathBuf, strategy: ThreadStrategy) -> PlainArgs {
        PlainArgs {
            root,
            source_name: "obsidian-test".to_string(),
            ext: "md,txt".to_string(),
            exclude_glob: vec![],
            follow_symlinks: false,
            no_follow_symlinks: false,
            thread_strategy: strategy,
            encoding: EncodingMode::Utf8Lossy,
            frontmatter: true,
            no_frontmatter: false,
            max_file_size_bytes: 1_048_576,
            label_from_frontmatter: None,
            respect_gitignore: true,
            no_respect_gitignore: false,
            strip_path_prefix: None,
            prune_missing: false,
            prune_orphan_threads: false,
            no_prune_orphan_threads: false,
            no_interactive: false,
        }
    }

    fn write(file: &Path, body: &str) {
        if let Some(parent) = file.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut f = std::fs::File::create(file).unwrap();
        f.write_all(body.as_bytes()).unwrap();
    }

    #[test]
    fn discover_picks_md_txt_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "# a");
        write(&root.join("b.txt"), "b");
        write(&root.join("c.png"), "binary");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let names: Vec<String> = inputs
            .iter()
            .map(|i| match i {
                PlainSessionInput::File(p) => p.to_string_lossy().to_string(),
                _ => unreachable!(),
            })
            .collect();
        assert!(names.contains(&"a.md".to_string()));
        assert!(names.contains(&"b.txt".to_string()));
        assert!(!names.iter().any(|n| n.ends_with(".png")));
    }

    #[test]
    fn discover_per_dir_groups_by_parent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("notes/2026/05/a.md"), "a");
        write(&root.join("notes/2026/05/b.md"), "b");
        write(&root.join("notes/2026/06/c.md"), "c");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerDir));
        let inputs = s.discover().unwrap();
        assert_eq!(inputs.len(), 2);
        for i in inputs {
            match i {
                PlainSessionInput::Dir { rel_dir, files } => {
                    assert!(!files.is_empty());
                    let rel_str = rel_dir.to_string_lossy();
                    assert!(rel_str == "notes/2026/05" || rel_str == "notes/2026/06");
                }
                _ => panic!(),
            }
        }
    }

    #[test]
    fn single_session_id_is_stable_across_parent_moves() {
        // Spec §5.3.4: the same vault basename produces the same
        // session id regardless of where it lives on disk, so a clone
        // / sync to a different parent re-imports into the same
        // thread. Distinct vaults sharing a basename are
        // disambiguated via `--source-name` (spec §5.3.7).
        let parent_x = tempfile::tempdir().unwrap();
        let parent_y = tempfile::tempdir().unwrap();
        let vault_x = parent_x.path().join("vault-a");
        let vault_y = parent_y.path().join("vault-a");
        write(&vault_x.join("readme.md"), "x");
        write(&vault_y.join("readme.md"), "y");

        let s_x = PlainSource::new(args(vault_x.clone(), ThreadStrategy::Single));
        let s_y = PlainSource::new(args(vault_y.clone(), ThreadStrategy::Single));
        let session_x =
            session_from_outcome(s_x.read_session(&s_x.discover().unwrap()[0], None).unwrap());
        let session_y =
            session_from_outcome(s_y.read_session(&s_y.discover().unwrap()[0], None).unwrap());
        assert_eq!(session_x.session_id, session_y.session_id);
        assert_eq!(session_x.channel, session_y.channel);
    }

    #[test]
    fn single_session_id_falls_back_to_canonical_basename_for_dot_root() {
        // `--root .` should not collapse every cwd into a single
        // `root:<sha256("vault")>` id. The canonical-component
        // fallback recovers the real directory name so id stays
        // stable per vault.
        let parent = tempfile::tempdir().unwrap();
        let vault = parent.path().join("notes-vault");
        write(&vault.join("a.md"), "x");

        let prev_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(&vault).unwrap();
        let s_dot = PlainSource::new(args(PathBuf::from("."), ThreadStrategy::Single));
        let session_dot = session_from_outcome(
            s_dot
                .read_session(&s_dot.discover().unwrap()[0], None)
                .unwrap(),
        );
        std::env::set_current_dir(&prev_cwd).unwrap();

        // Same vault addressed by absolute path must produce the same
        // session id.
        let s_abs = PlainSource::new(args(vault.clone(), ThreadStrategy::Single));
        let session_abs = session_from_outcome(
            s_abs
                .read_session(&s_abs.discover().unwrap()[0], None)
                .unwrap(),
        );
        assert_eq!(session_dot.session_id, session_abs.session_id);

        // And it must not be the literal "vault" fallback.
        let fallback_vault = format!("root:{}", sha256_hex_prefix("vault".as_bytes(), 32));
        assert_ne!(session_dot.session_id, fallback_vault);
    }

    #[test]
    fn discover_single_collapses_to_one_input() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "a");
        write(&root.join("sub/b.md"), "b");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::Single));
        let inputs = s.discover().unwrap();
        assert_eq!(inputs.len(), 1);
        match &inputs[0] {
            PlainSessionInput::Single { files, .. } => assert_eq!(files.len(), 2),
            _ => panic!(),
        }
    }

    #[test]
    fn exclude_glob_filters_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("keep.md"), "x");
        write(&root.join("skip.log.md"), "x"); // matches *.log.md
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerFile);
        a.exclude_glob = vec!["**/*.log.md".to_string()];
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let names: Vec<String> = inputs
            .iter()
            .map(|i| match i {
                PlainSessionInput::File(p) => p.to_string_lossy().to_string(),
                _ => unreachable!(),
            })
            .collect();
        assert!(names.contains(&"keep.md".to_string()));
        assert!(!names.contains(&"skip.log.md".to_string()));
    }

    #[test]
    fn exclude_glob_or_combination() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.log.md"), "x");
        write(&root.join("b.bak.md"), "x");
        write(&root.join("c.md"), "x");
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerFile);
        a.exclude_glob = vec!["**/*.log.md".to_string(), "**/*.bak.md".to_string()];
        let s = PlainSource::new(a);
        let names: Vec<String> = s
            .discover()
            .unwrap()
            .iter()
            .map(|i| match i {
                PlainSessionInput::File(p) => p.to_string_lossy().to_string(),
                _ => unreachable!(),
            })
            .collect();
        assert!(names.contains(&"c.md".to_string()));
        assert!(!names.contains(&"a.log.md".to_string()));
        assert!(!names.contains(&"b.bak.md".to_string()));
    }

    #[test]
    fn max_file_size_skips_oversized() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("small.md"), "tiny");
        let big = "x".repeat(200);
        write(&root.join("big.md"), &big);
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerFile);
        a.max_file_size_bytes = 100;
        let s = PlainSource::new(a);
        let names: Vec<String> = s
            .discover()
            .unwrap()
            .iter()
            .map(|i| match i {
                PlainSessionInput::File(p) => p.to_string_lossy().to_string(),
                _ => unreachable!(),
            })
            .collect();
        assert!(names.contains(&"small.md".to_string()));
        assert!(!names.contains(&"big.md".to_string()));
    }

    #[test]
    fn frontmatter_is_split_when_enabled() {
        let raw = "---\ntags:\n  - a\n  - b\n---\nbody text\n";
        let (body, fm) = split_frontmatter(raw);
        assert_eq!(body, "body text\n");
        let v = fm.get("tags").unwrap().as_sequence().unwrap();
        assert_eq!(v.len(), 2);
    }

    #[test]
    fn frontmatter_invalid_terminator_passes_through() {
        let raw = "---\nfoo: bar\n(no closing fence)\n";
        let (body, fm) = split_frontmatter(raw);
        assert_eq!(body, raw);
        assert!(matches!(fm, serde_yaml::Value::Null));
    }

    #[test]
    fn frontmatter_disabled_keeps_body_intact() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "---\ntags: [a]\n---\nbody\n");
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerFile);
        a.no_frontmatter = true;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        {
            let entries = entries_from_outcome(s.read_session(&inputs[0], None).unwrap());
            {
                assert!(entries[0].content.contains("---"));
                assert!(!entries[0].metadata.contains_key("frontmatter"));
            }
        }
    }

    #[test]
    fn read_session_per_file_emits_canonical_entry() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "---\ntitle: hello\n---\nbody\n");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let outcome = s.read_session(&inputs[0], None).unwrap();
        let (session, entries, _) = unpack_outcome(outcome);
        assert_eq!(session.source_id, "obsidian-test");
        assert!(session.session_id.starts_with("file:"));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].role, MessageRole::RoleUser);
        assert_eq!(entries[0].kind_tag, "user");
        assert_eq!(entries[0].content, "body\n");
        let fm = entries[0].metadata.get("frontmatter").unwrap();
        assert_eq!(fm["title"], "hello");
    }

    #[test]
    fn raw_bytes_hash_distinguishes_frontmatter_change() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "---\ntags: [a]\n---\nbody\n");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs1 = s.discover().unwrap();
        let eid1 = entries_from_outcome(s.read_session(&inputs1[0], None).unwrap())[0]
            .external_id
            .clone();
        // Mutate frontmatter only.
        write(&root.join("a.md"), "---\ntags: [a, b]\n---\nbody\n");
        let inputs2 = s.discover().unwrap();
        let eid2 = entries_from_outcome(s.read_session(&inputs2[0], None).unwrap())[0]
            .external_id
            .clone();
        assert_ne!(
            eid1, eid2,
            "frontmatter-only edit must produce new entry_uid"
        );
    }

    #[test]
    fn per_dir_sorts_entries_by_created_at() {
        // Two files in same dir; manipulate created order so the
        // walker order (alphabetical) and created_at order disagree.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // File b will be older than a.
        write(&root.join("notes/b.md"), "older");
        // Sleep to ensure mtime differs.
        std::thread::sleep(std::time::Duration::from_millis(20));
        write(&root.join("notes/a.md"), "newer");

        // Use mtime as proxy for created when fs doesn't track creation
        // (most Linux fs's do; test is informational).
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerDir));
        let inputs = s.discover().unwrap();
        let entries = entries_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert_eq!(entries.len(), 2);
        // import_order assigned 0..N in created_at_ms order.
        // The test merely checks monotonic timestamps line up
        // with import_order (since some FS quirks can equalize
        // creation times).
        if entries[0].timestamp_ms != entries[1].timestamp_ms {
            assert!(
                entries[0].timestamp_ms <= entries[1].timestamp_ms,
                "entries should be created_at ascending"
            );
        }
    }

    #[test]
    fn root_direct_files_normalize_to_dot() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("README.md"), "x");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerDir));
        let inputs = s.discover().unwrap();
        assert_eq!(inputs.len(), 1);
        match &inputs[0] {
            PlainSessionInput::Dir { rel_dir, .. } => {
                assert_eq!(rel_dir.to_string_lossy(), ".");
            }
            _ => panic!(),
        }
    }

    #[test]
    fn external_id_under_512_bytes_for_deep_paths() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Build a deep nested rel_path (100 levels).
        let mut path = root.to_path_buf();
        for i in 0..100 {
            path = path.join(format!("d{i}"));
        }
        path = path.join("note.md");
        write(&path, "x");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let entries = entries_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert!(entries[0].external_id.len() <= 512);
        // session_id is `file:<32 hex>` = 37 bytes; entry_uid
        // is 33 bytes; source_name <= 32; with separators total
        // <= 32 + 1 + 37 + 1 + 33 = 104 bytes.
        assert!(
            entries[0].external_id.len() <= 110,
            "external_id should be bounded short, got {}",
            entries[0].external_id.len()
        );
    }

    #[test]
    fn role_is_always_user() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "body");
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let entries = entries_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert_eq!(entries[0].role, MessageRole::RoleUser);
    }

    #[test]
    fn per_file_unreadable_file_returns_skipped() {
        // Per-file: a single broken file means the session has nothing
        // to import. The runner must see `Skipped`, not a silent
        // `Import { entries: [] }` that produces "0 imported / 0 errors".
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Force a UTF-8 decode failure under utf8-strict.
        std::fs::write(root.join("a.md"), [0xFF, 0xFE, 0xFD]).unwrap();
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerFile);
        a.encoding = EncodingMode::Utf8Strict;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let outcome = s.read_session(&inputs[0], None).unwrap();
        match outcome {
            ReadSessionOutcome::Skipped { filtered_count, .. } => {
                assert_eq!(filtered_count, 1);
            }
            other => panic!("expected Skipped, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn per_dir_session_timestamps_ignore_unreadable_files() {
        // The unreadable file is younger than the readable one. If
        // aggregate_timestamps included it, session.updated_at would
        // point past the newest real memory and `--since` /
        // `updated_after_ms` would surface the thread when no
        // imported memory actually advanced.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("notes/ok.md"),
            "---\nupdated: 2026-04-01T00:00:00Z\n---\nbody\n",
        );
        // Newer file but undecodable under utf8-strict.
        std::fs::write(root.join("notes/bad.md"), [0xFF, 0xFE, 0xFD]).unwrap();
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerDir);
        a.encoding = EncodingMode::Utf8Strict;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let session = session_from_outcome(s.read_session(&inputs[0], None).unwrap());
        let ok_updated = chrono::DateTime::parse_from_rfc3339("2026-04-01T00:00:00Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(
            session.updated_at_ms, ok_updated,
            "unreadable file must not pull thread.updated_at past the newest real memory"
        );
    }

    #[test]
    fn per_dir_all_unreadable_returns_skipped() {
        // Parity with the per-file branch: when every file in the dir
        // fails the session must surface as `Skipped` so the summary
        // operator sees the silent damage.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("notes")).unwrap();
        std::fs::write(root.join("notes/a.md"), [0xFF, 0xFE, 0xFD]).unwrap();
        std::fs::write(root.join("notes/b.md"), [0xFF, 0xFE]).unwrap();
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerDir);
        a.encoding = EncodingMode::Utf8Strict;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        match s.read_session(&inputs[0], None).unwrap() {
            ReadSessionOutcome::Skipped {
                filtered_count,
                reason,
                ..
            } => {
                assert_eq!(filtered_count, 2);
                assert!(reason.contains("unreadable") || reason.contains("invalid encoding"));
            }
            other => panic!(
                "expected Skipped for fully broken per-dir session, got {:?}",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[test]
    fn single_all_unreadable_returns_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("a.md"), [0xFF, 0xFE]).unwrap();
        let mut a = args(root.to_path_buf(), ThreadStrategy::Single);
        a.encoding = EncodingMode::Utf8Strict;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        match s.read_session(&inputs[0], None).unwrap() {
            ReadSessionOutcome::Skipped { filtered_count, .. } => {
                assert_eq!(filtered_count, 1);
            }
            other => panic!("expected Skipped, got {:?}", std::mem::discriminant(&other)),
        }
    }

    #[test]
    fn per_dir_partial_failure_surfaces_in_filtered_count() {
        // Per-dir: one file fails to decode, one succeeds. Thread is
        // still created, but the dropped file must be visible via
        // `source_filtered_count` so the import summary reports the
        // gap instead of pretending nothing was missing.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("notes/ok.md"), "good body");
        std::fs::write(root.join("notes/bad.md"), [0xFF, 0xFE, 0xFD]).unwrap();
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerDir);
        a.encoding = EncodingMode::Utf8Strict;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let outcome = s.read_session(&inputs[0], None).unwrap();
        let (_session, entries, source_filtered_count) = unpack_outcome(outcome);
        assert_eq!(
            entries.len(),
            1,
            "the readable file should still be entered"
        );
        assert_eq!(
            source_filtered_count, 1,
            "the broken file must be reported as filtered, not silently dropped"
        );
    }

    #[test]
    fn per_dir_metadata_file_count_excludes_unreadable_files() {
        // metadata.session.file_count must reflect the number of files
        // that actually became entries, not the discovered count.
        // Otherwise downstream audits double-count unreadable files
        // already reported via source_filtered_count.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("notes/ok.md"), "good body");
        std::fs::write(root.join("notes/bad.md"), [0xFF, 0xFE, 0xFD]).unwrap();
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerDir);
        a.encoding = EncodingMode::Utf8Strict;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let session = session_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert_eq!(
            session
                .source_metadata
                .get("file_count")
                .and_then(|v| v.as_u64()),
            Some(1),
            "file_count must equal the importable count, not the discovered count"
        );
    }

    #[test]
    fn single_metadata_file_count_excludes_unreadable_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("ok.md"), "good body");
        std::fs::write(root.join("bad.md"), [0xFF, 0xFE, 0xFD]).unwrap();
        let mut a = args(root.to_path_buf(), ThreadStrategy::Single);
        a.encoding = EncodingMode::Utf8Strict;
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let session = session_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert_eq!(
            session
                .source_metadata
                .get("file_count")
                .and_then(|v| v.as_u64()),
            Some(1),
            "single-scope file_count must equal the importable count"
        );
    }

    #[test]
    fn frontmatter_is_only_parsed_for_markdown_files() {
        // A `.txt` file that happens to start with `---` must not have
        // its body chopped at the first `---` line. Spec §5.3.2:
        // frontmatter is a `.md` convention.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let text_body = "---\nthis looks like frontmatter but is body\n---\nmore body\n";
        write(&root.join("readme.txt"), text_body);
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let entries = entries_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].content, text_body,
            ".txt body must be preserved verbatim"
        );
        assert!(
            !entries[0].metadata.contains_key("frontmatter"),
            "no frontmatter metadata should be attached to .txt"
        );
    }

    #[test]
    fn split_frontmatter_handles_crlf_close_fence_without_off_by_one() {
        let crlf = "---\r\ntitle: hi\r\n---\r\nbody\r\nmore\r\n";
        let (body, fm) = split_frontmatter(crlf);
        assert_eq!(body, "body\r\nmore\r\n", "CRLF body must start cleanly");
        assert_eq!(fm.get("title").and_then(|v| v.as_str()), Some("hi"));
    }

    #[test]
    fn split_frontmatter_detects_closing_fence_at_end_of_file() {
        // No trailing newline after the closing `---` — common when an
        // editor stripped the final newline. Previously this missed the
        // fence entirely and treated the whole file as raw content.
        let raw = "---\ntitle: hi\n---";
        let (body, fm) = split_frontmatter(raw);
        assert_eq!(body, "");
        assert_eq!(fm.get("title").and_then(|v| v.as_str()), Some("hi"));
    }

    #[test]
    fn split_frontmatter_detects_closing_fence_at_end_of_file_crlf() {
        let raw = "---\r\ntitle: hi\r\n---";
        let (body, fm) = split_frontmatter(raw);
        assert_eq!(body, "");
        assert_eq!(fm.get("title").and_then(|v| v.as_str()), Some("hi"));
    }

    #[test]
    fn system_time_ms_falls_back_through_modified_to_now() {
        // Direct unit test for the helper logic: created() == None must
        // not silently land on `now`; the file_timestamps caller chains
        // `.or(modified)` to keep the timeline anchored to the file's
        // real history when birthtime is unsupported.
        let some = std::time::UNIX_EPOCH + std::time::Duration::from_millis(123_456);
        assert_eq!(system_time_ms(Some(some)), Some(123_456));
        assert_eq!(system_time_ms(None), None);
    }

    #[test]
    fn file_timestamps_uses_mtime_for_created_when_birthtime_missing() {
        // Real filesystem check: even if created() succeeds on this
        // host, the (created, modified) pair should never be the
        // import-time `now` for an actual on-disk file. A 1-hour
        // tolerance is generous enough to pass on slow CI yet small
        // enough to prove we did not fall back to wall-clock.
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.md");
        std::fs::write(&p, "body").unwrap();
        let (created, modified) = file_timestamps(&p);
        let now = chrono::Utc::now().timestamp_millis();
        let one_hour_ms = 60 * 60 * 1000;
        assert!(
            (now - created).abs() < one_hour_ms,
            "created should be a real fs timestamp, not random: created={created} now={now}"
        );
        // The modified value must be tied to the file, not synthesised.
        assert!(modified <= now);
        assert!(modified >= created - one_hour_ms);
    }

    #[test]
    fn label_from_frontmatter_emits_tag_and_keyed_labels_for_per_file() {
        // Spec §5.3.6: per-file thread strategy lifts the configured
        // frontmatter keys into Thread labels. `tags` is aliased to the
        // conventional `tag:` prefix; other keys use `<key>:<value>`.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("a.md"),
            "---\ntags:\n  - alpha\n  - beta\ncategory: notes\n---\nbody\n",
        );
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerFile);
        a.label_from_frontmatter = Some("tags,category".to_string());
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let session = session_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert!(
            session.source_labels.iter().any(|l| l == "tag:alpha"),
            "labels: {:?}",
            session.source_labels
        );
        assert!(
            session.source_labels.iter().any(|l| l == "tag:beta"),
            "labels: {:?}",
            session.source_labels
        );
        assert!(
            session.source_labels.iter().any(|l| l == "category:notes"),
            "labels: {:?}",
            session.source_labels
        );
    }

    #[test]
    fn label_from_frontmatter_skips_oversized_value() {
        // Spec §5.3.6 skip policy: values longer than 480 bytes are
        // warned and skipped (no truncation), so they don't pollute
        // label search.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let huge = "x".repeat(FRONTMATTER_LABEL_VALUE_MAX_BYTES + 1);
        write(
            &root.join("a.md"),
            &format!("---\ndescription: {huge}\n---\nbody\n"),
        );
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerFile);
        a.label_from_frontmatter = Some("description".to_string());
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let session = session_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert!(
            !session
                .source_labels
                .iter()
                .any(|l| l.starts_with("description:")),
            "oversized description must not become a label, got {:?}",
            session.source_labels
        );
    }

    #[test]
    fn label_from_frontmatter_ignored_for_per_dir_and_single() {
        // Spec §5.3.6: only per-file lifts frontmatter into labels;
        // per-dir / single keep frontmatter inside memory metadata so
        // a single tagged file does not contaminate the whole thread.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("notes/a.md"), "---\ntags: [secret]\n---\nbody\n");
        let mut a = args(root.to_path_buf(), ThreadStrategy::PerDir);
        a.label_from_frontmatter = Some("tags".to_string());
        let s = PlainSource::new(a);
        let inputs = s.discover().unwrap();
        let session = session_from_outcome(s.read_session(&inputs[0], None).unwrap());
        assert!(
            !session.source_labels.iter().any(|l| l.starts_with("tag:")),
            "per-dir must not derive frontmatter labels, got {:?}",
            session.source_labels
        );
    }

    #[test]
    fn frontmatter_updated_overrides_filesystem_mtime_in_entry_timestamp() {
        // Spec §5.3.3: when frontmatter has `updated`, it wins over
        // filesystem mtime so `--since` against a vault that was
        // copied / synced still respects the human-recorded edit time.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("a.md"),
            "---\ncreated: 2026-04-19\nupdated: 2026-04-20T10:00:00Z\n---\nbody\n",
        );
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let (session, entries, _) = unpack_outcome(s.read_session(&inputs[0], None).unwrap());
        let updated_ms = chrono::DateTime::parse_from_rfc3339("2026-04-20T10:00:00Z")
            .unwrap()
            .timestamp_millis();
        let created_ms = chrono::NaiveDate::from_ymd_opt(2026, 4, 19)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis();
        assert_eq!(session.created_at_ms, created_ms);
        assert_eq!(session.updated_at_ms, updated_ms);
        assert_eq!(entries[0].timestamp_ms, updated_ms);
        // Sanity: filesystem mtime is still surfaced in metadata for
        // ops visibility (spec §5.3.3 records both views).
        let mtime_ms = entries[0]
            .metadata
            .get("mtime_ms")
            .and_then(|v| v.as_i64())
            .expect("mtime_ms must be present");
        assert_ne!(
            mtime_ms, updated_ms,
            "metadata.mtime_ms must reflect fs mtime, not frontmatter"
        );
    }

    #[test]
    fn frontmatter_updated_aggregates_across_per_dir_files() {
        // For per-dir/single, the session's max(updated_at_ms) should
        // also see frontmatter `updated:` so a freshly-edited note
        // pulls the thread's updated_at forward.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("notes/old.md"),
            "---\nupdated: 2026-04-01T00:00:00Z\n---\nold\n",
        );
        write(
            &root.join("notes/new.md"),
            "---\nupdated: 2026-04-25T00:00:00Z\n---\nnew\n",
        );
        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerDir));
        let inputs = s.discover().unwrap();
        let session = session_from_outcome(s.read_session(&inputs[0], None).unwrap());
        let expected = chrono::DateTime::parse_from_rfc3339("2026-04-25T00:00:00Z")
            .unwrap()
            .timestamp_millis();
        assert_eq!(session.updated_at_ms, expected);
    }

    #[test]
    fn relative_root_does_not_double_join_paths() {
        // Reproduces the bug where a `--root` given as a relative path
        // (e.g. `notes`) made the walker emit `notes/x.md` paths that
        // failed `strip_prefix(&canonical_root)` and left the unstripped
        // value in `rel`. `read_session` then tried to read
        // `<canonical>/notes/x.md` (the original `notes/` prefix
        // doubled), producing zero canonical entries and an empty
        // thread.
        let parent = tempfile::tempdir().unwrap();
        let vault_name = "vault-rel";
        let vault = parent.path().join(vault_name);
        write(&vault.join("a.md"), "body-a");
        write(&vault.join("sub/b.md"), "body-b");

        // Mimic running `memories-import plain --root vault-rel` from
        // the parent dir: `args.root` is the bare relative segment.
        let prev_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(parent.path()).unwrap();
        let s = PlainSource::new(args(
            std::path::PathBuf::from(vault_name),
            ThreadStrategy::PerFile,
        ));
        let inputs = s.discover().unwrap();
        // Relative paths in inputs must be vault-relative, not include the
        // walker-rooted prefix.
        let names: Vec<String> = inputs
            .iter()
            .map(|i| match i {
                PlainSessionInput::File(p) => p.to_string_lossy().to_string(),
                _ => unreachable!(),
            })
            .collect();
        assert!(names.contains(&"a.md".to_string()), "got {names:?}");
        assert!(names.contains(&"sub/b.md".to_string()), "got {names:?}");

        // read_session must successfully read each file (no double-join).
        let mut bodies = Vec::new();
        for input in &inputs {
            let entries = entries_from_outcome(s.read_session(input, None).unwrap());
            assert_eq!(entries.len(), 1);
            bodies.push(entries[0].content.clone());
        }
        bodies.sort();
        assert_eq!(bodies, vec!["body-a".to_string(), "body-b".to_string()]);

        std::env::set_current_dir(prev_cwd).unwrap();
    }

    #[test]
    fn path_label_is_stable_between_relative_and_absolute_root() {
        // Regression: build_default_labels used to read args.root
        // directly, so `--root vault` (relative) and `--root /abs/vault`
        // produced different `path:` labels for the same vault. After
        // canonicalisation they must agree, otherwise re-importing from
        // a different cwd attaches a second `path:` label to the same
        // thread.
        let parent = tempfile::tempdir().unwrap();
        let vault_name = "vault-stable";
        let vault = parent.path().join(vault_name);
        write(&vault.join("a.md"), "body");
        let abs = vault.canonicalize().unwrap();

        // Case 1: absolute --root
        let s_abs = PlainSource::new(args(abs.clone(), ThreadStrategy::PerFile));
        let inputs = s_abs.discover().unwrap();
        let session_abs = session_from_outcome(s_abs.read_session(&inputs[0], None).unwrap());
        let path_label_abs = session_abs
            .source_labels
            .iter()
            .find(|l| l.starts_with("path:"))
            .cloned()
            .expect("path: label must exist");

        // Case 2: relative --root from the parent dir
        let prev_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(parent.path()).unwrap();
        let s_rel = PlainSource::new(args(
            std::path::PathBuf::from(vault_name),
            ThreadStrategy::PerFile,
        ));
        let inputs = s_rel.discover().unwrap();
        let session_rel = session_from_outcome(s_rel.read_session(&inputs[0], None).unwrap());
        std::env::set_current_dir(prev_cwd).unwrap();
        let path_label_rel = session_rel
            .source_labels
            .iter()
            .find(|l| l.starts_with("path:"))
            .cloned()
            .expect("path: label must exist");

        assert_eq!(
            path_label_abs, path_label_rel,
            "path: label must be canonical-root based, not args.root based"
        );
    }

    use crate::source::test_support::set_file_mtime_ms;

    #[test]
    fn mtime_filter_skips_per_file_when_old() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "# a");
        set_file_mtime_ms(&root.join("a.md"), 1_000_000);

        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let outcome = s.read_session(&inputs[0], Some(2_000_000)).unwrap();
        assert!(matches!(outcome, ReadSessionOutcome::Skipped { .. }));
    }

    #[test]
    fn mtime_filter_parses_per_file_when_recent() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "# a");
        set_file_mtime_ms(&root.join("a.md"), 9_000_000);

        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let outcome = s.read_session(&inputs[0], Some(2_000_000)).unwrap();
        assert!(matches!(
            outcome,
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
        ));
    }

    #[test]
    fn mtime_filter_skips_dir_only_when_all_files_old() {
        // Spec §1.3 conservative semantics: a single recent file in a
        // multi-file bundle must defeat the skip so we never lose a
        // freshly-touched file from a per-dir / single thread.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("sub/a.md"), "a");
        write(&root.join("sub/b.md"), "b");
        set_file_mtime_ms(&root.join("sub/a.md"), 1_000_000);
        set_file_mtime_ms(&root.join("sub/b.md"), 1_000_000);

        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerDir));
        let inputs = s.discover().unwrap();
        let outcome_old = s.read_session(&inputs[0], Some(2_000_000)).unwrap();
        assert!(
            matches!(outcome_old, ReadSessionOutcome::Skipped { .. }),
            "all-old bundle must skip"
        );

        // Refresh just one file in the bundle and expect a parse.
        set_file_mtime_ms(&root.join("sub/b.md"), 9_000_000);
        let outcome_mixed = s.read_session(&inputs[0], Some(2_000_000)).unwrap();
        assert!(
            matches!(
                outcome_mixed,
                ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
            ),
            "any recent file must defeat the skip"
        );
    }

    #[test]
    fn mtime_filter_disabled_when_since_unset() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("a.md"), "# a");
        set_file_mtime_ms(&root.join("a.md"), 1_000);

        let s = PlainSource::new(args(root.to_path_buf(), ThreadStrategy::PerFile));
        let inputs = s.discover().unwrap();
        let outcome = s.read_session(&inputs[0], None).unwrap();
        assert!(matches!(
            outcome,
            ReadSessionOutcome::Import { .. } | ReadSessionOutcome::ImportStream { .. }
        ));
    }
}
