//! Phase A `--prune-missing` for the `plain` source.
//!
//! Spec: `docs/plain-prune-spec.md`. The prune step compares the set of
//! `external_id`s the current import emitted against the set the server
//! has under `<source-name>:` and `user_id`, then deletes the difference
//! after a per-candidate filesystem check. Renames / moves are out of
//! scope for Phase A — they look like delete + create here.
//!
//! Vault invariant: every Phase A behaviour assumes `--source-name` is
//! used with a single `--root`. Rotating either changes the
//! `external_id` shape and prunes the whole prior import.

use crate::client::ImportClient;
use crate::source::CanonicalEntry;
use anyhow::Result;
use protobuf::llm_memory::data::{MemoryId, ThreadId};
use protobuf::llm_memory::service::MemoryListEntry;
use std::collections::HashSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

/// Outcome counters surfaced into the run summary.
#[derive(Debug, Default, Clone, Copy)]
pub struct PruneSummary {
    pub candidates_considered: usize,
    pub excluded_path_missing: usize,
    pub excluded_still_on_fs: usize,
    pub memories_deleted: usize,
    pub threads_deleted: usize,
    pub errors: usize,
}

/// Reason the prune step did not run. Surfaced in the summary so the
/// operator can distinguish "nothing to do" from "skipped on purpose".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PruneSkipReason {
    /// `--prune-missing` was not set; the runner stayed add-only.
    NotRequested,
    /// `--dry-run` is mutually exclusive with prune (spec §dry-run).
    DryRun,
    /// Import had at least one error; prune is gated on a clean import
    /// to avoid deleting "missing" rows that were just re-added in a
    /// half-finished run.
    ImportHadErrors,
    /// Stdin is non-TTY but `--no-interactive` was not passed. We refuse
    /// to delete silently.
    NoInteractiveRequired,
    /// Nothing to do — the candidate set was empty post-filtering.
    NothingToPrune,
    /// User declined the confirmation prompt.
    UserAborted,
}

#[derive(Debug, Clone)]
pub enum PruneOutcome {
    Skipped(PruneSkipReason),
    Ran(PruneSummary),
}

/// Configuration knobs the runner passes in. All flags resolved on the
/// caller side so this module is `PlainArgs`-free and can be unit
/// tested with synthetic inputs.
#[derive(Debug, Clone)]
pub struct PruneConfig {
    pub source_name: String,
    pub user_id: i64,
    pub canonical_root: PathBuf,
    pub orphan_threads: bool,
    pub no_interactive: bool,
}

/// Build `D_external_id` and `D_path` from successful `CanonicalEntry`s.
///
/// `metadata.path` MUST be present on every plain entry — it is written
/// unconditionally in `build_entries_from_files`. Missing path is treated
/// as an implementation bug and panics rather than silently skipped.
pub fn extract_d_sets(entries: &[CanonicalEntry]) -> (HashSet<String>, HashSet<PathBuf>) {
    let mut d_external_id = HashSet::with_capacity(entries.len());
    let mut d_path = HashSet::with_capacity(entries.len());
    for entry in entries {
        d_external_id.insert(entry.external_id.clone());
        let path = entry
            .metadata
            .get("path")
            .and_then(|v| v.as_str())
            .expect("plain CanonicalEntry must carry metadata.path (impl bug)");
        d_path.insert(PathBuf::from(path));
    }
    (d_external_id, d_path)
}

/// Result of `extract_candidate` reduced to the fields prune actually
/// uses. Cleans up downstream `Option<Option<String>>` chains.
#[derive(Debug, Clone)]
pub struct PruneCandidate {
    pub memory_id: MemoryId,
    pub external_id: String,
    pub metadata_path: Option<PathBuf>,
    pub thread_id: Option<ThreadId>,
}

pub fn extract_candidate(entry: &MemoryListEntry) -> Option<PruneCandidate> {
    let memory = entry.memory.as_ref()?;
    let id = memory.id?;
    let data = memory.data.as_ref()?;
    let external_id = data.external_id.clone()?;
    let metadata_path = data
        .metadata
        .as_deref()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
        .and_then(|v| v.get("path").and_then(|p| p.as_str()).map(PathBuf::from));
    Some(PruneCandidate {
        memory_id: id,
        external_id,
        metadata_path,
        thread_id: entry.thread_id,
    })
}

/// Apply the §候補のフィルタ rules from the spec. Pure — no fs / RPC
/// access for the parts that depend on the caller-provided closures.
///
/// `path_exists_on_fs`: callable that returns true when a vault-relative
/// `metadata.path` still resolves to a file under `canonical_root`. We
/// inject it for testability; production passes a closure that calls
/// `fs::exists`.
pub fn filter_candidates(
    candidates: Vec<PruneCandidate>,
    d_path: &HashSet<PathBuf>,
    mut path_exists_on_fs: impl FnMut(&Path) -> bool,
) -> (Vec<PruneCandidate>, FilterStats) {
    let mut stats = FilterStats::default();
    let mut out = Vec::with_capacity(candidates.len());
    for cand in candidates {
        let Some(path) = cand.metadata_path.as_ref() else {
            tracing::warn!(
                external_id = %cand.external_id,
                "prune candidate missing metadata.path; excluding"
            );
            stats.excluded_path_missing += 1;
            continue;
        };
        if d_path.contains(path) {
            // Same path was re-imported with new content; old row is
            // properly stale and a deletion target.
            out.push(cand);
            continue;
        }
        if path_exists_on_fs(path) {
            // File is still on disk but did not become an entry this
            // run (--ext / --exclude-glob change, transient read or
            // strict-decode failure, etc.). Don't delete: the next
            // import will re-evaluate.
            tracing::warn!(
                external_id = %cand.external_id,
                path = %path.display(),
                "prune candidate still on filesystem; excluding"
            );
            stats.excluded_still_on_fs += 1;
            continue;
        }
        out.push(cand);
    }
    (out, stats)
}

#[derive(Debug, Default, Clone, Copy)]
pub struct FilterStats {
    pub excluded_path_missing: usize,
    pub excluded_still_on_fs: usize,
}

/// Production path: ask `std::fs` whether the file exists under the
/// canonical root. Symlinks resolve, hidden files count, anything that
/// `metadata()` returns Ok for is "still on fs".
pub fn fs_path_exists(canonical_root: &Path, rel: &Path) -> bool {
    canonical_root.join(rel).exists()
}

/// TTY-gated confirmation. Returns Ok(true) when the operator typed
/// `y` / `yes`, Ok(false) on anything else (incl. EOF). The caller is
/// expected to have already short-circuited on `--no-interactive`.
pub fn ask_confirmation_tty<W: std::io::Write>(
    candidates: &[PruneCandidate],
    out: &mut W,
) -> Result<bool> {
    use std::io::BufRead;
    writeln!(out, "About to delete {} memories:", candidates.len())?;
    for c in candidates.iter().take(10) {
        let path = c
            .metadata_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "<no path>".to_string());
        writeln!(out, "  - {} (path={})", c.external_id, path)?;
    }
    if candidates.len() > 10 {
        writeln!(out, "  ... and {} more", candidates.len() - 10)?;
    }
    write!(out, "Continue? [y/N] ")?;
    out.flush()?;
    let stdin = std::io::stdin();
    let mut line = String::new();
    let _ = stdin.lock().read_line(&mut line)?;
    let answer = line.trim().to_ascii_lowercase();
    Ok(answer == "y" || answer == "yes")
}

pub fn stdin_is_tty() -> bool {
    std::io::stdin().is_terminal()
}

/// Outcome of the interactive confirmation step. Letting `execute_prune`
/// take a `Confirm` closure keeps the TTY / stdin coupling out of the
/// core prune logic and lets unit tests force any of the three branches
/// without touching the real terminal.
#[derive(Debug, Clone, Copy)]
pub enum Confirm {
    /// Operator agreed (or `--no-interactive` was set).
    Proceed,
    /// Confirmation refused (typed `n`, EOF, …).
    Aborted,
    /// Stdin is non-TTY and `--no-interactive` was missing.
    NotTtyWithoutNoInteractive,
}

/// Production confirmation: skip when `--no-interactive`, refuse when
/// stdin is non-TTY, otherwise prompt on stderr.
pub fn default_confirm(cfg: &PruneConfig, candidates: &[PruneCandidate]) -> Result<Confirm> {
    if cfg.no_interactive {
        return Ok(Confirm::Proceed);
    }
    if !stdin_is_tty() {
        return Ok(Confirm::NotTtyWithoutNoInteractive);
    }
    let mut stderr = std::io::stderr();
    Ok(if ask_confirmation_tty(candidates, &mut stderr)? {
        Confirm::Proceed
    } else {
        Confirm::Aborted
    })
}

/// Drive the full prune sequence. The pre-filtered candidate list is
/// already trimmed by `filter_candidates`; this function only adds the
/// confirmation prompt + RPC dispatch + orphan-thread cleanup.
pub async fn execute_prune(
    client: &dyn ImportClient,
    cfg: &PruneConfig,
    final_candidates: Vec<PruneCandidate>,
    stats: PruneSummary,
) -> Result<PruneOutcome> {
    execute_prune_with_confirm(client, cfg, final_candidates, stats, default_confirm).await
}

/// Test-friendly variant: caller supplies the confirmation strategy so
/// the unit tests don't depend on whether the cargo runner inherited a
/// TTY (running `cargo test` in an interactive shell would otherwise
/// hit the real prompt and hang or diverge from CI).
pub async fn execute_prune_with_confirm<F>(
    client: &dyn ImportClient,
    cfg: &PruneConfig,
    final_candidates: Vec<PruneCandidate>,
    mut stats: PruneSummary,
    confirm: F,
) -> Result<PruneOutcome>
where
    F: FnOnce(&PruneConfig, &[PruneCandidate]) -> Result<Confirm>,
{
    if final_candidates.is_empty() {
        return Ok(PruneOutcome::Skipped(PruneSkipReason::NothingToPrune));
    }

    match confirm(cfg, &final_candidates)? {
        Confirm::Proceed => {}
        Confirm::Aborted => {
            return Ok(PruneOutcome::Skipped(PruneSkipReason::UserAborted));
        }
        Confirm::NotTtyWithoutNoInteractive => {
            return Ok(PruneOutcome::Skipped(
                PruneSkipReason::NoInteractiveRequired,
            ));
        }
    }

    let mut touched_threads: HashSet<i64> = HashSet::new();
    for cand in &final_candidates {
        match client.delete_memory(cand.memory_id).await {
            Ok(()) => {
                stats.memories_deleted += 1;
                if let Some(tid) = cand.thread_id.as_ref() {
                    touched_threads.insert(tid.value);
                }
            }
            Err(e) => {
                tracing::error!(
                    memory_id = cand.memory_id.value,
                    external_id = %cand.external_id,
                    "delete_memory failed: {e}"
                );
                stats.errors += 1;
            }
        }
    }

    if cfg.orphan_threads {
        for tid_value in touched_threads {
            let tid = ThreadId { value: tid_value };
            // count + delete failures both count toward stats.errors and
            // therefore exit code 1. The next import will not retry: the
            // memory was already deleted above, so this thread no longer
            // generates any prune candidates and would not re-enter
            // touched_threads. Surfacing the failure here is the only
            // signal the operator gets to clean up by hand.
            match client.count_memories_in_thread(tid).await {
                Ok(0) => match client.delete_thread(tid).await {
                    Ok(()) => stats.threads_deleted += 1,
                    Err(e) => {
                        tracing::error!(thread_id = tid_value, "delete_thread failed: {e}");
                        stats.errors += 1;
                    }
                },
                Ok(_) => { /* still has memories — leave alone */ }
                Err(e) => {
                    tracing::error!(
                        thread_id = tid_value,
                        "count_memories_in_thread failed: {e}"
                    );
                    stats.errors += 1;
                }
            }
        }
    }

    Ok(PruneOutcome::Ran(stats))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{CanonicalAddons, CanonicalEntry};
    use protobuf::llm_memory::data::{Memory, MemoryData, MessageRole};
    use protobuf::llm_memory::data::{MemoryId as PbMemoryId, ThreadId as PbThreadId};
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn make_entry(external_id: &str, path: &str) -> CanonicalEntry {
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "path".to_string(),
            serde_json::Value::String(path.to_string()),
        );
        CanonicalEntry {
            external_id: external_id.to_string(),
            parent_external_ids: vec![],
            role: MessageRole::RoleUser,
            content_type: protobuf::llm_memory::data::ContentType::Text,
            content: "x".to_string(),
            metadata,
            timestamp_ms: 100,
            import_order: 0,
            kind_tag: "user",
            canonical: CanonicalAddons::default(),
        }
    }

    fn make_list_entry(
        memory_id: i64,
        external_id: &str,
        path: Option<&str>,
        thread_id: Option<i64>,
    ) -> MemoryListEntry {
        let metadata_str = path.map(|p| serde_json::json!({ "path": p }).to_string());
        MemoryListEntry {
            memory: Some(Memory {
                id: Some(PbMemoryId { value: memory_id }),
                data: Some(MemoryData {
                    parent_ids: vec![],
                    user_id: None,
                    content: String::new(),
                    content_type: 0,
                    params: None,
                    metadata: metadata_str,
                    created_at: 0,
                    updated_at: 0,
                    role: 0,
                    external_id: Some(external_id.to_string()),
                    media_object_id: None,
                    thread_ids: Vec::new(),
                }),
                media: None,
            }),
            position: None,
            thread_total: None,
            thread_id: thread_id.map(|v| PbThreadId { value: v }),
            thread_owner_user_id: None,
            thread_description: None,
        }
    }

    #[test]
    fn extract_d_sets_collects_external_ids_and_paths() {
        let entries = vec![
            make_entry("p:s:abc", "a.md"),
            make_entry("p:s:def", "sub/b.md"),
        ];
        let (d_eid, d_path) = extract_d_sets(&entries);
        assert!(d_eid.contains("p:s:abc"));
        assert!(d_eid.contains("p:s:def"));
        assert!(d_path.contains(&PathBuf::from("a.md")));
        assert!(d_path.contains(&PathBuf::from("sub/b.md")));
    }

    #[test]
    #[should_panic(expected = "plain CanonicalEntry must carry metadata.path")]
    fn extract_d_sets_panics_on_missing_path() {
        let mut entry = make_entry("p:s:bad", "ignored");
        entry.metadata.remove("path");
        let _ = extract_d_sets(&[entry]);
    }

    /// Spec test 1: standard case.
    /// S = {a@v1, b@v1, c@v1, d@v1, e@v1}, D_external_id = {a@v1, c@v2, e@v1},
    /// D_path = {a, c, e}. Expected M = {b@v1, c@v1, d@v1}.
    #[test]
    fn standard_case_produces_correct_candidates() {
        let s = [
            make_list_entry(1, "p:a@v1", Some("a"), Some(10)),
            make_list_entry(2, "p:b@v1", Some("b"), Some(11)),
            make_list_entry(3, "p:c@v1", Some("c"), Some(12)),
            make_list_entry(4, "p:d@v1", Some("d"), Some(13)),
            make_list_entry(5, "p:e@v1", Some("e"), Some(14)),
        ];
        let d_external_id: HashSet<String> = ["p:a@v1", "p:c@v2", "p:e@v1"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let d_path: HashSet<PathBuf> = ["a", "c", "e"].iter().map(PathBuf::from).collect();

        let m_prime: Vec<PruneCandidate> = s
            .iter()
            .filter_map(extract_candidate)
            .filter(|c| !d_external_id.contains(&c.external_id))
            .collect();
        // Files for b@v1 and d@v1 are missing on fs (test fixture).
        let fs_files: HashSet<PathBuf> = HashSet::new();
        let (m, stats) = filter_candidates(m_prime, &d_path, |p| fs_files.contains(p));
        let eids: Vec<String> = m.iter().map(|c| c.external_id.clone()).collect();
        assert_eq!(eids, vec!["p:b@v1", "p:c@v1", "p:d@v1"]);
        assert_eq!(stats.excluded_path_missing, 0);
        assert_eq!(stats.excluded_still_on_fs, 0);
    }

    /// Spec test 2: initial import. S = {} → M = {}.
    #[test]
    fn initial_import_yields_empty_candidates() {
        let d_external_id: HashSet<String> =
            ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
        let d_path: HashSet<PathBuf> = ["a", "b", "c"].iter().map(PathBuf::from).collect();
        let m_prime: Vec<PruneCandidate> = Vec::<MemoryListEntry>::new()
            .iter()
            .filter_map(extract_candidate)
            .filter(|c| !d_external_id.contains(&c.external_id))
            .collect();
        let (m, _) = filter_candidates(m_prime, &d_path, |_| true);
        assert!(m.is_empty());
    }

    /// Spec test 3: vault entirely emptied. D_external_id = {} → all of S
    /// becomes candidates (modulo fs check).
    #[test]
    fn vault_emptied_targets_all_memories() {
        let s = [
            make_list_entry(1, "p:a", Some("a"), None),
            make_list_entry(2, "p:b", Some("b"), None),
            make_list_entry(3, "p:c", Some("c"), None),
        ];
        let d_external_id: HashSet<String> = HashSet::new();
        let d_path: HashSet<PathBuf> = HashSet::new();
        let m_prime: Vec<PruneCandidate> = s
            .iter()
            .filter_map(extract_candidate)
            .filter(|c| !d_external_id.contains(&c.external_id))
            .collect();
        let (m, _) = filter_candidates(m_prime, &d_path, |_| false);
        assert_eq!(m.len(), 3);
    }

    /// Spec test 4: a memory whose path is gone from `D_path` but still
    /// physically present on disk (e.g. `--exclude-glob` was tightened)
    /// must be excluded from the delete set.
    #[test]
    fn fs_present_excludes_candidate() {
        let s = [make_list_entry(7, "p:m@v1", Some("kept.md"), Some(99))];
        let d_external_id: HashSet<String> = HashSet::new();
        let d_path: HashSet<PathBuf> = HashSet::new();
        let on_fs: HashSet<PathBuf> = [PathBuf::from("kept.md")].into_iter().collect();
        let m_prime: Vec<PruneCandidate> = s
            .iter()
            .filter_map(extract_candidate)
            .filter(|c| !d_external_id.contains(&c.external_id))
            .collect();
        let (m, stats) = filter_candidates(m_prime, &d_path, |p| on_fs.contains(p));
        assert!(m.is_empty());
        assert_eq!(stats.excluded_still_on_fs, 1);
    }

    /// Spec test 5: a transient read failure on a still-present file
    /// must not lead to deletion. The candidate is in `S`, NOT in
    /// `D_external_id`, NOT in `D_path`, but still on fs → excluded.
    #[test]
    fn read_failure_keeps_old_memory() {
        let s = [make_list_entry(8, "p:a@v1", Some("a"), Some(1))];
        let d_external_id: HashSet<String> = HashSet::new();
        let d_path: HashSet<PathBuf> = HashSet::new();
        let on_fs: HashSet<PathBuf> = [PathBuf::from("a")].into_iter().collect();
        let m_prime: Vec<PruneCandidate> = s
            .iter()
            .filter_map(extract_candidate)
            .filter(|c| !d_external_id.contains(&c.external_id))
            .collect();
        let (m, stats) = filter_candidates(m_prime, &d_path, |p| on_fs.contains(p));
        assert!(m.is_empty());
        assert_eq!(stats.excluded_still_on_fs, 1);
    }

    /// Spec test 6: candidate with no `metadata.path` is excluded with a
    /// warning rather than risking a wrong delete.
    #[test]
    fn missing_metadata_path_excludes_candidate() {
        let s = [make_list_entry(9, "p:noid", None, None)];
        let d_external_id: HashSet<String> = HashSet::new();
        let d_path: HashSet<PathBuf> = HashSet::new();
        let m_prime: Vec<PruneCandidate> = s
            .iter()
            .filter_map(extract_candidate)
            .filter(|c| !d_external_id.contains(&c.external_id))
            .collect();
        let (m, stats) = filter_candidates(m_prime, &d_path, |_| false);
        assert!(m.is_empty());
        assert_eq!(stats.excluded_path_missing, 1);
    }

    // -- end-to-end driving via FakeImportClient -------------------------------

    /// In-memory test-only client. Mirrors the importer-side fake but
    /// adds the four prune RPCs so the prune unit tests can simulate
    /// server behaviour.
    pub struct FakePruneClient {
        pub memories: Mutex<Vec<MemoryListEntry>>,
        pub thread_counts: Mutex<HashMap<i64, i64>>,
        pub deleted_memories: Mutex<Vec<i64>>,
        pub deleted_threads: Mutex<Vec<i64>>,
        pub force_error_on_memory_id: Mutex<Option<i64>>,
        pub force_error_on_count_thread_id: Mutex<Option<i64>>,
        pub force_error_on_delete_thread_id: Mutex<Option<i64>>,
    }

    impl FakePruneClient {
        pub fn new(memories: Vec<MemoryListEntry>, thread_counts: HashMap<i64, i64>) -> Self {
            Self {
                memories: Mutex::new(memories),
                thread_counts: Mutex::new(thread_counts),
                deleted_memories: Mutex::new(Vec::new()),
                deleted_threads: Mutex::new(Vec::new()),
                force_error_on_memory_id: Mutex::new(None),
                force_error_on_count_thread_id: Mutex::new(None),
                force_error_on_delete_thread_id: Mutex::new(None),
            }
        }
    }

    use async_trait::async_trait;
    use protobuf::llm_memory::service::{
        AddMemoriesBatchRequest, AddMemoriesBatchResponse, UpdateMemoryParentsRequest,
        UpdateMemoryParentsResponse,
    };

    #[async_trait]
    impl ImportClient for FakePruneClient {
        async fn add_memories_batch(
            &self,
            _request: AddMemoriesBatchRequest,
        ) -> Result<AddMemoriesBatchResponse> {
            unimplemented!("FakePruneClient is for prune only")
        }
        async fn update_memory_parents(
            &self,
            _request: UpdateMemoryParentsRequest,
        ) -> Result<UpdateMemoryParentsResponse> {
            unimplemented!("FakePruneClient is for prune only")
        }
        async fn upload_media(
            &self,
            _header: crate::client::UploadMediaHeader,
            _bytes: Vec<u8>,
        ) -> Result<i64> {
            unimplemented!("FakePruneClient is for prune only")
        }
        async fn register_media_url(
            &self,
            _params: crate::client::RegisterMediaUrl,
        ) -> Result<i64> {
            unimplemented!("FakePruneClient is for prune only")
        }
        async fn find_memories_by_external_id_prefix(
            &self,
            _prefix: String,
            _user_id: i64,
        ) -> Result<Vec<MemoryListEntry>> {
            Ok(self.memories.lock().unwrap().clone())
        }
        async fn delete_memory(&self, memory_id: PbMemoryId) -> Result<()> {
            if let Some(err_id) = *self.force_error_on_memory_id.lock().unwrap()
                && err_id == memory_id.value
            {
                return Err(anyhow::anyhow!("simulated delete failure"));
            }
            self.deleted_memories.lock().unwrap().push(memory_id.value);
            Ok(())
        }
        async fn delete_thread(&self, thread_id: PbThreadId) -> Result<()> {
            if let Some(err_tid) = *self.force_error_on_delete_thread_id.lock().unwrap()
                && err_tid == thread_id.value
            {
                return Err(anyhow::anyhow!("simulated delete_thread failure"));
            }
            self.deleted_threads.lock().unwrap().push(thread_id.value);
            Ok(())
        }
        async fn count_memories_in_thread(&self, thread_id: PbThreadId) -> Result<i64> {
            if let Some(err_tid) = *self.force_error_on_count_thread_id.lock().unwrap()
                && err_tid == thread_id.value
            {
                return Err(anyhow::anyhow!("simulated count failure"));
            }
            Ok(*self
                .thread_counts
                .lock()
                .unwrap()
                .get(&thread_id.value)
                .unwrap_or(&0))
        }
    }

    /// Spec test 7: thread_id set / unset both delete the memory but
    /// only `Some(_)` participates in orphan-thread bookkeeping.
    #[tokio::test]
    async fn delete_runs_for_set_and_unset_thread_ids() {
        let candidates = vec![
            PruneCandidate {
                memory_id: PbMemoryId { value: 1 },
                external_id: "p:a".to_string(),
                metadata_path: Some(PathBuf::from("a")),
                thread_id: Some(PbThreadId { value: 100 }),
            },
            PruneCandidate {
                memory_id: PbMemoryId { value: 2 },
                external_id: "p:b".to_string(),
                metadata_path: Some(PathBuf::from("b")),
                thread_id: None,
            },
            PruneCandidate {
                memory_id: PbMemoryId { value: 3 },
                external_id: "p:c".to_string(),
                metadata_path: Some(PathBuf::from("c")),
                thread_id: Some(PbThreadId { value: 200 }),
            },
        ];
        // thread 100 has nothing left, thread 200 keeps a memory.
        let mut counts = HashMap::new();
        counts.insert(100, 0);
        counts.insert(200, 1);

        let fake = FakePruneClient::new(Vec::new(), counts);
        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: true,
            no_interactive: true,
        };
        let outcome = execute_prune(&fake, &cfg, candidates, PruneSummary::default())
            .await
            .unwrap();
        let stats = match outcome {
            PruneOutcome::Ran(s) => s,
            _ => panic!("expected Ran"),
        };
        assert_eq!(stats.memories_deleted, 3);
        assert_eq!(stats.threads_deleted, 1);
        // None-thread memory still got deleted.
        let deleted = fake.deleted_memories.lock().unwrap().clone();
        assert!(deleted.contains(&1));
        assert!(deleted.contains(&2));
        assert!(deleted.contains(&3));
        // Only thread 100 was deleted as orphan.
        assert_eq!(*fake.deleted_threads.lock().unwrap(), vec![100i64]);
    }

    /// Spec test 12 (per-dir part): when a thread still has memories
    /// after the partial prune, the orphan-thread step leaves it alone.
    #[tokio::test]
    async fn per_dir_thread_with_remaining_members_is_not_deleted() {
        // Two memories in thread 50; we delete one of them, count = 1
        // remains.
        let candidates = vec![PruneCandidate {
            memory_id: PbMemoryId { value: 11 },
            external_id: "p:per-dir-1".to_string(),
            metadata_path: Some(PathBuf::from("a")),
            thread_id: Some(PbThreadId { value: 50 }),
        }];
        let mut counts = HashMap::new();
        counts.insert(50, 2); // 2 left after the (yet-to-happen) delete
        let fake = FakePruneClient::new(Vec::new(), counts);
        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: true,
            no_interactive: true,
        };
        let outcome = execute_prune(&fake, &cfg, candidates, PruneSummary::default())
            .await
            .unwrap();
        let stats = match outcome {
            PruneOutcome::Ran(s) => s,
            _ => panic!("expected Ran"),
        };
        assert_eq!(stats.memories_deleted, 1);
        assert_eq!(stats.threads_deleted, 0);
        assert!(fake.deleted_threads.lock().unwrap().is_empty());
    }

    /// Spec test 12 (per-file part): a thread emptied by prune is removed
    /// when `--prune-orphan-threads=true`.
    #[tokio::test]
    async fn per_file_thread_emptied_by_prune_is_deleted() {
        let candidates = vec![PruneCandidate {
            memory_id: PbMemoryId { value: 21 },
            external_id: "p:per-file-1".to_string(),
            metadata_path: Some(PathBuf::from("a")),
            thread_id: Some(PbThreadId { value: 60 }),
        }];
        let mut counts = HashMap::new();
        counts.insert(60, 0);
        let fake = FakePruneClient::new(Vec::new(), counts);
        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: true,
            no_interactive: true,
        };
        let outcome = execute_prune(&fake, &cfg, candidates, PruneSummary::default())
            .await
            .unwrap();
        let stats = match outcome {
            PruneOutcome::Ran(s) => s,
            _ => panic!("expected Ran"),
        };
        assert_eq!(stats.memories_deleted, 1);
        assert_eq!(stats.threads_deleted, 1);
        assert_eq!(*fake.deleted_threads.lock().unwrap(), vec![60i64]);
    }

    /// Spec test 12 (negation): with `--no-prune-orphan-threads`
    /// (orphan_threads=false) the thread stays even when it would
    /// have been emptied.
    #[tokio::test]
    async fn orphan_thread_kept_when_disabled() {
        let candidates = vec![PruneCandidate {
            memory_id: PbMemoryId { value: 31 },
            external_id: "p:noop".to_string(),
            metadata_path: Some(PathBuf::from("a")),
            thread_id: Some(PbThreadId { value: 70 }),
        }];
        let mut counts = HashMap::new();
        counts.insert(70, 0);
        let fake = FakePruneClient::new(Vec::new(), counts);
        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: false,
            no_interactive: true,
        };
        let outcome = execute_prune(&fake, &cfg, candidates, PruneSummary::default())
            .await
            .unwrap();
        let stats = match outcome {
            PruneOutcome::Ran(s) => s,
            _ => panic!("expected Ran"),
        };
        assert_eq!(stats.memories_deleted, 1);
        assert_eq!(stats.threads_deleted, 0);
        assert!(fake.deleted_threads.lock().unwrap().is_empty());
    }

    /// Spec test 13: when stdin is not a TTY the importer must NOT delete
    /// silently. We inject a confirm-closure that simulates "no_interactive
    /// missing on a non-TTY stdin" so the assertion holds regardless of
    /// whether the cargo runner inherited a real terminal — driving the
    /// real `default_confirm` would either prompt the user (interactive
    /// shell) or pass (CI / piped stdin), both of which diverge from the
    /// invariant we want to pin.
    #[tokio::test]
    async fn non_tty_without_no_interactive_aborts() {
        let candidates = vec![PruneCandidate {
            memory_id: PbMemoryId { value: 41 },
            external_id: "p:abort".to_string(),
            metadata_path: Some(PathBuf::from("a")),
            thread_id: None,
        }];
        let fake = FakePruneClient::new(Vec::new(), HashMap::new());
        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: true,
            no_interactive: false,
        };
        let outcome =
            execute_prune_with_confirm(&fake, &cfg, candidates, PruneSummary::default(), |_, _| {
                Ok(Confirm::NotTtyWithoutNoInteractive)
            })
            .await
            .unwrap();
        match outcome {
            PruneOutcome::Skipped(PruneSkipReason::NoInteractiveRequired) => {}
            other => panic!("expected NoInteractiveRequired, got {:?}", other),
        }
        assert!(fake.deleted_memories.lock().unwrap().is_empty());
        assert!(fake.deleted_threads.lock().unwrap().is_empty());
    }

    /// Spec test 14: partial delete failure flows touched_threads
    /// correctly and keeps going on the rest of the candidate set.
    #[tokio::test]
    async fn partial_delete_failure_continues_and_reports() {
        let candidates: Vec<PruneCandidate> = (1..=5)
            .map(|i| PruneCandidate {
                memory_id: PbMemoryId { value: i },
                external_id: format!("p:m{i}"),
                metadata_path: Some(PathBuf::from(format!("f{i}"))),
                thread_id: Some(PbThreadId { value: 7 }),
            })
            .collect();
        // After delete (1 of 5 fails), thread 7 still has 1 memory left.
        let mut counts = HashMap::new();
        counts.insert(7, 1);
        let fake = FakePruneClient::new(Vec::new(), counts);
        *fake.force_error_on_memory_id.lock().unwrap() = Some(2);

        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: true,
            no_interactive: true,
        };
        let outcome = execute_prune(&fake, &cfg, candidates, PruneSummary::default())
            .await
            .unwrap();
        let stats = match outcome {
            PruneOutcome::Ran(s) => s,
            _ => panic!("expected Ran"),
        };
        assert_eq!(stats.memories_deleted, 4);
        assert_eq!(stats.errors, 1);
        // Thread 7 still has 1 memory → not deleted.
        assert!(fake.deleted_threads.lock().unwrap().is_empty());
    }

    /// Orphan-thread sweep failures must propagate to `stats.errors`
    /// (and therefore to the exit code). The next import will not
    /// retry: the memory was already deleted, so this thread does not
    /// re-enter the prune candidate set on a future run.
    #[tokio::test]
    async fn delete_thread_failure_is_counted_as_error() {
        let candidates = vec![PruneCandidate {
            memory_id: PbMemoryId { value: 1 },
            external_id: "p:a".to_string(),
            metadata_path: Some(PathBuf::from("a")),
            thread_id: Some(PbThreadId { value: 88 }),
        }];
        let mut counts = HashMap::new();
        counts.insert(88, 0);
        let fake = FakePruneClient::new(Vec::new(), counts);
        *fake.force_error_on_delete_thread_id.lock().unwrap() = Some(88);

        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: true,
            no_interactive: true,
        };
        let outcome = execute_prune(&fake, &cfg, candidates, PruneSummary::default())
            .await
            .unwrap();
        let stats = match outcome {
            PruneOutcome::Ran(s) => s,
            _ => panic!("expected Ran"),
        };
        assert_eq!(stats.memories_deleted, 1);
        assert_eq!(stats.threads_deleted, 0);
        assert_eq!(
            stats.errors, 1,
            "delete_thread failure must surface as a prune error"
        );
        assert!(fake.deleted_threads.lock().unwrap().is_empty());
    }

    /// Same contract for the count side: a failure to verify whether the
    /// thread is empty leaves us unable to decide and must error out.
    #[tokio::test]
    async fn count_memories_failure_is_counted_as_error() {
        let candidates = vec![PruneCandidate {
            memory_id: PbMemoryId { value: 1 },
            external_id: "p:a".to_string(),
            metadata_path: Some(PathBuf::from("a")),
            thread_id: Some(PbThreadId { value: 99 }),
        }];
        let counts = HashMap::new(); // count fake will fail before lookup
        let fake = FakePruneClient::new(Vec::new(), counts);
        *fake.force_error_on_count_thread_id.lock().unwrap() = Some(99);

        let cfg = PruneConfig {
            source_name: "p".to_string(),
            user_id: 1,
            canonical_root: PathBuf::from("/tmp"),
            orphan_threads: true,
            no_interactive: true,
        };
        let outcome = execute_prune(&fake, &cfg, candidates, PruneSummary::default())
            .await
            .unwrap();
        let stats = match outcome {
            PruneOutcome::Ran(s) => s,
            _ => panic!("expected Ran"),
        };
        assert_eq!(stats.memories_deleted, 1);
        assert_eq!(stats.threads_deleted, 0);
        assert_eq!(stats.errors, 1);
    }
}
