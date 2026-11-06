//! Dedicated Claude Code subagent transcripts
//! (`<projectKey>/<sessionId>/subagents/agent-<agentId>.jsonl`).
//!
//! Each transcript becomes its own Thread, a `delegated` child of the
//! transcript whose `Agent` tool_use spawned it. The relation is only
//! asserted when the sibling `.meta.json` names that tool_use id
//! (`toolUseId`) and the invoking transcript really contains it; every
//! other transcript is skipped rather than imported as an orphan.

use super::{
    MAIN_NATIVE_KIND, SUBAGENT_LABEL, agent_tool_use_ids, claude_identity, collect_jsonl_files,
    source_field_observation,
};
use crate::common::path::normalize_lexically;
use crate::parser::{RawEntry, SessionInfo, parse_raw_entry_str};
use crate::source::{
    CanonicalSession, ReadSessionOutcome, ThreadGroupConfidence, ThreadGroupSourceIdentity,
    mtime_skip_outcome,
};
use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const SUBAGENTS_DIR: &str = "subagents";
const SUBAGENT_NATIVE_KIND: &str = "subagent_transcript";
pub(super) const UNRESOLVED_REASON: &str = "subagent_parent_unresolved";
pub(super) const UNPARSEABLE_REASON: &str = "subagent_path_unparseable";
/// Location of a dedicated subagent transcript, decoded from its path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SubagentPath {
    pub project_key: String,
    pub session_id: String,
    pub agent_id: String,
    project_dir: PathBuf,
}

impl SubagentPath {
    /// `<project_dir>/<sessionId>/subagents/agent-<agentId>.jsonl`, or
    /// `None` for any other layout (including legacy flat
    /// `<project_dir>/agent-*.jsonl` files, which stay main transcripts).
    pub fn parse(path: &Path) -> Option<Self> {
        let file_name = path.file_name()?.to_str()?;
        let agent_id = file_name.strip_prefix("agent-")?.strip_suffix(".jsonl")?;
        if agent_id.is_empty() || agent_id.contains('.') {
            return None;
        }
        let subagents_dir = path.parent()?;
        if subagents_dir.file_name()?.to_str()? != SUBAGENTS_DIR {
            return None;
        }
        let session_dir = subagents_dir.parent()?;
        let project_dir = session_dir.parent()?;
        Some(Self {
            project_key: project_dir.file_name()?.to_str()?.to_string(),
            session_id: session_dir.file_name()?.to_str()?.to_string(),
            agent_id: agent_id.to_string(),
            project_dir: project_dir.to_path_buf(),
        })
    }

    /// Composite `native_id` (`<sessionId>/subagents/agent-<agentId>`);
    /// also the Thread's session key, so its channel and external ids
    /// never share the parent session's namespace.
    pub fn native_id(&self) -> String {
        subagent_native_id(&self.session_id, &self.agent_id)
    }

    fn main_transcript(&self) -> PathBuf {
        self.project_dir.join(format!("{}.jsonl", self.session_id))
    }

    fn sibling(&self, agent_id: &str) -> PathBuf {
        subagents_dir(&self.project_dir.join(&self.session_id))
            .join(format!("agent-{agent_id}.jsonl"))
    }

    fn identity(&self, agent_id: &str) -> ThreadGroupSourceIdentity {
        claude_identity(
            Some(&self.project_key),
            SUBAGENT_NATIVE_KIND,
            subagent_native_id(&self.session_id, agent_id),
        )
    }

    fn main_identity(&self) -> ThreadGroupSourceIdentity {
        claude_identity(
            Some(&self.project_key),
            MAIN_NATIVE_KIND,
            self.session_id.clone(),
        )
    }
}

/// How a transcript path must be read, judged on its normalized path.
enum TranscriptKind {
    Main,
    Subagent(SubagentPath),
    /// Sits in a `subagents/` directory but its location cannot be
    /// decoded (e.g. non-UTF-8 or a dotted agent id). Importing it as a
    /// main transcript would write it into the parent session's Thread,
    /// because its rows carry the parent's `sessionId`.
    Unparseable(String),
}

fn classify(path: &Path) -> (PathBuf, TranscriptKind) {
    let normalized = normalize_lexically(path);
    let kind = match SubagentPath::parse(&normalized) {
        Some(parsed) => TranscriptKind::Subagent(parsed),
        None if normalized
            .parent()
            .and_then(Path::file_name)
            .is_some_and(|name| name == SUBAGENTS_DIR) =>
        {
            TranscriptKind::Unparseable(format!("{UNPARSEABLE_REASON}: {}", normalized.display()))
        }
        None => TranscriptKind::Main,
    };
    (normalized, kind)
}

/// A transcript cleared for parsing: its normalized path and, for a
/// subagent transcript, the verified invoker.
pub(super) struct Prepared {
    pub normalized: PathBuf,
    pub subagent: Option<ResolvedSubagent>,
}

/// Everything that can end a read before the transcript is parsed. The
/// error is the outcome to report.
pub(super) fn prepare(
    input: &Path,
    subagents_enabled: bool,
    since_millis_with_margin: Option<i64>,
    cache: &InvokerToolUseIds,
) -> Result<Prepared, Box<ReadSessionOutcome>> {
    let (normalized, kind) = classify(input);
    let path = match kind {
        TranscriptKind::Main => None,
        TranscriptKind::Subagent(path) => Some(path),
        TranscriptKind::Unparseable(reason) => return Err(skipped(None, reason)),
    };
    if let Some(path) = &path
        && !subagents_enabled
    {
        return Err(skipped(
            Some(path.native_id()),
            "subagent import disabled".to_string(),
        ));
    }
    if let Some(skip) = mtime_skip_outcome(input, since_millis_with_margin) {
        return Err(Box::new(skip));
    }
    let subagent =
        match path {
            None => None,
            Some(path) => {
                let hint = path.native_id();
                Some(resolve(path, cache, &normalized).map_err(|detail| {
                    skipped(Some(hint), format!("{UNRESOLVED_REASON}: {detail}"))
                })?)
            }
        };
    Ok(Prepared {
        normalized,
        subagent,
    })
}

fn skipped(session_id_hint: Option<String>, reason: String) -> Box<ReadSessionOutcome> {
    Box::new(ReadSessionOutcome::Skipped {
        session_id_hint,
        reason,
        filtered_count: 0,
    })
}

fn subagent_native_id(session_id: &str, agent_id: &str) -> String {
    format!("{session_id}/{SUBAGENTS_DIR}/agent-{agent_id}")
}

fn subagents_dir(session_dir: &Path) -> PathBuf {
    session_dir.join(SUBAGENTS_DIR)
}

/// Sibling `agent-<id>.meta.json` written by Claude Code.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct SubagentMeta {
    agent_type: Option<String>,
    description: Option<String>,
    tool_use_id: Option<String>,
    spawn_depth: Option<u32>,
    parent_agent_id: Option<String>,
    is_fork: Option<bool>,
    request_shape: Option<String>,
    stopped_by_user: Option<bool>,
}

impl SubagentMeta {
    fn spawn_depth(&self) -> u32 {
        self.spawn_depth.unwrap_or(1)
    }

    fn parent_agent_id(&self) -> Option<&str> {
        self.parent_agent_id.as_deref().filter(|s| !s.is_empty())
    }
}

fn read_meta(transcript: &Path) -> Result<Option<SubagentMeta>, String> {
    let path = transcript.with_extension("meta.json");
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("read {}: {e}", path.display())),
    };
    serde_json::from_str(&raw)
        .map(Some)
        .map_err(|e| format!("parse {}: {e}", path.display()))
}

/// `Agent` tool_use ids per invoking transcript, built at most once per
/// run. Main transcripts seed it while being imported; a transcript
/// skipped as unchanged is scanned on first use.
#[derive(Default)]
pub(super) struct InvokerToolUseIds {
    by_path: Mutex<HashMap<PathBuf, Arc<HashSet<String>>>>,
}

impl InvokerToolUseIds {
    pub fn insert_from_entries(&self, path: &Path, entries: &[RawEntry]) {
        self.by_path
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(agent_tool_use_ids(entries)));
    }

    pub fn get(&self, path: &Path) -> Result<Arc<HashSet<String>>> {
        if let Some(ids) = self.by_path.lock().unwrap().get(path) {
            return Ok(ids.clone());
        }
        // Only lines naming the `Agent` tool can hold an id, so the rest
        // of a possibly multi-MB transcript is never deserialized.
        let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
        let mut ids = HashSet::new();
        for line in BufReader::new(file).lines() {
            let line = line.with_context(|| format!("read {}", path.display()))?;
            if !line.contains(r#""Agent""#) {
                continue;
            }
            if let Ok(entry) = parse_raw_entry_str(&line) {
                ids.extend(agent_tool_use_ids(std::slice::from_ref(&entry)));
            }
        }
        Ok(self
            .by_path
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(ids))
            .clone())
    }
}

/// A subagent transcript whose invoker was verified.
pub(super) struct ResolvedSubagent {
    path: SubagentPath,
    meta: SubagentMeta,
    /// The invoker first, up to the main session; never empty.
    ancestors: Vec<ThreadGroupSourceIdentity>,
}

/// Verify that `meta.toolUseId` names an `Agent` tool_use inside the
/// invoking transcript (`parentAgentId`'s transcript, else the main
/// transcript). A nested invoker must itself resolve: otherwise it is
/// never imported and this child would wait forever for its parent
/// Thread. The error is the skip detail.
pub(super) fn resolve(
    path: SubagentPath,
    cache: &InvokerToolUseIds,
    transcript: &Path,
) -> Result<ResolvedSubagent, String> {
    resolve_below(path, cache, transcript, u32::MAX)
}

/// `below` is the spawn depth of the child being resolved; requiring the
/// invoker to sit strictly closer to the main session makes a
/// `parentAgentId` cycle impossible.
fn resolve_below(
    path: SubagentPath,
    cache: &InvokerToolUseIds,
    transcript: &Path,
    below: u32,
) -> Result<ResolvedSubagent, String> {
    let meta = read_meta(transcript)?.ok_or("meta.json missing")?;
    let tool_use_id = meta
        .tool_use_id
        .clone()
        .filter(|s| !s.is_empty())
        .ok_or("meta.json has no toolUseId")?;
    let depth = meta.spawn_depth();
    if depth >= below {
        return Err(format!(
            "spawnDepth {depth} is not below its child's {below}"
        ));
    }
    let (invoker_path, invoker, invoker_ancestors) = match (meta.parent_agent_id(), depth) {
        (None, 1) => (path.main_transcript(), path.main_identity(), Vec::new()),
        (Some(parent), depth) if depth >= 2 => {
            let invoker_path = path.sibling(parent);
            let invoker_sub = SubagentPath {
                agent_id: parent.to_string(),
                ..path.clone()
            };
            let resolved = resolve_below(invoker_sub, cache, &invoker_path, depth)
                .map_err(|e| format!("invoker agent-{parent} unresolved: {e}"))?;
            (invoker_path, path.identity(parent), resolved.ancestors)
        }
        (parent, depth) => {
            return Err(format!(
                "spawnDepth {depth} contradicts parentAgentId {parent:?}"
            ));
        }
    };
    if !invoker_path.is_file() {
        return Err(format!(
            "invoking transcript {} not found",
            invoker_path.display()
        ));
    }
    let ids = cache
        .get(&invoker_path)
        .map_err(|e| format!("read {}: {e:#}", invoker_path.display()))?;
    if !ids.contains(&tool_use_id) {
        return Err(format!(
            "toolUseId {tool_use_id} is not an Agent call in {}",
            invoker_path.display()
        ));
    }
    // Nearest first; the main session is always last so its deletion
    // marker is always honoured.
    let mut ancestors = vec![invoker];
    ancestors.extend(invoker_ancestors);
    Ok(ResolvedSubagent {
        path,
        meta,
        ancestors,
    })
}

impl ResolvedSubagent {
    /// Key the session by the transcript's location. Rows repeat the
    /// parent's `sessionId`, and the file sits under `subagents/`, so
    /// neither can be taken from the rows / immediate parent directory.
    pub fn override_info(&self, info: &mut SessionInfo, entries: &[RawEntry]) {
        if let Some(other) = entries
            .iter()
            .filter_map(|e| e.session_id.as_deref())
            .find(|s| *s != self.path.session_id)
        {
            tracing::warn!(
                path_session_id = self.path.session_id.as_str(),
                row_session_id = other,
                "subagent transcript rows name a different sessionId; using the path"
            );
        }
        info.session_id = self.path.native_id();
        info.project_hash = Some(self.path.project_key.clone());
    }

    /// Turn the main-transcript session built from the same rows into
    /// the subagent Thread's session.
    pub fn apply(&self, session: &mut CanonicalSession, entries: &[RawEntry]) {
        let meta = &self.meta;
        if let Some(description) = describe(meta) {
            session.description = Some(description);
        }
        session.source_labels.push(SUBAGENT_LABEL.to_string());
        if let Some(obj) = session.source_metadata.as_object_mut() {
            obj.insert(SUBAGENT_LABEL.to_string(), self.metadata(entries));
        }
        let subject = self.path.identity(&self.path.agent_id);
        session.thread_group_observations = vec![source_field_observation(
            subject.clone(),
            self.ancestors[0].clone(),
            "delegated",
            ThreadGroupConfidence::Exact,
            "claude_code:subagents/agent.meta.json",
        )];
        session.source_identity = Some(subject);
        session.structural_ancestor_identities = self.ancestors.clone();
    }

    fn metadata(&self, entries: &[RawEntry]) -> serde_json::Value {
        let meta = &self.meta;
        let mut obj = serde_json::Map::new();
        obj.insert(
            "parent_session_id".into(),
            self.path.session_id.clone().into(),
        );
        obj.insert("agent_id".into(), self.path.agent_id.clone().into());
        obj.insert("spawn_depth".into(), meta.spawn_depth().into());
        let fork_parent_last_uuid = entries
            .iter()
            .find(|e| e.entry_type == "fork-context-ref")
            .and_then(|e| e.extra("parentLastUuid"))
            .and_then(|v| v.as_str());
        let optional: [(&str, Option<serde_json::Value>); 7] = [
            ("parent_agent_id", meta.parent_agent_id().map(Into::into)),
            ("agent_type", meta.agent_type.clone().map(Into::into)),
            ("tool_use_id", meta.tool_use_id.clone().map(Into::into)),
            ("is_fork", meta.is_fork.map(Into::into)),
            ("request_shape", meta.request_shape.clone().map(Into::into)),
            ("stopped_by_user", meta.stopped_by_user.map(Into::into)),
            (
                "fork_parent_last_uuid",
                fork_parent_last_uuid.map(Into::into),
            ),
        ];
        for (key, value) in optional {
            if let Some(value) = value {
                obj.insert(key.into(), value);
            }
        }
        serde_json::Value::Object(obj)
    }
}

fn describe(meta: &SubagentMeta) -> Option<String> {
    let description = meta.description.as_deref().filter(|s| !s.is_empty());
    let agent_type = meta.agent_type.as_deref().filter(|s| !s.is_empty());
    match (description, agent_type) {
        (Some(d), Some(t)) => Some(format!("{d} ({t})")),
        (Some(d), None) => Some(d.to_string()),
        (None, Some(t)) => Some(t.to_string()),
        (None, None) => None,
    }
}

/// Main transcripts of `project_dir`, each followed by its subagent
/// transcripts (invokers before the subagents they spawn). Session
/// directories without a main transcript come last.
pub(super) fn collect_project(project_dir: &Path, include_subagents: bool) -> Result<Vec<PathBuf>> {
    let project_dir = &normalize_lexically(project_dir);
    let mains = collect_jsonl_files(project_dir)?;
    if !include_subagents {
        return Ok(mains);
    }
    let mut out = Vec::new();
    let mut covered = HashSet::new();
    for main in mains {
        let stem = main.file_stem().map(|s| s.to_os_string());
        out.push(main);
        if let Some(stem) = stem {
            out.extend(ordered_subagents(&project_dir.join(&stem))?);
            covered.insert(stem);
        }
    }
    let mut orphan_dirs: Vec<PathBuf> = std::fs::read_dir(project_dir)
        .with_context(|| format!("read_dir {}", project_dir.display()))?
        .filter_map(|e| e.ok().map(|d| d.path()))
        .filter(|p| p.is_dir())
        .filter(|p| p.file_name().is_some_and(|n| !covered.contains(n)))
        .collect();
    orphan_dirs.sort();
    for dir in orphan_dirs {
        out.extend(ordered_subagents(&dir)?);
    }
    Ok(out)
}

fn ordered_subagents(session_dir: &Path) -> Result<Vec<PathBuf>> {
    let dir = subagents_dir(session_dir);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut files: Vec<(u32, PathBuf)> = std::fs::read_dir(&dir)
        .with_context(|| format!("read_dir {}", dir.display()))?
        .filter_map(|e| e.ok().map(|d| d.path()))
        .filter(|p| p.is_file() && SubagentPath::parse(p).is_some())
        .map(|p| {
            let depth = read_meta(&p).ok().flatten().map_or(1, |m| m.spawn_depth());
            (depth, p)
        })
        .collect();
    files.sort();
    Ok(files.into_iter().map(|(_, p)| p).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ClaudeCodeArgs;
    use crate::source::claude_code::ClaudeCodeSource;
    use crate::source::test_support::unpack_outcome;
    use crate::source::{
        ChatSource, ReadSessionOutcome, ThreadGroupConfidence, ThreadGroupIdentityScope,
    };
    use serde_json::json;
    use std::io::Write;
    use std::path::{Path, PathBuf};

    const PROJECT: &str = "-work-repo";
    const SESSION: &str = "11111111-2222-3333-4444-555555555555";

    struct Fixture {
        _tmp: tempfile::TempDir,
        claude_dir: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let tmp = tempfile::tempdir().unwrap();
            let claude_dir = tmp.path().join("claude");
            std::fs::create_dir_all(claude_dir.join("projects").join(PROJECT)).unwrap();
            Self {
                _tmp: tmp,
                claude_dir,
            }
        }

        fn project(&self) -> PathBuf {
            self.claude_dir.join("projects").join(PROJECT)
        }

        fn write_lines(path: &Path, lines: &[serde_json::Value]) {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let mut f = std::fs::File::create(path).unwrap();
            for line in lines {
                writeln!(f, "{line}").unwrap();
            }
        }

        fn agent_tool_use_line(
            uuid: &str,
            session: &str,
            tool_use_ids: &[&str],
        ) -> serde_json::Value {
            let blocks: Vec<_> = tool_use_ids
                .iter()
                .map(|id| json!({"type": "tool_use", "name": "Agent", "id": id, "input": {"prompt": "go"}}))
                .collect();
            json!({"type": "assistant", "uuid": uuid, "timestamp": "2026-10-01T00:00:01.000Z",
                   "sessionId": session, "cwd": "/work/repo", "gitBranch": "main",
                   "message": {"role": "assistant", "content": blocks}})
        }

        /// Main transcript invoking one `Agent` tool_use per id.
        fn main(&self, session: &str, tool_use_ids: &[&str]) -> PathBuf {
            let path = self.project().join(format!("{session}.jsonl"));
            let user = json!({"type": "user", "uuid": format!("{session}-u0"), "parentUuid": null,
                              "timestamp": "2026-10-01T00:00:00.000Z", "sessionId": session,
                              "cwd": "/work/repo", "gitBranch": "main",
                              "message": {"role": "user", "content": "please delegate"}});
            Self::write_lines(
                &path,
                &[
                    user,
                    Self::agent_tool_use_line(&format!("{session}-a0"), session, tool_use_ids),
                ],
            );
            path
        }

        fn subagent_path(&self, session: &str, agent_id: &str) -> PathBuf {
            self.project()
                .join(session)
                .join("subagents")
                .join(format!("agent-{agent_id}.jsonl"))
        }

        /// Subagent transcript whose rows carry the parent `sessionId`
        /// (as Claude Code writes them). `spawns` lists the `Agent`
        /// tool_use ids this subagent itself issues (nested spawns).
        fn subagent(
            &self,
            session: &str,
            agent_id: &str,
            meta: Option<serde_json::Value>,
            spawns: &[&str],
        ) -> PathBuf {
            let path = self.subagent_path(session, agent_id);
            let mut lines = vec![json!({"type": "user", "uuid": format!("{agent_id}-u1"),
                "parentUuid": null, "isSidechain": true, "agentId": agent_id,
                "timestamp": "2026-10-01T00:00:02.000Z", "sessionId": session,
                "cwd": "/work/repo", "gitBranch": "main",
                "message": {"role": "user", "content": "delegated task"}})];
            if !spawns.is_empty() {
                let mut line =
                    Self::agent_tool_use_line(&format!("{agent_id}-a1"), session, spawns);
                line["isSidechain"] = json!(true);
                line["agentId"] = json!(agent_id);
                lines.push(line);
            }
            Self::write_lines(&path, &lines);
            if let Some(meta) = meta {
                std::fs::write(path.with_extension("meta.json"), meta.to_string()).unwrap();
            }
            path
        }

        fn args(&self) -> ClaudeCodeArgs {
            ClaudeCodeArgs {
                session_file: None,
                project_dir: Some(self.project()),
                claude_dir: self.claude_dir.clone(),
                ..super::super::tests::args_with_file(PathBuf::new())
            }
        }

        fn source(&self) -> ClaudeCodeSource {
            ClaudeCodeSource::new(self.args())
        }
    }

    fn meta(tool_use_id: &str) -> serde_json::Value {
        json!({"agentType": "Explore", "description": "Find callers",
               "toolUseId": tool_use_id, "spawnDepth": 1})
    }

    fn nested_meta(tool_use_id: &str, parent_agent_id: &str, depth: u32) -> serde_json::Value {
        json!({"agentType": "general-purpose", "description": "Verify finding",
               "toolUseId": tool_use_id, "spawnDepth": depth, "parentAgentId": parent_agent_id})
    }

    fn native_id(agent_id: &str) -> String {
        format!("{SESSION}/subagents/agent-{agent_id}")
    }

    fn skip_reason(outcome: ReadSessionOutcome) -> String {
        match outcome {
            ReadSessionOutcome::Skipped { reason, .. } => reason,
            _ => panic!("expected Skipped"),
        }
    }

    #[test]
    fn discover_lists_each_session_then_its_subagents_by_depth() {
        let fx = Fixture::new();
        let main1 = fx.main(SESSION, &["t-b", "t-c"]);
        let b = fx.subagent(SESSION, "b", Some(meta("t-b")), &["t-a"]);
        let a = fx.subagent(SESSION, "a", Some(nested_meta("t-a", "b", 2)), &[]);
        let c = fx.subagent(SESSION, "c", Some(meta("t-c")), &[]);
        let main2 = fx.main("22222222-0000-0000-0000-000000000000", &[]);
        // Session directory whose main transcript is gone.
        let orphan = fx.subagent("33333333-0000-0000-0000-000000000000", "x", None, &[]);
        // Neither of these is a subagent transcript.
        std::fs::write(fx.project().join(SESSION).join("subagents/notes.txt"), "x").unwrap();
        Fixture::write_lines(
            &fx.project().join(SESSION).join("other/agent-z.jsonl"),
            &[json!({"type": "user"})],
        );

        let inputs = fx.source().discover().unwrap();
        assert_eq!(inputs, vec![main1, b, c, a, main2, orphan]);

        let mut args = fx.args();
        args.no_subagents = true;
        let mains = ClaudeCodeSource::new(args).discover().unwrap();
        assert_eq!(mains.len(), 2);
        assert!(
            mains
                .iter()
                .all(|p| p.parent() == Some(fx.project().as_path()))
        );
    }

    #[test]
    fn discover_all_projects_includes_subagents() {
        let fx = Fixture::new();
        let main = fx.main(SESSION, &["t-b"]);
        let b = fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);
        let mut args = fx.args();
        args.project_dir = None;
        args.all_projects = true;
        assert_eq!(
            ClaudeCodeSource::new(args).discover().unwrap(),
            vec![main, b]
        );
    }

    #[test]
    fn subagent_becomes_its_own_thread_delegated_from_the_main_session() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-b"]);
        let path = fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);

        let (session, entries, _) = unpack_outcome(fx.source().read_session(&path, None).unwrap());

        let composite = native_id("b");
        assert_eq!(session.session_id, composite);
        assert_eq!(session.channel, format!("claude_code:{composite}"));
        assert!(
            entries.iter().all(|e| e
                .external_id
                .starts_with(&format!("claude_code:{composite}:"))),
            "rows carry the parent sessionId but must not share its namespace"
        );
        let identity = session.source_identity.as_ref().unwrap();
        assert_eq!(identity.native_kind, "subagent_transcript");
        assert_eq!(identity.native_id, composite);
        assert_eq!(
            identity.identity_scope,
            ThreadGroupIdentityScope::Known(PROJECT.to_string())
        );

        assert_eq!(
            session.description.as_deref(),
            Some("Find callers (Explore)")
        );
        assert!(session.source_labels.contains(&"subagent".to_string()));
        // The agent type lives in metadata only: an extra label would split
        // the summary container away from the parent's.
        assert!(
            !session
                .source_labels
                .iter()
                .any(|l| l.starts_with("subagent_type:"))
        );
        assert!(
            session
                .source_labels
                .contains(&"agent:claude_code".to_string())
        );
        let sub = &session.source_metadata["subagent"];
        assert_eq!(sub["parent_session_id"], SESSION);
        assert_eq!(sub["agent_id"], "b");
        assert_eq!(sub["agent_type"], "Explore");
        assert_eq!(sub["tool_use_id"], "t-b");
        assert_eq!(sub["spawn_depth"], 1);
        assert!(sub.get("parent_agent_id").is_none());

        assert_eq!(session.thread_group_observations.len(), 1);
        let obs = &session.thread_group_observations[0];
        assert_eq!(obs.confidence, ThreadGroupConfidence::Exact);
        assert_eq!(obs.relation_kind.as_deref(), Some("delegated"));
        assert_eq!(obs.subject.native_id, composite);
        let parent = obs.candidate_parent.as_ref().unwrap();
        assert_eq!(
            (parent.native_kind, parent.native_id.as_str()),
            ("session", SESSION)
        );
        assert_eq!(
            obs.source_record_ref,
            "claude_code:subagents/agent.meta.json"
        );

        let ancestors: Vec<_> = session
            .structural_ancestor_identities
            .iter()
            .map(|i| i.native_id.as_str())
            .collect();
        assert_eq!(ancestors, vec![SESSION]);
    }

    #[test]
    fn nested_subagents_are_children_of_their_invoking_subagent() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-p1"]);
        fx.subagent(SESSION, "p1", Some(meta("t-p1")), &["t-p2"]);
        fx.subagent(SESSION, "p2", Some(nested_meta("t-p2", "p1", 2)), &["t-p3"]);
        let leaf = fx.subagent(SESSION, "p3", Some(nested_meta("t-p3", "p2", 3)), &[]);

        let (session, _, _) = unpack_outcome(fx.source().read_session(&leaf, None).unwrap());

        let parent = session.thread_group_observations[0]
            .candidate_parent
            .clone()
            .unwrap();
        assert_eq!(parent.native_kind, "subagent_transcript");
        assert_eq!(parent.native_id, native_id("p2"));
        assert_eq!(
            session.thread_group_observations[0].confidence,
            ThreadGroupConfidence::Exact
        );
        let ancestors: Vec<_> = session
            .structural_ancestor_identities
            .iter()
            .map(|i| i.native_id.clone())
            .collect();
        assert_eq!(
            ancestors,
            vec![native_id("p2"), native_id("p1"), SESSION.to_string()]
        );
        assert_eq!(session.source_metadata["subagent"]["parent_agent_id"], "p2");
        assert_eq!(session.source_metadata["subagent"]["spawn_depth"], 3);
    }

    #[test]
    fn fork_context_ref_moves_to_metadata_and_the_copied_agent_call_does_not_rebind() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-f"]);
        let path = fx.subagent_path(SESSION, "f");
        Fixture::write_lines(
            &path,
            &[
                json!({"type": "fork-context-ref", "agentId": "f", "parentSessionId": SESSION,
                       "parentLastUuid": "parent-last", "contextLength": 12}),
                // Fork transcripts start with a copy of the parent's Agent call.
                Fixture::agent_tool_use_line("f-copy", SESSION, &["t-f"]),
            ],
        );
        std::fs::write(
            path.with_extension("meta.json"),
            json!({"agentType": "fork", "isFork": true, "description": "Side task",
                   "toolUseId": "t-f", "spawnDepth": 1})
            .to_string(),
        )
        .unwrap();

        let (session, entries, _) = unpack_outcome(fx.source().read_session(&path, None).unwrap());
        let sub = &session.source_metadata["subagent"];
        assert_eq!(sub["is_fork"], true);
        assert_eq!(sub["fork_parent_last_uuid"], "parent-last");
        assert!(
            entries
                .iter()
                .all(|e| e.metadata.get("entry_type").and_then(|v| v.as_str())
                    != Some("fork-context-ref"))
        );
        let parent = session.thread_group_observations[0]
            .candidate_parent
            .clone()
            .unwrap();
        assert_eq!(parent.native_id, SESSION);
    }

    #[test]
    fn unresolvable_subagents_are_skipped_not_imported() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-ok"]);
        let cases = [
            ("nometa", None),
            (
                "notool",
                Some(json!({"agentType": "Explore", "spawnDepth": 1})),
            ),
            ("mismatch", Some(meta("t-other"))),
            (
                "depthnoparent",
                Some(json!({"agentType": "Explore", "toolUseId": "t-ok", "spawnDepth": 2})),
            ),
            ("parentdepth1", Some(nested_meta("t-ok", "ghost", 1))),
            ("ghostparent", Some(nested_meta("t-ok", "ghost", 2))),
        ];
        for (agent_id, meta) in cases {
            let path = fx.subagent(SESSION, agent_id, meta, &[]);
            let reason = skip_reason(fx.source().read_session(&path, None).unwrap());
            assert!(
                reason.starts_with("subagent_parent_unresolved"),
                "{agent_id}: {reason}"
            );
        }

        // Main transcript missing entirely.
        let orphan = fx.subagent(
            "99999999-0000-0000-0000-000000000000",
            "o",
            Some(meta("t-ok")),
            &[],
        );
        assert!(
            skip_reason(fx.source().read_session(&orphan, None).unwrap())
                .starts_with("subagent_parent_unresolved")
        );

        // Only the `Agent` tool counts; the legacy `Task` name does not.
        let task_session = "44444444-0000-0000-0000-000000000000";
        let main = fx.project().join(format!("{task_session}.jsonl"));
        let mut line = Fixture::agent_tool_use_line("task-a0", task_session, &["t-task"]);
        line["message"]["content"][0]["name"] = json!("Task");
        Fixture::write_lines(&main, &[line]);
        let task_child = fx.subagent(task_session, "t", Some(meta("t-task")), &[]);
        assert!(
            skip_reason(fx.source().read_session(&task_child, None).unwrap())
                .starts_with("subagent_parent_unresolved")
        );
    }

    #[test]
    fn disabled_subagents_are_skipped_even_when_named_explicitly() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-b"]);
        let path = fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);
        let mut args = fx.args();
        args.project_dir = None;
        args.session_file = Some(path.clone());
        args.no_subagents = true;
        let reason = skip_reason(
            ClaudeCodeSource::new(args)
                .read_session(&path, None)
                .unwrap(),
        );
        assert!(reason.contains("subagent import disabled"), "{reason}");
    }

    #[test]
    fn session_file_naming_a_subagent_is_imported_as_one() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-b"]);
        let path = fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);
        let mut args = fx.args();
        args.project_dir = None;
        args.session_file = Some(path.clone());
        let source = ClaudeCodeSource::new(args);
        assert_eq!(source.discover().unwrap(), vec![path.clone()]);
        let (session, _, _) = unpack_outcome(source.read_session(&path, None).unwrap());
        assert_eq!(session.session_id, native_id("b"));
    }

    #[test]
    fn non_normalized_subagent_paths_never_fall_back_to_the_main_thread() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-b"]);
        let path = fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);
        // `..` leaves the session directory without a file name, the same
        // way a relative `--session-file subagents/agent-b.jsonl` does.
        let detour = path
            .parent()
            .unwrap()
            .join("..")
            .join("subagents")
            .join("agent-b.jsonl");
        let (session, _, _) = unpack_outcome(fx.source().read_session(&detour, None).unwrap());
        assert_eq!(session.session_id, native_id("b"));
    }

    #[test]
    fn unparseable_files_under_subagents_are_skipped_not_imported_as_main() {
        let fx = Fixture::new();
        fx.main(SESSION, &[]);
        let odd = fx.project().join(SESSION).join("subagents/agent-x.y.jsonl");
        Fixture::write_lines(
            &odd,
            &[json!({"type": "user", "uuid": "u", "sessionId": SESSION,
                     "timestamp": "2026-10-01T00:00:00.000Z",
                     "message": {"role": "user", "content": "x"}})],
        );
        let reason = skip_reason(fx.source().read_session(&odd, None).unwrap());
        assert!(reason.starts_with("subagent_path_unparseable"), "{reason}");
    }

    #[test]
    fn subagent_resolves_when_its_main_transcript_is_unchanged_since() {
        let fx = Fixture::new();
        let main = fx.main(SESSION, &["t-b"]);
        let path = fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);
        crate::source::test_support::set_file_mtime_ms(&main, 1_000);
        let since = Some(10_000_000);
        let source = fx.source();
        assert!(matches!(
            source.read_session(&main, since).unwrap(),
            ReadSessionOutcome::Skipped { .. }
        ));
        let (session, _, _) = unpack_outcome(source.read_session(&path, since).unwrap());
        assert_eq!(
            session.thread_group_observations[0].confidence,
            ThreadGroupConfidence::Exact
        );
    }

    #[test]
    fn project_dir_ending_in_dot_dot_still_discovers_subagents() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-b"]);
        fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);
        let mut args = fx.args();
        args.project_dir = Some(fx.project().join(SESSION).join(".."));
        let source = ClaudeCodeSource::new(args);
        let inputs = source.discover().unwrap();
        assert_eq!(inputs.len(), 2, "{inputs:?}");
        let (session, _, _) = unpack_outcome(source.read_session(&inputs[1], None).unwrap());
        assert_eq!(session.session_id, native_id("b"));
    }

    #[test]
    fn symlinked_project_keeps_one_identity_scope_for_parent_and_child() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-b"]);
        fx.subagent(SESSION, "b", Some(meta("t-b")), &[]);
        let link = fx.claude_dir.join("projects").join("-linked-repo");
        std::os::unix::fs::symlink(fx.project(), &link).unwrap();
        let mut args = fx.args();
        args.project_dir = Some(link);
        let source = ClaudeCodeSource::new(args);
        let inputs = source.discover().unwrap();
        let scope = |path: &PathBuf| {
            unpack_outcome(source.read_session(path, None).unwrap())
                .0
                .source_identity
                .unwrap()
                .identity_scope
        };
        let expected = ThreadGroupIdentityScope::Known("-linked-repo".to_string());
        assert_eq!(scope(&inputs[0]), expected, "main");
        assert_eq!(scope(&inputs[1]), expected, "subagent");
    }

    #[test]
    fn child_of_an_unresolvable_invoker_is_skipped_too() {
        let fx = Fixture::new();
        fx.main(SESSION, &[]);
        // p1 cannot be resolved (no meta), so it is never imported; its
        // child would otherwise wait forever for a parent Thread.
        fx.subagent(SESSION, "p1", None, &["t-p2"]);
        let child = fx.subagent(SESSION, "p2", Some(nested_meta("t-p2", "p1", 2)), &[]);
        let reason = skip_reason(fx.source().read_session(&child, None).unwrap());
        assert!(reason.starts_with("subagent_parent_unresolved"), "{reason}");
    }

    #[test]
    fn malformed_meta_and_non_decreasing_depth_are_skipped() {
        let fx = Fixture::new();
        fx.main(SESSION, &["t-a"]);
        let bad = fx.subagent(SESSION, "bad", None, &[]);
        std::fs::write(bad.with_extension("meta.json"), "{not json").unwrap();
        assert!(
            skip_reason(fx.source().read_session(&bad, None).unwrap())
                .starts_with("subagent_parent_unresolved")
        );

        // Depth must shrink towards the main session; equal depths would
        // allow a parentAgentId cycle.
        fx.subagent(SESSION, "x", Some(nested_meta("t-x", "y", 2)), &["t-y"]);
        let y = fx.subagent(SESSION, "y", Some(nested_meta("t-y", "x", 2)), &["t-x"]);
        assert!(
            skip_reason(fx.source().read_session(&y, None).unwrap())
                .starts_with("subagent_parent_unresolved")
        );
    }

    #[test]
    fn main_transcript_has_no_structural_ancestors() {
        let fx = Fixture::new();
        let main = fx.main(SESSION, &["t-b"]);
        let (session, _, _) = unpack_outcome(fx.source().read_session(&main, None).unwrap());
        assert_eq!(session.session_id, SESSION);
        assert!(session.structural_ancestor_identities.is_empty());
        assert!(!session.source_labels.contains(&"subagent".to_string()));
    }

    #[test]
    fn invoker_tool_use_ids_are_built_once_per_transcript() {
        let fx = Fixture::new();
        let main = fx.main(SESSION, &["t-1", "t-2"]);
        let cache = InvokerToolUseIds::default();
        let first = cache.get(&main).unwrap();
        let second = cache.get(&main).unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert!(first.contains("t-1") && first.contains("t-2"));
    }

    #[test]
    fn subagent_path_requires_the_dedicated_layout() {
        let base = Path::new("/c/projects").join(PROJECT);
        let parsed =
            SubagentPath::parse(&base.join(SESSION).join("subagents").join("agent-abc.jsonl"))
                .unwrap();
        assert_eq!(parsed.project_key, PROJECT);
        assert_eq!(parsed.session_id, SESSION);
        assert_eq!(parsed.agent_id, "abc");
        assert!(SubagentPath::parse(&base.join("agent-abc.jsonl")).is_none());
        assert!(SubagentPath::parse(&base.join(SESSION).join("other/agent-abc.jsonl")).is_none());
        assert!(
            SubagentPath::parse(&base.join(SESSION).join("subagents/agent-abc.meta.json"))
                .is_none()
        );
        assert!(SubagentPath::parse(&base.join(SESSION).join("subagents/agent-.jsonl")).is_none());
    }
}
