//! OpenCode SQLite session importer.
//!
//! OpenCode's local database is not a public stability boundary.  This
//! module therefore validates the small contract we consume (session,
//! message, and part tables) and refuses to guess when the shape changes.

use super::{
    CanonicalAddons, CanonicalEntry, CanonicalSession, ChatSource, OPENCODE_ADAPTER_VERSION,
    ReadSessionOutcome, SourceDiagnostics, ThreadGroupConfidence, ThreadGroupEvidenceKind,
    ThreadGroupIdentityScope, ThreadGroupObservation, ThreadGroupPolarity,
    ThreadGroupSourceIdentity,
};
use crate::cli::OpenCodeArgs;
use crate::common::canonical::{
    AttachmentKind, AttachmentStorage, BuildToolResult, ToolStatus, build_attachment,
    build_elided_attachment, build_redacted_ref_attachment, build_tool_call, build_tool_output,
    size_config, tool_category,
};
use crate::common::git::resolve_repo_label;
use crate::common::ids::sha256_hex_prefix;
use crate::common::labels::{truncate_label_keep_head, truncate_label_keep_tail};
use crate::common::path::apply_path_prefix;
use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use protobuf::llm_memory::data::{ContentType, MessageRole};
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, Transaction, types::ValueRef};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const SOURCE_ID: &str = "opencode";
const IMPORT_SCHEMA: &str = "opencode-session-v1";
const MESSAGE_SCHEMA: &str = "opencode-message-v1";
const SESSION_CANDIDATE_SQL: &str = "SELECT (time_updated >= ?2 OR EXISTS(SELECT 1 FROM message m WHERE m.session_id=s.id AND m.time_updated >= ?2) OR EXISTS(SELECT 1 FROM part p WHERE p.session_id=s.id AND p.time_updated >= ?2)) FROM session s WHERE s.id=?1";
const KNOWN_TYPES: &[&str] = &[
    "user",
    "assistant",
    "tool_call",
    "tool_output",
    "system",
    "reasoning",
    "attachment",
];

#[derive(Debug, Clone)]
pub struct OpenCodeSource {
    args: OpenCodeArgs,
    db_path: PathBuf,
    requested_since: Option<i64>,
    forced_ancestor_ids: Arc<Mutex<HashSet<String>>>,
}

struct IncrementalDiscovery<'a> {
    conn: &'a Connection,
    has_parent_id: bool,
    has_archived: bool,
    selected_ids: &'a HashSet<String>,
    seen: HashSet<String>,
    visiting: HashSet<String>,
    forced_ancestors: HashSet<String>,
    inputs: Vec<SessionInput>,
}

#[derive(Debug, Clone)]
pub struct SessionInput {
    pub id: String,
}

impl OpenCodeSource {
    pub fn new(args: OpenCodeArgs, requested_since: Option<i64>) -> Result<Self> {
        if args
            .include_types
            .split(',')
            .any(|part| part.trim().is_empty())
        {
            bail!("--include-types contains an empty element");
        }
        validate_include_types(&args.include_types_set())?;
        if let Some(id) = args.session_id.as_deref() {
            validate_id(id, "session", "ses")?;
        }
        Ok(Self {
            db_path: args.resolved_db(),
            args,
            requested_since,
            forced_ancestor_ids: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    fn open_readonly(&self) -> Result<Connection> {
        if !self.db_path.exists() {
            bail!(
                "OpenCode database does not exist: {}",
                self.db_path.display()
            );
        }
        // The CLI documents a plain filesystem path. SQLITE_OPEN_URI would
        // reinterpret `?` / `#` (both legal in Unix filenames) as URI
        // syntax, so the URI flag is only honored when the user passes an
        // actual `file:` URI.
        let flags = if self
            .db_path
            .to_str()
            .is_some_and(|value| value.starts_with("file:"))
        {
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI
        } else {
            OpenFlags::SQLITE_OPEN_READ_ONLY
        };
        let conn = Connection::open_with_flags(&self.db_path, flags).with_context(|| {
            format!(
                "open OpenCode database read-only: {}",
                self.db_path.display()
            )
        })?;
        // Do not set journal_mode or immutable=1. SQLite itself coordinates
        // WAL readers and creates/uses the shared-memory sidecar as needed.
        conn.execute_batch("PRAGMA query_only = ON")
            .context("enable SQLite query-only mode")?;
        Ok(conn)
    }

    fn has_column(conn: &Connection, table: &str, column: &str) -> Result<bool> {
        let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{table}\")"))?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let name: String = row.get(1)?;
            if name == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn session_parent_and_archived(
        conn: &Connection,
        id: &str,
        has_parent_id: bool,
        has_archived: bool,
    ) -> Result<Option<(Option<String>, Option<i64>)>> {
        let parent_column = if has_parent_id { "parent_id" } else { "NULL" };
        let archived_column = if has_archived {
            "time_archived"
        } else {
            "NULL"
        };
        let sql = format!("SELECT {parent_column}, {archived_column} FROM session WHERE id=?1");
        conn.query_row(&sql, [id], |row| Ok((row.get(0)?, row.get(1)?)))
            .optional()
            .map_err(Into::into)
    }

    fn append_incremental_session(
        &self,
        discovery: &mut IncrementalDiscovery<'_>,
        id: &str,
    ) -> Result<()> {
        if discovery.seen.contains(id) || discovery.visiting.contains(id) {
            // A malformed database can contain a parent cycle. The path is
            // already represented by the active recursion, so stop without
            // emitting another input or recursing forever.
            return Ok(());
        }
        if validate_id(id, "parent session", "ses").is_err() {
            // Keep malformed parent references in the input stream so the
            // normal per-session read error identifies the bad source row.
            discovery.seen.insert(id.to_string());
            discovery.inputs.push(SessionInput { id: id.to_string() });
            return Ok(());
        }

        let Some((parent_id, archived_at_ms)) = Self::session_parent_and_archived(
            discovery.conn,
            id,
            discovery.has_parent_id,
            discovery.has_archived,
        )?
        else {
            // A missing parent remains a session-scoped read error instead
            // of aborting discovery or hiding the changed child session.
            discovery.seen.insert(id.to_string());
            discovery.inputs.push(SessionInput { id: id.to_string() });
            return Ok(());
        };
        if archived_at_ms.is_some() && !self.args.include_archived {
            // Do not cross an archived session when the existing archived
            // policy excludes it; its ancestors are not independently useful
            // because the parent edge would still be absent.
            return Ok(());
        }

        discovery.visiting.insert(id.to_string());
        if let Some(parent_id) = parent_id {
            self.append_incremental_session(discovery, &parent_id)?;
        }
        discovery.visiting.remove(id);

        if discovery.seen.insert(id.to_string()) {
            if !discovery.selected_ids.contains(id) {
                discovery.forced_ancestors.insert(id.to_string());
            }
            discovery.inputs.push(SessionInput { id: id.to_string() });
        }
        Ok(())
    }

    fn validate_schema(conn: &Connection) -> Result<()> {
        for (table, required) in [
            (
                "session",
                &[
                    "id",
                    "project_id",
                    "directory",
                    "title",
                    "version",
                    "time_created",
                    "time_updated",
                ][..],
            ),
            (
                "message",
                &["id", "session_id", "time_created", "time_updated", "data"][..],
            ),
            (
                "part",
                &[
                    "id",
                    "message_id",
                    "session_id",
                    "time_created",
                    "time_updated",
                    "data",
                ][..],
            ),
        ] {
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
                [table],
                |row| row.get(0),
            )?;
            if !exists {
                bail!("OpenCode database is missing required table `{table}`");
            }
            for column in required {
                if !Self::has_column(conn, table, column)? {
                    bail!("OpenCode database is missing required column `{table}.{column}`");
                }
            }
        }
        Ok(())
    }

    fn session_is_candidate(&self, conn: &Connection, id: &str) -> Result<bool> {
        let Some(since) = self.requested_since else {
            return Ok(true);
        };
        let hit: bool = conn.query_row(SESSION_CANDIDATE_SQL, (id, since), |row| row.get(0))?;
        Ok(hit)
    }

    fn read_session_row(tx: &Transaction<'_>, id: &str) -> Result<SessionRow> {
        let required = tx
            .query_row(
                "SELECT id, project_id, directory, title, version, time_created, time_updated FROM session WHERE id=?1",
                [id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                        row.get::<_, i64>(5)?,
                        row.get::<_, i64>(6)?,
                    ))
                },
            )
            .with_context(|| format!("read OpenCode session {id}"))?;
        let optional = |column: &str| -> Result<Option<String>> {
            if !OpenCodeSource::has_column(tx, "session", column)? {
                return Ok(None);
            }
            tx.query_row(
                &format!("SELECT \"{column}\" FROM session WHERE id=?1"),
                [id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        };
        let optional_i64 = |column: &str| -> Result<Option<i64>> {
            if !OpenCodeSource::has_column(tx, "session", column)? {
                return Ok(None);
            }
            tx.query_row(
                &format!("SELECT \"{column}\" FROM session WHERE id=?1"),
                [id],
                |row| row.get(0),
            )
            .map_err(Into::into)
        };
        Ok(SessionRow {
            id: required.0,
            project_id: required.1,
            directory: required.2,
            title: required.3,
            version: required.4,
            created_at_ms: required.5,
            updated_at_ms: required.6,
            parent_id: optional("parent_id")?,
            workspace_id: optional("workspace_id")?,
            path: optional("path")?,
            slug: optional("slug")?,
            agent: optional("agent")?,
            model: optional("model")?,
            archived_at_ms: optional_i64("time_archived")?,
        })
    }

    fn read_messages(tx: &Transaction<'_>, session_id: &str) -> Result<Vec<MessageRow>> {
        let mut stmt = tx.prepare("SELECT id, session_id, time_created, time_updated, data FROM message WHERE session_id=?1 ORDER BY time_created ASC, id ASC")?;
        let rows = stmt
            .query_map([session_id], MessageRow::from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    fn read_parts(tx: &Transaction<'_>, session_id: &str) -> Result<Vec<PartRow>> {
        let mut stmt = tx.prepare("SELECT id, message_id, session_id, time_created, time_updated, data FROM part WHERE session_id=?1 ORDER BY id ASC")?;
        let rows = stmt
            .query_map([session_id], PartRow::from_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}

impl ChatSource for OpenCodeSource {
    type SessionInput = SessionInput;

    fn id(&self) -> &str {
        SOURCE_ID
    }

    fn input_label(&self, input: &Self::SessionInput) -> String {
        input.id.clone()
    }

    fn discover(&self) -> Result<Vec<Self::SessionInput>> {
        let conn = self.open_readonly()?;
        Self::validate_schema(&conn)?;
        self.forced_ancestor_ids
            .lock()
            .map_err(|_| anyhow!("OpenCode ancestor state lock poisoned"))?
            .clear();
        if let Some(id) = self.args.session_id.as_deref() {
            // A single-session import must not scan or validate unrelated
            // rows. This also keeps an invalid row in another session from
            // preventing a targeted import.
            let exists: bool = conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM session WHERE id=?1)",
                [id],
                |row| row.get(0),
            )?;
            if !exists {
                bail!("OpenCode session does not exist: {id}");
            }
            return Ok(vec![SessionInput { id: id.to_string() }]);
        }
        let has_archived = Self::has_column(&conn, "session", "time_archived")?;
        let has_parent_id = Self::has_column(&conn, "session", "parent_id")?;
        let mut stmt = conn.prepare("SELECT id FROM session ORDER BY time_created ASC, id ASC")?;
        let mut selected_ids = Vec::new();
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        for row in rows {
            let id = row?;
            // Keep malformed IDs in the candidate set so `--all-sessions`
            // reports a session-scoped error while continuing with other
            // sessions. A single `--session-id` is validated at construction
            // time and fails as a CLI/configuration error instead.
            if validate_id(&id, "session", "ses").is_err() {
                selected_ids.push(id);
                continue;
            }
            let archived: Option<i64> = if has_archived {
                conn.query_row(
                    "SELECT time_archived FROM session WHERE id=?1",
                    [&id],
                    |row| row.get(0),
                )?
            } else {
                None
            };
            if archived.is_some() && !self.args.include_archived {
                continue;
            }
            if !self.session_is_candidate(&conn, &id)? {
                continue;
            }
            selected_ids.push(id);
        }
        let Some(_) = self.requested_since else {
            return Ok(selected_ids
                .into_iter()
                .map(|id| SessionInput { id })
                .collect());
        };

        let selected_set = selected_ids.iter().cloned().collect::<HashSet<_>>();
        let mut discovery = IncrementalDiscovery {
            conn: &conn,
            has_parent_id,
            has_archived,
            selected_ids: &selected_set,
            seen: HashSet::new(),
            visiting: HashSet::new(),
            forced_ancestors: HashSet::new(),
            inputs: Vec::new(),
        };
        for id in &selected_ids {
            self.append_incremental_session(&mut discovery, id)?;
        }
        *self
            .forced_ancestor_ids
            .lock()
            .map_err(|_| anyhow!("OpenCode ancestor state lock poisoned"))? =
            discovery.forced_ancestors;
        Ok(discovery.inputs)
    }

    fn entry_since(&self, _requested_since: Option<i64>) -> Option<i64> {
        None
    }

    fn read_session(
        &self,
        input: &Self::SessionInput,
        _since_millis_with_margin: Option<i64>,
    ) -> Result<ReadSessionOutcome> {
        validate_id(&input.id, "session", "ses")?;
        let mut conn = self.open_readonly()?;
        Self::validate_schema(&conn)?;
        let tx = conn.transaction().context("begin OpenCode read snapshot")?;
        let session = Self::read_session_row(&tx, &input.id)?;
        if session.archived_at_ms.is_some() && !self.args.include_archived {
            return Ok(ReadSessionOutcome::Skipped {
                session_id_hint: Some(input.id.clone()),
                reason: "archived".to_string(),
                filtered_count: 0,
            });
        }
        let forced_ancestor = self
            .forced_ancestor_ids
            .lock()
            .map_err(|_| anyhow!("OpenCode ancestor state lock poisoned"))?
            .contains(&input.id);
        if !forced_ancestor && !self.session_is_candidate_tx(&tx, &input.id)? {
            return Ok(ReadSessionOutcome::Skipped {
                session_id_hint: Some(input.id.clone()),
                reason: "unchanged since requested timestamp".to_string(),
                filtered_count: 0,
            });
        }
        let messages = Self::read_messages(&tx, &input.id)?;
        let parts = Self::read_parts(&tx, &input.id)?;
        let outcome = normalize_session(&session, messages, parts, &self.args)?;
        drop(tx);
        Ok(outcome)
    }
}

impl OpenCodeSource {
    fn session_is_candidate_tx(&self, tx: &Transaction<'_>, id: &str) -> Result<bool> {
        let Some(since) = self.requested_since else {
            return Ok(true);
        };
        let hit: bool = tx.query_row(SESSION_CANDIDATE_SQL, (id, since), |row| row.get(0))?;
        Ok(hit)
    }
}

#[derive(Debug, Clone)]
struct SessionRow {
    id: String,
    project_id: Option<String>,
    directory: Option<String>,
    title: Option<String>,
    version: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
    parent_id: Option<String>,
    workspace_id: Option<String>,
    path: Option<String>,
    slug: Option<String>,
    agent: Option<String>,
    model: Option<String>,
    archived_at_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct MessageRow {
    id: String,
    session_id: String,
    created_at_ms: i64,
    updated_at_ms: i64,
    data: Value,
}

impl MessageRow {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            session_id: row.get(1)?,
            created_at_ms: row.get(2)?,
            updated_at_ms: row.get(3)?,
            data: parse_json_column(row, 4)?,
        })
    }
}

#[derive(Debug, Clone)]
struct PartRow {
    id: String,
    message_id: String,
    session_id: String,
    created_at_ms: i64,
    updated_at_ms: i64,
    data: Value,
}

impl PartRow {
    fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            message_id: row.get(1)?,
            session_id: row.get(2)?,
            created_at_ms: row.get(3)?,
            updated_at_ms: row.get(4)?,
            data: parse_json_column(row, 5)?,
        })
    }
}

fn parse_json_column(row: &Row<'_>, index: usize) -> rusqlite::Result<Value> {
    match row.get_ref(index)? {
        ValueRef::Text(bytes) => serde_json::from_slice(bytes).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(e),
            )
        }),
        ValueRef::Blob(bytes) => serde_json::from_slice(bytes).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Blob,
                Box::new(e),
            )
        }),
        _ => Err(rusqlite::Error::InvalidColumnType(
            index,
            "data".to_string(),
            rusqlite::types::Type::Text,
        )),
    }
}

fn validate_include_types(types: &HashSet<String>) -> Result<()> {
    if types.is_empty() {
        bail!("--include-types must not be empty");
    }
    for value in types {
        if !KNOWN_TYPES.contains(&value.as_str()) {
            bail!("unknown OpenCode include type: {value}");
        }
    }
    Ok(())
}

fn validate_id(value: &str, label: &str, prefix: &str) -> Result<()> {
    if value.is_empty() || !value.starts_with(prefix) || value.contains('\0') || value.contains(':')
    {
        bail!("invalid OpenCode {label} id");
    }
    Ok(())
}

fn object<'a>(value: &'a Value, what: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{what} must be a JSON object"))
}

fn non_empty_string(value: Option<&Value>, what: &str) -> Result<String> {
    let value = value
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{what} must be a string"))?;
    if value.is_empty() {
        bail!("{what} must not be empty");
    }
    Ok(value.to_string())
}

fn optional_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .filter(|s| !s.is_empty())
}

fn positive_ms(value: Option<&Value>, what: &str) -> Result<Option<i64>> {
    let Some(value) = value else { return Ok(None) };
    if value.is_null() {
        return Ok(None);
    }
    let n = value
        .as_i64()
        .ok_or_else(|| anyhow!("{what} must be an integer"))?;
    if n < 0 {
        bail!("{what} must not be negative");
    }
    Ok((n > 0).then_some(n))
}

fn message_completed_at(data: &Map<String, Value>) -> Result<Option<i64>> {
    let Some(time) = data.get("time") else {
        return Ok(None);
    };
    let time = object(time, "message.time")?;
    positive_ms(time.get("completed"), "message.time.completed")
}

fn normalized_model(value: Option<&Value>) -> Result<Option<Value>> {
    let Some(value) = value else { return Ok(None) };
    let obj = object(value, "session.model")?;
    let mut out = Map::new();
    if let Some(v) = obj.get("id").or_else(|| obj.get("modelID")) {
        out.insert(
            "id".to_string(),
            Value::String(non_empty_string(Some(v), "model.id")?),
        );
    }
    if let Some(v) = obj.get("providerID") {
        out.insert(
            "provider_id".to_string(),
            Value::String(non_empty_string(Some(v), "model.providerID")?),
        );
    }
    if let Some(v) = obj.get("variant")
        && !v.is_null()
    {
        out.insert(
            "variant".to_string(),
            Value::String(non_empty_string(Some(v), "model.variant")?),
        );
    }
    Ok((!out.is_empty()).then_some(Value::Object(out)))
}

fn session_metadata(session: &SessionRow) -> Result<Value> {
    if session.created_at_ms < 0
        || session.updated_at_ms < 0
        || session.archived_at_ms.is_some_and(|value| value < 0)
    {
        bail!("session timestamps must not be negative");
    }
    if let Some(parent) = &session.parent_id {
        validate_id(parent, "parent session", "ses")?;
    }
    let model_value = session
        .model
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?;
    let mut out = Map::new();
    out.insert("id".to_string(), Value::String(session.id.clone()));
    for (key, value) in [
        ("project_id", session.project_id.clone()),
        ("workspace_id", session.workspace_id.clone()),
        ("directory", session.directory.clone()),
        ("path", session.path.clone()),
        ("title", session.title.clone()),
        ("slug", session.slug.clone()),
        ("agent", session.agent.clone()),
        ("opencode_version", session.version.clone()),
        ("parent_session_id", session.parent_id.clone()),
    ] {
        if let Some(value) = value.filter(|v| !v.is_empty()) {
            out.insert(key.to_string(), Value::String(value));
        }
    }
    if let Some(model) = normalized_model(model_value.as_ref())? {
        out.insert("model".to_string(), model);
    }
    out.insert("created_at_ms".to_string(), json!(session.created_at_ms));
    out.insert("updated_at_ms".to_string(), json!(session.updated_at_ms));
    if let Some(value) = session.archived_at_ms {
        out.insert("archived_at_ms".to_string(), json!(value));
    }
    Ok(Value::Object(out))
}

fn normalize_session(
    session: &SessionRow,
    messages: Vec<MessageRow>,
    parts: Vec<PartRow>,
    args: &OpenCodeArgs,
) -> Result<ReadSessionOutcome> {
    validate_id(&session.id, "session", "ses")?;
    if session.created_at_ms < 0 || session.updated_at_ms < 0 {
        bail!("session timestamps must not be negative");
    }
    let session_meta = session_metadata(session)?;
    let mut messages_by_id = HashMap::new();
    let mut parts_by_message: HashMap<String, Vec<PartRow>> = HashMap::new();
    for message in messages {
        validate_id(&message.id, "message", "msg")?;
        if message.created_at_ms < 0 || message.updated_at_ms < 0 {
            bail!("message timestamps must not be negative");
        }
        if message.session_id != session.id {
            bail!("message belongs to another session");
        }
        if messages_by_id.insert(message.id.clone(), message).is_some() {
            bail!("duplicate message id");
        }
    }
    for part in parts {
        validate_id(&part.id, "part", "prt")?;
        if part.created_at_ms < 0 || part.updated_at_ms < 0 {
            bail!("part timestamps must not be negative");
        }
        if part.session_id != session.id || !messages_by_id.contains_key(&part.message_id) {
            bail!("part has invalid session/message reference");
        }
        parts_by_message
            .entry(part.message_id.clone())
            .or_default()
            .push(part);
    }
    let mut part_ids = HashSet::new();
    for values in parts_by_message.values() {
        for part in values {
            if !part_ids.insert(part.id.clone()) {
                bail!("duplicate part id");
            }
            let data = object(&part.data, "part.data")?;
            non_empty_string(data.get("type"), "part.type")?;
        }
    }
    for values in parts_by_message.values_mut() {
        values.sort_by(|a, b| a.id.cmp(&b.id));
    }

    let mut parent_messages = HashMap::new();
    for message in messages_by_id.values() {
        let data = object(&message.data, "message.data")?;
        let role = non_empty_string(data.get("role"), "message.role")?;
        if role == "assistant" {
            let parent = non_empty_string(data.get("parentID"), "assistant.parentID")?;
            validate_id(&parent, "parent message", "msg")?;
            parent_messages.insert(message.id.clone(), parent);
        }
    }
    detect_parent_cycle(&parent_messages)?;

    let ordered = order_messages(&messages_by_id, &parent_messages)?;
    let include = args.include_types_set();
    let mut entries = Vec::new();
    let mut filtered = 0usize;
    let mut diagnostics = SourceDiagnostics::default();
    let mut usage_messages = HashSet::new();
    let mut usage_warning_seen = HashSet::new();
    let mut parent_warning_seen = HashSet::new();
    let mut message_anchor: HashMap<String, String> = HashMap::new();
    for message in ordered {
        let data = object(&message.data, "message.data")?;
        let role_name = non_empty_string(data.get("role"), "message.role")?;
        if role_name != "user" && role_name != "assistant" {
            diagnostics
                .warning_exclusions_by_reason
                .entry("unknown_role".to_string())
                .and_modify(|n| *n += parts_by_message.get(&message.id).map_or(0, Vec::len))
                .or_insert(parts_by_message.get(&message.id).map_or(0, Vec::len));
            continue;
        }
        if role_name == "assistant" && message_completed_at(data)?.is_none() {
            diagnostics.deferred += parts_by_message.get(&message.id).map_or(0, Vec::len);
            continue;
        }
        let mut previous: Option<String> = None;
        for part in parts_by_message.remove(&message.id).unwrap_or_default() {
            let part_type = part.data.get("type").and_then(Value::as_str).unwrap_or("");
            let tool_status = part
                .data
                .get("state")
                .and_then(Value::as_object)
                .and_then(|state| state.get("status"))
                .and_then(Value::as_str);
            if part_type == "tool"
                && tool_status == Some("completed")
                && part
                    .data
                    .get("state")
                    .and_then(|v| v.get("time"))
                    .and_then(|v| v.get("compacted"))
                    .is_some()
            {
                *diagnostics
                    .ignored_by_reason
                    .entry("compacted_tool_output".to_string())
                    .or_default() += 1;
                let attachment_count = part
                    .data
                    .get("state")
                    .and_then(|v| v.get("attachments"))
                    .and_then(Value::as_array)
                    .map_or(0, Vec::len);
                *diagnostics
                    .ignored_by_reason
                    .entry("compacted_tool_attachment".to_string())
                    .or_default() += attachment_count;
            }
            let produced = match normalize_part(
                session,
                &message,
                &part,
                &role_name,
                &mut diagnostics,
                &mut usage_warning_seen,
            ) {
                Ok(value) => value,
                Err(error) if part_type == "file" && is_attachment_error(&error) => {
                    let reason = attachment_error_reason(&error);
                    *diagnostics
                        .warning_exclusions_by_reason
                        .entry(reason.to_string())
                        .or_default() += 1;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if produced.is_empty() {
                if matches!(
                    part_type,
                    "step-start" | "step-finish" | "snapshot" | "retry"
                ) {
                    *diagnostics
                        .ignored_by_reason
                        .entry("known_telemetry".to_string())
                        .or_default() += 1;
                } else if matches!(part_type, "text" | "reasoning") {
                    let reason = if part.data.get("ignored").and_then(Value::as_bool) == Some(true)
                    {
                        "ignored_text"
                    } else {
                        "empty_text"
                    };
                    *diagnostics
                        .ignored_by_reason
                        .entry(reason.to_string())
                        .or_default() += 1;
                } else if part_type == "tool" {
                    let status = part
                        .data
                        .get("state")
                        .and_then(|v| v.get("status"))
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if matches!(status, "pending" | "running") {
                        diagnostics.deferred += 1;
                    } else if !matches!(status, "completed" | "error") {
                        *diagnostics
                            .warning_exclusions_by_reason
                            .entry("unknown_tool_status".to_string())
                            .or_default() += 1;
                    }
                } else if !matches!(part_type, "subtask" | "patch" | "agent" | "compaction") {
                    *diagnostics
                        .warning_exclusions_by_reason
                        .entry("unknown_part_type".to_string())
                        .or_default() += 1;
                }
            }
            for mut entry in produced {
                if !include.contains(entry.kind_tag) {
                    filtered += 1;
                    continue;
                }
                if !usage_messages.insert(message.id.clone())
                    && let Some(opencode) = entry
                        .metadata
                        .get_mut("opencode")
                        .and_then(Value::as_object_mut)
                {
                    opencode.remove("usage");
                }
                // Attachments are emitted from a tool output. If the caller
                // selected `attachment` without `tool_output`, drop the
                // generated output parent rather than sending a dangling
                // external ID to the server.
                if !include.contains("tool_output") {
                    entry
                        .parent_external_ids
                        .retain(|parent| !parent.ends_with(":output"));
                }
                if let Some(parent_id) = parent_messages.get(&message.id) {
                    if let Some(parent) = message_anchor.get(parent_id) {
                        entry.parent_external_ids.push(parent.clone());
                    } else if parent_warning_seen.insert(message.id.clone()) {
                        *diagnostics
                            .warnings_by_reason
                            .entry("parent_message_unresolved".to_string())
                            .or_default() += 1;
                    }
                }
                if let Some(prev) = previous.as_ref() {
                    entry.parent_external_ids.insert(0, prev.clone());
                }
                let mut seen_parents = HashSet::new();
                entry
                    .parent_external_ids
                    .retain(|parent| seen_parents.insert(parent.clone()));
                previous = Some(entry.external_id.clone());
                message_anchor.insert(message.id.clone(), entry.external_id.clone());
                entries.push(entry);
            }
        }
    }
    // The importer uses `import_order` as the primary stable key. Assign it
    // after parent-aware ordering so a child with an earlier source
    // timestamp cannot be sorted ahead of its parent by the shared path.
    for (index, entry) in entries.iter_mut().enumerate() {
        entry.import_order = index as i64;
    }
    Ok(ReadSessionOutcome::Import {
        session: build_session(session, session_meta, args)?,
        entries,
        source_filtered_count: filtered,
        diagnostics,
    })
}

fn order_messages(
    messages: &HashMap<String, MessageRow>,
    parents: &HashMap<String, String>,
) -> Result<Vec<MessageRow>> {
    // Kahn-style topological ordering with a ready-queue keyed by
    // `(created_at_ms, id)` so each step pops the earliest ready message
    // in O(log n) instead of rescanning every pending message. This keeps
    // ordering linear-ish for long sessions instead of O(n²).
    let mut remaining_children: HashMap<&str, usize> = HashMap::with_capacity(messages.len());
    let mut children_of: HashMap<&str, Vec<&str>> = HashMap::with_capacity(messages.len());
    let mut ready: std::collections::BTreeSet<(i64, &str)> = std::collections::BTreeSet::new();
    for (id, message) in messages {
        let parent = parents.get(id);
        match parent {
            // Missing parent (or parent outside this session's set) is
            // treated as a root and does not block the child.
            Some(p) if messages.contains_key(p.as_str()) => {
                remaining_children.insert(id.as_str(), 1);
                children_of.entry(p.as_str()).or_default().push(id.as_str());
            }
            _ => {
                remaining_children.insert(id.as_str(), 0);
                ready.insert((message.created_at_ms, id.as_str()));
            }
        }
    }
    let mut ordered = Vec::with_capacity(messages.len());
    while let Some(&(_, id)) = ready.iter().next() {
        ready.remove(&(messages[id].created_at_ms, id));
        let message = messages
            .get(id)
            .expect("ready message must exist in messages");
        ordered.push(message.clone());
        if let Some(children) = children_of.get(id) {
            for child in children {
                let count = remaining_children
                    .get_mut(*child)
                    .expect("child must be pending");
                *count -= 1;
                if *count == 0 {
                    let child_message = messages.get(*child).expect("child must exist in messages");
                    ready.insert((child_message.created_at_ms, *child));
                }
            }
        }
    }
    if ordered.len() != messages.len() {
        bail!("assistant parent cycle detected");
    }
    Ok(ordered)
}

fn detect_parent_cycle(parents: &HashMap<String, String>) -> Result<()> {
    fn visit(
        id: &str,
        parents: &HashMap<String, String>,
        visiting: &mut HashSet<String>,
        done: &mut HashSet<String>,
    ) -> Result<()> {
        if done.contains(id) {
            return Ok(());
        }
        if !visiting.insert(id.to_string()) {
            bail!("assistant parent cycle detected");
        }
        if let Some(parent) = parents.get(id)
            && parents.contains_key(parent)
        {
            visit(parent, parents, visiting, done)?;
        }
        visiting.remove(id);
        done.insert(id.to_string());
        Ok(())
    }
    let mut visiting = HashSet::new();
    let mut done = HashSet::new();
    for id in parents.keys() {
        visit(id, parents, &mut visiting, &mut done)?;
    }
    Ok(())
}

fn build_session(
    session: &SessionRow,
    metadata: Value,
    args: &OpenCodeArgs,
) -> Result<CanonicalSession> {
    let directory = session.directory.clone();
    let mut labels = vec!["coding_agent".to_string(), "agent:opencode".to_string()];
    if let Some(dir) = directory.as_deref().filter(|d| !d.is_empty()) {
        let prefixes = args.path_prefixes();
        let display = apply_path_prefix(dir, &prefixes);
        labels.push(truncate_label_keep_tail("path:", display));
        if let Some((_, name)) = dir.rsplit_once('/')
            && !name.is_empty()
        {
            labels.push(truncate_label_keep_tail("dir:", name));
        }
        labels.extend(resolve_repo_label(Some(Path::new(dir))));
    }
    if let Some(model) = metadata
        .get("model")
        .and_then(|m| m.get("provider_id"))
        .and_then(Value::as_str)
    {
        labels.push(truncate_label_keep_head("provider:", model));
    }
    labels.retain(|label| !label.is_empty());
    Ok(CanonicalSession {
        source_id: SOURCE_ID.to_string(),
        session_id: session.id.clone(),
        channel: format!("{SOURCE_ID}:{}", session.id),
        description: session.title.clone().filter(|v| !v.is_empty()),
        cwd: directory,
        git_branch: None,
        created_at_ms: session.created_at_ms,
        updated_at_ms: session.updated_at_ms,
        source_labels: labels,
        source_metadata: metadata.clone(),
        thread_metadata: Some(
            json!({"source": SOURCE_ID, "import_schema": IMPORT_SCHEMA, "session": metadata}),
        ),
        source_identity: Some(opencode_identity(&session.id)),
        thread_group_observations: observations_from_session(
            &session.id,
            session.parent_id.as_deref(),
        ),
    })
}

/// Map the OpenCode `session.parent_id` field to the adapter observation
/// contract. A present, non-empty parent id is the durable parent
/// reference that makes `delegated` an `exact` source field. An absent
/// or empty parent is the distinct no-parent state and emits nothing;
/// OpenCode fork candidates have no durable parent ref and must not be
/// inferred here (spec 5.2.1).
fn observations_from_session(
    session_id: &str,
    parent_id: Option<&str>,
) -> Vec<ThreadGroupObservation> {
    let Some(parent_id) = parent_id.filter(|id| !id.is_empty()) else {
        return Vec::new();
    };
    vec![ThreadGroupObservation {
        subject: opencode_identity(session_id),
        candidate_parent: Some(opencode_identity(parent_id)),
        relation_kind: Some("delegated".into()),
        evidence_kind: ThreadGroupEvidenceKind::SourceField,
        polarity: ThreadGroupPolarity::Supports,
        confidence: ThreadGroupConfidence::Exact,
        source_record_ref: "opencode:session.parent_id".to_string(),
        adapter_version: OPENCODE_ADAPTER_VERSION,
    }]
}

fn opencode_identity(native_id: &str) -> ThreadGroupSourceIdentity {
    ThreadGroupSourceIdentity {
        source: SOURCE_ID.into(),
        native_kind: "session",
        owner_scope: None,
        identity_scope: ThreadGroupIdentityScope::Known(String::new()),
        native_id: native_id.to_string(),
    }
}

fn normalize_part(
    session: &SessionRow,
    message: &MessageRow,
    part: &PartRow,
    role: &str,
    diagnostics: &mut SourceDiagnostics,
    usage_warning_seen: &mut HashSet<String>,
) -> Result<Vec<CanonicalEntry>> {
    if part.session_id != session.id || part.message_id != message.id {
        bail!("part reference mismatch");
    }
    let data = object(&part.data, "part.data")?;
    let kind = non_empty_string(data.get("type"), "part.type")?;
    if matches!(
        kind.as_str(),
        "text" | "reasoning" | "tool" | "file" | "subtask" | "patch" | "agent" | "compaction"
    ) {
        for field in ["agent", "finish"] {
            if let Some(value) = data.get(field)
                && !value.is_null()
                && !value.is_string()
            {
                bail!("part.{field} must be a string");
            }
        }
        for field in ["ignored", "synthetic"] {
            if let Some(value) = data.get(field)
                && !value.is_null()
                && !value.is_boolean()
            {
                bail!("part.{field} must be a boolean");
            }
        }
    }
    let message_data = object(&message.data, "message.data")?;
    if role == "assistant"
        && let Some(summary) = message_data.get("summary")
        && !summary.is_null()
        && !summary.is_boolean()
    {
        bail!("message.summary must be a boolean");
    }
    let role_enum = if role == "user" {
        MessageRole::RoleUser
    } else {
        MessageRole::RoleAssistant
    };
    let base = format!(
        "{SOURCE_ID}:{}:msg:{}:part:{}",
        session.id, message.id, part.id
    );
    let mut common = Map::new();
    common.insert("message_id".to_string(), Value::String(message.id.clone()));
    common.insert("part_id".to_string(), Value::String(part.id.clone()));
    common.insert("part_type".to_string(), Value::String(kind.clone()));
    common.insert(
        "message_created_at_ms".to_string(),
        json!(message.created_at_ms),
    );
    common.insert(
        "message_updated_at_ms".to_string(),
        json!(message.updated_at_ms),
    );
    common.insert("part_created_at_ms".to_string(), json!(part.created_at_ms));
    common.insert("part_updated_at_ms".to_string(), json!(part.updated_at_ms));
    if let Some(completed) = message_completed_at(message_data)? {
        common.insert("message_completed_at_ms".to_string(), json!(completed));
    }
    if let Some(agent) = optional_string(data.get("agent")) {
        common.insert("agent".to_string(), Value::String(agent));
    }
    if let Some(finish) = optional_string(data.get("finish")) {
        common.insert("finish".to_string(), Value::String(finish));
    }
    if role == "assistant"
        && message_data
            .get("summary")
            .and_then(Value::as_bool)
            .is_some()
    {
        common.insert(
            "is_summary".to_string(),
            message_data
                .get("summary")
                .cloned()
                .unwrap_or(Value::Bool(false)),
        );
    }
    if let Some(model) = message_model(message_data, role)? {
        common.insert("model".to_string(), model);
    }
    if role == "assistant"
        && let Some(usage) =
            normalize_usage(message_data, &message.id, diagnostics, usage_warning_seen)?
    {
        common.insert("usage".to_string(), usage);
    }
    if let Some(parent) = message_data.get("parentID").and_then(Value::as_str) {
        common.insert(
            "parent_message_id".to_string(),
            Value::String(parent.to_string()),
        );
    }
    if role == "assistant"
        && let Some(error) = normalize_error_metadata(message_data)?
    {
        common.insert("error".to_string(), error);
    }
    match kind.as_str() {
        "text" | "reasoning" => {
            if let Some(time) = data.get("time")
                && !time.is_null()
                && !time.is_object()
            {
                bail!("part.time must be an object");
            }
            let timestamp = part_timestamp(part, message, &kind)?;
            let text = non_empty_or_empty_string(data.get("text"), "part.text")?;
            if text.is_empty() || data.get("ignored").and_then(Value::as_bool) == Some(true) {
                return Ok(Vec::new());
            }
            if data.get("synthetic").and_then(Value::as_bool) == Some(true) {
                common.insert("synthetic".to_string(), Value::Bool(true));
            }
            let kind_tag = if kind == "reasoning" {
                "reasoning"
            } else if role == "user" {
                "user"
            } else {
                "assistant"
            };
            let entry_role = if kind == "reasoning" {
                MessageRole::RoleAssistant
            } else {
                role_enum
            };
            Ok(vec![entry(
                base,
                entry_role,
                ContentType::Text,
                &text,
                common,
                timestamp,
                kind_tag,
            )])
        }
        "tool" => {
            let state = object(
                data.get("state")
                    .ok_or_else(|| anyhow!("tool.state is required"))?,
                "tool.state",
            )?;
            let status = non_empty_string(state.get("status"), "tool.state.status")?;
            if matches!(status.as_str(), "pending" | "running") {
                return Ok(Vec::new());
            }
            normalize_tool_part(
                base,
                data,
                common,
                part,
                message,
                &session.id,
                &message.id,
                diagnostics,
            )
        }
        "file" => {
            let timestamp = part_timestamp(part, message, &kind)?;
            let normalized = normalize_file_part(base, data, role_enum, common, timestamp);
            if normalized.is_ok() {
                record_attachment_warnings(data, diagnostics);
            }
            normalized
        }
        "subtask" | "patch" | "agent" | "compaction" => {
            let timestamp = part_timestamp(part, message, &kind)?;
            if let Some(runtime) = runtime_metadata(&kind, data)? {
                common.insert(kind.clone(), runtime);
            }
            let text = match kind.as_str() {
                "subtask" => format!(
                    "{}\n{}",
                    non_empty_or_empty_string(data.get("description"), "subtask.description")?,
                    non_empty_or_empty_string(data.get("prompt"), "subtask.prompt")?
                ),
                "patch" => data
                    .get("files")
                    .ok_or_else(|| anyhow!("patch.files is required"))?
                    .to_string(),
                "agent" => non_empty_or_empty_string(data.get("name"), "agent.name")?,
                _ => "OpenCode compaction".to_string(),
            };
            if text.is_empty() {
                return Ok(Vec::new());
            }
            Ok(vec![entry(
                base,
                MessageRole::RoleMeta,
                ContentType::Text,
                &text,
                common,
                timestamp,
                "system",
            )])
        }
        "step-start" | "step-finish" | "snapshot" | "retry" => Ok(Vec::new()),
        _ => Ok(Vec::new()),
    }
}

fn message_model(data: &Map<String, Value>, role: &str) -> Result<Option<Value>> {
    if role == "user" {
        return normalized_model(data.get("model"));
    }
    let mut model = Map::new();
    if let Some(value) = data.get("providerID") {
        model.insert(
            "provider_id".to_string(),
            Value::String(non_empty_string(Some(value), "message.providerID")?),
        );
    }
    if let Some(value) = data.get("modelID") {
        model.insert(
            "id".to_string(),
            Value::String(non_empty_string(Some(value), "message.modelID")?),
        );
    }
    if let Some(value) = data.get("variant")
        && !value.is_null()
    {
        model.insert(
            "variant".to_string(),
            Value::String(non_empty_string(Some(value), "message.variant")?),
        );
    }
    Ok((!model.is_empty()).then_some(Value::Object(model)))
}

fn normalize_error_metadata(data: &Map<String, Value>) -> Result<Option<Value>> {
    let Some(error) = data.get("error") else {
        return Ok(None);
    };
    let error = object(error, "message.error")?;
    let mut out = Map::new();
    if let Some(name) = error.get("name") {
        out.insert(
            "name".to_string(),
            Value::String(non_empty_or_empty_string(Some(name), "message.error.name")?),
        );
    }
    if let Some(data) = error.get("data") {
        let data = object(data, "message.error.data")?;
        if let Some(message) = data.get("message") {
            out.insert(
                "message".to_string(),
                Value::String(non_empty_or_empty_string(
                    Some(message),
                    "message.error.data.message",
                )?),
            );
        }
    }
    Ok((!out.is_empty()).then_some(Value::Object(out)))
}

fn runtime_metadata(kind: &str, data: &Map<String, Value>) -> Result<Option<Value>> {
    let mut out = Map::new();
    match kind {
        "subtask" => {
            for field in ["agent", "description"] {
                let value =
                    non_empty_or_empty_string(data.get(field), &format!("subtask.{field}"))?;
                out.insert(field.to_string(), Value::String(value));
            }
            if let Some(value) = data.get("command") {
                out.insert(
                    "command".to_string(),
                    Value::String(non_empty_or_empty_string(Some(value), "subtask.command")?),
                );
            }
            if let Some(model) = normalized_model(data.get("model"))? {
                out.insert("model".to_string(), model);
            }
        }
        "patch" => {
            let files = data
                .get("files")
                .ok_or_else(|| anyhow!("patch.files is required"))?;
            if !files.is_array()
                || files
                    .as_array()
                    .is_some_and(|values| values.iter().any(|value| !value.is_string()))
            {
                bail!("patch.files must be an array of strings");
            }
            let hash = non_empty_or_empty_string(data.get("hash"), "patch.hash")?;
            out.insert("patch_hash".to_string(), Value::String(hash));
        }
        "compaction" => {
            let auto = data
                .get("auto")
                .ok_or_else(|| anyhow!("compaction.auto is required"))?
                .as_bool()
                .ok_or_else(|| anyhow!("compaction.auto must be a boolean"))?;
            out.insert("auto".to_string(), Value::Bool(auto));
            for field in ["auto", "overflow"] {
                if field == "auto" {
                    continue;
                }
                if let Some(value) = data.get(field) {
                    let value = value
                        .as_bool()
                        .ok_or_else(|| anyhow!("compaction.{field} must be a boolean"))?;
                    out.insert(field.to_string(), Value::Bool(value));
                }
            }
            if let Some(value) = data.get("tail_start_id") {
                let value = non_empty_or_empty_string(Some(value), "compaction.tail_start_id")?;
                validate_id(&value, "compaction tail_start_id", "msg")?;
                out.insert("tail_start_id".to_string(), Value::String(value));
            }
        }
        _ => return Ok(None),
    }
    Ok((!out.is_empty()).then_some(Value::Object(out)))
}

fn normalize_usage(
    data: &Map<String, Value>,
    message_id: &str,
    diagnostics: &mut SourceDiagnostics,
    seen: &mut HashSet<String>,
) -> Result<Option<Value>> {
    let Some(tokens) = data.get("tokens") else {
        let mut usage = Map::new();
        if let Some(cost) = data.get("cost") {
            if let Some(value) = numeric_nonnegative(cost) {
                usage.insert("cost".to_string(), value);
            } else {
                usage_warning(diagnostics, seen, message_id, "cost");
            }
        }
        return Ok((!usage.is_empty()).then_some(Value::Object(usage)));
    };
    let mut usage = Map::new();
    if let Some(cost) = data.get("cost") {
        if let Some(value) = numeric_nonnegative(cost) {
            usage.insert("cost".to_string(), value);
        } else {
            usage_warning(diagnostics, seen, message_id, "cost");
        }
    }
    let tokens = object(tokens, "message.tokens")?;
    for key in ["total", "input", "output", "reasoning"] {
        if let Some(value) = tokens.get(key) {
            if let Some(number) = numeric_nonnegative(value) {
                usage.insert(key.to_string(), number);
            } else {
                usage_warning(diagnostics, seen, message_id, key);
            }
        }
    }
    if let Some(cache) = tokens.get("cache") {
        let cache = object(cache, "message.tokens.cache")?;
        let mut output = Map::new();
        for key in ["read", "write"] {
            if let Some(value) = cache.get(key) {
                if let Some(number) = numeric_nonnegative(value) {
                    output.insert(key.to_string(), number);
                } else {
                    usage_warning(diagnostics, seen, message_id, &format!("cache.{key}"));
                }
            }
        }
        if !output.is_empty() {
            usage.insert("cache".to_string(), Value::Object(output));
        }
    }
    Ok((!usage.is_empty()).then_some(Value::Object(usage)))
}

fn usage_warning(
    diagnostics: &mut SourceDiagnostics,
    seen: &mut HashSet<String>,
    message_id: &str,
    field: &str,
) {
    if seen.insert(format!("{message_id}:{field}")) {
        *diagnostics
            .warnings_by_reason
            .entry("invalid_usage_value".to_string())
            .or_default() += 1;
    }
}

fn numeric_nonnegative(value: &Value) -> Option<Value> {
    let number = value.as_f64()?;
    (number.is_finite() && number >= 0.0).then(|| value.clone())
}

fn tool_timestamp(part: &PartRow, message: &MessageRow, output: bool) -> Result<i64> {
    let data = object(&part.data, "part.data")?;
    let state = object(
        data.get("state")
            .ok_or_else(|| anyhow!("tool.state is required"))?,
        "tool.state",
    )?;
    let time = state.get("time").and_then(Value::as_object);
    let start = positive_ms(time.and_then(|value| value.get("start")), "tool.time.start")?;
    let end = positive_ms(time.and_then(|value| value.get("end")), "tool.time.end")?;
    if let (Some(start), Some(end)) = (start, end)
        && end < start
    {
        bail!("tool end precedes start");
    }
    let fallback = (part.created_at_ms > 0)
        .then_some(part.created_at_ms)
        .or_else(|| (message.created_at_ms > 0).then_some(message.created_at_ms))
        .ok_or_else(|| anyhow!("no valid tool entry timestamp"))?;
    if output {
        Ok(end.or(start).unwrap_or(fallback))
    } else {
        Ok(start.unwrap_or(fallback))
    }
}

fn part_timestamp(part: &PartRow, message: &MessageRow, kind: &str) -> Result<i64> {
    let data = object(&part.data, "part.data")?;
    let candidate = match kind {
        "text" | "reasoning" => positive_ms(
            data.get("time").and_then(|v| v.get("start")),
            "part.time.start",
        )?,
        "tool" => {
            let state = data.get("state").and_then(Value::as_object);
            let start = positive_ms(
                state
                    .and_then(|v| v.get("time"))
                    .and_then(|v| v.get("start")),
                "tool.time.start",
            )?;
            let end = positive_ms(
                state.and_then(|v| v.get("time")).and_then(|v| v.get("end")),
                "tool.time.end",
            )?;
            if let (Some(start), Some(end)) = (start, end)
                && end < start
            {
                bail!("tool end precedes start");
            }
            if matches!(
                state.and_then(|v| v.get("status")).and_then(Value::as_str),
                Some("completed" | "error")
            ) {
                end.or(start)
            } else {
                start
            }
        }
        _ => None,
    };
    candidate
        .or_else(|| (part.created_at_ms > 0).then_some(part.created_at_ms))
        .or_else(|| (message.created_at_ms > 0).then_some(message.created_at_ms))
        .ok_or_else(|| anyhow!("no valid entry timestamp"))
}

#[allow(clippy::too_many_arguments)]
fn normalize_tool_part(
    base: String,
    data: &Map<String, Value>,
    mut common: Map<String, Value>,
    part: &PartRow,
    message: &MessageRow,
    session_id: &str,
    message_id: &str,
    diagnostics: &mut SourceDiagnostics,
) -> Result<Vec<CanonicalEntry>> {
    let state = object(
        data.get("state")
            .ok_or_else(|| anyhow!("tool.state is required"))?,
        "tool.state",
    )?;
    let status = non_empty_string(state.get("status"), "tool.state.status")?;
    if !matches!(
        status.as_str(),
        "pending" | "running" | "completed" | "error"
    ) {
        return Ok(Vec::new());
    }
    let name = non_empty_string(data.get("tool"), "tool.name")?;
    let call_id = non_empty_string(data.get("callID"), "tool.callID")?;
    common.insert("call_id".to_string(), Value::String(call_id.clone()));
    common.insert("tool_status".to_string(), Value::String(status.clone()));
    if matches!(status.as_str(), "pending" | "running") {
        return Ok(Vec::new());
    }
    let input = state.get("input").cloned().unwrap_or(Value::Null);
    if !input.is_object() {
        bail!("terminal tool input must be an object");
    }
    let terminal_time = state
        .get("time")
        .ok_or_else(|| anyhow!("terminal tool time is required"))?;
    let terminal_time = object(terminal_time, "tool.time")?;
    for field in ["start", "end"] {
        let value = terminal_time
            .get(field)
            .ok_or_else(|| anyhow!("tool.time.{field} is required"))?;
        if value.is_null() {
            bail!("tool.time.{field} must be an integer");
        }
        positive_ms(Some(value), &format!("tool.time.{field}"))?;
    }
    let call_timestamp = tool_timestamp(part, message, false)?;
    let output_timestamp = tool_timestamp(part, message, true)?;
    if let Some(value) = terminal_time.get("compacted") {
        positive_ms(Some(value), "tool.time.compacted")?;
    }
    if status == "completed" {
        // Validate the completed-state result before converting it to text.
        let _ = non_empty_or_empty_string(state.get("output"), "tool.output")?;
        let _ = non_empty_or_empty_string(state.get("title"), "tool.title")?;
        let metadata = state
            .get("metadata")
            .ok_or_else(|| anyhow!("completed tool metadata is required"))?;
        if !metadata.is_object() {
            bail!("tool.metadata must be an object");
        }
    } else {
        let _ = non_empty_or_empty_string(state.get("error"), "tool.error")?;
        if let Some(metadata) = state.get("metadata")
            && !metadata.is_object()
        {
            bail!("tool.metadata must be an object");
        }
    }
    let tool_call = build_tool_call(
        SOURCE_ID,
        Some(&name),
        Some(&call_id),
        Some(&input),
        Some("opencode"),
    );
    let mut call = entry(
        format!("{base}:call"),
        MessageRole::RoleAssistant,
        ContentType::Tool,
        &tool_call.content,
        common.clone(),
        call_timestamp,
        "tool_call",
    );
    call.canonical.tool = Some(with_tool_metadata(tool_call, &name, &call_id, "call"));
    let output = if status == "completed" {
        non_empty_or_empty_string(state.get("output"), "tool.output")?
    } else {
        non_empty_or_empty_string(state.get("error"), "tool.error")?
    };
    let tool_output = build_tool_output(
        SOURCE_ID,
        Some(&name),
        Some(&call_id),
        Some(&output),
        Some(if status == "completed" {
            ToolStatus::Ok
        } else {
            ToolStatus::Error
        }),
        Some("opencode"),
    );
    let output_id = format!("{base}:output");
    let mut result = entry(
        output_id.clone(),
        MessageRole::RoleTool,
        ContentType::Tool,
        &tool_output.content,
        common.clone(),
        output_timestamp,
        "tool_output",
    );
    result.canonical.tool = Some(with_tool_metadata(tool_output, &name, &call_id, "output"));
    // Only completed tools may have their output elided by compaction; an
    // errored tool must always keep its normalized `state.error` output.
    let compacted =
        status == "completed" && state.get("time").and_then(|v| v.get("compacted")).is_some();
    let mut out = if compacted {
        vec![call]
    } else {
        vec![call, result]
    };
    let mut seen = HashSet::new();
    if !compacted && let Some(attachments) = state.get("attachments").and_then(Value::as_array) {
        let mut valid_attachments = Vec::new();
        for attachment in attachments {
            let object = match attachment.as_object() {
                Some(value) => value,
                None => {
                    warning_exclusion(diagnostics, "invalid_file_part");
                    continue;
                }
            };
            let id = match object.get("id").and_then(Value::as_str) {
                Some(value) => value,
                None => {
                    warning_exclusion(diagnostics, "invalid_file_part");
                    continue;
                }
            };
            if validate_id(id, "attachment", "prt").is_err() || !seen.insert(id.to_string()) {
                warning_exclusion(diagnostics, "invalid_file_part");
                continue;
            }
            if object.get("type").and_then(Value::as_str) != Some("file") {
                warning_exclusion(diagnostics, "invalid_file_part");
                continue;
            }
            if object.get("sessionID").and_then(Value::as_str) != Some(session_id)
                || object.get("messageID").and_then(Value::as_str) != Some(message_id)
            {
                warning_exclusion(diagnostics, "invalid_file_part");
                continue;
            }
            valid_attachments.push((id.to_string(), object));
        }
        valid_attachments.sort_by(|left, right| left.0.cmp(&right.0));
        for (id, object) in valid_attachments {
            // Internal attachments keep the same common OpenCode metadata as
            // normal file parts (message/part timestamps, part_type, model…)
            // so provenance reconstruction stays consistent across both
            // paths, plus tool-output specific identifiers.
            let mut metadata = common.clone();
            metadata.insert("call_id".to_string(), Value::String(call_id.clone()));
            metadata.insert("attachment_id".to_string(), Value::String(id.clone()));
            metadata.insert("part_id".to_string(), Value::String(part.id.clone()));
            metadata.insert("part_type".to_string(), Value::String("file".to_string()));
            match normalize_file_part(
                format!("{output_id}:attachment:{id}"),
                object,
                MessageRole::RoleTool,
                metadata,
                output_timestamp,
            ) {
                Ok(mut values) => {
                    record_attachment_warnings(object, diagnostics);
                    for value in &mut values {
                        value.parent_external_ids.push(output_id.clone());
                    }
                    out.extend(values);
                }
                Err(error) => warning_exclusion(diagnostics, attachment_error_reason(&error)),
            }
        }
    }
    Ok(out)
}

fn non_empty_or_empty_string(value: Option<&Value>, what: &str) -> Result<String> {
    value
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("{what} must be a string"))
}

fn with_tool_metadata(result: BuildToolResult, name: &str, call_id: &str, kind: &str) -> Value {
    let mut value = result.tool;
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "category".to_string(),
            tool_category(SOURCE_ID, name).map_or(Value::Null, |v| Value::String(v.to_string())),
        );
        obj.insert("call_id".to_string(), Value::String(call_id.to_string()));
        obj.insert("kind".to_string(), Value::String(kind.to_string()));
    }
    value
}

fn normalize_file_part(
    base: String,
    data: &Map<String, Value>,
    role: MessageRole,
    mut common: Map<String, Value>,
    timestamp: i64,
) -> Result<Vec<CanonicalEntry>> {
    let Some(mime_value) = data.get("mime") else {
        bail!("file.mime is required");
    };
    if !mime_value.is_string() {
        bail!("file.mime must be a string");
    }
    if data.get("filename").is_some() && !data.get("filename").is_some_and(Value::is_string) {
        bail!("file.filename must be a string");
    }
    let raw_mime = mime_value
        .as_str()
        .expect("file.mime was validated as a string");
    if !raw_mime.trim().is_empty() && !valid_mime(raw_mime) {
        bail!("file.mime is not a valid MIME type");
    }
    if let Some(mime) = data.get("mime").and_then(Value::as_str) {
        common.insert("mime".to_string(), Value::String(mime.to_string()));
    }
    if let Some(filename) = data.get("filename").and_then(Value::as_str) {
        common.insert("filename".to_string(), Value::String(filename.to_string()));
    }
    let mime = if raw_mime.trim().is_empty() {
        "application/octet-stream"
    } else {
        raw_mime
    };
    let url = non_empty_string(data.get("url"), "file.url")?;
    if url.contains('\0') {
        bail!("file.url contains NUL");
    }
    let alt = data.get("filename").and_then(Value::as_str);
    let normalized = normalize_mime(mime);
    let result = if url
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
    {
        let parsed = parse_data_url(&url)?;
        let header_mime = parsed.media_type.as_deref().map(normalize_mime);
        if header_mime.as_deref() != Some(normalized.as_str())
            || !parsed.base64
            || !is_media_mime(&normalized)
            || parsed.bytes.is_none()
        {
            let (size, sha) = parsed.digest();
            build_elided_attachment(attachment_kind(&normalized), Some(mime), size, &sha, alt)
        } else {
            let bytes = parsed
                .bytes
                .ok_or_else(|| anyhow!("data URL payload is empty"))?;
            build_attachment(
                attachment_kind(&normalized),
                AttachmentStorage::InlineBase64,
                Some(mime),
                Some(&base64::engine::general_purpose::STANDARD.encode(bytes)),
                None,
                None,
                None,
                alt,
            )
        }
    } else {
        let parsed = url::Url::parse(&url);
        if parsed.is_err()
            && url.split_once(':').is_some_and(|(scheme, _)| {
                matches!(
                    scheme.to_ascii_lowercase().as_str(),
                    "http" | "https" | "file"
                )
            })
        {
            bail!("file.url is not a valid URL");
        }
        let kind = attachment_kind(&normalized);
        let redacted = parsed.as_ref().is_ok_and(|u| {
            !u.username().is_empty()
                || u.password().is_some()
                || u.query().is_some()
                || u.fragment().is_some()
        });
        if redacted {
            build_redacted_ref_attachment(kind, Some(mime), &hash_url(&url), alt)
        } else if parsed.as_ref().is_ok_and(|u| {
            matches!(u.scheme(), "http" | "https")
                && u.host_str().is_some()
                && is_media_mime(&normalized)
        }) {
            build_attachment(
                attachment_kind(&normalized),
                AttachmentStorage::Url,
                Some(mime),
                None,
                Some(&url),
                None,
                None,
                alt,
            )
        } else {
            build_attachment(
                kind,
                AttachmentStorage::Ref,
                Some(mime),
                None,
                Some(&url),
                None,
                None,
                alt,
            )
        }
    };
    let mut item = entry(
        base,
        role,
        result.content_type,
        &result.content,
        common,
        timestamp,
        "attachment",
    );
    item.canonical.attachment = Some(result.attachment);
    Ok(vec![item])
}

fn normalize_mime(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn valid_mime(value: &str) -> bool {
    let base = value.split(';').next().unwrap_or("").trim();
    if base.is_empty()
        || base
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte == 0)
    {
        return false;
    }
    let Some((major, subtype)) = base.split_once('/') else {
        return false;
    };
    !major.is_empty()
        && !subtype.is_empty()
        && major.bytes().all(is_mime_token_byte)
        && subtype.bytes().all(is_mime_token_byte)
}

fn is_mime_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn warning_exclusion(diagnostics: &mut SourceDiagnostics, reason: &str) {
    *diagnostics
        .warning_exclusions_by_reason
        .entry(reason.to_string())
        .or_default() += 1;
}

fn attachment_error_reason(error: &anyhow::Error) -> &'static str {
    let text = error.to_string();
    if text.contains("data URL") || text.contains("base64") || text.contains("percent") {
        "invalid_data_url"
    } else {
        "invalid_file_part"
    }
}

fn is_attachment_error(error: &anyhow::Error) -> bool {
    let text = error.to_string();
    text.starts_with("file.")
        || text.contains("data URL")
        || text.contains("base64")
        || text.contains("percent")
}

fn record_attachment_warnings(data: &Map<String, Value>, diagnostics: &mut SourceDiagnostics) {
    if data
        .get("mime")
        .and_then(Value::as_str)
        .is_some_and(|value| value.trim().is_empty())
    {
        *diagnostics
            .warnings_by_reason
            .entry("empty_mime_fallback".to_string())
            .or_default() += 1;
    }
    let Some(url) = data.get("url").and_then(Value::as_str) else {
        return;
    };
    if !url
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
    {
        return;
    }
    let Ok(parsed) = parse_data_url(url) else {
        return;
    };
    if !parsed.base64 {
        *diagnostics
            .warnings_by_reason
            .entry("data_url_non_base64".to_string())
            .or_default() += 1;
        return;
    }
    let file_mime = normalize_mime(
        data.get("mime")
            .and_then(Value::as_str)
            .unwrap_or("application/octet-stream"),
    );
    let header_mime = parsed.media_type.as_deref().map(normalize_mime);
    if header_mime.as_deref() != Some(file_mime.as_str()) {
        *diagnostics
            .warnings_by_reason
            .entry("data_url_mime_mismatch".to_string())
            .or_default() += 1;
    }
}

fn is_media_mime(value: &str) -> bool {
    value.starts_with("image/") || value.starts_with("audio/") || value.starts_with("video/")
}

fn attachment_kind(mime: &str) -> AttachmentKind {
    if mime.starts_with("image/") {
        AttachmentKind::Image
    } else if mime.starts_with("audio/") {
        AttachmentKind::Audio
    } else if mime.starts_with("video/") {
        AttachmentKind::Video
    } else {
        AttachmentKind::Ref
    }
}

struct ParsedDataUrl {
    media_type: Option<String>,
    base64: bool,
    bytes: Option<Vec<u8>>,
    size: u64,
    sha256: String,
}

impl ParsedDataUrl {
    fn digest(&self) -> (u64, String) {
        (self.size, self.sha256.clone())
    }
}

fn parse_data_url(value: &str) -> Result<ParsedDataUrl> {
    if value.bytes().any(|b| b == b'#') {
        bail!("data URL fragments are not accepted");
    }
    if !value
        .get(..5)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("data:"))
    {
        bail!("invalid data URL");
    }
    let rest = value.get(5..).ok_or_else(|| anyhow!("invalid data URL"))?;
    let (header, payload) = rest
        .split_once(',')
        .ok_or_else(|| anyhow!("data URL has no comma"))?;
    if header.chars().any(|c| c.is_ascii_whitespace()) {
        bail!("data URL header contains whitespace");
    }
    let mut fields = header.split(';');
    let first = fields.next().unwrap_or("");
    let mut base64_flag = false;
    let mut media = if first.is_empty() {
        None
    } else {
        Some(normalize_mime(first))
    };
    let parameters: Vec<&str> = fields.collect();
    for (index, parameter) in parameters.iter().enumerate() {
        if parameter.eq_ignore_ascii_case("base64") {
            if index + 1 != parameters.len() || base64_flag {
                bail!("invalid data URL parameter");
            }
            base64_flag = true;
            continue;
        }
        // MIME parameters are intentionally ignored after parsing because
        // attachment classification uses the media type essence only. A
        // bare non-base64 parameter is not valid MIME syntax, so require an
        // explicit non-empty name and value while preserving quoted values.
        let Some((name, parameter_value)) = parameter.split_once('=') else {
            bail!("invalid data URL parameter");
        };
        if name.is_empty() || parameter_value.is_empty() {
            bail!("invalid data URL parameter");
        }
    }
    if media.as_deref() == Some("") {
        media = None;
    }
    if let Some(media_type) = media.as_deref()
        && !valid_mime(media_type)
    {
        bail!("data URL has an invalid MIME type");
    }
    let (bytes, size, sha256) = if base64_flag {
        decode_base64_bounded(payload, size_config().attachment_inline_max)?
    } else {
        let (size, sha256) = percent_decode_digest(payload)?;
        (None, size, sha256)
    };
    if size == 0 {
        bail!("empty data URL payload");
    }
    Ok(ParsedDataUrl {
        media_type: media,
        base64: base64_flag,
        bytes,
        size,
        sha256,
    })
}

fn decode_base64_bounded(value: &str, max_inline: usize) -> Result<(Option<Vec<u8>>, u64, String)> {
    if value.is_empty() || !value.len().is_multiple_of(4) {
        bail!("invalid base64 data URL");
    }
    let mut hasher = Sha256::new();
    let mut retained = Vec::new();
    let mut size = 0u64;
    let mut oversized = false;
    let chunks = value.as_bytes().chunks(4);
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        if chunk.contains(&b'=') && index + 1 != chunk_count {
            bail!("invalid base64 data URL");
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(chunk)
            .map_err(|_| anyhow!("invalid base64 data URL"))?;
        size = size.saturating_add(decoded.len() as u64);
        hasher.update(&decoded);
        if !oversized && size <= max_inline as u64 {
            retained.extend_from_slice(&decoded);
        } else {
            oversized = true;
            retained.clear();
        }
    }
    let sha256 = hex_digest(hasher.finalize());
    let bytes = (!oversized).then_some(retained);
    Ok((bytes, size, sha256))
}

fn percent_decode_digest(value: &str) -> Result<(u64, String)> {
    let mut hasher = Sha256::new();
    let mut size = 0u64;
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                bail!("invalid percent escape");
            }
            let hi = (bytes[index + 1] as char)
                .to_digit(16)
                .ok_or_else(|| anyhow!("invalid percent escape"))?;
            let lo = (bytes[index + 2] as char)
                .to_digit(16)
                .ok_or_else(|| anyhow!("invalid percent escape"))?;
            let byte = ((hi << 4) | lo) as u8;
            if byte == 0 {
                bail!("data URL contains NUL");
            }
            hasher.update([byte]);
            size = size.saturating_add(1);
            index += 3;
        } else {
            if bytes[index] == 0 {
                bail!("data URL contains NUL");
            }
            hasher.update([bytes[index]]);
            size = size.saturating_add(1);
            index += 1;
        }
    }
    Ok((size, hex_digest(hasher.finalize())))
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn entry(
    id: String,
    role: MessageRole,
    content_type: ContentType,
    content: &str,
    opencode: Map<String, Value>,
    timestamp: i64,
    kind: &'static str,
) -> CanonicalEntry {
    // The parser contract stores OpenCode-specific fields exclusively in
    // `metadata.opencode` so `build_memory_data` never persists them as
    // duplicated top-level free-namespace keys.
    let mut opencode = opencode;
    opencode.insert(
        "import_schema".to_string(),
        Value::String(MESSAGE_SCHEMA.to_string()),
    );
    CanonicalEntry {
        external_id: id,
        parent_external_ids: Vec::new(),
        role,
        content_type,
        content: content.to_string(),
        metadata: {
            let mut m = Map::new();
            m.insert("opencode".to_string(), Value::Object(opencode));
            m
        },
        timestamp_ms: timestamp,
        import_order: timestamp,
        kind_tag: kind,
        canonical: CanonicalAddons::default(),
    }
}

fn hash_url(value: &str) -> String {
    sha256_hex_prefix(value.as_bytes(), 64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::git::git_fixture::{origin_config, repo_fixture};
    use std::path::Path;
    use tempfile::NamedTempFile;

    #[test]
    fn ids_reject_delimiters_and_wrong_prefix() {
        assert!(validate_id("ses-ok", "session", "ses").is_ok());
        assert!(validate_id("msg:bad", "message", "msg").is_err());
        assert!(validate_id("abc", "message", "msg").is_err());
    }

    #[test]
    fn model_is_allowlisted() {
        let value = normalized_model(Some(
            &json!({"id":"gpt", "providerID":"openai", "secret":"x"}),
        ))
        .unwrap()
        .unwrap();
        assert_eq!(value, json!({"id":"gpt", "provider_id":"openai"}));
    }

    #[test]
    fn user_object_summary_is_accepted_without_is_summary_metadata() {
        let session = SessionRow {
            id: "ses-1".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            data: json!({"role": "user", "summary": {"kind": "compaction"}}),
        };
        let part = PartRow {
            id: "prt-1".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({"type": "text", "text": "hello"}),
        };
        let entries = normalize_part(
            &session,
            &message,
            &part,
            "user",
            &mut SourceDiagnostics::default(),
            &mut HashSet::new(),
        )
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert!(
            entries[0].metadata["opencode"]
                .as_object()
                .unwrap()
                .get("is_summary")
                .is_none()
        );
    }

    #[test]
    fn build_session_adds_repository_label_for_git_directory() {
        let (_temp, repo) = repo_fixture(&origin_config("https://example.com/opencode.git"));
        let session = SessionRow {
            id: "ses-repo".to_string(),
            project_id: None,
            directory: Some(repo.to_string_lossy().to_string()),
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };

        let canonical = build_session(&session, json!({}), &args(Path::new("/tmp"))).unwrap();

        assert!(
            canonical
                .source_labels
                .iter()
                .any(|label| label == "repo:example.com/opencode")
        );
    }

    #[test]
    fn reasoning_is_always_stored_as_assistant_role() {
        let session = SessionRow {
            id: "ses-1".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            data: json!({"role": "user"}),
        };
        let part = PartRow {
            id: "prt-reasoning".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({"type": "reasoning", "text": "internal reasoning"}),
        };

        let entries = normalize_part(
            &session,
            &message,
            &part,
            "user",
            &mut SourceDiagnostics::default(),
            &mut HashSet::new(),
        )
        .unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].role, MessageRole::RoleAssistant);
        assert_eq!(entries[0].kind_tag, "reasoning");
    }

    #[test]
    fn unknown_tool_status_is_excluded_without_validating_future_payload() {
        let session = SessionRow {
            id: "ses-1".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            data: json!({"role": "user"}),
        };
        let part = PartRow {
            id: "prt-future-tool".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({
                "type": "tool",
                "state": {"status": "future-status"}
            }),
        };

        let outcome = normalize_session(
            &session,
            vec![message],
            vec![part],
            &args(Path::new("/tmp")),
        )
        .unwrap();
        let ReadSessionOutcome::Import {
            entries,
            diagnostics,
            ..
        } = outcome
        else {
            panic!("expected imported session");
        };

        assert!(entries.is_empty());
        assert_eq!(
            diagnostics
                .warning_exclusions_by_reason
                .get("unknown_tool_status"),
            Some(&1)
        );
    }

    #[test]
    fn unknown_and_telemetry_parts_skip_future_payload_validation() {
        let session = SessionRow {
            id: "ses-1".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            data: json!({"role": "user"}),
        };
        let parts = vec![
            PartRow {
                id: "prt-future".to_string(),
                message_id: "msg-1".to_string(),
                session_id: "ses-1".to_string(),
                created_at_ms: 10,
                updated_at_ms: 20,
                data: json!({
                    "type": "future-part",
                    "agent": {"name": "future"},
                    "ignored": ["future"]
                }),
            },
            PartRow {
                id: "prt-telemetry".to_string(),
                message_id: "msg-1".to_string(),
                session_id: "ses-1".to_string(),
                created_at_ms: 30,
                updated_at_ms: 40,
                data: json!({
                    "type": "step-start",
                    "agent": {"name": "future"},
                    "synthetic": {"value": true}
                }),
            },
        ];

        let outcome =
            normalize_session(&session, vec![message], parts, &args(Path::new("/tmp"))).unwrap();
        let ReadSessionOutcome::Import {
            entries,
            diagnostics,
            ..
        } = outcome
        else {
            panic!("expected imported session");
        };

        assert!(entries.is_empty());
        assert_eq!(
            diagnostics
                .warning_exclusions_by_reason
                .get("unknown_part_type"),
            Some(&1)
        );
        assert_eq!(
            diagnostics.ignored_by_reason.get("known_telemetry"),
            Some(&1)
        );
    }

    #[test]
    fn url_hash_is_stable() {
        assert_eq!(
            hash_url("https://example.test/a"),
            hash_url("https://example.test/a")
        );
    }

    #[test]
    fn parent_message_is_ordered_before_earlier_child() {
        let mut messages = HashMap::new();
        messages.insert(
            "msg-child".to_string(),
            MessageRow {
                id: "msg-child".to_string(),
                session_id: "ses-1".to_string(),
                created_at_ms: 1,
                updated_at_ms: 1,
                data: json!({"role":"assistant", "parentID":"msg-parent"}),
            },
        );
        messages.insert(
            "msg-parent".to_string(),
            MessageRow {
                id: "msg-parent".to_string(),
                session_id: "ses-1".to_string(),
                created_at_ms: 2,
                updated_at_ms: 2,
                data: json!({"role":"user"}),
            },
        );
        let mut parents = HashMap::new();
        parents.insert("msg-child".to_string(), "msg-parent".to_string());
        let ordered = order_messages(&messages, &parents).unwrap();
        assert_eq!(
            ordered
                .iter()
                .map(|message| message.id.as_str())
                .collect::<Vec<_>>(),
            vec!["msg-parent", "msg-child"]
        );
    }

    #[test]
    fn data_url_parser_accepts_mime_parameters_and_rejects_nonterminal_flag() {
        let parsed = parse_data_url("data:image/png;charset=utf-8;base64,aGVsbG8=").unwrap();
        assert_eq!(parsed.bytes.as_deref(), Some(b"hello".as_slice()));
        assert_eq!(parsed.size, 5);
        assert_eq!(parsed.media_type.as_deref(), Some("image/png"));
        assert!(parse_data_url("data:image/png;charset=utf-8;base64;foo,aGVsbG8=").is_err());
    }

    #[test]
    fn valid_mime_rejects_extra_slashes_and_accepts_token_subtypes() {
        assert!(valid_mime("image/png"));
        assert!(valid_mime("application/vnd.api+json"));
        assert!(!valid_mime("image/png/evil"));
        assert!(!valid_mime("image//png"));
        assert!(!valid_mime("image/png evil"));
    }

    #[test]
    fn completed_tool_does_not_persist_title_or_provider_metadata() {
        let data_value = json!({
            "type": "tool",
            "tool": "read",
            "callID": "call-1",
            "state": {
                "status": "completed",
                "input": {"path": "src/lib.rs"},
                "output": "ok",
                "title": "secret title",
                "metadata": {"authorization": "secret-token"},
                "time": {"start": 10, "end": 20}
            }
        });
        let part = PartRow {
            id: "prt-tool".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: data_value.clone(),
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({"role": "assistant"}),
        };
        let mut diagnostics = SourceDiagnostics::default();
        let entries = normalize_tool_part(
            "tool".to_string(),
            data_value.as_object().unwrap(),
            Map::new(),
            &part,
            &message,
            "ses-1",
            "msg-1",
            &mut diagnostics,
        )
        .unwrap();

        for entry in entries {
            let serialized = serde_json::to_string(&entry.metadata).unwrap();
            assert!(!serialized.contains("secret title"));
            assert!(!serialized.contains("secret-token"));
            let opencode = entry.metadata["opencode"].as_object().unwrap();
            assert!(opencode.get("title").is_none());
            assert!(opencode.get("tool_metadata").is_none());
        }
    }

    #[test]
    fn internal_tool_attachments_are_sorted_and_keep_outer_part_id() {
        let data_value = json!({
            "type": "tool",
            "tool": "read",
            "callID": "call-1",
            "state": {
                "status": "completed",
                "input": {},
                "output": "ok",
                "title": "read",
                "metadata": {},
                "time": {"start": 10, "end": 20},
                "attachments": [
                    {
                        "id": "prt-z",
                        "type": "file",
                        "sessionID": "ses-1",
                        "messageID": "msg-1",
                        "mime": "image/png",
                        "url": "https://example.com/z.png"
                    },
                    {
                        "id": "prt-a",
                        "type": "file",
                        "sessionID": "ses-1",
                        "messageID": "msg-1",
                        "mime": "image/png",
                        "url": "https://example.com/a.png"
                    }
                ]
            }
        });
        let part = PartRow {
            id: "prt-tool".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: data_value.clone(),
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({"role": "assistant"}),
        };
        let mut diagnostics = SourceDiagnostics::default();
        let entries = normalize_tool_part(
            "tool".to_string(),
            data_value.as_object().unwrap(),
            Map::new(),
            &part,
            &message,
            "ses-1",
            "msg-1",
            &mut diagnostics,
        )
        .unwrap();

        let attachments: Vec<_> = entries
            .iter()
            .filter(|entry| entry.kind_tag == "attachment")
            .collect();
        assert_eq!(attachments.len(), 2);
        assert_eq!(attachments[0].external_id, "tool:output:attachment:prt-a");
        assert_eq!(attachments[1].external_id, "tool:output:attachment:prt-z");
        for entry in attachments {
            let opencode = entry.metadata["opencode"].as_object().unwrap();
            assert_eq!(opencode["part_id"], "prt-tool");
            assert_eq!(opencode["call_id"], "call-1");
        }
    }

    #[test]
    fn compacted_aggregation_only_applies_to_completed_tools() {
        let session = SessionRow {
            id: "ses-1".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            data: json!({"role": "user"}),
        };
        let tool = |id: &str, status: &str, state_extra: Value| {
            let mut state = json!({
                "status": status,
                "time": {"compacted": 30},
            });
            if let (Some(base), Some(extra)) = (state.as_object_mut(), state_extra.as_object()) {
                for (key, value) in extra {
                    base.insert(key.clone(), value.clone());
                }
            }
            PartRow {
                id: id.to_string(),
                message_id: "msg-1".to_string(),
                session_id: "ses-1".to_string(),
                created_at_ms: 10,
                updated_at_ms: 20,
                data: json!({
                    "type": "tool",
                    "tool": "read",
                    "callID": id,
                    "state": state,
                }),
            }
        };
        let parts = vec![
            tool(
                "prt-pending",
                "pending",
                json!({
                    "attachments": [{"id": "pending-attachment"}]
                }),
            ),
            tool(
                "prt-running",
                "running",
                json!({
                    "attachments": [{"id": "running-attachment"}]
                }),
            ),
            tool(
                "prt-completed",
                "completed",
                json!({
                    "input": {},
                    "output": "ok",
                    "title": "read",
                    "metadata": {},
                    "time": {"start": 10, "end": 20, "compacted": 30},
                    "attachments": [{"id": "completed-attachment"}]
                }),
            ),
        ];
        let outcome =
            normalize_session(&session, vec![message], parts, &args(Path::new("/tmp"))).unwrap();
        let ReadSessionOutcome::Import {
            entries,
            diagnostics,
            ..
        } = outcome
        else {
            panic!("expected imported session");
        };

        assert_eq!(diagnostics.deferred, 2);
        assert_eq!(
            diagnostics.ignored_by_reason.get("compacted_tool_output"),
            Some(&1)
        );
        assert_eq!(
            diagnostics
                .ignored_by_reason
                .get("compacted_tool_attachment"),
            Some(&1)
        );
        assert_eq!(
            entries.len(),
            1,
            "only the completed compacted call remains"
        );
    }

    #[test]
    fn malformed_data_url_does_not_record_classification_warnings() {
        let session = SessionRow {
            id: "ses-1".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            data: json!({"role": "user"}),
        };
        let part = PartRow {
            id: "prt-file".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({
                "type": "file",
                "mime": "",
                "url": "data:image/png;base64,%"
            }),
        };
        let mut diagnostics = SourceDiagnostics::default();
        let error = normalize_part(
            &session,
            &message,
            &part,
            "user",
            &mut diagnostics,
            &mut HashSet::new(),
        )
        .unwrap_err();

        assert!(is_attachment_error(&error));
        assert_eq!(
            diagnostics
                .warning_exclusions_by_reason
                .get("invalid_data_url"),
            None,
        );
        assert!(diagnostics.warnings_by_reason.is_empty());
    }

    #[test]
    fn malformed_internal_data_url_only_counts_attachment_exclusion() {
        let data_value = json!({
            "type": "tool",
            "tool": "read",
            "callID": "call-1",
            "state": {
                "status": "completed",
                "input": {},
                "output": "ok",
                "title": "read",
                "metadata": {},
                "time": {"start": 10, "end": 20},
                "attachments": [{
                    "id": "prt-a",
                    "type": "file",
                    "sessionID": "ses-1",
                    "messageID": "msg-1",
                    "mime": "",
                    "url": "data:image/png;base64,%"
                }]
            }
        });
        let part = PartRow {
            id: "prt-tool".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: data_value.clone(),
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({"role": "assistant"}),
        };
        let mut diagnostics = SourceDiagnostics::default();
        normalize_tool_part(
            "tool".to_string(),
            data_value.as_object().unwrap(),
            Map::new(),
            &part,
            &message,
            "ses-1",
            "msg-1",
            &mut diagnostics,
        )
        .unwrap();

        assert_eq!(
            diagnostics
                .warning_exclusions_by_reason
                .get("invalid_data_url"),
            Some(&1)
        );
        assert!(diagnostics.warnings_by_reason.is_empty());
    }

    #[test]
    fn missing_mime_normal_file_is_excluded_as_invalid_file_part() {
        let session = SessionRow {
            id: "ses-1".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 1,
            updated_at_ms: 2,
            data: json!({"role": "user"}),
        };
        let part = PartRow {
            id: "prt-file".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({"type": "file", "url": "https://example.com/file"}),
        };
        let outcome = normalize_session(
            &session,
            vec![message],
            vec![part],
            &args(Path::new("/tmp")),
        )
        .unwrap();
        let ReadSessionOutcome::Import {
            entries,
            diagnostics,
            ..
        } = outcome
        else {
            panic!("expected imported session");
        };

        assert!(entries.is_empty());
        assert_eq!(
            diagnostics
                .warning_exclusions_by_reason
                .get("invalid_file_part"),
            Some(&1)
        );
        assert!(diagnostics.warnings_by_reason.is_empty());
    }

    #[test]
    fn missing_mime_internal_attachment_is_excluded_as_invalid_file_part() {
        let data_value = json!({
            "type": "tool",
            "tool": "read",
            "callID": "call-1",
            "state": {
                "status": "completed",
                "input": {},
                "output": "ok",
                "title": "read",
                "metadata": {},
                "time": {"start": 10, "end": 20},
                "attachments": [{
                    "id": "prt-a",
                    "type": "file",
                    "sessionID": "ses-1",
                    "messageID": "msg-1",
                    "url": "https://example.com/file"
                }]
            }
        });
        let part = PartRow {
            id: "prt-tool".to_string(),
            message_id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: data_value.clone(),
        };
        let message = MessageRow {
            id: "msg-1".to_string(),
            session_id: "ses-1".to_string(),
            created_at_ms: 10,
            updated_at_ms: 20,
            data: json!({"role": "assistant"}),
        };
        let mut diagnostics = SourceDiagnostics::default();
        normalize_tool_part(
            "tool".to_string(),
            data_value.as_object().unwrap(),
            Map::new(),
            &part,
            &message,
            "ses-1",
            "msg-1",
            &mut diagnostics,
        )
        .unwrap();

        assert_eq!(
            diagnostics
                .warning_exclusions_by_reason
                .get("invalid_file_part"),
            Some(&1)
        );
        assert!(diagnostics.warnings_by_reason.is_empty());
    }

    #[test]
    fn headerless_base64_data_url_records_mime_mismatch_warning() {
        let data = json!({
            "mime": "image/png",
            "url": "data:;base64,aGVsbG8="
        });
        let mut diagnostics = SourceDiagnostics::default();

        record_attachment_warnings(data.as_object().unwrap(), &mut diagnostics);

        assert_eq!(
            diagnostics.warnings_by_reason.get("data_url_mime_mismatch"),
            Some(&1)
        );
    }

    fn args(path: &Path) -> OpenCodeArgs {
        OpenCodeArgs {
            session_id: None,
            all_sessions: true,
            opencode_db: Some(path.to_path_buf()),
            include_types: "user,assistant,tool_call,tool_output,system,reasoning,attachment"
                .into(),
            strip_path_prefix: None,
            include_archived: false,
        }
    }

    #[test]
    fn reads_a_minimal_open_code_database() {
        let file = NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE session(id TEXT, project_id TEXT, directory TEXT, title TEXT, version TEXT, time_created INTEGER, time_updated INTEGER);\
             CREATE TABLE message(id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);\
             CREATE TABLE part(id TEXT, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);",
        ).unwrap();
        conn.execute(
            "INSERT INTO session VALUES ('ses-1','p','/work','hello','1.18.21',1,2)",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO message VALUES ('msg-1','ses-1',1,2,?)",
            [r#"{"role":"user"}"#],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO part VALUES ('prt-1','msg-1','ses-1',1,2,?)",
            [r#"{"type":"text","text":"hello"}"#],
        )
        .unwrap();
        drop(conn);
        let source = OpenCodeSource::new(args(file.path()), None).unwrap();
        let inputs = source.discover().unwrap();
        assert_eq!(inputs.len(), 1);
        let outcome = source.read_session(&inputs[0], None).unwrap();
        let entries = crate::source::test_support::entries_from_outcome(outcome);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content, "hello");
    }

    fn parent_chain_schema(conn: &Connection) {
        conn.execute_batch(
            "CREATE TABLE session(id TEXT, project_id TEXT, directory TEXT, title TEXT, version TEXT, time_created INTEGER, time_updated INTEGER, parent_id TEXT, time_archived INTEGER);
             CREATE TABLE message(id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);
             CREATE TABLE part(id TEXT, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);",
        )
        .unwrap();
    }

    fn insert_parent_chain_session(
        conn: &Connection,
        id: &str,
        parent_id: Option<&str>,
        created_at: i64,
        updated_at: i64,
        archived_at: Option<i64>,
    ) {
        conn.execute(
            "INSERT INTO session(id, time_created, time_updated, parent_id, time_archived) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![id, created_at, updated_at, parent_id, archived_at],
        )
        .unwrap();
    }

    fn incremental_source(path: &Path, include_archived: bool) -> OpenCodeSource {
        let mut options = args(path);
        options.include_archived = include_archived;
        OpenCodeSource::new(options, Some(100)).unwrap()
    }

    #[test]
    fn connection_and_transaction_since_filter_match_entity_boundaries() {
        let file = NamedTempFile::new().unwrap();
        let mut conn = Connection::open(file.path()).unwrap();
        parent_chain_schema(&conn);

        for (session_id, updated_at) in [
            ("ses-session-at", 100),
            ("ses-session-before", 99),
            ("ses-message-at", 0),
            ("ses-message-before", 0),
            ("ses-part-at", 0),
            ("ses-part-before", 0),
        ] {
            conn.execute(
                "INSERT INTO session(id, time_created, time_updated) VALUES (?1, 0, ?2)",
                rusqlite::params![session_id, updated_at],
            )
            .unwrap();
        }
        for (session_id, updated_at) in [("ses-message-at", 100), ("ses-message-before", 99)] {
            conn.execute(
                "INSERT INTO message(id, session_id, time_created, time_updated, data) VALUES (?1, ?2, 0, ?3, '{}')",
                rusqlite::params![format!("msg-{session_id}"), session_id, updated_at],
            )
            .unwrap();
        }
        for (session_id, updated_at) in [("ses-part-at", 100), ("ses-part-before", 99)] {
            conn.execute(
                "INSERT INTO part(id, message_id, session_id, time_created, time_updated, data) VALUES (?1, 'msg-part', ?2, 0, ?3, '{}')",
                rusqlite::params![format!("prt-{session_id}"), session_id, updated_at],
            )
            .unwrap();
        }

        let source = OpenCodeSource::new(args(file.path()), Some(100)).unwrap();
        let expected = [
            ("ses-session-at", true),
            ("ses-session-before", false),
            ("ses-message-at", true),
            ("ses-message-before", false),
            ("ses-part-at", true),
            ("ses-part-before", false),
        ];

        for (session_id, expected_hit) in expected {
            assert_eq!(
                source.session_is_candidate(&conn, session_id).unwrap(),
                expected_hit,
                "Connection predicate for {session_id}"
            );
        }

        let tx = conn.transaction().unwrap();
        for (session_id, expected_hit) in expected {
            assert_eq!(
                source.session_is_candidate_tx(&tx, session_id).unwrap(),
                expected_hit,
                "Transaction predicate for {session_id}"
            );
        }
    }

    #[test]
    fn since_discovery_includes_older_parent_before_changed_child() {
        let file = NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        parent_chain_schema(&conn);
        insert_parent_chain_session(&conn, "ses-parent", None, 1, 10, None);
        insert_parent_chain_session(&conn, "ses-child", Some("ses-parent"), 2, 200, None);
        drop(conn);

        let source = incremental_source(file.path(), false);
        let inputs = source.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-parent", "ses-child"]
        );
        assert!(matches!(
            source.read_session(&inputs[0], None),
            Ok(ReadSessionOutcome::Import { .. }) | Ok(ReadSessionOutcome::ImportStream { .. })
        ));
        let child = crate::source::test_support::session_from_outcome(
            source.read_session(&inputs[1], None).unwrap(),
        );
        assert_eq!(
            child.thread_group_observations[0]
                .candidate_parent
                .as_ref()
                .map(|identity| identity.native_id.as_str()),
            Some("ses-parent")
        );
    }

    #[test]
    fn targeted_session_id_does_not_expand_to_parent_ancestors() {
        let file = NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        parent_chain_schema(&conn);
        insert_parent_chain_session(&conn, "ses-parent", None, 1, 10, None);
        insert_parent_chain_session(&conn, "ses-child", Some("ses-parent"), 2, 200, None);
        drop(conn);

        let mut options = args(file.path());
        options.session_id = Some("ses-child".to_string());
        options.all_sessions = false;
        let source = OpenCodeSource::new(options, Some(100)).unwrap();

        let inputs = source.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-child"]
        );
    }

    #[test]
    fn standalone_session_since_filter_ignores_unrelated_changed_sessions() {
        let file = NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        parent_chain_schema(&conn);
        insert_parent_chain_session(&conn, "ses-target", None, 1, 99, None);
        insert_parent_chain_session(&conn, "ses-unrelated", None, 2, 100, None);
        drop(conn);

        let mut options = args(file.path());
        options.session_id = Some("ses-target".to_string());
        options.all_sessions = false;
        let source = OpenCodeSource::new(options, Some(100)).unwrap();

        let inputs = source.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-target"]
        );
        assert!(matches!(
            source.read_session(&inputs[0], None).unwrap(),
            ReadSessionOutcome::Skipped {
                session_id_hint: Some(id),
                ref reason,
                ..
            } if id == "ses-target" && reason == "unchanged since requested timestamp"
        ));
    }

    #[test]
    fn since_discovery_closes_multi_level_ancestors_without_duplicates() {
        let file = NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        parent_chain_schema(&conn);
        insert_parent_chain_session(&conn, "ses-root", None, 1, 10, None);
        insert_parent_chain_session(&conn, "ses-parent", Some("ses-root"), 2, 20, None);
        insert_parent_chain_session(&conn, "ses-child-a", Some("ses-parent"), 3, 200, None);
        insert_parent_chain_session(&conn, "ses-child-b", Some("ses-parent"), 4, 201, None);
        drop(conn);

        let source = incremental_source(file.path(), false);
        let inputs = source.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-root", "ses-parent", "ses-child-a", "ses-child-b"]
        );
    }

    #[test]
    fn archived_ancestors_follow_include_archived_policy() {
        let file = NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        parent_chain_schema(&conn);
        insert_parent_chain_session(&conn, "ses-archived", None, 1, 10, Some(50));
        insert_parent_chain_session(&conn, "ses-child", Some("ses-archived"), 2, 200, None);
        drop(conn);

        let without_archived = incremental_source(file.path(), false);
        let inputs = without_archived.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-child"]
        );

        let with_archived = incremental_source(file.path(), true);
        let inputs = with_archived.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-archived", "ses-child"]
        );
    }

    #[test]
    fn missing_and_cyclic_ancestors_are_session_scoped_and_finite() {
        let missing_file = NamedTempFile::new().unwrap();
        let conn = Connection::open(missing_file.path()).unwrap();
        parent_chain_schema(&conn);
        insert_parent_chain_session(&conn, "ses-child", Some("ses-missing"), 1, 200, None);
        drop(conn);

        let source = incremental_source(missing_file.path(), false);
        let inputs = source.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-missing", "ses-child"]
        );
        assert!(source.read_session(&inputs[0], None).is_err());
        assert!(matches!(
            source.read_session(&inputs[1], None),
            Ok(ReadSessionOutcome::Import { .. }) | Ok(ReadSessionOutcome::ImportStream { .. })
        ));

        let cycle_file = NamedTempFile::new().unwrap();
        let conn = Connection::open(cycle_file.path()).unwrap();
        parent_chain_schema(&conn);
        insert_parent_chain_session(&conn, "ses-a", Some("ses-b"), 1, 10, None);
        insert_parent_chain_session(&conn, "ses-b", Some("ses-a"), 2, 20, None);
        insert_parent_chain_session(&conn, "ses-child", Some("ses-a"), 3, 200, None);
        drop(conn);

        let source = incremental_source(cycle_file.path(), false);
        let inputs = source.discover().unwrap();
        assert_eq!(
            inputs
                .iter()
                .map(|input| input.id.as_str())
                .collect::<Vec<_>>(),
            vec!["ses-b", "ses-a", "ses-child"]
        );
    }

    #[test]
    fn parent_session_id_emits_an_exact_delegated_observation() {
        let file = NamedTempFile::new().unwrap();
        let conn = Connection::open(file.path()).unwrap();
        conn.execute_batch(
            "CREATE TABLE session(id TEXT, project_id TEXT, directory TEXT, title TEXT, version TEXT, time_created INTEGER, time_updated INTEGER, parent_id TEXT);\
             CREATE TABLE message(id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);\
             CREATE TABLE part(id TEXT, message_id TEXT, session_id TEXT, time_created INTEGER, time_updated INTEGER, data TEXT);",
        ).unwrap();
        conn.execute(
            "INSERT INTO session VALUES ('ses-child','p','/work','hello','1.18.21',1,2,'ses-parent')",
            [],
        ).unwrap();
        conn.execute(
            "INSERT INTO message VALUES ('msg-1','ses-child',1,2,?)",
            [r#"{"role":"user"}"#],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO part VALUES ('prt-1','msg-1','ses-child',1,2,?)",
            [r#"{"type":"text","text":"hello"}"#],
        )
        .unwrap();
        drop(conn);

        let source = OpenCodeSource::new(args(file.path()), None).unwrap();
        let input = source.discover().unwrap().remove(0);
        let session = crate::source::test_support::session_from_outcome(
            source.read_session(&input, None).unwrap(),
        );
        assert_eq!(session.thread_group_observations.len(), 1);
        let observation = &session.thread_group_observations[0];
        assert_eq!(
            session
                .source_identity
                .as_ref()
                .map(|identity| identity.native_id.as_str()),
            Some("ses-child")
        );
        assert_eq!(
            observation.subject,
            session.source_identity.clone().unwrap()
        );
        assert_eq!(observation.confidence, ThreadGroupConfidence::Exact);
        assert_eq!(
            observation.evidence_kind,
            ThreadGroupEvidenceKind::SourceField
        );
        assert_eq!(observation.polarity, ThreadGroupPolarity::Supports);
        assert_eq!(observation.adapter_version, OPENCODE_ADAPTER_VERSION);
        assert_eq!(observation.relation_kind.as_deref(), Some("delegated"));
        assert_eq!(observation.subject.owner_scope, None);
        assert_eq!(observation.subject.native_kind, "session");
        assert_eq!(
            observation.subject.identity_scope,
            ThreadGroupIdentityScope::Known(String::new())
        );
        assert_eq!(
            observation
                .candidate_parent
                .as_ref()
                .map(|identity| identity.native_id.as_str()),
            Some("ses-parent")
        );
        // The reference must stay a stable locator, never the raw payload.
        assert_eq!(observation.source_record_ref, "opencode:session.parent_id");
    }

    #[test]
    fn absent_or_empty_parent_id_emits_no_observation() {
        assert!(observations_from_session("ses-child", None).is_empty());
        assert!(observations_from_session("ses-child", Some("")).is_empty());
    }

    #[test]
    fn root_session_has_identity_without_parent_observation() {
        let session = SessionRow {
            id: "ses-root".to_string(),
            project_id: None,
            directory: None,
            title: None,
            version: None,
            created_at_ms: 1,
            updated_at_ms: 2,
            parent_id: None,
            workspace_id: None,
            path: None,
            slug: None,
            agent: None,
            model: None,
            archived_at_ms: None,
        };

        let canonical = build_session(&session, json!({}), &args(Path::new("/tmp"))).unwrap();

        let identity = canonical
            .source_identity
            .expect("root OpenCode sessions still have source identity");
        assert_eq!(identity.source, SOURCE_ID);
        assert_eq!(identity.native_kind, "session");
        assert_eq!(
            identity.identity_scope,
            ThreadGroupIdentityScope::Known(String::new())
        );
        assert_eq!(identity.native_id, "ses-root");
        assert!(canonical.thread_group_observations.is_empty());
    }
}
