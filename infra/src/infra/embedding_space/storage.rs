//! Storage identifiers: tie the RDB, each LanceDB directory, and the
//! embedding state directory together so a process never serves (or a
//! migration never modifies) a mismatched set, e.g. a restored RDB next
//! to another installation's vector stores.

use crate::infra::embedding_space::bootstrap::VectorTable;
use crate::infra::startup_error::StartupError;
use crate::infra::vector_table;
use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Overrides where the embedding state directory lives.
pub const STATE_DIR_ENV: &str = "MEMORY_EMBEDDING_STATE_DIR";
/// Identifier file kept in each local LanceDB directory.
pub const VECTOR_STORE_ID_FILE: &str = ".memories-storage-id";
const STATE_FILE: &str = "storage.json";
const STATE_FORMAT_VERSION: u32 = 1;

const KEY_RDB_ID: &str = "memories.storage.rdb_id";
const KEY_STATE_DIR_ID: &str = "memories.storage.state_dir_id";

/// `MEMORY_EMBEDDING_STATE_DIR`, defaulting to a sibling of the memory
/// LanceDB directory.
pub fn state_dir_from_env() -> PathBuf {
    if let Ok(dir) = std::env::var(STATE_DIR_ENV)
        && !dir.is_empty()
    {
        return PathBuf::from(dir);
    }
    let memory_uri = std::env::var("MEMORY_LANCEDB_URI")
        .unwrap_or_else(|_| "data/lancedb/memories.lancedb".to_string());
    default_state_dir(&memory_uri)
}

pub fn default_state_dir(memory_uri: &str) -> PathBuf {
    let local = local_path(memory_uri).unwrap_or_else(|| PathBuf::from(memory_uri));
    let trimmed = local.to_string_lossy().trim_end_matches('/').to_string();
    PathBuf::from(format!("{trimmed}.embedding-state"))
}

/// Local filesystem path of a LanceDB URI, or `None` for object storage.
pub fn local_path(uri: &str) -> Option<PathBuf> {
    if let Some(rest) = uri.strip_prefix("file://") {
        return Some(PathBuf::from(rest));
    }
    (!uri.contains("://")).then(|| PathBuf::from(uri))
}

pub fn new_identifier() -> String {
    use rand::Rng as _;
    let mut bytes = [0u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Content of `<state dir>/storage.json`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateDirRecord {
    pub format_version: u32,
    pub state_dir_id: String,
    pub rdb_id: String,
    /// Vector store identifier per table label.
    pub vector_stores: BTreeMap<String, String>,
}

/// What one vector table and its directory record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreObservation {
    pub label: &'static str,
    /// `None` for object storage, where no identifier file is kept.
    pub dir: Option<PathBuf>,
    pub dir_id: Option<String>,
    pub table_rdb_id: Option<String>,
    pub table_state_dir_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageObservation {
    pub rdb_id: String,
    pub state: Option<StateDirRecord>,
    pub stores: Vec<StoreObservation>,
}

/// One disagreeing pair, e.g. `("rdb", "vector:memory")`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Mismatch {
    pub left: String,
    pub right: String,
}

impl Mismatch {
    fn new(left: impl Into<String>, right: impl Into<String>) -> Self {
        Self {
            left: left.into(),
            right: right.into(),
        }
    }
}

/// Writes that record identifiers not recorded yet.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageWrites {
    /// Write the state file with this content (when it changed).
    pub state: Option<StateDirRecord>,
    /// `(label, directory, id)` identifier files to create.
    pub dir_ids: Vec<(&'static str, PathBuf, String)>,
    /// Tables whose manifest lacks the RDB / state directory ids.
    pub tables: Vec<&'static str>,
}

/// Compare the recorded identifiers. Missing records are planned as
/// writes (first use); conflicting ones are reported.
pub fn check(
    obs: &StorageObservation,
    new_id: &mut dyn FnMut() -> String,
) -> Result<StorageWrites, Vec<Mismatch>> {
    let mut mismatches = Vec::new();
    let mut writes = StorageWrites::default();
    let mut state = obs.state.clone().unwrap_or_else(|| StateDirRecord {
        format_version: STATE_FORMAT_VERSION,
        state_dir_id: new_id(),
        rdb_id: obs.rdb_id.clone(),
        vector_stores: BTreeMap::new(),
    });
    // Tables sharing a directory share its identifier.
    let mut assigned: BTreeMap<PathBuf, String> = BTreeMap::new();
    if state.rdb_id != obs.rdb_id {
        mismatches.push(Mismatch::new("rdb", "state_dir"));
    }
    for s in &obs.stores {
        let vector = format!("vector:{}", s.label);
        if s.table_rdb_id.as_ref().is_some_and(|id| id != &obs.rdb_id) {
            mismatches.push(Mismatch::new("rdb", vector.clone()));
        }
        if s.table_state_dir_id
            .as_ref()
            .is_some_and(|id| id != &state.state_dir_id)
        {
            mismatches.push(Mismatch::new("state_dir", vector.clone()));
        }
        if s.table_rdb_id.is_none() || s.table_state_dir_id.is_none() {
            writes.tables.push(s.label);
        }
        let Some(dir) = &s.dir else { continue };
        let recorded = state.vector_stores.get(s.label).cloned();
        let dir_id = match (&s.dir_id, &recorded) {
            (Some(d), Some(r)) if d != r => {
                mismatches.push(Mismatch::new("state_dir", vector.clone()));
                continue;
            }
            // The state dir knows this store but its directory lost the
            // identifier: it is not the directory that was recorded.
            (None, Some(_)) => {
                mismatches.push(Mismatch::new("state_dir", vector.clone()));
                continue;
            }
            (Some(d), _) => d.clone(),
            (None, None) => match assigned.get(dir) {
                Some(id) => id.clone(),
                None => {
                    let id = new_id();
                    writes.dir_ids.push((s.label, dir.clone(), id.clone()));
                    id
                }
            },
        };
        assigned.insert(dir.clone(), dir_id.clone());
        state.vector_stores.insert(s.label.to_string(), dir_id);
    }
    if !mismatches.is_empty() {
        return Err(mismatches);
    }
    if obs.state.as_ref() != Some(&state) {
        writes.state = Some(state);
    }
    Ok(writes)
}

/// The check of commands that change vector tables: besides
/// [`check`], every table that holds rows must already carry the RDB
/// identifier. A table with rows but no identifier may belong to any
/// installation (for example a store picked up from another
/// environment's configuration), so it is never modified on trust.
pub fn check_for_change(
    obs: &StorageObservation,
    non_empty: &[&'static str],
) -> Result<(), Vec<Mismatch>> {
    let mut mismatches = check(obs, &mut new_identifier).err().unwrap_or_default();
    for s in &obs.stores {
        if non_empty.contains(&s.label) && s.table_rdb_id.is_none() {
            mismatches.push(Mismatch::new(
                "rdb",
                format!("vector:{}:unrecorded", s.label),
            ));
        }
    }
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(mismatches)
    }
}

pub fn mismatch_error(mismatches: &[Mismatch]) -> StartupError {
    StartupError::EmbeddingStorageMismatch {
        mismatches: serde_json::to_string(mismatches).expect("mismatches serialize"),
    }
}

/// Read the RDB identifier written by the storage-identity migration.
pub async fn read_rdb_id(pool: &infra_utils::infra::rdb::RdbPool) -> Result<String> {
    let row: Option<(String,)> = sqlx::query_as(
        "SELECT storage_id FROM memories_storage_identity WHERE identity_key = 'rdb'",
    )
    .fetch_optional(pool)
    .await
    .context(
        "reading the RDB storage identifier failed; apply the pending schema migrations \
         (memories-db-migrate local apply / release) before starting memories",
    )?;
    row.map(|(id,)| id)
        .context("memories_storage_identity has no RDB identifier row")
}

fn read_state_file(state_dir: &Path) -> Result<Option<StateDirRecord>> {
    let path = state_dir.join(STATE_FILE);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let record: StateDirRecord = serde_json::from_slice(&bytes)
                .with_context(|| format!("parsing {}", path.display()))?;
            if record.format_version != STATE_FORMAT_VERSION {
                anyhow::bail!(
                    "{} has unsupported format version {}",
                    path.display(),
                    record.format_version
                );
            }
            Ok(Some(record))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

/// Write `content` to `path` atomically (temporary file + rename).
pub fn write_atomic(path: &Path, content: &[u8]) -> Result<()> {
    let dir = path.parent().context("path has no parent directory")?;
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let tmp = dir.join(format!(
        ".{}.tmp-{}",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("file"),
        std::process::id()
    ));
    {
        use std::io::Write as _;
        let mut f =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        f.write_all(content)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path).with_context(|| format!("renaming into {}", path.display()))?;
    Ok(())
}

fn read_dir_id(dir: &Path) -> Result<Option<String>> {
    match std::fs::read_to_string(dir.join(VECTOR_STORE_ID_FILE)) {
        Ok(s) => Ok(Some(s.trim().to_string()).filter(|s| !s.is_empty())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).with_context(|| format!("reading the identifier of {}", dir.display())),
    }
}

/// A vector table with the LanceDB URI it lives in.
pub struct StoreTable<'a> {
    pub table: &'a VectorTable,
    pub uri: &'a str,
}

pub async fn observe(
    rdb_id: String,
    state_dir: &Path,
    stores: &[StoreTable<'_>],
) -> Result<StorageObservation> {
    let mut out = Vec::with_capacity(stores.len());
    for s in stores {
        let config = vector_table::read_manifest_config(&s.table.table)
            .await
            .context("LanceDB table manifest is not readable")?;
        let dir = local_path(s.uri);
        let dir_id = match &dir {
            Some(d) => read_dir_id(d)?,
            None => None,
        };
        out.push(StoreObservation {
            label: s.table.label,
            dir,
            dir_id,
            table_rdb_id: config.get(KEY_RDB_ID).cloned().filter(|v| !v.is_empty()),
            table_state_dir_id: config
                .get(KEY_STATE_DIR_ID)
                .cloned()
                .filter(|v| !v.is_empty()),
        });
    }
    Ok(StorageObservation {
        rdb_id,
        state: read_state_file(state_dir)?,
        stores: out,
    })
}

pub async fn apply(
    writes: &StorageWrites,
    obs: &StorageObservation,
    state_dir: &Path,
    stores: &[StoreTable<'_>],
) -> Result<()> {
    for (_, dir, id) in &writes.dir_ids {
        write_atomic(&dir.join(VECTOR_STORE_ID_FILE), id.as_bytes())?;
    }
    let state_dir_id = match (&writes.state, &obs.state) {
        (Some(s), _) | (None, Some(s)) => s.state_dir_id.clone(),
        (None, None) => unreachable!("check always yields a state record"),
    };
    if let Some(state) = &writes.state {
        write_atomic(
            &state_dir.join(STATE_FILE),
            &serde_json::to_vec_pretty(state).expect("state record serializes"),
        )?;
    }
    for label in &writes.tables {
        let t = stores
            .iter()
            .find(|s| s.table.label == *label)
            .expect("writes only name observed tables");
        let written = vector_table::write_manifest_config(
            &t.table.table,
            vec![
                (KEY_RDB_ID.to_string(), obs.rdb_id.clone()),
                (KEY_STATE_DIR_ID.to_string(), state_dir_id.clone()),
            ],
        )
        .await?;
        if !written {
            anyhow::bail!("LanceDB table manifest is not writable");
        }
    }
    Ok(())
}

/// Verify the storage set and record missing identifiers. A mismatch is
/// returned as `StartupError::EmbeddingStorageMismatch` in
/// `anyhow::Error`.
pub async fn verify_and_record(
    rdb_id: String,
    state_dir: &Path,
    stores: &[StoreTable<'_>],
) -> Result<()> {
    let obs = observe(rdb_id, state_dir, stores).await?;
    let writes =
        check(&obs, &mut new_identifier).map_err(|m| anyhow::Error::new(mismatch_error(&m)))?;
    apply(&writes, &obs, state_dir, stores).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> impl FnMut() -> String {
        let mut n = 0;
        move || {
            n += 1;
            format!("id{n}")
        }
    }

    fn store(label: &'static str, dir: &str) -> StoreObservation {
        StoreObservation {
            label,
            dir: Some(PathBuf::from(dir)),
            dir_id: None,
            table_rdb_id: None,
            table_state_dir_id: None,
        }
    }

    fn first_use() -> StorageObservation {
        StorageObservation {
            rdb_id: "rdb".into(),
            state: None,
            stores: vec![store("memory", "/m"), store("thread", "/t")],
        }
    }

    /// Apply planned writes to an observation, as a restart would see it.
    fn after(obs: &StorageObservation, w: &StorageWrites) -> StorageObservation {
        let state = w.state.clone().or(obs.state.clone()).unwrap();
        StorageObservation {
            rdb_id: obs.rdb_id.clone(),
            state: Some(state.clone()),
            stores: obs
                .stores
                .iter()
                .map(|s| StoreObservation {
                    dir_id: s.dir_id.clone().or_else(|| {
                        w.dir_ids
                            .iter()
                            .find(|(_, d, _)| Some(d) == s.dir.as_ref())
                            .map(|(_, _, id)| id.clone())
                    }),
                    table_rdb_id: Some(obs.rdb_id.clone()),
                    table_state_dir_id: Some(state.state_dir_id.clone()),
                    ..s.clone()
                })
                .collect(),
        }
    }

    #[test]
    fn first_use_records_everything_then_matches() {
        let obs = first_use();
        let w = check(&obs, &mut ids()).unwrap();
        assert_eq!(w.dir_ids.len(), 2);
        assert_eq!(w.tables, vec!["memory", "thread"]);
        let state = w.state.clone().unwrap();
        assert_eq!(state.rdb_id, "rdb");
        assert_eq!(state.vector_stores.len(), 2);
        let again = check(&after(&obs, &w), &mut ids()).unwrap();
        assert_eq!(again, StorageWrites::default());
    }

    #[test]
    fn tables_sharing_a_directory_share_its_identifier() {
        let obs = StorageObservation {
            stores: vec![store("memory", "/same"), store("thread", "/same")],
            ..first_use()
        };
        let w = check(&obs, &mut ids()).unwrap();
        assert_eq!(w.dir_ids.len(), 1);
        let state = w.state.unwrap();
        assert_eq!(state.vector_stores["memory"], state.vector_stores["thread"]);
    }

    fn settled() -> StorageObservation {
        let obs = first_use();
        let w = check(&obs, &mut ids()).unwrap();
        after(&obs, &w)
    }

    fn pairs(r: Result<StorageWrites, Vec<Mismatch>>) -> Vec<(String, String)> {
        r.unwrap_err()
            .into_iter()
            .map(|m| (m.left, m.right))
            .collect()
    }

    #[test]
    fn another_rdb_is_detected_against_state_and_tables() {
        let obs = StorageObservation {
            rdb_id: "other".into(),
            ..settled()
        };
        assert_eq!(
            pairs(check(&obs, &mut ids())),
            vec![
                ("rdb".into(), "state_dir".into()),
                ("rdb".into(), "vector:memory".into()),
                ("rdb".into(), "vector:thread".into()),
            ]
        );
    }

    #[test]
    fn another_vector_directory_is_detected() {
        let mut obs = settled();
        obs.stores[1].dir_id = Some("foreign".into());
        assert_eq!(
            pairs(check(&obs, &mut ids())),
            vec![("state_dir".into(), "vector:thread".into())]
        );
        obs.stores[1].dir_id = None;
        assert_eq!(
            pairs(check(&obs, &mut ids())),
            vec![("state_dir".into(), "vector:thread".into())]
        );
    }

    #[test]
    fn lost_or_replaced_state_directory_is_detected() {
        let mut obs = settled();
        obs.state = None;
        let mut fresh = 0;
        let got = pairs(check(&obs, &mut || {
            fresh += 1;
            format!("fresh{fresh}")
        }));
        assert!(got.contains(&("state_dir".into(), "vector:memory".into())));
    }

    #[test]
    fn newly_enabled_table_is_recorded() {
        let mut obs = settled();
        obs.stores.push(store("reflection_intent", "/r"));
        let w = check(&obs, &mut ids()).unwrap();
        assert_eq!(w.tables, vec!["reflection_intent"]);
        assert_eq!(w.dir_ids.len(), 1);
        assert!(
            w.state
                .unwrap()
                .vector_stores
                .contains_key("reflection_intent")
        );
    }

    #[test]
    fn object_storage_skips_directory_identifiers() {
        let obs = StorageObservation {
            stores: vec![StoreObservation {
                dir: None,
                ..store("memory", "")
            }],
            ..first_use()
        };
        let w = check(&obs, &mut ids()).unwrap();
        assert!(w.dir_ids.is_empty());
        assert!(w.state.unwrap().vector_stores.is_empty());
    }

    #[test]
    fn changes_refuse_tables_with_rows_but_no_identifier() {
        let obs = first_use();
        assert!(
            check_for_change(&obs, &[]).is_ok(),
            "empty unrecorded tables are fine"
        );
        let err = check_for_change(&obs, &["thread"]).unwrap_err();
        assert_eq!(err, vec![Mismatch::new("rdb", "vector:thread:unrecorded")]);
        assert!(check_for_change(&settled(), &["memory", "thread"]).is_ok());
    }

    #[test]
    fn default_state_dir_is_a_sibling_of_the_memory_store() {
        assert_eq!(
            default_state_dir("data/lancedb/memories.lancedb/"),
            PathBuf::from("data/lancedb/memories.lancedb.embedding-state")
        );
        assert_eq!(local_path("s3://b/x"), None);
        assert_eq!(local_path("file:///a/b"), Some(PathBuf::from("/a/b")));
    }

    #[tokio::test]
    async fn identifiers_persist_across_restarts_on_disk() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let uri = dir.path().join("lance").to_string_lossy().to_string();
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]));
        let table = VectorTable {
            label: "memory",
            table: vector_table::open_or_create(&uri, "t", &schema)
                .await?
                .table,
        };
        let stores = [StoreTable {
            table: &table,
            uri: &uri,
        }];
        let state_dir = dir.path().join("state");
        verify_and_record("rdb".into(), &state_dir, &stores).await?;
        assert!(state_dir.join(STATE_FILE).is_file());
        assert!(Path::new(&uri).join(VECTOR_STORE_ID_FILE).is_file());
        verify_and_record("rdb".into(), &state_dir, &stores).await?;
        let err = verify_and_record("other".into(), &state_dir, &stores)
            .await
            .unwrap_err();
        assert!(matches!(
            err.downcast_ref::<StartupError>(),
            Some(StartupError::EmbeddingStorageMismatch { .. })
        ));
        Ok(())
    }
}
