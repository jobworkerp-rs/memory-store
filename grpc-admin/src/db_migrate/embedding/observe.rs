//! Read-only observation of the RDB, the vector stores, the embedding
//! index, and the storage identifiers. Nothing is created or written:
//! missing tables and indexes are observed as empty.

use super::counts::Counts;
use super::inspect::Observation;
use super::output::{SpaceValue, UnavailableReason};
use anyhow::Result;
use infra::infra::embedding_dispatch::ImageSearchMode;
use infra::infra::embedding_index::scan::{ScanConfig, ScanTable};
use infra::infra::embedding_index::{EmbeddingIndex, TableLabel};
use infra::infra::embedding_space::bootstrap::{VectorTable, scan_row_models};
use infra::infra::embedding_space::record::{TableRecord, read_table_record};
use infra::infra::embedding_space::storage;
use infra::infra::embedding_space::{SpaceComponents, SpaceId};
use infra::infra::media_object::rdb::MediaObjectRepositoryImpl;
use infra_utils::infra::rdb::RdbPool;
use std::collections::BTreeSet;
use std::path::PathBuf;

/// One configured vector store.
pub use infra::infra::embedding_space::replace::StoreSpec;

/// The embedding configuration memories would start with.
#[derive(Debug, Clone)]
pub struct EmbeddingEnv {
    pub stores: Vec<StoreSpec>,
    /// `None` when no vector store is enabled.
    pub current: Option<SpaceComponents>,
    pub state_dir: PathBuf,
    pub image_search_mode: ImageSearchMode,
    pub max_content_len: usize,
}

use infra::infra::embedding_space::vector_store_enabled as enabled;

/// The vector stores memories would open, with their table geometry
/// (vector size and distance of the first enabled store).
type Stores = (
    Vec<StoreSpec>,
    Option<(usize, infra::infra::memory_vector::config::DistanceType)>,
);

fn configured_stores() -> Result<Stores> {
    let mut stores = Vec::new();
    let mut geometry = None;
    if enabled("MEMORY_VECTOR_ENABLED") {
        let c = infra::infra::memory_vector::config::VectorDBConfig::from_env()?;
        geometry.get_or_insert((c.vector_size, c.distance_type));
        stores.push(StoreSpec {
            label: TableLabel::Memory,
            uri: c.uri,
            table_name: c.table_name,
        });
        if enabled("REFLECTION_INTENT_VECTOR_ENABLED") {
            let c = infra::infra::reflection_intent_vector::config::ReflectionIntentVectorConfig::from_env()?;
            stores.push(StoreSpec {
                label: TableLabel::ReflectionIntent,
                uri: c.uri,
                table_name: c.table_name,
            });
        }
    }
    if enabled("THREAD_VECTOR_ENABLED") {
        let c = infra::infra::thread_vector::config::ThreadVectorDBConfig::from_env()?;
        geometry.get_or_insert((c.vector_size, c.distance_type));
        stores.push(StoreSpec {
            label: TableLabel::Thread,
            uri: c.uri,
            table_name: c.table_name,
        });
    }
    Ok((stores, geometry))
}

/// The configured stores alone, without resolving the embedding model
/// (for commands that only guard the stores, such as `local apply`).
pub fn stores_from_env() -> Result<Vec<StoreSpec>> {
    Ok(configured_stores()?.0)
}

impl EmbeddingEnv {
    /// Read the same environment the server reads.
    pub fn from_env() -> Result<Self> {
        let (stores, geometry) = configured_stores()?;
        let current = match geometry {
            Some((size, distance)) => Some(SpaceComponents::resolve(
                &infra::infra::memory_vector::dispatcher::workers_yaml_path_from_env(),
                u32::try_from(size).unwrap_or(u32::MAX),
                distance.as_str(),
            )?),
            None => None,
        };
        Ok(Self {
            stores,
            current,
            state_dir: storage::state_dir_from_env(),
            image_search_mode: ImageSearchMode::from_env(),
            max_content_len: infra::infra::embedding_dispatch::max_content_len_from_env(),
        })
    }
}

/// Each configured table, `None` where it does not exist yet.
pub async fn open_all(
    specs: &[StoreSpec],
) -> Result<Vec<(StoreSpec, Option<infra::infra::vector_table::Table>)>> {
    let mut out = Vec::with_capacity(specs.len());
    for s in specs {
        out.push((
            s.clone(),
            infra::infra::vector_table::open_existing(&s.uri, &s.table_name).await?,
        ));
    }
    Ok(out)
}

/// The migration marker of each opened table (`None` for absent ones).
pub async fn markers_of(
    tables: &[(StoreSpec, Option<infra::infra::vector_table::Table>)],
) -> Result<Vec<Option<infra::infra::embedding_space::MigrationMarker>>> {
    let mut out = Vec::with_capacity(tables.len());
    for (_, table) in tables {
        out.push(match table {
            Some(t) => read_table_record(t).await?.marker,
            None => None,
        });
    }
    Ok(out)
}

/// An opened store: the table (if it exists) and its directory's index.
struct Opened {
    config: StoreSpec,
    table: Option<infra::infra::vector_table::Table>,
    index: Option<EmbeddingIndex>,
    record: TableRecord,
    is_empty: bool,
}

/// Classify why a store could not be opened: a missing path or denied
/// access is an environment problem; anything else means the data
/// cannot be read as LanceDB.
fn unavailable_reason(e: &anyhow::Error) -> UnavailableReason {
    let text = format!("{e:#}").to_ascii_lowercase();
    if text.contains("permission denied")
        || text.contains("no such file")
        || text.contains("not found")
        || text.contains("connection refused")
    {
        UnavailableReason::ResourceUnavailable
    } else {
        UnavailableReason::ResourceCorrupt
    }
}

async fn open_store(config: &StoreSpec) -> Result<Opened> {
    let table = infra::infra::vector_table::open_existing(&config.uri, &config.table_name).await?;
    let index = EmbeddingIndex::open_existing(&config.uri).await?;
    let (record, is_empty) = match &table {
        Some(t) => (read_table_record(t).await?, t.count_rows(None).await? == 0),
        None => (TableRecord::default(), true),
    };
    Ok(Opened {
        config: config.clone(),
        table,
        index,
        record,
        is_empty,
    })
}

/// Observe everything `inspect` and `plan` report on.
pub async fn observe(pool: &'static RdbPool, env: &EmbeddingEnv) -> Observation {
    match observe_inner(pool, env).await {
        Ok(obs) => obs,
        Err(e) => Observation {
            unavailable: Some(unavailable_reason(&e)),
            ..Default::default()
        },
    }
}

async fn observe_inner(pool: &'static RdbPool, env: &EmbeddingEnv) -> Result<Observation> {
    // Fail early when the RDB itself cannot be used.
    sqlx::query("SELECT 1").execute(pool).await?;
    let mut opened = Vec::with_capacity(env.stores.len());
    for config in &env.stores {
        opened.push(open_store(config).await?);
    }
    let backup_supported = Some(
        env.stores
            .iter()
            .all(|s| storage::local_path(&s.uri).is_some()),
    );
    let has_markers = opened.iter().any(|o| o.record.marker.is_some());
    let markers: Vec<_> = opened.iter().map(|o| o.record.marker.clone()).collect();
    let (attempt, attempt_unreadable) = match super::attempt::load(&env.state_dir) {
        Ok(a) => (a, false),
        Err(super::attempt::LoadError::UnknownFormat(_)) => (None, true),
        Err(super::attempt::LoadError::Other(e)) => return Err(e),
    };
    #[cfg(feature = "postgres")]
    let operation_locked = super::lock::operation_locked_pg(pool).await;
    #[cfg(not(feature = "postgres"))]
    let operation_locked = super::lock::operation_locked(&env.state_dir);
    let storage_mismatch =
        storage_mismatch(pool, &env.state_dir, &env.stores, StorageCheck::Conflicts).await?;

    let Some(current) = &env.current else {
        return Ok(Observation {
            space: SpaceValue::None,
            counts: Some(Counts::default()),
            backup_supported,
            nothing_to_embed: true,
            ..Default::default()
        });
    };
    let space = recorded_space(&opened, current).await?;
    let idg = infra::infra::IdGeneratorWrapper::new();
    let memory_repo = infra::infra::memory::rdb::MemoryRepositoryImpl::new(idg.clone(), pool);
    let media = MediaObjectRepositoryImpl::new(idg.clone(), pool);
    let thread_repo = infra::infra::thread::rdb::ThreadRepositoryImpl::new(idg, pool);
    let has = |l: TableLabel| env.stores.iter().any(|s| s.label == l);
    let nothing_to_embed = opened.iter().all(|o| o.is_empty)
        && !infra::infra::embedding_space::rdb_targets::rdb_has_embedding_target(
            &infra::infra::embedding_space::rdb_targets::RdbTargetSources {
                memories: (has(TableLabel::Memory) || has(TableLabel::ReflectionIntent))
                    .then_some((&memory_repo, &media)),
                threads: has(TableLabel::Thread).then_some(&thread_repo),
                image_search_mode: env.image_search_mode,
            },
        )
        .await?;

    // Target counts do not depend on the space; with an unknown space
    // only the per-state split is meaningless (and not reported).
    let scan_space = match &space {
        SpaceValue::Id(id) => SpaceId(id.clone()),
        SpaceValue::Unknown | SpaceValue::None => current.space_id(),
    };
    let counts = Some(scan(pool, &media, env, &opened, scan_space).await?);
    Ok(Observation {
        unavailable: None,
        storage_mismatch,
        has_markers,
        markers,
        attempt,
        attempt_unreadable,
        operation_locked,
        space,
        counts,
        backup_supported,
        nothing_to_embed,
    })
}

/// The space the tables belong to, as `inspect` reports it. An
/// unrecorded table with rows of a single labeled model is reported in
/// the configured space, as startup would record it; mixed or unlabeled
/// rows, or tables recorded in different spaces, are unknown.
async fn recorded_space(opened: &[Opened], current: &SpaceComponents) -> Result<SpaceValue> {
    let recorded: BTreeSet<String> = opened
        .iter()
        .filter_map(|o| o.record.space.as_ref().map(|s| s.space_id.to_string()))
        .collect();
    if recorded.len() > 1 {
        return Ok(SpaceValue::Unknown);
    }
    for o in opened
        .iter()
        .filter(|o| o.record.space.is_none() && !o.is_empty)
    {
        let table = o.table.as_ref().expect("a non-empty table exists");
        let models = scan_row_models(table).await?;
        if models.has_unlabeled || models.names.len() != 1 {
            return Ok(SpaceValue::Unknown);
        }
    }
    Ok(match recorded.into_iter().next() {
        Some(id) => SpaceValue::Id(id),
        None if opened.iter().all(|o| o.is_empty) => SpaceValue::None,
        None => SpaceValue::Id(current.space_id().to_string()),
    })
}

/// Compare the recorded storage identifiers without recording anything.
/// An RDB without the identifier (schema not migrated yet) cannot be
/// compared and is not reported as a mismatch.
/// How strictly storage identifiers are checked (spec §3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageCheck {
    /// Only recorded identifiers that contradict each other (reading
    /// commands, and RDB commands that never change the stores).
    Conflicts,
    /// Changing embedding commands: anything that cannot be verified,
    /// including a table with rows but no RDB identifier, is a mismatch,
    /// so a store is never modified on trust.
    ForChange,
}

/// Whether the RDB, the configured stores, and the state directory fail
/// `check` as one storage set.
pub async fn storage_mismatch(
    pool: &RdbPool,
    state_dir: &std::path::Path,
    specs: &[StoreSpec],
    check: StorageCheck,
) -> Result<bool> {
    let strict = check == StorageCheck::ForChange;
    let checked: Result<bool> = async {
        let tables = open_all(specs).await?;
        let mut present = Vec::new();
        let mut non_empty = Vec::new();
        for (spec, table) in &tables {
            let Some(table) = table else { continue };
            if strict && table.count_rows(None).await? > 0 {
                non_empty.push(spec.label.as_str());
            }
            present.push((
                VectorTable {
                    label: spec.label.as_str(),
                    table: table.clone(),
                },
                spec.uri.as_str(),
            ));
        }
        let Ok(rdb_id) = storage::read_rdb_id(pool).await else {
            // Nothing can conflict before the RDB has its identifier, but
            // nothing with rows can be verified either.
            return Ok(!non_empty.is_empty());
        };
        let stores: Vec<storage::StoreTable> = present
            .iter()
            .map(|(table, uri)| storage::StoreTable { table, uri })
            .collect();
        let obs = storage::observe(rdb_id, state_dir, &stores).await?;
        Ok(if strict {
            storage::check_for_change(&obs, &non_empty).is_err()
        } else {
            storage::check(&obs, &mut storage::new_identifier).is_err()
        })
    }
    .await;
    match checked {
        Err(_) if strict => Ok(true),
        other => other,
    }
}

async fn scan(
    pool: &'static RdbPool,
    media: &MediaObjectRepositoryImpl,
    env: &EmbeddingEnv,
    opened: &[Opened],
    space: SpaceId,
) -> Result<Counts> {
    let tables = opened
        .iter()
        .map(|o| ScanTable {
            label: o.config.label,
            table: o.table.clone(),
            index: o.index.clone(),
        })
        .collect();
    Counts::scan(
        pool,
        media,
        ScanConfig {
            space,
            image_search_mode: env.image_search_mode,
            max_content_len: env.max_content_len,
            page_size: 500,
        },
        tables,
    )
    .await
}
