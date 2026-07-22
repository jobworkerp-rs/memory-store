use anyhow::{Context, Result, bail};
use app::app::reflection::finalize::{compose_aggregate_labels, sha256_join_pipe};
use clap::{Parser, Subcommand};
use common::external_id::{EXTERNAL_ID_MAX_BYTES, namespace_for_external_id, owner_scoped};
use infra::infra::IdGeneratorWrapper;
use infra::infra::memory_kind_migration::{
    Classification, ClassificationMapping, ThreadEvidence, classify_thread, parse_mapping_json,
};
use infra::infra::module::rdb_pool_by_env;
use infra_utils::infra::rdb::{Rdb, RdbPool, RdbTransaction};
use protobuf::llm_memory::data::{MemoryKind, MessageRole};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::{fs::OpenOptions, io};

#[cfg(feature = "postgres")]
const P1: &str = "$1";
#[cfg(not(feature = "postgres"))]
const P1: &str = "?";
#[cfg(feature = "postgres")]
const MEMORY_METADATA_EXPR: &str = "m.metadata::TEXT";
#[cfg(feature = "postgres")]
const THREAD_METADATA_EXPR: &str = "t.metadata::TEXT";
#[cfg(not(feature = "postgres"))]
const MEMORY_METADATA_EXPR: &str = "m.metadata";
#[cfg(not(feature = "postgres"))]
const THREAD_METADATA_EXPR: &str = "t.metadata";
const GENERATED_USER_ID_MIN: i64 = 100_000;

fn server_legacy_generated_owner(memory_kind: i32) -> Option<i64> {
    match memory_kind {
        value
            if (MemoryKind::ThreadSummary as i32..=MemoryKind::MonthlySummary as i32)
                .contains(&value) =>
        {
            Some(100_000)
        }
        value if value == MemoryKind::Personality as i32 => Some(200_000),
        value if value == MemoryKind::Reflection as i32 => Some(300_000),
        _ => None,
    }
}

fn is_server_legacy_owner(memory_kind: i32, user_id: i64) -> bool {
    server_legacy_generated_owner(memory_kind) == Some(user_id)
}

fn client_uses_generated_owner_range(user_id: i64) -> bool {
    user_id >= GENERATED_USER_ID_MIN
}

#[derive(Debug)]
struct CommitOutcomeUnknown;

impl fmt::Display for CommitOutcomeUnknown {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("migration commit outcome is unknown")
    }
}

impl std::error::Error for CommitOutcomeUnknown {}

fn should_retain_apply_audit_after_error(commit_outcome_unknown: bool) -> bool {
    commit_outcome_unknown
}

fn should_retain_client_journal_after_error(journal_bytes_written: u64) -> bool {
    journal_bytes_written > 0
}

#[derive(Parser, Debug)]
#[command(name = "migrate-memory-kind")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Validate the operator mapping and write a reproducible preflight audit.
    Plan {
        #[arg(long)]
        mapping: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Apply a classified memory-kind migration and write its immutable audit.
    Apply {
        #[arg(long)]
        mapping: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Verify database state against an apply audit without changing it.
    Verify {
        #[arg(long)]
        audit: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Best-effort backfill for single-user client databases such as Lookback.
    ClientApply {
        /// The same operator mapping supplied to the server-side migration.
        #[arg(long)]
        mapping: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
    /// Dump and remove unresolved client-only records before retrying the migration.
    ClientPruneUnresolved {
        #[arg(long)]
        audit: PathBuf,
        #[arg(long)]
        output: PathBuf,
        /// Permit client recovery to remove unresolved rows even when they
        /// are referenced by otherwise retained client records.
        #[arg(long)]
        force: bool,
    },
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PreflightAudit {
    mapping_sha256: String,
    summary_labels: std::collections::BTreeMap<String, i32>,
    explicit_owners_by_thread_id: std::collections::BTreeMap<i64, i64>,
    summary_label_count: usize,
    explicit_owner_count: usize,
    threads: Vec<ThreadAudit>,
    unresolved_thread_ids: Vec<i64>,
    unresolved_memory_ids: Vec<i64>,
    reflection_split_thread_ids: Vec<i64>,
    unsupported_thread_reference_columns: Vec<String>,
    external_id_moves: Vec<ExternalIdMoveAudit>,
    unresolved_external_id_memory_ids: Vec<i64>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ThreadAudit {
    thread_id: i64,
    classification: String,
    labels: Vec<String>,
    metadata_source_user_id: Option<i64>,
    direct_metadata_owner_user_ids: Vec<i64>,
    owner_candidate_user_ids: Vec<i64>,
    has_invalid_metadata: bool,
    has_missing_summary_source_thread: bool,
    has_summary_metadata: bool,
    aggregate_metadata_kind: Option<i32>,
    source_thread_ids: Vec<i64>,
    source_memory_ids: Vec<i64>,
    reflection_origin_user_ids: Vec<i64>,
    reflection_split_owners: Vec<ReflectionSplitOwnerAudit>,
    has_sidecar_thread_mismatch: bool,
    has_reflection_origin_owner_mismatch: bool,
    has_reverse_reflection_origin_reference: bool,
    has_reverse_few_shot_usage_reference: bool,
    has_invalid_aggregate_source_hierarchy: bool,
}

#[derive(sqlx::FromRow)]
struct ThreadRow {
    id: i64,
    user_id: i64,
    memory_kind: Option<i32>,
}

#[derive(sqlx::FromRow)]
struct IdRow {
    value: i64,
}

#[derive(sqlx::FromRow)]
struct IdOwnerRow {
    id: i64,
    user_id: i64,
}

#[derive(Clone, sqlx::FromRow)]
struct MembershipRow {
    thread_id: i64,
    memory_id: i64,
}

#[derive(Clone, sqlx::FromRow)]
struct ExternalIdRow {
    id: i64,
    external_id: String,
    metadata: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ExternalIdMoveAudit {
    memory_id: i64,
    old_external_id: String,
    new_external_id: String,
    thread_creator_user_id: i64,
}

#[derive(sqlx::FromRow)]
struct ReflectionSplitRow {
    origin_user_id: i64,
    memory_id: i64,
    position: i32,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ReflectionSplitOwnerAudit {
    origin_user_id: i64,
    memory_count: usize,
    memories: Vec<ReflectionSplitMemoryAudit>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ReflectionSplitMemoryAudit {
    memory_id: i64,
    position: i32,
}

#[derive(sqlx::FromRow)]
struct MetadataRow {
    metadata: Option<String>,
}

fn reflection_split_owner_audit(rows: Vec<ReflectionSplitRow>) -> Vec<ReflectionSplitOwnerAudit> {
    let mut owners = BTreeMap::<i64, Vec<ReflectionSplitMemoryAudit>>::new();
    for row in rows {
        owners
            .entry(row.origin_user_id)
            .or_default()
            .push(ReflectionSplitMemoryAudit {
                memory_id: row.memory_id,
                position: row.position,
            });
    }
    owners
        .into_iter()
        .map(|(origin_user_id, mut memories)| {
            memories.sort_by_key(|memory| (memory.position, memory.memory_id));
            ReflectionSplitOwnerAudit {
                origin_user_id,
                memory_count: memories.len(),
                memories,
            }
        })
        .collect()
}

fn conflicting_memory_ids(
    memberships: &[MembershipRow],
    resolved_thread_targets: &BTreeMap<i64, (i64, i32)>,
    non_resolved_thread_ids: &BTreeSet<i64>,
    default_system_memory_ids: &BTreeSet<i64>,
) -> Vec<i64> {
    let mut targets_by_memory: BTreeMap<i64, BTreeSet<(i64, i32)>> = BTreeMap::new();
    let mut membership_counts = BTreeMap::<i64, usize>::new();
    let mut has_non_resolved_membership = BTreeSet::new();
    for membership in memberships {
        // A thread's default system prompt is infrastructure shared by
        // otherwise unrelated threads. It is not a memory belonging to the
        // thread's classified payload and must remain RAW.
        if default_system_memory_ids.contains(&membership.memory_id) {
            continue;
        }
        *membership_counts.entry(membership.memory_id).or_default() += 1;
        if non_resolved_thread_ids.contains(&membership.thread_id) {
            has_non_resolved_membership.insert(membership.memory_id);
        } else if let Some(&target) = resolved_thread_targets.get(&membership.thread_id) {
            targets_by_memory
                .entry(membership.memory_id)
                .or_default()
                .insert(target);
        }
    }
    let mut conflicts = targets_by_memory
        .into_iter()
        .filter_map(|(memory_id, targets)| (targets.len() > 1).then_some(memory_id))
        .collect::<BTreeSet<_>>();
    conflicts.extend(has_non_resolved_membership.into_iter().filter(|memory_id| {
        membership_counts
            .get(memory_id)
            .is_some_and(|count| *count > 1)
    }));
    conflicts.into_iter().collect()
}

fn owner_candidate_ids(candidates: &BTreeSet<i64>) -> Vec<i64> {
    candidates.iter().copied().collect()
}

fn thread_owner_sql() -> String {
    format!("SELECT CAST(user_id AS BIGINT) FROM thread WHERE id = {P1}")
}

#[derive(Default)]
struct SourceThreadOwnerEvidence {
    owners: BTreeSet<i64>,
    has_invalid_hierarchy: bool,
    is_missing: bool,
}

/// Resolves a provenance thread to its human owner without mistaking a
/// generated summary's stored worker owner for an end-user owner.
async fn resolve_source_thread_owner(
    tx: &mut RdbTransaction<'_>,
    mapping: &ClassificationMapping,
    thread_id: i64,
    visiting: &mut BTreeSet<i64>,
) -> Result<SourceThreadOwnerEvidence> {
    let owner_sql = thread_owner_sql();
    let Some(stored_owner) = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(owner_sql))
        .bind(thread_id)
        .fetch_optional(&mut **tx)
        .await
        .context("read source thread owner for memory kind migration plan")?
    else {
        return Ok(SourceThreadOwnerEvidence {
            is_missing: true,
            ..Default::default()
        });
    };
    if stored_owner < GENERATED_USER_ID_MIN {
        return Ok(SourceThreadOwnerEvidence {
            owners: BTreeSet::from([stored_owner]),
            ..Default::default()
        });
    }

    let labels_sql = format!("SELECT label FROM thread_label WHERE thread_id = {P1}");
    let labels = sqlx::query_scalar::<Rdb, String>(sqlx::AssertSqlSafe(labels_sql))
        .bind(thread_id)
        .fetch_all(&mut **tx)
        .await
        .context("read source thread labels for memory kind migration plan")?;
    let metadata_sql = format!(
        "SELECT {MEMORY_METADATA_EXPR} AS metadata FROM memory m \
         JOIN thread_memory tm ON tm.memory_id = m.id WHERE tm.thread_id = {P1}"
    );
    let metadata_rows = sqlx::query_as::<Rdb, MetadataRow>(sqlx::AssertSqlSafe(metadata_sql))
        .bind(thread_id)
        .fetch_all(&mut **tx)
        .await
        .context("read source thread metadata for memory kind migration plan")?;
    let (
        _,
        has_summary_metadata,
        aggregate_metadata_kind,
        has_aggregate_metadata_conflict,
        has_invalid_metadata,
    ) = metadata_evidence(&metadata_rows);
    let summary_kinds = labels
        .iter()
        .filter_map(|label| mapping.summary_labels.get(label).copied())
        .collect::<BTreeSet<_>>();
    let is_mapped_summary = summary_kinds.len() == 1
        && !has_aggregate_metadata_conflict
        && !has_invalid_metadata
        && summary_kinds.first().is_some_and(|kind| {
            (*kind == MemoryKind::ThreadSummary as i32
                && has_summary_metadata
                && aggregate_metadata_kind.is_none())
                || aggregate_metadata_kind == Some(*kind)
        });
    if !has_summary_metadata && aggregate_metadata_kind.is_none() {
        // A high-valued user id on an ordinary thread is a real owner, not
        // enough evidence to rewrite the thread as a generated artifact.
        return Ok(SourceThreadOwnerEvidence {
            owners: BTreeSet::from([stored_owner]),
            ..Default::default()
        });
    }
    if !is_mapped_summary || !visiting.insert(thread_id) {
        return Ok(SourceThreadOwnerEvidence {
            has_invalid_hierarchy: true,
            ..Default::default()
        });
    }
    let source_thread_ids = source_thread_ids(&metadata_rows);
    if source_thread_ids.is_empty() {
        visiting.remove(&thread_id);
        return Ok(SourceThreadOwnerEvidence {
            has_invalid_hierarchy: true,
            ..Default::default()
        });
    }
    let mut evidence = SourceThreadOwnerEvidence::default();
    for source_thread_id in source_thread_ids {
        let source = Box::pin(resolve_source_thread_owner(
            tx,
            mapping,
            source_thread_id,
            visiting,
        ))
        .await?;
        evidence.owners.extend(source.owners);
        evidence.has_invalid_hierarchy |= source.has_invalid_hierarchy;
        evidence.is_missing |= source.is_missing;
    }
    visiting.remove(&thread_id);
    evidence.has_invalid_hierarchy |= evidence.owners.len() != 1;
    Ok(evidence)
}

/// The single owner candidate, or `None` when the set is empty or
/// conflicting (more than one candidate). Callers that need to tell
/// "no evidence" apart from "conflicting evidence" read `candidates.len()`
/// separately (e.g. `ThreadEvidence::has_metadata_owner_conflict`).
fn single_owner(candidates: &BTreeSet<i64>) -> Option<i64> {
    (candidates.len() == 1).then(|| *candidates.first().expect("len == 1"))
}

fn metadata_id(value: &serde_json::Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
        .filter(|id| *id > 0)
}

fn metadata_id_values_are_valid(metadata: &serde_json::Value, keys: &[&str]) -> bool {
    keys.iter().all(|key| {
        let Some(value) = metadata.get(key) else {
            return true;
        };
        value
            .as_array()
            .map(|values| values.iter().all(|value| metadata_id(value).is_some()))
            .unwrap_or_else(|| metadata_id(value).is_some())
    })
}

fn aggregate_metadata_value_is_valid(key: &str, value: &serde_json::Value) -> bool {
    let Some(value) = value.as_str() else {
        return false;
    };
    let parse_number = |value: &str| value.parse::<u32>().ok();
    match key {
        "daily_date" => {
            let parts = value.split('-').collect::<Vec<_>>();
            parts.len() == 3
                && parts
                    .iter()
                    .all(|part| part.chars().all(|ch| ch.is_ascii_digit()))
                && parts[0].len() == 4
                && parts[1].len() == 2
                && parts[2].len() == 2
                && matches!(
                    (
                        parse_number(parts[0]),
                        parse_number(parts[1]),
                        parse_number(parts[2]),
                    ),
                    (Some(year), Some(month), Some(day))
                        if is_valid_calendar_date(year, month, day)
                )
        }
        "iso_week" => {
            let parts = value.split("-W").collect::<Vec<_>>();
            parts.len() == 2
                && parts[0].len() == 4
                && parts[1].len() == 2
                && parts
                    .iter()
                    .all(|part| part.chars().all(|ch| ch.is_ascii_digit()))
                && parse_number(parts[0]).is_some_and(|year| year > 0)
                && parse_number(parts[1]).is_some_and(|week| (1..=53).contains(&week))
        }
        "month" => {
            let parts = value.split('-').collect::<Vec<_>>();
            parts.len() == 2
                && parts[0].len() == 4
                && parts[1].len() == 2
                && parts
                    .iter()
                    .all(|part| part.chars().all(|ch| ch.is_ascii_digit()))
                && parse_number(parts[0]).is_some_and(|year| year > 0)
                && parse_number(parts[1]).is_some_and(|month| (1..=12).contains(&month))
        }
        _ => false,
    }
}

fn is_valid_calendar_date(year: u32, month: u32, day: u32) -> bool {
    if year == 0 || !(1..=12).contains(&month) {
        return false;
    }
    let leap_year =
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let days_in_month = match month {
        2 if leap_year => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    (1..=days_in_month).contains(&day)
}

fn metadata_evidence(rows: &[MetadataRow]) -> (BTreeSet<i64>, bool, Option<i32>, bool, bool) {
    let mut owners = BTreeSet::new();
    let mut aggregate_kinds = BTreeSet::new();
    let mut has_summary_metadata = false;
    let mut has_invalid_metadata = false;
    for row in rows {
        let Some(raw) = row.metadata.as_deref() else {
            continue;
        };
        let Ok(metadata) = serde_json::from_str::<serde_json::Value>(raw) else {
            has_invalid_metadata = true;
            continue;
        };
        if let Some(value) = metadata.get("source_user_id") {
            if let Some(id) = metadata_id(value) {
                owners.insert(id);
            } else {
                has_invalid_metadata = true;
            }
        }
        has_invalid_metadata |= !metadata_id_values_are_valid(
            &metadata,
            &[
                "source_thread_id",
                "source_thread_ids",
                "source_memory_id",
                "source_memory_ids",
            ],
        );
        let aggregate_kind_keys = [
            ("daily_date", MemoryKind::DailySummary as i32),
            ("iso_week", MemoryKind::WeeklySummary as i32),
            ("month", MemoryKind::MonthlySummary as i32),
        ];
        for (key, kind) in aggregate_kind_keys {
            if let Some(value) = metadata.get(key) {
                aggregate_kinds.insert(kind);
                has_invalid_metadata |= !aggregate_metadata_value_is_valid(key, value);
            }
        }
        has_summary_metadata |= metadata.get("summary_version").is_some()
            || metadata.get("source_thread_id").is_some()
            || metadata.get("source_thread_ids").is_some();
    }
    (
        owners,
        has_summary_metadata,
        (aggregate_kinds.len() == 1).then(|| *aggregate_kinds.first().expect("len == 1")),
        aggregate_kinds.len() > 1,
        has_invalid_metadata,
    )
}

fn source_thread_ids(rows: &[MetadataRow]) -> BTreeSet<i64> {
    metadata_id_set(rows, &["source_thread_id", "source_thread_ids"])
}

fn source_memory_ids(rows: &[MetadataRow]) -> BTreeSet<i64> {
    metadata_id_set(rows, &["source_memory_id", "source_memory_ids"])
}

fn metadata_id_set(rows: &[MetadataRow], keys: &[&str]) -> BTreeSet<i64> {
    let mut ids = BTreeSet::new();
    for row in rows {
        let Some(raw) = row.metadata.as_deref() else {
            continue;
        };
        let Ok(metadata) = serde_json::from_str::<serde_json::Value>(raw) else {
            continue;
        };
        for key in keys {
            let Some(value) = metadata.get(key) else {
                continue;
            };
            let values: Vec<&serde_json::Value> = value
                .as_array()
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .collect();
            let values = if values.is_empty() {
                vec![value]
            } else {
                values
            };
            for value in values {
                if let Some(id) = metadata_id(value) {
                    ids.insert(id);
                }
            }
        }
    }
    ids
}

#[cfg(feature = "postgres")]
fn preflight_snapshot_sql() -> Option<&'static str> {
    Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
}

#[cfg(not(feature = "postgres"))]
fn preflight_snapshot_sql() -> Option<&'static str> {
    None
}

async fn configure_preflight_snapshot(tx: &mut RdbTransaction<'_>) -> Result<()> {
    let Some(sql) = preflight_snapshot_sql() else {
        return Ok(());
    };
    // PostgreSQL defaults to READ COMMITTED, which can observe a different
    // committed state for each query. Configure this before the first read.
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .execute(&mut **tx)
        .await
        .context("configure consistent read-only migration plan snapshot")?;
    Ok(())
}

#[cfg(feature = "postgres")]
async fn configure_apply_snapshot(tx: &mut RdbTransaction<'_>) -> Result<()> {
    sqlx::query(sqlx::AssertSqlSafe(
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
    ))
    .execute(&mut **tx)
    .await
    .context("configure consistent migration apply snapshot")?;
    Ok(())
}

#[cfg(not(feature = "postgres"))]
async fn configure_apply_snapshot(_: &mut RdbTransaction<'_>) -> Result<()> {
    Ok(())
}

#[derive(Default)]
struct AggregateSourceEvidence {
    owners: BTreeSet<i64>,
    has_invalid_hierarchy: bool,
    has_invalid_metadata: bool,
}

/// Weekly and monthly summaries chain through aggregate input memories.
/// Legacy daily summaries may retain raw input-memory provenance, but their
/// source thread ids are the authoritative ownership evidence. Raw input ids
/// are validated when present; their absence is valid after source retention
/// has been pruned.
fn expected_aggregate_input_kind(aggregate_kind: i32) -> Option<i32> {
    match aggregate_kind {
        k if k == MemoryKind::WeeklySummary as i32 => Some(MemoryKind::DailySummary as i32),
        k if k == MemoryKind::MonthlySummary as i32 => Some(MemoryKind::WeeklySummary as i32),
        _ => None,
    }
}

/// Resolve aggregate-summary ownership from the input memories' origin metadata.
/// Aggregate-memory `user_id` can be a legacy reserved worker id, so it is not
/// usable as an ownership signal.
async fn aggregate_source_evidence(
    tx: &mut RdbTransaction<'_>,
    mapping: &ClassificationMapping,
    aggregate_kind: i32,
    initial_memory_ids: &BTreeSet<i64>,
) -> Result<AggregateSourceEvidence> {
    if aggregate_kind == MemoryKind::DailySummary as i32 {
        let mut evidence = AggregateSourceEvidence::default();
        for memory_id in initial_memory_ids {
            let exists_sql = format!("SELECT 1 FROM memory m WHERE m.id = {P1}");
            let exists = sqlx::query_scalar::<Rdb, i32>(sqlx::AssertSqlSafe(exists_sql))
                .bind(memory_id)
                .fetch_optional(&mut **tx)
                .await
                .context("read daily summary input memory for memory kind migration plan")?
                .is_some();
            evidence.has_invalid_hierarchy |= !exists;
        }
        return Ok(evidence);
    }
    let Some(expected_kind) = expected_aggregate_input_kind(aggregate_kind) else {
        return Ok(AggregateSourceEvidence {
            has_invalid_hierarchy: true,
            ..Default::default()
        });
    };
    let mut evidence = AggregateSourceEvidence {
        has_invalid_hierarchy: initial_memory_ids.is_empty(),
        ..Default::default()
    };
    let mut pending = initial_memory_ids
        .iter()
        .copied()
        .map(|memory_id| (memory_id, expected_kind))
        .collect::<Vec<_>>();
    let mut visited = BTreeMap::new();
    while let Some((memory_id, expected_kind)) = pending.pop() {
        if let Some(previous_expected_kind) = visited.insert(memory_id, expected_kind) {
            evidence.has_invalid_hierarchy |= previous_expected_kind != expected_kind;
            continue;
        }
        let metadata_sql =
            format!("SELECT {MEMORY_METADATA_EXPR} AS metadata FROM memory m WHERE m.id = {P1}");
        let Some(metadata) = sqlx::query_as::<Rdb, MetadataRow>(sqlx::AssertSqlSafe(metadata_sql))
            .bind(memory_id)
            .fetch_optional(&mut **tx)
            .await
            .context("read aggregate summary source metadata for memory kind migration plan")?
        else {
            evidence.has_invalid_hierarchy = true;
            continue;
        };
        let rows = [metadata];
        let (
            metadata_owners,
            has_summary_metadata,
            aggregate_metadata_kind,
            has_aggregate_metadata_conflict,
            has_invalid_metadata,
        ) = metadata_evidence(&rows);
        evidence.owners.extend(metadata_owners);
        evidence.has_invalid_metadata |= has_invalid_metadata;
        let actual_kind = if has_aggregate_metadata_conflict {
            evidence.has_invalid_hierarchy = true;
            None
        } else {
            aggregate_metadata_kind
                .or(has_summary_metadata.then_some(MemoryKind::ThreadSummary as i32))
        };
        if actual_kind != Some(expected_kind) {
            evidence.has_invalid_hierarchy = true;
        }
        // A daily summary's inputs are raw memories in the legacy schema;
        // its source-thread ids are therefore the authoritative ownership
        // evidence. Higher aggregate tiers point to generated memories and
        // must be followed recursively instead.
        if actual_kind == Some(MemoryKind::DailySummary as i32) {
            let source_thread_ids = source_thread_ids(&rows);
            evidence.has_invalid_hierarchy |= source_thread_ids.is_empty();
            for source_thread_id in source_thread_ids {
                let source = resolve_source_thread_owner(
                    tx,
                    mapping,
                    source_thread_id,
                    &mut BTreeSet::new(),
                )
                .await?;
                evidence.owners.extend(source.owners);
                evidence.has_invalid_hierarchy |= source.has_invalid_hierarchy || source.is_missing;
            }
            // Daily summaries can outlive their raw inputs. If provenance is
            // retained, however, every referenced input must still exist.
            for source_memory_id in source_memory_ids(&rows) {
                let exists_sql = format!("SELECT 1 FROM memory m WHERE m.id = {P1}");
                let exists = sqlx::query_scalar::<Rdb, i32>(sqlx::AssertSqlSafe(exists_sql))
                    .bind(source_memory_id)
                    .fetch_optional(&mut **tx)
                    .await
                    .context(
                        "read nested daily summary input memory for memory kind migration plan",
                    )?
                    .is_some();
                evidence.has_invalid_hierarchy |= !exists;
            }
        }
        if actual_kind != Some(MemoryKind::DailySummary as i32)
            && let Some(actual_kind) = actual_kind
            && let Some(next_expected_kind) = expected_aggregate_input_kind(actual_kind)
        {
            let source_memory_ids = source_memory_ids(&rows);
            evidence.has_invalid_hierarchy |= source_memory_ids.is_empty();
            pending.extend(
                source_memory_ids
                    .into_iter()
                    .map(|memory_id| (memory_id, next_expected_kind)),
            );
        }
    }
    Ok(evidence)
}

async fn build_preflight_audit_in_tx(
    mapping_bytes: &[u8],
    tx: &mut RdbTransaction<'_>,
) -> Result<PreflightAudit> {
    let mapping_text = std::str::from_utf8(mapping_bytes).context("mapping must be UTF-8")?;
    let mapping = parse_mapping_json(mapping_text).context("invalid memory kind mapping")?;
    let mapping_sha256 = Sha256::digest(mapping_bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let unsupported_thread_reference_columns = unsupported_thread_reference_columns(tx).await?;
    let rows =
        sqlx::query_as::<Rdb, ThreadRow>("SELECT id, user_id, memory_kind FROM thread ORDER BY id")
            .fetch_all(&mut **tx)
            .await
            .context("read threads for memory kind migration plan")?;
    let all_thread_ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
    let mut threads = Vec::with_capacity(rows.len());
    let mut unresolved_thread_ids = Vec::new();
    // Every branch reports a memory id that fails one FK-style invariant
    // this migration relies on (no real FK constraints exist across these
    // tables, so dangling rows must be detected explicitly). A regular
    // memory may intentionally have no thread membership and is migrated as
    // RAW using its own author ID.
    //   1. a reflection sidecar row whose memory no longer exists
    //   2. a reflection sidecar row whose (sidecar) thread no longer exists
    //   3. a reflection sidecar row whose origin_thread_id no longer exists
    //   4. a reflection sidecar row not backed by a thread_memory row on
    //      the same (thread_id, memory_id) pair
    //   5. a thread_memory row whose thread no longer exists
    //   6. a thread_memory row whose memory no longer exists
    let orphan_sql = "\
        SELECT tri.memory_id AS value FROM thread_reflection_index tri \
            LEFT JOIN memory m ON m.id = tri.memory_id \
            WHERE m.id IS NULL \
        UNION \
        SELECT tri.memory_id AS value FROM thread_reflection_index tri \
            LEFT JOIN thread t ON t.id = tri.thread_id \
            WHERE t.id IS NULL \
        UNION \
        SELECT tri.memory_id AS value FROM thread_reflection_index tri \
            LEFT JOIN thread origin ON origin.id = tri.origin_thread_id \
            WHERE origin.id IS NULL \
        UNION \
        SELECT tri.memory_id AS value FROM thread_reflection_index tri \
            LEFT JOIN thread_memory tm \
                ON tm.memory_id = tri.memory_id AND tm.thread_id = tri.thread_id \
            WHERE tm.memory_id IS NULL \
        UNION \
        SELECT tm.memory_id AS value FROM thread_memory tm \
            LEFT JOIN thread t ON t.id = tm.thread_id \
            WHERE t.id IS NULL \
        UNION \
        SELECT tm.memory_id AS value FROM thread_memory tm \
            LEFT JOIN memory m ON m.id = tm.memory_id \
            WHERE m.id IS NULL";
    let mut unresolved_memory_ids: Vec<i64> = sqlx::query_as::<Rdb, IdRow>(orphan_sql)
        .fetch_all(&mut **tx)
        .await
        .context("read orphan memories for memory kind migration plan")?
        .into_iter()
        .map(|row| row.value)
        .collect();
    let duplicate_reflection_membership_sql = "SELECT tri.memory_id AS value \
        FROM thread_reflection_index tri JOIN thread_memory tm ON tm.memory_id = tri.memory_id \
        GROUP BY tri.memory_id HAVING COUNT(*) != 1";
    unresolved_memory_ids.extend(
        sqlx::query_as::<Rdb, IdRow>(duplicate_reflection_membership_sql)
            .fetch_all(&mut **tx)
            .await
            .context("read duplicate reflection memory memberships for memory kind migration plan")?
            .into_iter()
            .map(|row| row.value),
    );
    let aggregate_key_orphan_sql = "SELECT tak.thread_id AS value FROM thread_aggregate_key tak LEFT JOIN thread t ON t.id = tak.thread_id WHERE t.id IS NULL";
    unresolved_thread_ids.extend(
        sqlx::query_as::<Rdb, IdRow>(aggregate_key_orphan_sql)
            .fetch_all(&mut **tx)
            .await
            .context("read dangling reflection aggregate keys for memory kind migration plan")?
            .into_iter()
            .map(|row| row.value),
    );
    let mut reflection_split_thread_ids = Vec::new();
    let mut resolved_thread_targets = BTreeMap::new();
    for row in rows {
        let labels_sql =
            format!("SELECT label FROM thread_label WHERE thread_id = {P1} ORDER BY label");
        let labels: Vec<String> =
            sqlx::query_scalar::<Rdb, String>(sqlx::AssertSqlSafe(labels_sql))
                .bind(row.id)
                .fetch_all(&mut **tx)
                .await
                .context("read thread labels for memory kind migration plan")?;
        let owner_sql = format!(
            "SELECT origin_user_id AS value FROM thread_reflection_index WHERE thread_id = {P1}"
        );
        let owners = sqlx::query_as::<Rdb, IdRow>(sqlx::AssertSqlSafe(owner_sql))
            .bind(row.id)
            .fetch_all(&mut **tx)
            .await
            .context("read reflection owners for memory kind migration plan")?
            .into_iter()
            .map(|r| r.value)
            .collect::<BTreeSet<_>>();
        let has_reflection_sidecar = !owners.is_empty();
        let membership_sql = format!(
            "SELECT COUNT(*) FROM thread_memory tm LEFT JOIN thread_reflection_index tri ON tri.memory_id = tm.memory_id AND tri.thread_id = tm.thread_id WHERE tm.thread_id = {P1} AND tri.memory_id IS NULL"
        );
        let non_sidecar_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(membership_sql))
            .bind(row.id)
            .fetch_one(&mut **tx)
            .await
            .context("read reflection membership for memory kind migration plan")?;
        let reflection_role = MessageRole::RoleReflection as i32;
        let unindexed_reflection_sql = format!(
            "SELECT COUNT(*) FROM thread_memory tm JOIN memory m ON m.id = tm.memory_id LEFT JOIN thread_reflection_index tri ON tri.memory_id = m.id AND tri.thread_id = tm.thread_id WHERE tm.thread_id = {P1} AND m.role = {reflection_role} AND tri.memory_id IS NULL"
        );
        let reflection_without_sidecar_count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(unindexed_reflection_sql))
                .bind(row.id)
                .fetch_one(&mut **tx)
                .await
                .context("read unindexed reflection membership for memory kind migration plan")?;
        let metadata_sql = format!(
            "SELECT {MEMORY_METADATA_EXPR} AS metadata FROM memory m JOIN thread_memory tm ON tm.memory_id = m.id WHERE tm.thread_id = {P1}"
        );
        let metadata_rows = sqlx::query_as::<Rdb, MetadataRow>(sqlx::AssertSqlSafe(metadata_sql))
            .bind(row.id)
            .fetch_all(&mut **tx)
            .await
            .context("read memory metadata for memory kind migration plan")?;
        let (
            direct_metadata_owners,
            has_summary_metadata,
            aggregate_metadata_kind,
            has_aggregate_metadata_conflict,
            mut has_invalid_metadata,
        ) = metadata_evidence(&metadata_rows);
        let source_thread_ids = source_thread_ids(&metadata_rows);
        let source_memory_ids = source_memory_ids(&metadata_rows);
        let mut owner_candidates = direct_metadata_owners.clone();
        let mut has_invalid_aggregate_source_hierarchy = false;
        let mut has_missing_summary_source_thread = false;
        if let Some(aggregate_kind) = aggregate_metadata_kind {
            let aggregate_evidence =
                aggregate_source_evidence(tx, &mapping, aggregate_kind, &source_memory_ids).await?;
            owner_candidates.extend(aggregate_evidence.owners);
            has_invalid_aggregate_source_hierarchy = aggregate_evidence.has_invalid_hierarchy;
            has_invalid_metadata |= aggregate_evidence.has_invalid_metadata;
            if aggregate_kind == MemoryKind::DailySummary as i32 {
                for source_thread_id in &source_thread_ids {
                    let source = resolve_source_thread_owner(
                        tx,
                        &mapping,
                        *source_thread_id,
                        &mut BTreeSet::new(),
                    )
                    .await?;
                    owner_candidates.extend(source.owners);
                    has_invalid_aggregate_source_hierarchy |= source.has_invalid_hierarchy;
                    has_missing_summary_source_thread |= source.is_missing;
                }
            }
        } else {
            for source_thread_id in &source_thread_ids {
                let source = resolve_source_thread_owner(
                    tx,
                    &mapping,
                    *source_thread_id,
                    &mut BTreeSet::new(),
                )
                .await?;
                owner_candidates.extend(source.owners);
                has_invalid_aggregate_source_hierarchy |= source.has_invalid_hierarchy;
                has_missing_summary_source_thread |= source.is_missing;
            }
        }
        let metadata_source_user_id = single_owner(&owner_candidates);
        let mismatch_sql = format!(
            "SELECT COUNT(*) FROM thread_memory tm JOIN thread_reflection_index tri ON tri.memory_id = tm.memory_id WHERE tm.thread_id = {P1} AND tri.thread_id != tm.thread_id"
        );
        let sidecar_thread_mismatch_count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(mismatch_sql))
                .bind(row.id)
                .fetch_one(&mut **tx)
                .await
                .context("read reflection sidecar consistency for memory kind migration plan")?;
        let origin_owner_mismatch_sql = format!(
            "SELECT COUNT(*) FROM thread_reflection_index tri JOIN thread origin ON origin.id = tri.origin_thread_id WHERE tri.thread_id = {P1} AND origin.user_id != tri.origin_user_id"
        );
        let reflection_origin_owner_mismatch_count: i64 =
            sqlx::query_scalar(sqlx::AssertSqlSafe(origin_owner_mismatch_sql))
                .bind(row.id)
                .fetch_one(&mut **tx)
                .await
                .context(
                    "read reflection origin owner consistency for memory kind migration plan",
                )?;
        let reverse_origin_reference_sql =
            format!("SELECT COUNT(*) FROM thread_reflection_index WHERE origin_thread_id = {P1}");
        let reverse_origin_reference_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
            reverse_origin_reference_sql,
        ))
        .bind(row.id)
        .fetch_one(&mut **tx)
        .await
        .context(
            "read reflection origin-thread reverse references for memory kind migration plan",
        )?;
        let few_shot_usage_reference_sql = format!(
            "SELECT COUNT(*) FROM reflection_few_shot_usage WHERE used_in_thread_id = {P1}"
        );
        let few_shot_usage_reference_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(
            few_shot_usage_reference_sql,
        ))
        .bind(row.id)
        .fetch_one(&mut **tx)
        .await
        .context(
            "read reflection few-shot usage reverse references for memory kind migration plan",
        )?;
        let classification = classify_thread(
            &ThreadEvidence {
                thread_id: row.id,
                legacy_user_id: row.user_id,
                labels: labels.clone(),
                metadata_source_user_id,
                has_invalid_metadata,
                has_missing_summary_source_thread,
                has_metadata_owner_conflict: owner_candidates.len() > 1,
                has_summary_metadata,
                aggregate_metadata_kind,
                has_aggregate_metadata_conflict,
                has_invalid_aggregate_source_hierarchy,
                has_reflection_sidecar,
                has_sidecar_thread_mismatch: sidecar_thread_mismatch_count > 0,
                has_reflection_origin_owner_mismatch: reflection_origin_owner_mismatch_count > 0,
                reflection_origin_user_ids: owners.clone(),
                has_non_sidecar_memory: non_sidecar_count > 0,
                role_reflection_without_sidecar: reflection_without_sidecar_count > 0,
            },
            &mapping,
        );
        let mut reflection_split_owners = Vec::new();
        let classification_text = match classification {
            Classification::Resolved {
                memory_kind,
                owner_user_id,
            } => {
                let replaces_reflection_thread =
                    should_replace_reflection_thread(memory_kind, has_reflection_sidecar);
                let existing_reflection_state = if replaces_reflection_thread {
                    reflection_thread_migration_state(&mut *tx, &row, &labels, &owners).await?
                } else {
                    ExistingReflectionState::NotMigrated
                };
                if existing_reflection_state == ExistingReflectionState::Complete {
                    // This thread is not deleted on a retry, so references to
                    // it are safe and must not turn an idempotent apply into
                    // an unresolved migration.
                    resolved_thread_targets.insert(row.id, (owner_user_id, memory_kind));
                    format!("resolved:kind={memory_kind},owner={owner_user_id}")
                } else if existing_reflection_state == ExistingReflectionState::Incomplete {
                    // A matching key alone is insufficient evidence of a completed
                    // replacement. Retrying must not silently accept a partial move.
                    unresolved_thread_ids.push(row.id);
                    "unresolved:partial_reflection_migration".to_string()
                } else if let Some(reason) = replaces_reflection_thread
                    .then(|| {
                        reverse_reference_unresolved_reason(
                            reverse_origin_reference_count,
                            few_shot_usage_reference_count,
                        )
                    })
                    .flatten()
                {
                    unresolved_thread_ids.push(row.id);
                    reason.to_string()
                } else if replaces_reflection_thread {
                    // Every legacy reflection aggregate is replaced, including
                    // the single-owner shape, so online aggregation discovers
                    // the existing history through the new owner-scoped key.
                    reflection_split_thread_ids.push(row.id);
                    reflection_split_owners = fetch_reflection_split_owners(tx, row.id).await?;
                    "reflection_split_required".to_string()
                } else {
                    resolved_thread_targets.insert(row.id, (owner_user_id, memory_kind));
                    format!("resolved:kind={memory_kind},owner={owner_user_id}")
                }
            }
            Classification::Unresolved { reason } => {
                unresolved_thread_ids.push(row.id);
                format!("unresolved:{reason}")
            }
            Classification::ReflectionSplitRequired { .. } => {
                if let Some(reason) = reverse_reference_unresolved_reason(
                    reverse_origin_reference_count,
                    few_shot_usage_reference_count,
                ) {
                    unresolved_thread_ids.push(row.id);
                    reason.to_string()
                } else {
                    reflection_split_thread_ids.push(row.id);
                    reflection_split_owners = fetch_reflection_split_owners(tx, row.id).await?;
                    "reflection_split_required".to_string()
                }
            }
        };
        threads.push(ThreadAudit {
            thread_id: row.id,
            classification: classification_text,
            labels,
            metadata_source_user_id,
            direct_metadata_owner_user_ids: owner_candidate_ids(&direct_metadata_owners),
            owner_candidate_user_ids: owner_candidate_ids(&owner_candidates),
            has_invalid_metadata,
            has_missing_summary_source_thread,
            has_summary_metadata,
            aggregate_metadata_kind,
            source_thread_ids: source_thread_ids.iter().copied().collect(),
            source_memory_ids: source_memory_ids.iter().copied().collect(),
            reflection_origin_user_ids: owners.into_iter().collect(),
            reflection_split_owners,
            has_sidecar_thread_mismatch: sidecar_thread_mismatch_count > 0,
            has_reflection_origin_owner_mismatch: reflection_origin_owner_mismatch_count > 0,
            has_reverse_reflection_origin_reference: has_reference_count(
                reverse_origin_reference_count,
            ),
            has_reverse_few_shot_usage_reference: has_reference_count(
                few_shot_usage_reference_count,
            ),
            has_invalid_aggregate_source_hierarchy,
        });
    }
    let memberships =
        sqlx::query_as::<Rdb, MembershipRow>("SELECT thread_id, memory_id FROM thread_memory")
            .fetch_all(&mut **tx)
            .await
            .context("read memory memberships for migration plan")?;
    let default_system_memory_ids = sqlx::query_scalar::<Rdb, i64>(
        "SELECT DISTINCT default_system_memory_id FROM thread WHERE default_system_memory_id IS NOT NULL",
    )
    .fetch_all(&mut **tx)
    .await
    .context("read default system memories for memory kind migration plan")?
    .into_iter()
    .collect::<BTreeSet<_>>();
    let non_resolved_thread_ids = unresolved_thread_ids
        .iter()
        .chain(&reflection_split_thread_ids)
        .copied()
        .collect::<BTreeSet<_>>();
    unresolved_memory_ids.extend(conflicting_memory_ids(
        &memberships,
        &resolved_thread_targets,
        &non_resolved_thread_ids,
        &default_system_memory_ids,
    ));
    let (external_id_moves, unresolved_external_id_memory_ids) =
        build_external_id_moves(tx, &memberships, &resolved_thread_targets).await?;
    unresolved_memory_ids.sort_unstable();
    unresolved_memory_ids.dedup();
    unresolved_thread_ids.sort_unstable();
    unresolved_thread_ids.dedup();
    if !unsupported_thread_reference_columns.is_empty() {
        unresolved_thread_ids.extend(all_thread_ids);
        unresolved_thread_ids.sort_unstable();
        unresolved_thread_ids.dedup();
    }
    Ok(PreflightAudit {
        mapping_sha256,
        summary_labels: mapping.summary_labels.clone(),
        explicit_owners_by_thread_id: mapping.explicit_owners_by_thread_id.clone(),
        summary_label_count: mapping.summary_labels.len(),
        explicit_owner_count: mapping.explicit_owners_by_thread_id.len(),
        threads,
        unresolved_thread_ids,
        unresolved_memory_ids,
        reflection_split_thread_ids,
        unsupported_thread_reference_columns,
        external_id_moves,
        unresolved_external_id_memory_ids,
    })
}

async fn build_preflight_audit(mapping_bytes: &[u8]) -> Result<PreflightAudit> {
    let pool = rdb_pool_by_env().await?;
    let mut tx = pool
        .begin()
        .await
        .context("begin migration plan transaction")?;
    configure_preflight_snapshot(&mut tx).await?;
    let audit = build_preflight_audit_in_tx(mapping_bytes, &mut tx).await?;
    tx.commit()
        .await
        .context("finish migration plan transaction")?;
    Ok(audit)
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ApplyAudit {
    format_version: u32,
    mapping_sha256: String,
    initial_thread_ids: Vec<i64>,
    initial_memory_ids: Vec<i64>,
    threads: Vec<ImmutableRowAudit>,
    memories: Vec<ImmutableRowAudit>,
    reflection_moves: Vec<ReflectionMoveAudit>,
    external_id_moves: Vec<ExternalIdMoveAudit>,
    updated_threads: u64,
    updated_memories: u64,
}

#[derive(sqlx::FromRow)]
struct SchemaColumnRow {
    table_name: String,
    column_name: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ImmutableRowAudit {
    id: i64,
    created_at: i64,
    updated_at: i64,
    external_id: Option<String>,
    expected_user_id: i64,
    expected_memory_kind: i32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ReflectionMoveAudit {
    old_thread_id: i64,
    new_thread_id: i64,
    origin_user_id: i64,
    labels: Vec<String>,
    labels_hash: String,
    description: Option<String>,
    channel: Option<String>,
    metadata: Option<String>,
    created_at: i64,
    updated_at: i64,
    memories: Vec<ReflectionSplitMemoryAudit>,
    memory_timestamps: Vec<ReflectionMemoryTimestampAudit>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ReflectionMemoryTimestampAudit {
    memory_id: i64,
    #[serde(alias = "author_user_id")]
    expected_user_id: i64,
    created_at: i64,
    updated_at: i64,
    external_id: Option<String>,
}

#[derive(Debug, serde::Serialize)]
struct VerifyAudit {
    format_version: u32,
    mapping_sha256: String,
    verified_thread_ids: Vec<i64>,
    verified_memory_ids: Vec<i64>,
    failures: Vec<VerifyFailure>,
}

#[derive(Debug, serde::Serialize)]
struct VerifyFailure {
    check: String,
    id: Option<i64>,
    detail: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ClientApplyAudit {
    format_version: u32,
    status: String,
    mapping_sha256: String,
    updated_threads: u64,
    updated_thread_owners: u64,
    updated_memories: u64,
    updated_external_ids: u64,
    retained_threads: u64,
    retained_memories: u64,
    raw_threads: u64,
    raw_memories: u64,
    external_id_moves: Vec<ExternalIdMoveAudit>,
    reflection_moves: Vec<ReflectionMoveAudit>,
    warnings: Vec<ClientApplyWarning>,
    failures: Vec<ClientApplyFailure>,
}

fn write_client_audit(file: &mut std::fs::File, audit: &ClientApplyAudit) -> Result<()> {
    file.set_len(0)?;
    file.rewind()?;
    file.write_all(&serde_json::to_vec_pretty(audit)?)?;
    file.sync_all()?;
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct ClientApplyWarning {
    entity: String,
    id: Option<i64>,
    check: String,
    detail: String,
}

#[derive(Debug, serde::Serialize)]
struct ClientPruneDump {
    format_version: u32,
    status: String,
    memory_ids: Vec<i64>,
    thread_ids: Vec<i64>,
    records: serde_json::Value,
    deleted_memory_rows: u64,
    deleted_thread_rows: u64,
    deleted_membership_rows: u64,
}

fn write_client_prune_dump(file: &mut std::fs::File, dump: &ClientPruneDump) -> Result<()> {
    file.set_len(0)?;
    file.rewind()?;
    file.write_all(&serde_json::to_vec_pretty(dump)?)?;
    file.sync_all()?;
    Ok(())
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ClientApplyFailure {
    entity: String,
    id: i64,
    detail: String,
}

#[derive(sqlx::FromRow)]
struct ImmutableRow {
    id: i64,
    user_id: i64,
    created_at: i64,
    updated_at: i64,
    external_id: Option<String>,
}

#[derive(sqlx::FromRow)]
struct OwnershipRow {
    created_at: i64,
    updated_at: i64,
    user_id: i64,
    memory_kind: Option<i32>,
    external_id: Option<String>,
}

/// Audits thread rows: the migration reassigns a thread's `user_id` to the
/// resolved target owner, so `expected_user_id` comes from `targets`.
fn thread_ownership_audit(
    rows: Vec<ImmutableRow>,
    targets: &BTreeMap<i64, (i64, i32)>,
) -> Vec<ImmutableRowAudit> {
    rows.into_iter()
        .filter_map(|row| {
            targets.get(&row.id).map(
                |&(target_user_id, expected_memory_kind)| ImmutableRowAudit {
                    id: row.id,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    external_id: row.external_id,
                    expected_user_id: target_user_id,
                    expected_memory_kind,
                },
            )
        })
        .collect()
}

/// Audits memory rows: generated legacy authors are implementation owners,
/// not human authors, and are reassigned to the resolved thread owner.
/// Real authors remain immutable.
fn memory_ownership_audit(
    rows: Vec<ImmutableRow>,
    targets: &BTreeMap<i64, (i64, i32)>,
) -> Vec<ImmutableRowAudit> {
    rows.into_iter()
        .filter_map(|row| {
            targets.get(&row.id).map(
                |&(target_user_id, expected_memory_kind)| ImmutableRowAudit {
                    id: row.id,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    external_id: row.external_id,
                    expected_user_id: if is_server_legacy_owner(expected_memory_kind, row.user_id) {
                        target_user_id
                    } else {
                        row.user_id
                    },
                    expected_memory_kind,
                },
            )
        })
        .collect()
}

fn standalone_memory_target(author_user_id: i64) -> (i64, i32) {
    (author_user_id, MemoryKind::Raw as i32)
}

#[derive(sqlx::FromRow)]
struct ThreadCloneRow {
    description: Option<String>,
    channel: Option<String>,
    embedding: Option<Vec<u8>>,
    embedding_dim: Option<i32>,
    metadata: Option<String>,
    created_at: i64,
    updated_at: i64,
}

fn resolved_target(classification: &str) -> Option<(i64, i32)> {
    let rest = classification.strip_prefix("resolved:kind=")?;
    let (kind, owner) = rest.split_once(",owner=")?;
    Some((owner.parse().ok()?, kind.parse().ok()?))
}

fn aggregate_namespace_for_external_id(kind: i32, external_id: &str) -> Option<&'static str> {
    let namespace = match kind {
        value if value == MemoryKind::DailySummary as i32 => "daily",
        value if value == MemoryKind::WeeklySummary as i32 => "weekly",
        value if value == MemoryKind::MonthlySummary as i32 => "monthly",
        _ => return None,
    };
    external_id
        .starts_with(&format!("{namespace}:"))
        .then_some(namespace)
}

async fn build_external_id_moves(
    tx: &mut RdbTransaction<'_>,
    memberships: &[MembershipRow],
    thread_targets: &BTreeMap<i64, (i64, i32)>,
) -> Result<(Vec<ExternalIdMoveAudit>, Vec<i64>)> {
    let sql = format!(
        "SELECT id, external_id, {MEMORY_METADATA_EXPR} AS metadata FROM memory m WHERE external_id IS NOT NULL ORDER BY id"
    );
    let rows = sqlx::query_as::<Rdb, ExternalIdRow>(sqlx::AssertSqlSafe(sql))
        .fetch_all(&mut **tx)
        .await?;
    plan_external_id_moves(&rows, memberships, thread_targets)
}

fn plan_external_id_moves(
    rows: &[ExternalIdRow],
    memberships: &[MembershipRow],
    thread_targets: &BTreeMap<i64, (i64, i32)>,
) -> Result<(Vec<ExternalIdMoveAudit>, Vec<i64>)> {
    let existing = rows
        .iter()
        .map(|row| (row.external_id.clone(), row.id))
        .collect::<BTreeMap<_, _>>();
    let mut memberships_by_memory = BTreeMap::<i64, Vec<i64>>::new();
    for membership in memberships {
        memberships_by_memory
            .entry(membership.memory_id)
            .or_default()
            .push(membership.thread_id);
    }
    let mut moves = Vec::new();
    let mut unresolved = Vec::new();
    let mut targets = BTreeMap::<String, i64>::new();
    for row in rows {
        let source = row.metadata.as_deref().and_then(|metadata| {
            serde_json::from_str::<serde_json::Value>(metadata)
                .ok()
                .and_then(|value| value.get("source")?.as_str().map(str::to_owned))
        });
        let is_aggregate_external_id = ["daily:", "weekly:", "monthly:"]
            .iter()
            .any(|prefix| row.external_id.starts_with(prefix));
        let is_candidate = source.is_some() || is_aggregate_external_id;
        let mut mark_unresolved_if_candidate = || {
            if is_candidate {
                unresolved.push(row.id);
            }
        };
        let memberships = memberships_by_memory
            .get(&row.id)
            .map(Vec::as_slice)
            .unwrap_or_default();
        if memberships.len() != 1 {
            mark_unresolved_if_candidate();
            continue;
        }
        let thread_id = memberships[0];
        let Some(&(creator, kind)) = thread_targets.get(&thread_id) else {
            mark_unresolved_if_candidate();
            continue;
        };
        let namespace = if let Some(source) = source.as_deref() {
            namespace_for_external_id(source, &row.external_id)
        } else {
            aggregate_namespace_for_external_id(kind, &row.external_id).map(str::to_string)
        };
        let Some(namespace) = namespace else {
            if is_candidate {
                unresolved.push(row.id);
            }
            continue;
        };
        let expected_prefix = format!("{namespace}:{creator}:");
        if row.external_id.starts_with(&expected_prefix) {
            continue;
        }
        let new_external_id =
            owner_scoped(&namespace, creator, &row.external_id, EXTERNAL_ID_MAX_BYTES)?;
        if existing
            .get(&new_external_id)
            .is_some_and(|existing_id| *existing_id != row.id)
            || targets.insert(new_external_id.clone(), row.id).is_some()
        {
            unresolved.push(row.id);
            continue;
        }
        moves.push(ExternalIdMoveAudit {
            memory_id: row.id,
            old_external_id: row.external_id.clone(),
            new_external_id,
            thread_creator_user_id: creator,
        });
    }
    unresolved.sort_unstable();
    unresolved.dedup();
    Ok((moves, unresolved))
}

#[cfg(feature = "postgres")]
const AGGREGATE_KEY_LOOKUP_SQL: &str =
    "SELECT thread_id FROM thread_aggregate_key WHERE user_id = $1 AND labels_hash = $2";
#[cfg(not(feature = "postgres"))]
const AGGREGATE_KEY_LOOKUP_SQL: &str =
    "SELECT thread_id FROM thread_aggregate_key WHERE user_id = ? AND labels_hash = ?";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExistingReflectionState {
    NotMigrated,
    Complete,
    Incomplete,
}

fn reflection_membership_state(
    sidecar_count: i64,
    membership_count: i64,
    all_membership_count: i64,
) -> ExistingReflectionState {
    if sidecar_count > 0
        && sidecar_count == membership_count
        && sidecar_count == all_membership_count
    {
        ExistingReflectionState::Complete
    } else {
        ExistingReflectionState::Incomplete
    }
}

fn reflection_thread_membership_sql() -> String {
    format!(
        "SELECT COUNT(*) FROM thread_memory tm JOIN thread_reflection_index tri ON tri.memory_id = tm.memory_id AND tri.thread_id = tm.thread_id WHERE tri.thread_id = {P1}"
    )
}

async fn reflection_thread_migration_state(
    tx: &mut RdbTransaction<'_>,
    thread: &ThreadRow,
    labels: &[String],
    origin_user_ids: &BTreeSet<i64>,
) -> Result<ExistingReflectionState> {
    if origin_user_ids.len() != 1
        || thread.memory_kind != Some(MemoryKind::Reflection as i32)
        || thread.user_id != *origin_user_ids.first().expect("len == 1")
    {
        return Ok(ExistingReflectionState::NotMigrated);
    }
    let labels_hash = sha256_join_pipe(&compose_aggregate_labels(labels));
    let key_thread_id = sqlx::query_scalar::<Rdb, i64>(AGGREGATE_KEY_LOOKUP_SQL)
        .bind(thread.user_id)
        .bind(labels_hash)
        .fetch_optional(&mut **tx)
        .await
        .context("read aggregate key for reflection migration idempotency")?;
    if !reflection_aggregate_key_matches(thread.id, key_thread_id) {
        return Ok(ExistingReflectionState::NotMigrated);
    }
    let sidecar_sql =
        format!("SELECT COUNT(*) FROM thread_reflection_index WHERE thread_id = {P1}");
    let sidecar_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sidecar_sql))
        .bind(thread.id)
        .fetch_one(&mut **tx)
        .await?;
    let membership_sql = format!("SELECT COUNT(*) FROM thread_memory WHERE thread_id = {P1}");
    let membership_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(membership_sql))
        .bind(thread.id)
        .fetch_one(&mut **tx)
        .await?;
    let all_membership_sql = reflection_thread_membership_sql();
    let all_membership_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(all_membership_sql))
        .bind(thread.id)
        .fetch_one(&mut **tx)
        .await?;
    Ok(reflection_membership_state(
        sidecar_count,
        membership_count,
        all_membership_count,
    ))
}

fn reflection_aggregate_key_matches(thread_id: i64, key_thread_id: Option<i64>) -> bool {
    key_thread_id == Some(thread_id)
}

fn expected_thread_ids_after_moves(
    initial_thread_ids: &[i64],
    reflection_moves: &[ReflectionMoveAudit],
) -> BTreeSet<i64> {
    let mut expected = initial_thread_ids.iter().copied().collect::<BTreeSet<_>>();
    for movement in reflection_moves {
        expected.remove(&movement.old_thread_id);
        expected.insert(movement.new_thread_id);
    }
    expected
}

#[cfg(feature = "postgres")]
const REFLECTION_MEMBERSHIP_SQL: &str = "SELECT tm.position, tri.origin_user_id FROM thread_memory tm \
    JOIN thread_reflection_index tri ON tri.memory_id = tm.memory_id \
    WHERE tm.thread_id = $1 AND tm.memory_id = $2 AND tri.thread_id = $3";
#[cfg(not(feature = "postgres"))]
const REFLECTION_MEMBERSHIP_SQL: &str = "SELECT tm.position, tri.origin_user_id FROM thread_memory tm \
    JOIN thread_reflection_index tri ON tri.memory_id = tm.memory_id \
    WHERE tm.thread_id = ? AND tm.memory_id = ? AND tri.thread_id = ?";

const POSTGRES_REFLECTION_THREAD_INSERT_SQL: &str = "INSERT INTO thread (id, default_system_memory_id, user_id, description, channel, embedding, embedding_dim, created_at, updated_at, metadata, memory_kind) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11)";
const SQLITE_REFLECTION_THREAD_INSERT_SQL: &str = "INSERT INTO thread (id, default_system_memory_id, user_id, description, channel, embedding, embedding_dim, created_at, updated_at, metadata, memory_kind) VALUES (?,?,?,?,?,?,?,?,?,?,?)";

fn reflection_thread_insert_sql() -> &'static str {
    if cfg!(feature = "postgres") {
        POSTGRES_REFLECTION_THREAD_INSERT_SQL
    } else {
        SQLITE_REFLECTION_THREAD_INSERT_SQL
    }
}

fn aggregate_key_conflicts(existing_thread_id: Option<i64>, old_thread_id: i64) -> bool {
    existing_thread_id.is_some_and(|thread_id| thread_id != old_thread_id)
}

fn replacement_updated_at(
    old_updated_at: i64,
    memory_updated_at: impl IntoIterator<Item = i64>,
) -> i64 {
    memory_updated_at.into_iter().fold(old_updated_at, i64::max)
}

fn has_reference_count(reference_count: i64) -> bool {
    reference_count > 0
}

/// Picks the unresolved reason when a legacy reflection thread is still
/// reachable via a reverse reference, or `None` if neither reference exists.
///
/// Both callers must agree on which reason wins when a thread is referenced
/// both ways, so this is centralized rather than re-checked per call site.
fn reverse_reference_unresolved_reason(
    origin_reference_count: i64,
    few_shot_usage_reference_count: i64,
) -> Option<&'static str> {
    if has_reference_count(origin_reference_count) {
        Some("unresolved:reflection_thread_referenced_as_origin")
    } else if has_reference_count(few_shot_usage_reference_count) {
        Some("unresolved:reflection_thread_referenced_by_few_shot_usage")
    } else {
        None
    }
}

async fn fetch_reflection_split_owners(
    tx: &mut RdbTransaction<'_>,
    thread_id: i64,
) -> Result<Vec<ReflectionSplitOwnerAudit>> {
    let split_sql = format!(
        "SELECT tri.origin_user_id, tm.memory_id, tm.position FROM thread_reflection_index tri JOIN thread_memory tm ON tm.memory_id = tri.memory_id AND tm.thread_id = tri.thread_id WHERE tri.thread_id = {P1} ORDER BY tri.origin_user_id, tm.position, tm.memory_id"
    );
    let split_rows = sqlx::query_as::<Rdb, ReflectionSplitRow>(sqlx::AssertSqlSafe(split_sql))
        .bind(thread_id)
        .fetch_all(&mut **tx)
        .await
        .context("read reflection split audit details for memory kind migration plan")?;
    Ok(reflection_split_owner_audit(split_rows))
}

fn should_replace_reflection_thread(memory_kind: i32, has_reflection_sidecar: bool) -> bool {
    memory_kind == MemoryKind::Reflection as i32 && has_reflection_sidecar
}

fn reflection_default_system_memory_id() -> Option<i64> {
    None
}

fn reflection_membership_matches(
    membership: Option<(i32, i64)>,
    expected_position: i32,
    expected_origin_user_id: i64,
) -> bool {
    membership == Some((expected_position, expected_origin_user_id))
}

fn is_known_thread_reference_column(table_name: &str, column_name: &str) -> bool {
    matches!(
        (table_name, column_name),
        ("thread_memory", "thread_id")
            | ("thread_label", "thread_id")
            | ("thread_reflection_index", "thread_id")
            | ("thread_reflection_index", "origin_thread_id")
            | ("thread_aggregate_key", "thread_id")
            | ("reflection_few_shot_usage", "used_in_thread_id")
    )
}

#[cfg(feature = "postgres")]
const THREAD_REFERENCE_COLUMNS_SQL: &str = "SELECT table_name, column_name \
    FROM information_schema.columns WHERE table_schema = current_schema() \
    AND column_name LIKE '%thread_id'";
#[cfg(not(feature = "postgres"))]
const THREAD_REFERENCE_COLUMNS_SQL: &str = "SELECT m.name AS table_name, p.name AS column_name \
    FROM sqlite_master AS m JOIN pragma_table_info(m.name) AS p \
    WHERE m.type = 'table' AND p.name LIKE '%thread_id'";

async fn unsupported_thread_reference_columns(tx: &mut RdbTransaction<'_>) -> Result<Vec<String>> {
    let columns = sqlx::query_as::<Rdb, SchemaColumnRow>(THREAD_REFERENCE_COLUMNS_SQL)
        .fetch_all(&mut **tx)
        .await
        .context("inspect aggregate-thread reference columns")?;
    Ok(columns
        .into_iter()
        .filter(|column| !is_known_thread_reference_column(&column.table_name, &column.column_name))
        .map(|column| format!("{}.{}", column.table_name, column.column_name))
        .collect())
}

async fn ensure_apply_schema(tx: &mut RdbTransaction<'_>) -> Result<()> {
    // A harmless query validates both expand columns and reflection tables.
    // The migration intentionally never runs DDL: operators control schema rollout.
    sqlx::query("SELECT t.memory_kind, m.memory_kind FROM thread t JOIN memory m ON 1 = 0")
        .execute(&mut **tx)
        .await
        .context("memory_kind columns are not available; apply the expand DDL first")?;
    sqlx::query("SELECT thread_id, labels_hash FROM thread_aggregate_key WHERE 1 = 0")
        .execute(&mut **tx)
        .await
        .context("reflection aggregate schema is not available")?;
    sqlx::query("SELECT memory_id, thread_id FROM thread_reflection_index WHERE 1 = 0")
        .execute(&mut **tx)
        .await
        .context("reflection sidecar schema is not available")?;
    Ok(())
}

async fn immutable_rows(tx: &mut RdbTransaction<'_>, table: &str) -> Result<Vec<ImmutableRow>> {
    let external_id = if table == "memory" {
        "external_id"
    } else {
        "NULL"
    };
    let sql = format!(
        "SELECT id, user_id, created_at, updated_at, {external_id} AS external_id FROM {table} ORDER BY id"
    );
    sqlx::query_as::<Rdb, ImmutableRow>(sqlx::AssertSqlSafe(sql))
        .fetch_all(&mut **tx)
        .await
        .with_context(|| format!("read immutable {table} rows"))
}

fn require_single_external_id_update(rows_affected: u64, memory_id: i64) -> Result<u64> {
    if rows_affected != 1 {
        bail!("external_id changed during migration preflight for memory {memory_id}");
    }
    Ok(rows_affected)
}

async fn apply_mapping(mapping_bytes: &[u8], audit_file: &mut std::fs::File) -> Result<ApplyAudit> {
    let pool = rdb_pool_by_env().await?;
    let id_generator = IdGeneratorWrapper::new();
    let mut tx = pool
        .begin()
        .await
        .context("begin migration apply transaction")?;
    configure_apply_snapshot(&mut tx).await?;
    ensure_apply_schema(&mut tx).await?;
    let preflight = build_preflight_audit_in_tx(mapping_bytes, &mut tx).await?;
    if !preflight.unresolved_thread_ids.is_empty()
        || !preflight.unresolved_memory_ids.is_empty()
        || !preflight.unresolved_external_id_memory_ids.is_empty()
    {
        bail!("preflight is unresolved; no rows were updated");
    }
    let threads = immutable_rows(&mut tx, "thread").await?;
    let memories = immutable_rows(&mut tx, "memory").await?;
    let thread_targets = preflight
        .threads
        .iter()
        .filter_map(|thread| {
            resolved_target(&thread.classification).map(|target| (thread.thread_id, target))
        })
        .collect::<BTreeMap<_, _>>();
    let memberships =
        sqlx::query_as::<Rdb, MembershipRow>("SELECT thread_id, memory_id FROM thread_memory")
            .fetch_all(&mut *tx)
            .await
            .context("read memory memberships for apply audit")?;
    let default_system_memory_ids = sqlx::query_scalar::<Rdb, i64>(
        "SELECT DISTINCT default_system_memory_id FROM thread WHERE default_system_memory_id IS NOT NULL",
    )
    .fetch_all(&mut *tx)
    .await
    .context("read default system memories for apply audit")?
    .into_iter()
    .collect::<BTreeSet<_>>();
    let mut memory_targets = BTreeMap::new();
    for membership in memberships {
        if default_system_memory_ids.contains(&membership.memory_id) {
            continue;
        }
        if let Some(&target) = thread_targets.get(&membership.thread_id) {
            memory_targets.insert(membership.memory_id, target);
        }
    }
    for memory_id in &default_system_memory_ids {
        memory_targets.insert(*memory_id, (0, MemoryKind::Raw as i32));
    }
    let standalone_memories = sqlx::query_as::<Rdb, IdOwnerRow>(
        "SELECT m.id, m.user_id FROM memory m \
         WHERE NOT EXISTS (SELECT 1 FROM thread_memory tm WHERE tm.memory_id = m.id)",
    )
    .fetch_all(&mut *tx)
    .await
    .context("read standalone memories for apply audit")?;
    for memory in standalone_memories {
        memory_targets.insert(memory.id, standalone_memory_target(memory.user_id));
    }
    let mut updated_threads = 0;
    let mut updated_memories = 0;
    // Preflight is deliberately rebuilt immediately before this transaction.
    // Conditional updates make a successful retry a true no-op for normal rows.
    for thread in &preflight.threads {
        if let Some((owner, kind)) = resolved_target(&thread.classification) {
            // SQLite uses one anonymous placeholder per occurrence; use a portable query instead.
            let sql = if cfg!(feature = "postgres") {
                "UPDATE thread SET user_id = $1, memory_kind = $2 WHERE id = $3 AND (user_id != $1 OR memory_kind IS NULL OR memory_kind != $2)"
            } else {
                "UPDATE thread SET user_id = ?, memory_kind = ? WHERE id = ? AND (user_id != ? OR memory_kind IS NULL OR memory_kind != ?)"
            };
            let mut q = sqlx::query::<Rdb>(sql)
                .bind(owner)
                .bind(kind)
                .bind(thread.thread_id);
            if !cfg!(feature = "postgres") {
                q = q.bind(owner).bind(kind);
            }
            updated_threads += q.execute(&mut *tx).await?.rows_affected();
            // i64::MIN never matches a real user_id, so kinds with no legacy
            // synthetic owner (server_legacy_generated_owner returns None)
            // leave the memory's user_id untouched via the CASE below.
            let legacy_owner = server_legacy_generated_owner(kind).unwrap_or(i64::MIN);
            let memory_sql = if cfg!(feature = "postgres") {
                "UPDATE memory SET memory_kind = $1, user_id = CASE WHEN user_id = $2 THEN $3 ELSE user_id END WHERE id IN (SELECT memory_id FROM thread_memory WHERE thread_id = $4) AND id NOT IN (SELECT default_system_memory_id FROM thread WHERE default_system_memory_id IS NOT NULL) AND (memory_kind IS NULL OR memory_kind != $1 OR user_id = $2)"
            } else {
                "UPDATE memory SET memory_kind = ?, user_id = CASE WHEN user_id = ? THEN ? ELSE user_id END WHERE id IN (SELECT memory_id FROM thread_memory WHERE thread_id = ?) AND id NOT IN (SELECT default_system_memory_id FROM thread WHERE default_system_memory_id IS NOT NULL) AND (memory_kind IS NULL OR memory_kind != ? OR user_id = ?)"
            };
            let mut q = sqlx::query::<Rdb>(memory_sql)
                .bind(kind)
                .bind(legacy_owner)
                .bind(owner)
                .bind(thread.thread_id);
            if !cfg!(feature = "postgres") {
                q = q.bind(kind).bind(legacy_owner);
            }
            updated_memories += q.execute(&mut *tx).await?.rows_affected();
        }
    }
    let standalone_memory_sql = if cfg!(feature = "postgres") {
        "UPDATE memory SET memory_kind = $1 \
         WHERE NOT EXISTS (SELECT 1 FROM thread_memory tm WHERE tm.memory_id = memory.id) \
           AND (memory_kind IS NULL OR memory_kind != $1)"
    } else {
        "UPDATE memory SET memory_kind = ? \
         WHERE NOT EXISTS (SELECT 1 FROM thread_memory tm WHERE tm.memory_id = memory.id) \
           AND (memory_kind IS NULL OR memory_kind != ?)"
    };
    let mut standalone_update =
        sqlx::query::<Rdb>(standalone_memory_sql).bind(MemoryKind::Raw as i32);
    if !cfg!(feature = "postgres") {
        standalone_update = standalone_update.bind(MemoryKind::Raw as i32);
    }
    updated_memories += standalone_update.execute(&mut *tx).await?.rows_affected();
    let default_memory_sql = if cfg!(feature = "postgres") {
        "UPDATE memory SET memory_kind = $1 WHERE id IN (SELECT default_system_memory_id FROM thread WHERE default_system_memory_id IS NOT NULL) AND (memory_kind IS NULL OR memory_kind != $1)"
    } else {
        "UPDATE memory SET memory_kind = ? WHERE id IN (SELECT default_system_memory_id FROM thread WHERE default_system_memory_id IS NOT NULL) AND (memory_kind IS NULL OR memory_kind != ?)"
    };
    let mut default_memory_update =
        sqlx::query::<Rdb>(default_memory_sql).bind(MemoryKind::Raw as i32);
    if !cfg!(feature = "postgres") {
        default_memory_update = default_memory_update.bind(MemoryKind::Raw as i32);
    }
    updated_memories += default_memory_update
        .execute(&mut *tx)
        .await?
        .rows_affected();
    for movement in &preflight.external_id_moves {
        let sql = if cfg!(feature = "postgres") {
            "UPDATE memory SET external_id = $1 WHERE id = $2 AND external_id = $3"
        } else {
            "UPDATE memory SET external_id = ? WHERE id = ? AND external_id = ?"
        };
        let updated = sqlx::query::<Rdb>(sql)
            .bind(&movement.new_external_id)
            .bind(movement.memory_id)
            .bind(&movement.old_external_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        updated_memories += require_single_external_id_update(updated, movement.memory_id)?;
    }
    let mut reflection_moves = Vec::new();
    for thread in &preflight.threads {
        if thread.classification != "reflection_split_required" {
            continue;
        }
        let clone_sql = format!(
            "SELECT description, channel, embedding, embedding_dim, {THREAD_METADATA_EXPR} AS metadata, created_at, updated_at FROM thread t WHERE id = {P1}"
        );
        let old = sqlx::query_as::<Rdb, ThreadCloneRow>(sqlx::AssertSqlSafe(clone_sql))
            .bind(thread.thread_id)
            .fetch_one(&mut *tx)
            .await?;
        for group in &thread.reflection_split_owners {
            let labels = compose_aggregate_labels(&thread.labels);
            let labels_hash = sha256_join_pipe(&labels);
            let existing_key = sqlx::query_scalar::<Rdb, i64>(AGGREGATE_KEY_LOOKUP_SQL)
                .bind(group.origin_user_id)
                .bind(&labels_hash)
                .fetch_optional(&mut *tx)
                .await?;
            if aggregate_key_conflicts(existing_key, thread.thread_id) {
                bail!(
                    "aggregate key collision for reflection owner {}",
                    group.origin_user_id
                );
            }
            if existing_key == Some(thread.thread_id) {
                let sql = if cfg!(feature = "postgres") {
                    "DELETE FROM thread_aggregate_key WHERE user_id = $1 AND labels_hash = $2 AND thread_id = $3"
                } else {
                    "DELETE FROM thread_aggregate_key WHERE user_id = ? AND labels_hash = ? AND thread_id = ?"
                };
                sqlx::query::<Rdb>(sql)
                    .bind(group.origin_user_id)
                    .bind(&labels_hash)
                    .bind(thread.thread_id)
                    .execute(&mut *tx)
                    .await?;
            }
            let mut memory_timestamps = Vec::with_capacity(group.memories.len());
            for memory in &group.memories {
                let sql = format!(
                    "SELECT user_id, created_at, updated_at, external_id FROM memory WHERE id = {P1}"
                );
                let (author_user_id, created_at, updated_at, external_id) =
                    sqlx::query_as::<Rdb, (i64, i64, i64, Option<String>)>(sqlx::AssertSqlSafe(
                        sql,
                    ))
                    .bind(memory.memory_id)
                    .fetch_one(&mut *tx)
                    .await?;
                memory_timestamps.push(ReflectionMemoryTimestampAudit {
                    memory_id: memory.memory_id,
                    expected_user_id: if is_server_legacy_owner(
                        MemoryKind::Reflection as i32,
                        author_user_id,
                    ) {
                        group.origin_user_id
                    } else {
                        author_user_id
                    },
                    created_at,
                    updated_at,
                    external_id,
                });
            }
            let new_updated_at = replacement_updated_at(
                old.updated_at,
                memory_timestamps.iter().map(|memory| memory.updated_at),
            );
            let new_thread_id = id_generator.generate_id()?;
            sqlx::query::<Rdb>(reflection_thread_insert_sql())
                .bind(new_thread_id)
                // Aggregate reflection threads do not carry a default system
                // memory. Copying one across owner splits would violate the
                // thread/memory kind invariant.
                .bind(reflection_default_system_memory_id())
                .bind(group.origin_user_id)
                .bind(&old.description)
                .bind(&old.channel)
                .bind(&old.embedding)
                .bind(old.embedding_dim)
                .bind(old.created_at)
                .bind(new_updated_at)
                .bind(&old.metadata)
                .bind(MemoryKind::Reflection as i32)
                .execute(&mut *tx)
                .await?;
            for label in &labels {
                let sql = if cfg!(feature = "postgres") {
                    "INSERT INTO thread_label (thread_id, label, created_at) VALUES ($1,$2,$3)"
                } else {
                    "INSERT INTO thread_label (thread_id, label, created_at) VALUES (?,?,?)"
                };
                sqlx::query::<Rdb>(sql)
                    .bind(new_thread_id)
                    .bind(label)
                    .bind(old.created_at)
                    .execute(&mut *tx)
                    .await?;
            }
            // Reflection always has a legacy synthetic owner (300_000), so this never falls through to None.
            let reflection_legacy_owner =
                server_legacy_generated_owner(MemoryKind::Reflection as i32)
                    .expect("Reflection kind always has a legacy generated owner");
            for memory in &group.memories {
                let sql = if cfg!(feature = "postgres") {
                    "UPDATE memory SET memory_kind = $1, user_id = CASE WHEN user_id = $2 THEN $3 ELSE user_id END WHERE id = $4"
                } else {
                    "UPDATE memory SET memory_kind = ?, user_id = CASE WHEN user_id = ? THEN ? ELSE user_id END WHERE id = ?"
                };
                updated_memories += sqlx::query::<Rdb>(sql)
                    .bind(MemoryKind::Reflection as i32)
                    .bind(reflection_legacy_owner)
                    .bind(group.origin_user_id)
                    .bind(memory.memory_id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
                memory_targets.insert(
                    memory.memory_id,
                    (group.origin_user_id, MemoryKind::Reflection as i32),
                );
                let sql = if cfg!(feature = "postgres") {
                    "UPDATE thread_reflection_index SET thread_id = $1 WHERE memory_id = $2"
                } else {
                    "UPDATE thread_reflection_index SET thread_id = ? WHERE memory_id = ?"
                };
                sqlx::query::<Rdb>(sql)
                    .bind(new_thread_id)
                    .bind(memory.memory_id)
                    .execute(&mut *tx)
                    .await?;
                let sql = if cfg!(feature = "postgres") {
                    "INSERT INTO thread_memory (thread_id, memory_id, position, created_at) SELECT $1, memory_id, position, created_at FROM thread_memory WHERE thread_id = $2 AND memory_id = $3"
                } else {
                    "INSERT INTO thread_memory (thread_id, memory_id, position, created_at) SELECT ?, memory_id, position, created_at FROM thread_memory WHERE thread_id = ? AND memory_id = ?"
                };
                sqlx::query::<Rdb>(sql)
                    .bind(new_thread_id)
                    .bind(thread.thread_id)
                    .bind(memory.memory_id)
                    .execute(&mut *tx)
                    .await?;
            }
            let sql = if cfg!(feature = "postgres") {
                "INSERT INTO thread_aggregate_key (user_id, labels_hash, thread_id, created_at) VALUES ($1,$2,$3,$4)"
            } else {
                "INSERT INTO thread_aggregate_key (user_id, labels_hash, thread_id, created_at) VALUES (?,?,?,?)"
            };
            sqlx::query::<Rdb>(sql)
                .bind(group.origin_user_id)
                .bind(&labels_hash)
                .bind(new_thread_id)
                .bind(old.created_at)
                .execute(&mut *tx)
                .await?;
            reflection_moves.push(ReflectionMoveAudit {
                old_thread_id: thread.thread_id,
                new_thread_id,
                origin_user_id: group.origin_user_id,
                labels,
                labels_hash,
                description: old.description.clone(),
                channel: old.channel.clone(),
                metadata: old.metadata.clone(),
                created_at: old.created_at,
                updated_at: new_updated_at,
                memories: group.memories.clone(),
                memory_timestamps,
            });
        }
        let sql = if cfg!(feature = "postgres") {
            "DELETE FROM thread_memory WHERE thread_id = $1"
        } else {
            "DELETE FROM thread_memory WHERE thread_id = ?"
        };
        sqlx::query::<Rdb>(sql)
            .bind(thread.thread_id)
            .execute(&mut *tx)
            .await?;
        let sql = if cfg!(feature = "postgres") {
            "DELETE FROM thread_label WHERE thread_id = $1"
        } else {
            "DELETE FROM thread_label WHERE thread_id = ?"
        };
        sqlx::query::<Rdb>(sql)
            .bind(thread.thread_id)
            .execute(&mut *tx)
            .await?;
        let sql = if cfg!(feature = "postgres") {
            "DELETE FROM thread_aggregate_key WHERE thread_id = $1"
        } else {
            "DELETE FROM thread_aggregate_key WHERE thread_id = ?"
        };
        sqlx::query::<Rdb>(sql)
            .bind(thread.thread_id)
            .execute(&mut *tx)
            .await?;
        let sql = if cfg!(feature = "postgres") {
            "DELETE FROM thread WHERE id = $1"
        } else {
            "DELETE FROM thread WHERE id = ?"
        };
        sqlx::query::<Rdb>(sql)
            .bind(thread.thread_id)
            .execute(&mut *tx)
            .await?;
    }
    let replaced_thread_ids = reflection_moves
        .iter()
        .map(|movement| movement.old_thread_id)
        .collect::<BTreeSet<_>>();
    let initial_thread_ids = threads.iter().map(|thread| thread.id).collect();
    let initial_memory_ids = memories.iter().map(|memory| memory.id).collect();
    let mut memory_audit = memory_ownership_audit(memories, &memory_targets);
    let external_id_moves_by_memory: BTreeMap<i64, &ExternalIdMoveAudit> = preflight
        .external_id_moves
        .iter()
        .map(|movement| (movement.memory_id, movement))
        .collect();
    for row in &mut memory_audit {
        if let Some(movement) = external_id_moves_by_memory.get(&row.id) {
            row.external_id = Some(movement.new_external_id.clone());
        }
    }
    let audit = ApplyAudit {
        format_version: 8,
        mapping_sha256: preflight.mapping_sha256,
        initial_thread_ids,
        initial_memory_ids,
        threads: thread_ownership_audit(
            threads
                .into_iter()
                .filter(|row| !replaced_thread_ids.contains(&row.id))
                .collect(),
            &thread_targets,
        ),
        memories: memory_audit,
        reflection_moves,
        external_id_moves: preflight.external_id_moves,
        updated_threads,
        updated_memories,
    };
    let json = serde_json::to_vec_pretty(&audit)?;
    audit_file
        .write_all(&json)
        .context("write apply audit before migration commit")?;
    audit_file
        .sync_all()
        .context("sync apply audit before migration commit")?;
    tx.commit().await.map_err(|error| {
        anyhow::Error::new(CommitOutcomeUnknown).context(format!(
            "commit memory kind migration failed after the apply audit was synced: {error}"
        ))
    })?;
    Ok(audit)
}

async fn verify_apply_audit(audit: &ApplyAudit) -> Result<VerifyAudit> {
    if !matches!(audit.format_version, 7 | 8)
        || audit.mapping_sha256.len() != 64
        || !audit.mapping_sha256.bytes().all(|b| b.is_ascii_hexdigit())
    {
        bail!("invalid apply audit format");
    }
    let pool = rdb_pool_by_env().await?;
    let mut tx = pool.begin().await?;
    configure_preflight_snapshot(&mut tx).await?;
    let mut failures = Vec::new();
    for row in &audit.threads {
        let sql = format!(
            "SELECT created_at, updated_at, user_id, memory_kind, NULL AS external_id FROM thread WHERE id = {P1}"
        );
        match sqlx::query_as::<Rdb, OwnershipRow>(sqlx::AssertSqlSafe(sql))
            .bind(row.id)
            .fetch_optional(&mut *tx)
            .await?
        {
            Some(actual)
                if actual.created_at == row.created_at
                    && actual.updated_at == row.updated_at
                    && actual.user_id == row.expected_user_id
                    && actual.memory_kind == Some(row.expected_memory_kind)
                    && actual.external_id == row.external_id => {}
            _ => failures.push(VerifyFailure {
                check: "thread_creator_kind_or_immutable".into(),
                id: Some(row.id),
                detail: "missing or timestamp changed".into(),
            }),
        }
    }
    for row in &audit.memories {
        let sql = format!(
            "SELECT created_at, updated_at, user_id, memory_kind, external_id FROM memory WHERE id = {P1}"
        );
        match sqlx::query_as::<Rdb, OwnershipRow>(sqlx::AssertSqlSafe(sql))
            .bind(row.id)
            .fetch_optional(&mut *tx)
            .await?
        {
            Some(actual)
                if actual.created_at == row.created_at
                    && actual.updated_at == row.updated_at
                    && actual.user_id == row.expected_user_id
                    && actual.memory_kind == Some(row.expected_memory_kind)
                    && actual.external_id == row.external_id => {}
            _ => failures.push(VerifyFailure {
                check: "memory_owner_kind_or_immutable".into(),
                id: Some(row.id),
                detail: "missing or timestamp changed".into(),
            }),
        }
    }
    for movement in &audit.external_id_moves {
        let old_sql = format!("SELECT COUNT(*) FROM memory WHERE external_id = {P1}");
        let old_count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(old_sql))
            .bind(&movement.old_external_id)
            .fetch_one(&mut *tx)
            .await?;
        if old_count != 0 {
            failures.push(VerifyFailure {
                check: "legacy_external_id_removed".into(),
                id: Some(movement.memory_id),
                detail: "legacy external ID remains".into(),
            });
        }
    }
    for movement in &audit.reflection_moves {
        let old_sql = format!("SELECT COUNT(*) FROM thread WHERE id = {P1}");
        let old_exists: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(old_sql))
            .bind(movement.old_thread_id)
            .fetch_one(&mut *tx)
            .await?;
        if old_exists != 0 {
            failures.push(VerifyFailure {
                check: "old_reflection_thread_removed".into(),
                id: Some(movement.old_thread_id),
                detail: "old thread remains".into(),
            });
        }
        let key = sqlx::query_scalar::<Rdb, i64>(AGGREGATE_KEY_LOOKUP_SQL)
            .bind(movement.origin_user_id)
            .bind(&movement.labels_hash)
            .fetch_optional(&mut *tx)
            .await?;
        if key != Some(movement.new_thread_id) {
            failures.push(VerifyFailure {
                check: "aggregate_key".into(),
                id: Some(movement.new_thread_id),
                detail: "missing or points elsewhere".into(),
            });
        }
        let thread_sql = format!(
            "SELECT default_system_memory_id, user_id, memory_kind, description, channel, {THREAD_METADATA_EXPR} AS metadata, created_at, updated_at FROM thread t WHERE id = {P1}"
        );
        let target = sqlx::query_as::<
            Rdb,
            (
                Option<i64>,
                i64,
                Option<i32>,
                Option<String>,
                Option<String>,
                Option<String>,
                i64,
                i64,
            ),
        >(sqlx::AssertSqlSafe(thread_sql))
        .bind(movement.new_thread_id)
        .fetch_optional(&mut *tx)
        .await?;
        if target
            != Some((
                reflection_default_system_memory_id(),
                movement.origin_user_id,
                Some(MemoryKind::Reflection as i32),
                movement.description.clone(),
                movement.channel.clone(),
                movement.metadata.clone(),
                movement.created_at,
                movement.updated_at,
            ))
        {
            failures.push(VerifyFailure {
                check: "reflection_thread_attributes".into(),
                id: Some(movement.new_thread_id),
                detail: "default memory, creator, kind, copied attributes, or timestamps mismatch"
                    .into(),
            });
        }
        let labels_sql =
            format!("SELECT label FROM thread_label WHERE thread_id = {P1} ORDER BY label");
        let labels = sqlx::query_scalar::<Rdb, String>(sqlx::AssertSqlSafe(labels_sql))
            .bind(movement.new_thread_id)
            .fetch_all(&mut *tx)
            .await?;
        if labels != movement.labels {
            failures.push(VerifyFailure {
                check: "reflection_thread_labels".into(),
                id: Some(movement.new_thread_id),
                detail: "labels mismatch".into(),
            });
        }
        for memory in &movement.memories {
            let membership = sqlx::query_as::<Rdb, (i32, i64)>(REFLECTION_MEMBERSHIP_SQL)
                .bind(movement.new_thread_id)
                .bind(memory.memory_id)
                .bind(movement.new_thread_id)
                .fetch_optional(&mut *tx)
                .await?;
            if !reflection_membership_matches(membership, memory.position, movement.origin_user_id)
            {
                failures.push(VerifyFailure {
                    check: "reflection_membership".into(),
                    id: Some(memory.memory_id),
                    detail: "sidecar origin user or position mismatch".into(),
                });
            }
            let expected_timestamp = movement
                .memory_timestamps
                .iter()
                .find(|timestamp| timestamp.memory_id == memory.memory_id);
            let memory_sql = format!(
                "SELECT user_id, memory_kind, created_at, updated_at, external_id FROM memory WHERE id = {P1}"
            );
            let target = sqlx::query_as::<Rdb, (i64, Option<i32>, i64, i64, Option<String>)>(
                sqlx::AssertSqlSafe(memory_sql),
            )
            .bind(memory.memory_id)
            .fetch_optional(&mut *tx)
            .await?;
            if target
                != expected_timestamp.map(|timestamp| {
                    (
                        timestamp.expected_user_id,
                        Some(MemoryKind::Reflection as i32),
                        timestamp.created_at,
                        timestamp.updated_at,
                        timestamp.external_id.clone(),
                    )
                })
            {
                failures.push(VerifyFailure {
                    check: "reflection_memory_owner_kind_or_immutable".into(),
                    id: Some(memory.memory_id),
                    detail: "owner, kind, or timestamps mismatch".into(),
                });
            }
        }
    }
    let actual_thread_ids = sqlx::query_scalar::<Rdb, i64>("SELECT id FROM thread ORDER BY id")
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected_thread_ids =
        expected_thread_ids_after_moves(&audit.initial_thread_ids, &audit.reflection_moves);
    if actual_thread_ids != expected_thread_ids {
        failures.push(VerifyFailure {
            check: "thread_id_set".into(),
            id: None,
            detail: format!(
                "expected {} thread IDs, found {}",
                expected_thread_ids.len(),
                actual_thread_ids.len()
            ),
        });
    }
    let actual_memory_ids = sqlx::query_scalar::<Rdb, i64>("SELECT id FROM memory ORDER BY id")
        .fetch_all(&mut *tx)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    let expected_memory_ids = audit
        .initial_memory_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if actual_memory_ids != expected_memory_ids {
        failures.push(VerifyFailure {
            check: "memory_id_set".into(),
            id: None,
            detail: format!(
                "expected {} memory IDs, found {}",
                expected_memory_ids.len(),
                actual_memory_ids.len()
            ),
        });
    }
    tx.commit().await?;
    Ok(VerifyAudit {
        format_version: 5,
        mapping_sha256: audit.mapping_sha256.clone(),
        verified_thread_ids: audit.threads.iter().map(|r| r.id).collect(),
        verified_memory_ids: audit.memories.iter().map(|r| r.id).collect(),
        failures,
    })
}

fn client_warning(
    entity: &str,
    id: Option<i64>,
    check: &str,
    detail: impl Into<String>,
) -> ClientApplyWarning {
    ClientApplyWarning {
        entity: entity.to_string(),
        id,
        check: check.to_string(),
        detail: detail.into(),
    }
}

async fn update_client_external_id(pool: &RdbPool, movement: &ExternalIdMoveAudit) -> Result<u64> {
    let sql = if cfg!(feature = "postgres") {
        "UPDATE memory SET external_id = $1 WHERE id = $2 AND external_id = $3"
    } else {
        "UPDATE memory SET external_id = ? WHERE id = ? AND external_id = ?"
    };
    Ok(sqlx::query::<Rdb>(sql)
        .bind(&movement.new_external_id)
        .bind(movement.memory_id)
        .bind(&movement.old_external_id)
        .execute(pool)
        .await?
        .rows_affected())
}

/// Replaces one legacy reflection aggregate using the same persisted shape as
/// the server migration. The caller owns the transaction so client mode can
/// roll back only this aggregate when its data is malformed.
async fn replace_reflection_aggregate(
    tx: &mut RdbTransaction<'_>,
    id_generator: &IdGeneratorWrapper,
    thread: &ThreadAudit,
) -> Result<(Vec<ReflectionMoveAudit>, u64)> {
    let clone_sql = format!(
        "SELECT description, channel, embedding, embedding_dim, {THREAD_METADATA_EXPR} AS metadata, created_at, updated_at FROM thread t WHERE id = {P1}"
    );
    let old = sqlx::query_as::<Rdb, ThreadCloneRow>(sqlx::AssertSqlSafe(clone_sql))
        .bind(thread.thread_id)
        .fetch_one(&mut **tx)
        .await?;
    let mut moves = Vec::new();
    let mut updated_memories = 0;
    for group in &thread.reflection_split_owners {
        let labels = compose_aggregate_labels(&thread.labels);
        let labels_hash = sha256_join_pipe(&labels);
        let existing = sqlx::query_scalar::<Rdb, i64>(AGGREGATE_KEY_LOOKUP_SQL)
            .bind(group.origin_user_id)
            .bind(&labels_hash)
            .fetch_optional(&mut **tx)
            .await?;
        if aggregate_key_conflicts(existing, thread.thread_id) {
            bail!(
                "aggregate key collision for reflection owner {}",
                group.origin_user_id
            );
        }
        if existing == Some(thread.thread_id) {
            let sql = if cfg!(feature = "postgres") {
                "DELETE FROM thread_aggregate_key WHERE user_id = $1 AND labels_hash = $2 AND thread_id = $3"
            } else {
                "DELETE FROM thread_aggregate_key WHERE user_id = ? AND labels_hash = ? AND thread_id = ?"
            };
            sqlx::query::<Rdb>(sql)
                .bind(group.origin_user_id)
                .bind(&labels_hash)
                .bind(thread.thread_id)
                .execute(&mut **tx)
                .await?;
        }
        let mut timestamps = Vec::new();
        for memory in &group.memories {
            let sql = format!(
                "SELECT user_id, created_at, updated_at, external_id FROM memory WHERE id = {P1}"
            );
            let (author_user_id, created_at, updated_at, external_id) =
                sqlx::query_as::<Rdb, (i64, i64, i64, Option<String>)>(sqlx::AssertSqlSafe(sql))
                    .bind(memory.memory_id)
                    .fetch_one(&mut **tx)
                    .await?;
            timestamps.push(ReflectionMemoryTimestampAudit {
                memory_id: memory.memory_id,
                expected_user_id: if client_uses_generated_owner_range(author_user_id) {
                    group.origin_user_id
                } else {
                    author_user_id
                },
                created_at,
                updated_at,
                external_id,
            });
        }
        let updated_at = replacement_updated_at(
            old.updated_at,
            timestamps.iter().map(|memory| memory.updated_at),
        );
        let new_thread_id = id_generator.generate_id()?;
        sqlx::query::<Rdb>(reflection_thread_insert_sql())
            .bind(new_thread_id)
            .bind(reflection_default_system_memory_id())
            .bind(group.origin_user_id)
            .bind(&old.description)
            .bind(&old.channel)
            .bind(&old.embedding)
            .bind(old.embedding_dim)
            .bind(old.created_at)
            .bind(updated_at)
            .bind(&old.metadata)
            .bind(MemoryKind::Reflection as i32)
            .execute(&mut **tx)
            .await?;
        for label in &labels {
            let sql = if cfg!(feature = "postgres") {
                "INSERT INTO thread_label (thread_id, label, created_at) VALUES ($1,$2,$3)"
            } else {
                "INSERT INTO thread_label (thread_id, label, created_at) VALUES (?,?,?)"
            };
            sqlx::query::<Rdb>(sql)
                .bind(new_thread_id)
                .bind(label)
                .bind(old.created_at)
                .execute(&mut **tx)
                .await?;
        }
        let kind_sql = if cfg!(feature = "postgres") {
            "UPDATE memory SET memory_kind = $1, user_id = CASE WHEN user_id >= $2 THEN $3 ELSE user_id END WHERE id = $4"
        } else {
            "UPDATE memory SET memory_kind = ?, user_id = CASE WHEN user_id >= ? THEN ? ELSE user_id END WHERE id = ?"
        };
        for memory in &group.memories {
            updated_memories += sqlx::query::<Rdb>(kind_sql)
                .bind(MemoryKind::Reflection as i32)
                .bind(GENERATED_USER_ID_MIN)
                .bind(group.origin_user_id)
                .bind(memory.memory_id)
                .execute(&mut **tx)
                .await?
                .rows_affected();
            let index_sql = if cfg!(feature = "postgres") {
                "UPDATE thread_reflection_index SET thread_id = $1 WHERE memory_id = $2"
            } else {
                "UPDATE thread_reflection_index SET thread_id = ? WHERE memory_id = ?"
            };
            sqlx::query::<Rdb>(index_sql)
                .bind(new_thread_id)
                .bind(memory.memory_id)
                .execute(&mut **tx)
                .await?;
            let member_sql = if cfg!(feature = "postgres") {
                "INSERT INTO thread_memory (thread_id, memory_id, position, created_at) SELECT $1, memory_id, position, created_at FROM thread_memory WHERE thread_id = $2 AND memory_id = $3"
            } else {
                "INSERT INTO thread_memory (thread_id, memory_id, position, created_at) SELECT ?, memory_id, position, created_at FROM thread_memory WHERE thread_id = ? AND memory_id = ?"
            };
            sqlx::query::<Rdb>(member_sql)
                .bind(new_thread_id)
                .bind(thread.thread_id)
                .bind(memory.memory_id)
                .execute(&mut **tx)
                .await?;
        }
        let key_sql = if cfg!(feature = "postgres") {
            "INSERT INTO thread_aggregate_key (user_id, labels_hash, thread_id, created_at) VALUES ($1,$2,$3,$4)"
        } else {
            "INSERT INTO thread_aggregate_key (user_id, labels_hash, thread_id, created_at) VALUES (?,?,?,?)"
        };
        sqlx::query::<Rdb>(key_sql)
            .bind(group.origin_user_id)
            .bind(&labels_hash)
            .bind(new_thread_id)
            .bind(old.created_at)
            .execute(&mut **tx)
            .await?;
        moves.push(ReflectionMoveAudit {
            old_thread_id: thread.thread_id,
            new_thread_id,
            origin_user_id: group.origin_user_id,
            labels,
            labels_hash,
            description: old.description.clone(),
            channel: old.channel.clone(),
            metadata: old.metadata.clone(),
            created_at: old.created_at,
            updated_at,
            memories: group.memories.clone(),
            memory_timestamps: timestamps,
        });
    }
    for table in ["thread_memory", "thread_label", "thread_aggregate_key"] {
        let sql = if cfg!(feature = "postgres") {
            format!("DELETE FROM {table} WHERE thread_id = $1")
        } else {
            format!("DELETE FROM {table} WHERE thread_id = ?")
        };
        sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(thread.thread_id)
            .execute(&mut **tx)
            .await?;
    }
    let sql = if cfg!(feature = "postgres") {
        "DELETE FROM thread WHERE id = $1"
    } else {
        "DELETE FROM thread WHERE id = ?"
    };
    sqlx::query::<Rdb>(sql)
        .bind(thread.thread_id)
        .execute(&mut **tx)
        .await?;
    Ok((moves, updated_memories))
}

/// Applies the server's preflight decisions with independent writes.  A
/// malformed legacy row is recorded and skipped, while unrelated resolved
/// rows keep the server-compatible transformation.
async fn apply_client_mapping_with_pool(
    pool: &RdbPool,
    mapping_bytes: &[u8],
    journal: Option<&mut std::fs::File>,
) -> Result<ClientApplyAudit> {
    let preflight = {
        let mut tx = pool
            .begin()
            .await
            .context("begin client migration preflight")?;
        configure_preflight_snapshot(&mut tx).await?;
        ensure_apply_schema(&mut tx).await?;
        let audit = build_preflight_audit_in_tx(mapping_bytes, &mut tx).await?;
        tx.commit()
            .await
            .context("finish client migration preflight")?;
        audit
    };
    let mut audit = ClientApplyAudit {
        format_version: 2,
        status: "in_progress".to_string(),
        mapping_sha256: preflight.mapping_sha256,
        updated_threads: 0,
        updated_thread_owners: 0,
        updated_memories: 0,
        updated_external_ids: 0,
        retained_threads: 0,
        retained_memories: 0,
        raw_threads: 0,
        raw_memories: 0,
        external_id_moves: Vec::new(),
        reflection_moves: Vec::new(),
        warnings: Vec::new(),
        failures: Vec::new(),
    };
    let unresolved_threads = preflight
        .unresolved_thread_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let unresolved_memories = preflight
        .unresolved_memory_ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut unavailable_thread_targets = unresolved_threads.clone();
    for id in &preflight.unresolved_thread_ids {
        audit.warnings.push(client_warning(
            "thread",
            Some(*id),
            "unresolved_preflight",
            "server-compatible classification could not resolve this thread",
        ));
    }
    for id in &preflight.unresolved_memory_ids {
        audit.warnings.push(client_warning(
            "memory",
            Some(*id),
            "unresolved_preflight",
            "server-compatible classification could not resolve this memory",
        ));
    }
    if let Some(file) = journal {
        write_client_audit(file, &audit)
            .context("persist client migration journal before updates")?;
    }
    for thread in &preflight.threads {
        let Some((owner, kind)) = resolved_target(&thread.classification) else {
            continue;
        };
        if unresolved_threads.contains(&thread.thread_id) {
            continue;
        }
        let previous_owner_sql = format!("SELECT user_id FROM thread WHERE id = {P1}");
        let previous_owner =
            match sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(previous_owner_sql))
                .bind(thread.thread_id)
                .fetch_optional(pool)
                .await
            {
                Ok(owner) => owner,
                Err(error) => {
                    unavailable_thread_targets.insert(thread.thread_id);
                    audit.failures.push(ClientApplyFailure {
                        entity: "thread".into(),
                        id: thread.thread_id,
                        detail: format!("read owner before update failed: {error}"),
                    });
                    continue;
                }
            };
        let sql = if cfg!(feature = "postgres") {
            "UPDATE thread SET user_id = $1, memory_kind = $2 WHERE id = $3 AND (user_id != $1 OR memory_kind IS NULL OR memory_kind != $2)"
        } else {
            "UPDATE thread SET user_id = ?, memory_kind = ? WHERE id = ? AND (user_id != ? OR memory_kind IS NULL OR memory_kind != ?)"
        };
        let mut query = sqlx::query::<Rdb>(sql)
            .bind(owner)
            .bind(kind)
            .bind(thread.thread_id);
        if !cfg!(feature = "postgres") {
            query = query.bind(owner).bind(kind);
        }
        match query.execute(pool).await {
            Ok(result) => {
                audit.updated_threads += result.rows_affected();
                if result.rows_affected() == 1 && previous_owner != Some(owner) {
                    audit.updated_thread_owners += 1;
                }
            }
            Err(error) => {
                unavailable_thread_targets.insert(thread.thread_id);
                audit.failures.push(ClientApplyFailure {
                    entity: "thread".into(),
                    id: thread.thread_id,
                    detail: error.to_string(),
                });
            }
        }
        if kind == MemoryKind::Raw as i32 {
            audit.raw_threads += 1;
        }
    }
    let targets = preflight
        .threads
        .iter()
        .filter_map(|thread| {
            (!unavailable_thread_targets.contains(&thread.thread_id))
                .then(|| resolved_target(&thread.classification))
                .flatten()
                .map(|target| (thread.thread_id, target))
        })
        .collect::<BTreeMap<_, _>>();
    let memberships =
        sqlx::query_as::<Rdb, MembershipRow>("SELECT thread_id, memory_id FROM thread_memory")
            .fetch_all(pool)
            .await?;
    let default_memories = sqlx::query_as::<Rdb, IdOwnerRow>(
        "SELECT DISTINCT m.id, m.user_id FROM memory m \
         JOIN thread t ON t.default_system_memory_id = m.id",
    )
    .fetch_all(pool)
    .await?;
    let default_memory_ids = default_memories
        .iter()
        .map(|memory| memory.id)
        .collect::<BTreeSet<_>>();
    let mut memory_targets = BTreeMap::new();
    for membership in &memberships {
        if !default_memory_ids.contains(&membership.memory_id)
            && !unresolved_memories.contains(&membership.memory_id)
            && let Some(target) = targets.get(&membership.thread_id)
        {
            memory_targets.insert(membership.memory_id, *target);
        }
    }
    for memory in default_memories {
        memory_targets.insert(memory.id, (memory.user_id, MemoryKind::Raw as i32));
    }
    let standalone = sqlx::query_as::<Rdb, IdOwnerRow>("SELECT m.id, m.user_id FROM memory m WHERE NOT EXISTS (SELECT 1 FROM thread_memory tm WHERE tm.memory_id = m.id)").fetch_all(pool).await?;
    for memory in standalone {
        memory_targets.insert(memory.id, standalone_memory_target(memory.user_id));
    }
    let client_memory_update_sql = if cfg!(feature = "postgres") {
        format!(
            "UPDATE memory SET memory_kind = $1, user_id = CASE WHEN user_id >= {GENERATED_USER_ID_MIN} THEN $2 ELSE user_id END WHERE id = $3 AND (memory_kind IS NULL OR memory_kind != $1 OR user_id >= {GENERATED_USER_ID_MIN})"
        )
    } else {
        format!(
            "UPDATE memory SET memory_kind = ?, user_id = CASE WHEN user_id >= {GENERATED_USER_ID_MIN} THEN ? ELSE user_id END WHERE id = ? AND (memory_kind IS NULL OR memory_kind != ? OR user_id >= {GENERATED_USER_ID_MIN})"
        )
    };
    for (memory_id, (owner, kind)) in memory_targets {
        if unresolved_memories.contains(&memory_id) {
            continue;
        }
        let mut query = sqlx::query::<Rdb>(sqlx::AssertSqlSafe(client_memory_update_sql.as_str()))
            .bind(kind)
            .bind(owner)
            .bind(memory_id);
        if !cfg!(feature = "postgres") {
            query = query.bind(kind);
        }
        match query.execute(pool).await {
            Ok(result) => audit.updated_memories += result.rows_affected(),
            Err(error) => audit.failures.push(ClientApplyFailure {
                entity: "memory".into(),
                id: memory_id,
                detail: error.to_string(),
            }),
        }
        if kind == MemoryKind::Raw as i32 {
            audit.raw_memories += 1;
        }
    }
    let external_id_sql = format!(
        "SELECT id, external_id, {MEMORY_METADATA_EXPR} AS metadata FROM memory m WHERE external_id IS NOT NULL ORDER BY id"
    );
    let external_id_rows =
        sqlx::query_as::<Rdb, ExternalIdRow>(sqlx::AssertSqlSafe(external_id_sql))
            .fetch_all(pool)
            .await?;
    let (external_id_moves, unresolved_external_id_memory_ids) =
        plan_external_id_moves(&external_id_rows, &memberships, &targets)?;
    for id in &unresolved_external_id_memory_ids {
        audit.warnings.push(client_warning(
            "memory",
            Some(*id),
            "external_id_not_converted",
            "external_id could not be converted with the server-compatible rules",
        ));
    }
    for movement in &external_id_moves {
        match update_client_external_id(pool, movement).await {
            Ok(1) => {
                audit.updated_external_ids += 1;
                audit.external_id_moves.push(movement.clone());
            }
            Ok(_) => audit.warnings.push(client_warning(
                "memory",
                Some(movement.memory_id),
                "external_id_not_updated",
                "external_id changed after the migration read",
            )),
            Err(error) => audit.failures.push(ClientApplyFailure {
                entity: "memory".into(),
                id: movement.memory_id,
                detail: format!("external_id update failed: {error}"),
            }),
        }
    }
    let id_generator = IdGeneratorWrapper::new();
    for thread in &preflight.threads {
        if thread.classification != "reflection_split_required"
            || unavailable_thread_targets.contains(&thread.thread_id)
        {
            continue;
        }
        let mut tx = match pool.begin().await {
            Ok(tx) => tx,
            Err(error) => return Err(error).context("begin client reflection replacement"),
        };
        match replace_reflection_aggregate(&mut tx, &id_generator, thread).await {
            Ok((moves, updated_memories)) => match tx.commit().await {
                Ok(()) => {
                    audit.updated_memories += updated_memories;
                    audit.reflection_moves.extend(moves);
                }
                Err(error) => {
                    return Err(anyhow::Error::new(CommitOutcomeUnknown).context(format!(
                        "client reflection replacement commit failed for thread {}: {error}",
                        thread.thread_id
                    )));
                }
            },
            Err(error) => {
                let _ = tx.rollback().await;
                audit.failures.push(ClientApplyFailure {
                    entity: "reflection_thread".into(),
                    id: thread.thread_id,
                    detail: error.to_string(),
                });
            }
        }
    }
    Ok(audit)
}

async fn apply_client_mapping(
    mapping_bytes: &[u8],
    journal: Option<&mut std::fs::File>,
) -> Result<ClientApplyAudit> {
    let pool = rdb_pool_by_env().await?;
    apply_client_mapping_with_pool(&pool, mapping_bytes, journal).await
}

#[cfg(not(feature = "postgres"))]
async fn prune_client_unresolved_with_pool(
    pool: &RdbPool,
    audit: &ClientApplyAudit,
    output: &std::path::Path,
) -> Result<ClientPruneDump> {
    prune_client_unresolved_with_pool_force(pool, audit, output, false).await
}

async fn prune_client_unresolved_with_pool_force(
    pool: &RdbPool,
    audit: &ClientApplyAudit,
    output: &std::path::Path,
    force: bool,
) -> Result<ClientPruneDump> {
    if audit.status != "completed" || !audit.failures.is_empty() {
        bail!("only a completed client audit without failures can be pruned");
    }
    if audit
        .warnings
        .iter()
        .any(|warning| warning.check != "unresolved_preflight")
    {
        bail!("client audit contains warnings that are not safe to prune");
    }
    let mut memory_ids = BTreeSet::new();
    let mut thread_ids = BTreeSet::new();
    for warning in &audit.warnings {
        let Some(id) = warning.id else {
            bail!("unresolved warning has no id");
        };
        match warning.entity.as_str() {
            "memory" => {
                memory_ids.insert(id);
            }
            "thread" => {
                thread_ids.insert(id);
            }
            entity => bail!("unresolved warning has unsupported entity {entity}"),
        }
    }
    let mut schema_tx = pool
        .begin()
        .await
        .context("begin client unresolved prune schema check")?;
    let unknown_references = unsupported_thread_reference_columns(&mut schema_tx).await?;
    schema_tx.commit().await?;
    if !unknown_references.is_empty() {
        bail!(
            "refusing unresolved prune with unknown thread reference columns: {}",
            unknown_references.join(", ")
        );
    }
    let mut tx = pool
        .begin()
        .await
        .context("begin client unresolved prune")?;
    let memory_ids = memory_ids.into_iter().collect::<Vec<_>>();
    let thread_ids = thread_ids.into_iter().collect::<Vec<_>>();
    for thread_id in &thread_ids {
        let sql = format!("SELECT memory_id FROM thread_reflection_index WHERE thread_id = {P1}");
        let reflection_memory_ids = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .fetch_all(&mut *tx)
            .await?;
        if !force
            && let Some(memory_id) = reflection_memory_ids
                .into_iter()
                .find(|memory_id| !memory_ids.contains(memory_id))
        {
            bail!(
                "refusing unresolved prune: thread {thread_id} contains retained reflection memory {memory_id} via thread_id"
            );
        }
        let sql =
            format!("SELECT memory_id FROM thread_reflection_index WHERE origin_thread_id = {P1}");
        let reflection_memory_ids = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .fetch_all(&mut *tx)
            .await?;
        if !force
            && let Some(memory_id) = reflection_memory_ids
                .into_iter()
                .find(|memory_id| !memory_ids.contains(memory_id))
        {
            bail!(
                "refusing unresolved prune: thread {thread_id} is referenced by retained reflection memory {memory_id} via origin_thread_id"
            );
        }
    }
    for memory_id in &memory_ids {
        let sql = format!(
            "SELECT tm.thread_id FROM thread_memory tm JOIN thread t ON t.id = tm.thread_id WHERE tm.memory_id = {P1}"
        );
        let referencing_thread_ids = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .fetch_all(&mut *tx)
            .await?;
        if !force
            && let Some(thread_id) = referencing_thread_ids
                .into_iter()
                .find(|thread_id| !thread_ids.contains(thread_id))
        {
            bail!(
                "refusing unresolved prune: memory {memory_id} is shared with retained thread {thread_id} via thread_memory"
            );
        }
        let sql = format!(
            "SELECT DISTINCT m.id FROM memory m JOIN json_each(m.parent_ids) AS parent WHERE CAST(parent.value AS INTEGER) = {P1}"
        );
        let referencing_memory_ids = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .fetch_all(&mut *tx)
            .await?;
        if !force
            && let Some(referencing_memory_id) = referencing_memory_ids
                .into_iter()
                .find(|referencing_memory_id| !memory_ids.contains(referencing_memory_id))
        {
            bail!(
                "refusing unresolved prune: memory {memory_id} is referenced by retained memory {referencing_memory_id} via parent_ids"
            );
        }
        let sql = format!("SELECT id FROM thread WHERE default_system_memory_id = {P1}");
        let referencing_thread_ids = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .fetch_all(&mut *tx)
            .await?;
        if !force
            && let Some(thread_id) = referencing_thread_ids
                .into_iter()
                .find(|thread_id| !thread_ids.contains(thread_id))
        {
            bail!(
                "refusing unresolved prune: memory {memory_id} is referenced by retained thread {thread_id} via default_system_memory_id"
            );
        }
        let sql = format!("SELECT memory_id FROM reflection_fact WHERE fact_memory_id = {P1}");
        let referencing_memory_ids = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .fetch_all(&mut *tx)
            .await?;
        if !force
            && let Some(reflection_memory_id) = referencing_memory_ids
                .into_iter()
                .find(|reflection_memory_id| !memory_ids.contains(reflection_memory_id))
        {
            bail!(
                "refusing unresolved prune: memory {memory_id} is referenced by retained reflection memory {reflection_memory_id} via reflection_fact"
            );
        }
        let sql = format!(
            "SELECT memory_id FROM thread_reflection_index WHERE previous_reflection_id = {P1}"
        );
        let referencing_memory_ids = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .fetch_all(&mut *tx)
            .await?;
        if !force
            && let Some(reflection_memory_id) = referencing_memory_ids
                .into_iter()
                .find(|reflection_memory_id| !memory_ids.contains(reflection_memory_id))
        {
            bail!(
                "refusing unresolved prune: memory {memory_id} is referenced by retained reflection memory {reflection_memory_id} via previous_reflection_id"
            );
        }
    }
    let records = client_prune_dump_records(&mut tx, &memory_ids, &thread_ids).await?;
    let dump = ClientPruneDump {
        format_version: 3,
        status: "prepared".into(),
        memory_ids: memory_ids.clone(),
        thread_ids: thread_ids.clone(),
        records,
        deleted_memory_rows: 0,
        deleted_thread_rows: 0,
        deleted_membership_rows: 0,
    };
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)
        .with_context(|| format!("create client prune dump {}", output.display()))?;
    write_client_prune_dump(&mut file, &dump)?;
    let mut deleted_membership_rows = 0;
    let mut deleted_memory_rows = 0;
    let mut deleted_thread_rows = 0;
    for memory_id in &memory_ids {
        if force {
            // Client recovery deliberately preserves unrelated records, so
            // detach their optional references before removing the target.
            let sql = format!(
                "UPDATE thread SET default_system_memory_id = NULL WHERE default_system_memory_id = {P1}"
            );
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
                .bind(memory_id)
                .execute(&mut *tx)
                .await?;
            let sql = format!("DELETE FROM reflection_fact WHERE fact_memory_id = {P1}");
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
                .bind(memory_id)
                .execute(&mut *tx)
                .await?;
            let sql =
                format!("DELETE FROM thread_reflection_index WHERE previous_reflection_id = {P1}");
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
                .bind(memory_id)
                .execute(&mut *tx)
                .await?;
        }
        let sql = format!("DELETE FROM thread_memory WHERE memory_id = {P1}");
        deleted_membership_rows += sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        let sql = format!("DELETE FROM memory_rating WHERE memory_id = {P1}");
        sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .execute(&mut *tx)
            .await?;
        for table in [
            "reflection_failure_mode",
            "reflection_tool",
            "reflection_tool_outcome",
            "reflection_fact",
            "reflection_applied_target",
            "reflection_few_shot_usage",
        ] {
            let sql = format!("DELETE FROM {table} WHERE memory_id = {P1}");
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
                .bind(memory_id)
                .execute(&mut *tx)
                .await?;
        }
        let sql = format!("DELETE FROM thread_reflection_index WHERE memory_id = {P1}");
        sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .execute(&mut *tx)
            .await?;
        // Migration-target client databases do not use media_object, so this
        // destructive compatibility path intentionally does not adjust ref_count.
        let sql = format!("DELETE FROM memory WHERE id = {P1}");
        deleted_memory_rows += sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(memory_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    }
    for thread_id in &thread_ids {
        let sql = format!("DELETE FROM thread_memory WHERE thread_id = {P1}");
        deleted_membership_rows += sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
        for table in ["thread_label", "thread_aggregate_key"] {
            let sql = format!("DELETE FROM {table} WHERE thread_id = {P1}");
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
                .bind(thread_id)
                .execute(&mut *tx)
                .await?;
        }
        let sql = format!(
            "DELETE FROM thread_reflection_index WHERE thread_id = {P1} OR origin_thread_id = {P1}"
        );
        sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .execute(&mut *tx)
            .await?;
        let sql = format!("DELETE FROM reflection_few_shot_usage WHERE used_in_thread_id = {P1}");
        sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .execute(&mut *tx)
            .await?;
        let sql = format!("DELETE FROM thread WHERE id = {P1}");
        deleted_thread_rows += sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .execute(&mut *tx)
            .await?
            .rows_affected();
    }
    let commit_pending_dump = ClientPruneDump {
        status: "commit_pending".into(),
        deleted_memory_rows,
        deleted_thread_rows,
        deleted_membership_rows,
        ..dump
    };
    write_client_prune_dump(&mut file, &commit_pending_dump)
        .context("persist client unresolved prune dump before commit")?;
    tx.commit().await.map_err(|error| {
        anyhow::Error::new(CommitOutcomeUnknown).context(format!(
            "client unresolved prune commit failed after the dump recorded commit_pending; inspect {} before retrying: {error}",
            output.display()
        ))
    })?;
    let dump = ClientPruneDump {
        status: "completed".into(),
        ..commit_pending_dump
    };
    write_client_prune_dump(&mut file, &dump).with_context(|| {
        format!(
            "client unresolved prune committed, but its dump remains commit_pending at {}",
            output.display()
        )
    })?;
    Ok(dump)
}

async fn client_prune_dump_records(
    tx: &mut RdbTransaction<'_>,
    memory_ids: &[i64],
    thread_ids: &[i64],
) -> Result<serde_json::Value> {
    // Client pruning is only supported for the SQLite schema. JSON objects
    // preserve the stored payload before any destructive statement executes.
    #[cfg(feature = "postgres")]
    {
        let _ = (tx, memory_ids, thread_ids);
        bail!("client unresolved prune is SQLite-only");
    }
    #[cfg(not(feature = "postgres"))]
    {
        use sqlx::Row;
        let mut result = serde_json::Map::new();
        for (name, sql, ids) in [
            (
                "memory",
                "SELECT json_object('id',id,'parent_ids',parent_ids,'user_id',user_id,'content',content,'content_type',content_type,'params',params,'metadata',metadata,'created_at',created_at,'updated_at',updated_at,'role',role,'external_id',external_id,'media_object_id',media_object_id,'memory_kind',memory_kind) value FROM memory WHERE id = ?",
                memory_ids,
            ),
            (
                "thread",
                "SELECT json_object('id',id,'default_system_memory_id',default_system_memory_id,'user_id',user_id,'description',description,'channel',channel,'embedding_hex',CASE WHEN embedding IS NULL THEN NULL ELSE hex(embedding) END,'embedding_dim',embedding_dim,'created_at',created_at,'updated_at',updated_at,'metadata',metadata,'memory_kind',memory_kind) value FROM thread WHERE id = ?",
                thread_ids,
            ),
        ] {
            let mut values = Vec::new();
            for id in ids {
                for row in sqlx::query(sql).bind(id).fetch_all(&mut **tx).await? {
                    values.push(serde_json::from_str::<serde_json::Value>(
                        row.try_get::<String, _>("value")?.as_str(),
                    )?);
                }
            }
            result.insert(name.into(), serde_json::Value::Array(values));
        }
        let mut thread_memories = Vec::new();
        for id in memory_ids {
            for row in sqlx::query("SELECT json_object('thread_id',thread_id,'memory_id',memory_id,'position',position,'created_at',created_at) value FROM thread_memory WHERE memory_id = ?")
                .bind(id).fetch_all(&mut **tx).await?
            {
                thread_memories.push(serde_json::from_str::<serde_json::Value>(
                    row.try_get::<String, _>("value")?.as_str(),
                )?);
            }
        }
        for id in thread_ids {
            for row in sqlx::query("SELECT json_object('thread_id',thread_id,'memory_id',memory_id,'position',position,'created_at',created_at) value FROM thread_memory WHERE thread_id = ?")
                .bind(id).fetch_all(&mut **tx).await?
            {
                let value = serde_json::from_str::<serde_json::Value>(
                    row.try_get::<String, _>("value")?.as_str(),
                )?;
                if !thread_memories.contains(&value) {
                    thread_memories.push(value);
                }
            }
        }
        result.insert(
            "thread_memory".into(),
            serde_json::Value::Array(thread_memories),
        );
        for (name, sql, ids) in [
            (
                "thread_label",
                "SELECT json_object('thread_id',thread_id,'label',label,'created_at',created_at) value FROM thread_label WHERE thread_id = ?",
                thread_ids,
            ),
            (
                "thread_aggregate_key",
                "SELECT json_object('user_id',user_id,'labels_hash',labels_hash,'thread_id',thread_id,'created_at',created_at) value FROM thread_aggregate_key WHERE thread_id = ?",
                thread_ids,
            ),
        ] {
            let mut values = Vec::new();
            for id in ids {
                for row in sqlx::query(sql).bind(id).fetch_all(&mut **tx).await? {
                    values.push(serde_json::from_str::<serde_json::Value>(
                        row.try_get::<String, _>("value")?.as_str(),
                    )?);
                }
            }
            result.insert(name.into(), serde_json::Value::Array(values));
        }
        let mut memory_ratings = Vec::new();
        for id in memory_ids {
            for row in sqlx::query("SELECT json_object('id',id,'memory_id',memory_id,'user_id',user_id,'rating',rating,'metadata',metadata,'created_at',created_at,'updated_at',updated_at) value FROM memory_rating WHERE memory_id = ?")
                .bind(id).fetch_all(&mut **tx).await?
            {
                memory_ratings.push(serde_json::from_str::<serde_json::Value>(
                    row.try_get::<String, _>("value")?.as_str(),
                )?);
            }
        }
        result.insert(
            "memory_rating".into(),
            serde_json::Value::Array(memory_ratings),
        );
        for (name, sql) in [
            (
                "reflection_failure_mode",
                "SELECT json_object('memory_id',memory_id,'mode',mode) value FROM reflection_failure_mode WHERE memory_id = ?",
            ),
            (
                "reflection_tool",
                "SELECT json_object('memory_id',memory_id,'tool',tool) value FROM reflection_tool WHERE memory_id = ?",
            ),
            (
                "reflection_tool_outcome",
                "SELECT json_object('memory_id',memory_id,'tool',tool,'contribution',contribution,'error_kind',error_kind) value FROM reflection_tool_outcome WHERE memory_id = ?",
            ),
            (
                "reflection_fact",
                "SELECT json_object('memory_id',memory_id,'fact_memory_id',fact_memory_id,'fact_kind',fact_kind,'turn_index',turn_index,'weight',weight,'note',note,'links_json',links_json) value FROM reflection_fact WHERE memory_id = ?",
            ),
            (
                "reflection_applied_target",
                "SELECT json_object('memory_id',memory_id,'target',target,'mitigation_fingerprint',mitigation_fingerprint,'applied_at',applied_at) value FROM reflection_applied_target WHERE memory_id = ?",
            ),
        ] {
            let mut values = Vec::new();
            for id in memory_ids {
                for row in sqlx::query(sql).bind(id).fetch_all(&mut **tx).await? {
                    values.push(serde_json::from_str::<serde_json::Value>(
                        row.try_get::<String, _>("value")?.as_str(),
                    )?);
                }
            }
            result.insert(name.into(), serde_json::Value::Array(values));
        }
        let reflection_index_value_sql = "SELECT json_object('memory_id',memory_id,'thread_id',thread_id,'origin_thread_id',origin_thread_id,'origin_user_id',origin_user_id,'origin_channel',origin_channel,'outcome',outcome,'score',score,'score_self',score_self,'score_heuristic',score_heuristic,'task_category',task_category,'reflection_aspect',reflection_aspect,'dataset_quality',dataset_quality,'summary_embedding_status',summary_embedding_status,'summary_embedding_error',summary_embedding_error,'intent_embedding_status',intent_embedding_status,'intent_embedding_error',intent_embedding_error,'prompt_version',prompt_version,'target_model_version',target_model_version,'experiment_id',experiment_id,'experiment_variant',experiment_variant,'previous_reflection_id',previous_reflection_id,'pinned',pinned,'is_recurrence',is_recurrence,'mitigation_fingerprint',mitigation_fingerprint,'created_at',created_at,'updated_at',updated_at) value FROM thread_reflection_index";
        let mut reflection_index = Vec::new();
        for id in thread_ids {
            let sql =
                format!("{reflection_index_value_sql} WHERE thread_id = ? OR origin_thread_id = ?");
            for row in sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(id)
                .bind(id)
                .fetch_all(&mut **tx)
                .await?
            {
                let value = serde_json::from_str::<serde_json::Value>(
                    row.try_get::<String, _>("value")?.as_str(),
                )?;
                if !reflection_index.contains(&value) {
                    reflection_index.push(value);
                }
            }
        }
        for id in memory_ids {
            let sql = format!("{reflection_index_value_sql} WHERE memory_id = ?");
            for row in sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(id)
                .fetch_all(&mut **tx)
                .await?
            {
                let value = serde_json::from_str::<serde_json::Value>(
                    row.try_get::<String, _>("value")?.as_str(),
                )?;
                if !reflection_index.contains(&value) {
                    reflection_index.push(value);
                }
            }
        }
        result.insert(
            "thread_reflection_index".into(),
            serde_json::Value::Array(reflection_index),
        );
        let mut few_shot_usage = Vec::new();
        for id in memory_ids {
            for row in sqlx::query("SELECT json_object('memory_id',memory_id,'used_in_thread_id',used_in_thread_id,'used_at',used_at) value FROM reflection_few_shot_usage WHERE memory_id = ?")
                .bind(id).fetch_all(&mut **tx).await?
            {
                few_shot_usage.push(serde_json::from_str::<serde_json::Value>(
                    row.try_get::<String, _>("value")?.as_str(),
                )?);
            }
        }
        for id in thread_ids {
            for row in sqlx::query("SELECT json_object('memory_id',memory_id,'used_in_thread_id',used_in_thread_id,'used_at',used_at) value FROM reflection_few_shot_usage WHERE used_in_thread_id = ?")
                .bind(id).fetch_all(&mut **tx).await?
            {
                let value = serde_json::from_str::<serde_json::Value>(
                    row.try_get::<String, _>("value")?.as_str(),
                )?;
                if !few_shot_usage.contains(&value) {
                    few_shot_usage.push(value);
                }
            }
        }
        result.insert(
            "reflection_few_shot_usage".into(),
            serde_json::Value::Array(few_shot_usage),
        );
        Ok(serde_json::Value::Object(result))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    match cli.command {
        Command::Plan { mapping, output } => {
            let bytes = std::fs::read(&mapping)
                .with_context(|| format!("read mapping {}", mapping.display()))?;
            let audit = build_preflight_audit(&bytes).await?;
            let json = serde_json::to_vec_pretty(&audit)?;
            if output.exists() {
                bail!("refusing to overwrite audit {}", output.display());
            }
            std::fs::write(&output, json)
                .with_context(|| format!("write audit {}", output.display()))?;
            println!(
                "validated mapping; preflight audit written to {}",
                output.display()
            );
            Ok(())
        }
        Command::Apply { mapping, output } => {
            let bytes = std::fs::read(&mapping)
                .with_context(|| format!("read mapping {}", mapping.display()))?;
            let mut audit_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .with_context(|| format!("reserve audit {} before migration", output.display()))?;
            let _audit = match apply_mapping(&bytes, &mut audit_file).await {
                Ok(audit) => audit,
                Err(error) => {
                    drop(audit_file);
                    if should_retain_apply_audit_after_error(
                        error.downcast_ref::<CommitOutcomeUnknown>().is_some(),
                    ) {
                        return Err(error.context(format!(
                            "apply audit {} was retained because the commit outcome is unknown; run verify before retrying",
                            output.display()
                        )));
                    }
                    if let Err(remove_error) = std::fs::remove_file(&output)
                        && remove_error.kind() != io::ErrorKind::NotFound
                    {
                        return Err(error.context(format!(
                            "also failed to remove incomplete audit {}: {remove_error}",
                            output.display()
                        )));
                    }
                    return Err(error);
                }
            };
            println!("migration applied; audit written to {}", output.display());
            Ok(())
        }
        Command::Verify { audit, output } => {
            let input = std::fs::read(&audit)
                .with_context(|| format!("read apply audit {}", audit.display()))?;
            let apply: ApplyAudit =
                serde_json::from_slice(&input).context("invalid apply audit JSON")?;
            let verification = verify_apply_audit(&apply).await?;
            let mut output_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .with_context(|| format!("reserve verification audit {}", output.display()))?;
            output_file
                .write_all(&serde_json::to_vec_pretty(&verification)?)
                .with_context(|| format!("write verification {}", output.display()))?;
            output_file
                .sync_all()
                .with_context(|| format!("sync verification {}", output.display()))?;
            if verification.failures.is_empty() {
                println!("migration verified; audit written to {}", output.display());
                Ok(())
            } else {
                bail!("migration verification failed; see {}", output.display())
            }
        }
        Command::ClientApply { mapping, output } => {
            let mapping_bytes = std::fs::read(&mapping)
                .with_context(|| format!("read mapping {}", mapping.display()))?;
            let mut output_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&output)
                .with_context(|| format!("reserve client migration audit {}", output.display()))?;
            let audit = match apply_client_mapping(&mapping_bytes, Some(&mut output_file)).await {
                Ok(audit) => audit,
                Err(error) => {
                    let journal_bytes_written =
                        output_file.metadata().ok().map(|metadata| metadata.len());
                    drop(output_file);
                    if journal_bytes_written
                        .is_some_and(|bytes| !should_retain_client_journal_after_error(bytes))
                    {
                        if let Err(remove_error) = std::fs::remove_file(&output)
                            && remove_error.kind() != io::ErrorKind::NotFound
                        {
                            return Err(error.context(format!(
                                "client migration failed before its journal was written and empty audit {} could not be removed: {remove_error}",
                                output.display()
                            )));
                        }
                        return Err(error.context("client migration failed before its journal was written; the empty audit reservation was removed"));
                    }
                    return Err(error.context(format!(
                        "client migration journal {} was retained; inspect its in_progress status before retrying",
                        output.display()
                    )));
                }
            };
            drop(output_file);
            let completed = PathBuf::from(format!("{}.completed", output.display()));
            let mut completed_file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&completed)
                .with_context(|| {
                    format!("reserve completed client audit {}", completed.display())
                })?;
            let mut completed_audit = audit;
            completed_audit.status = "completed".to_string();
            if let Err(error) = write_client_audit(&mut completed_file, &completed_audit) {
                return Err(error.context(format!(
                    "write completed client audit {}; in-progress journal {} was retained",
                    completed.display(),
                    output.display()
                )));
            }
            drop(completed_file);
            if let Err(error) = std::fs::rename(&completed, &output) {
                return Err(anyhow::Error::new(error).context(format!(
                    "promote completed client audit {}; journal {} and completed audit were retained",
                    completed.display(), output.display()
                )));
            }
            println!(
                "client migration completed; audit written to {} ({} warnings, {} row failures)",
                output.display(),
                completed_audit.warnings.len(),
                completed_audit.failures.len()
            );
            Ok(())
        }
        Command::ClientPruneUnresolved {
            audit,
            output,
            force,
        } => {
            let bytes = std::fs::read(&audit)
                .with_context(|| format!("read client audit {}", audit.display()))?;
            let audit: ClientApplyAudit =
                serde_json::from_slice(&bytes).context("invalid client audit JSON")?;
            let dump = prune_client_unresolved_with_pool_force(
                &rdb_pool_by_env().await?,
                &audit,
                &output,
                force,
            )
            .await?;
            println!(
                "client unresolved records pruned; dump written to {} ({} memories, {} threads, {} memberships removed)",
                output.display(),
                dump.deleted_memory_rows,
                dump.deleted_thread_rows,
                dump.deleted_membership_rows,
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(feature = "postgres"))]
    use sqlx::Row;

    #[cfg(not(feature = "postgres"))]
    async fn client_fixture_pool() -> sqlx::SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "CREATE TABLE thread (id INTEGER PRIMARY KEY, default_system_memory_id INTEGER, user_id INTEGER NOT NULL, description TEXT, channel TEXT, embedding BLOB, embedding_dim INTEGER, created_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0, metadata TEXT, memory_kind INTEGER); \
             CREATE TABLE memory (id INTEGER PRIMARY KEY, parent_ids TEXT, user_id INTEGER NOT NULL DEFAULT 0, content TEXT NOT NULL DEFAULT '', content_type INTEGER NOT NULL DEFAULT 0, params TEXT, metadata TEXT, created_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0, role INTEGER NOT NULL DEFAULT 0, external_id TEXT, media_object_id INTEGER, memory_kind INTEGER); \
             CREATE TABLE thread_memory (thread_id INTEGER NOT NULL, memory_id INTEGER NOT NULL, position INTEGER NOT NULL DEFAULT 0, created_at INTEGER NOT NULL DEFAULT 0); \
             CREATE TABLE thread_label (thread_id INTEGER NOT NULL, label TEXT NOT NULL, created_at INTEGER NOT NULL DEFAULT 0); \
             CREATE TABLE thread_reflection_index (memory_id INTEGER NOT NULL PRIMARY KEY, thread_id INTEGER NOT NULL, origin_thread_id INTEGER NOT NULL, origin_user_id INTEGER NOT NULL, origin_channel TEXT, outcome INTEGER NOT NULL DEFAULT 0, score REAL NOT NULL DEFAULT 0, score_self REAL NOT NULL DEFAULT 0, score_heuristic REAL NOT NULL DEFAULT 0, task_category INTEGER NOT NULL DEFAULT 0, reflection_aspect INTEGER NOT NULL DEFAULT 0, dataset_quality INTEGER NOT NULL DEFAULT 1, summary_embedding_status INTEGER NOT NULL DEFAULT 1, summary_embedding_error TEXT, intent_embedding_status INTEGER NOT NULL DEFAULT 1, intent_embedding_error TEXT, prompt_version TEXT NOT NULL DEFAULT '', target_model_version TEXT, experiment_id TEXT, experiment_variant TEXT, previous_reflection_id INTEGER, pinned INTEGER NOT NULL DEFAULT 0, is_recurrence INTEGER NOT NULL DEFAULT 0, mitigation_fingerprint TEXT, created_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0); \
             CREATE TABLE thread_aggregate_key (user_id INTEGER NOT NULL, labels_hash TEXT NOT NULL, thread_id INTEGER NOT NULL, created_at INTEGER NOT NULL DEFAULT 0); \
             CREATE TABLE reflection_failure_mode (memory_id INTEGER NOT NULL, mode TEXT NOT NULL); \
             CREATE TABLE reflection_tool (memory_id INTEGER NOT NULL, tool TEXT NOT NULL); \
             CREATE TABLE reflection_tool_outcome (memory_id INTEGER NOT NULL, tool TEXT NOT NULL, contribution INTEGER NOT NULL, error_kind TEXT NOT NULL); \
             CREATE TABLE reflection_fact (memory_id INTEGER NOT NULL, fact_memory_id INTEGER NOT NULL, fact_kind INTEGER NOT NULL, turn_index INTEGER NOT NULL, weight REAL, note TEXT, links_json TEXT); \
             CREATE TABLE reflection_applied_target (memory_id INTEGER NOT NULL, target TEXT NOT NULL, mitigation_fingerprint TEXT, applied_at INTEGER NOT NULL); \
             CREATE TABLE reflection_few_shot_usage (memory_id INTEGER NOT NULL, used_in_thread_id INTEGER NOT NULL, used_at INTEGER NOT NULL); \
             CREATE TABLE memory_rating (id INTEGER PRIMARY KEY, memory_id INTEGER NOT NULL, user_id INTEGER NOT NULL, rating REAL NOT NULL, metadata TEXT, created_at INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL DEFAULT 0);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[cfg(not(feature = "postgres"))]
    fn completed_unresolved_memory_audit(memory_id: i64) -> ClientApplyAudit {
        ClientApplyAudit {
            format_version: 2,
            status: "completed".into(),
            mapping_sha256: String::new(),
            updated_threads: 0,
            updated_thread_owners: 0,
            updated_memories: 0,
            updated_external_ids: 0,
            retained_threads: 0,
            retained_memories: 0,
            raw_threads: 0,
            raw_memories: 0,
            external_id_moves: Vec::new(),
            reflection_moves: Vec::new(),
            warnings: vec![client_warning(
                "memory",
                Some(memory_id),
                "unresolved_preflight",
                "test unresolved memory",
            )],
            failures: Vec::new(),
        }
    }

    #[test]
    fn client_prune_dump_serializes_commit_pending_with_counts() {
        let dump = ClientPruneDump {
            format_version: 3,
            status: "commit_pending".into(),
            memory_ids: vec![10],
            thread_ids: vec![1],
            records: serde_json::Value::Object(serde_json::Map::new()),
            deleted_memory_rows: 1,
            deleted_thread_rows: 1,
            deleted_membership_rows: 2,
        };

        let value = serde_json::to_value(dump).unwrap();

        assert_eq!(value["status"], "commit_pending");
        assert_eq!(value["deleted_memory_rows"], 1);
        assert_eq!(value["deleted_thread_rows"], 1);
        assert_eq!(value["deleted_membership_rows"], 2);
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_backfills_from_labels_and_external_ids_without_deleting_rows() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 7, NULL), (2, 8, NULL), (3, 9, 6); \
             INSERT INTO memory (id, memory_kind, external_id, metadata) VALUES \
               (10, NULL, 'daily:1:2026-07-20:_all', '{\"summary_version\":1,\"source_user_id\":7}'), \
               (11, NULL, 'daily:1:2026-07-20:work', '{\"summary_version\":1,\"source_user_id\":8}'), \
               (12, NULL, 'import:orphan', NULL), \
               (13, 6, 'personality:3', NULL); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10), (2, 11); \
             INSERT INTO thread_label (thread_id, label) VALUES (1, 'summary'), (1, 'user:7'), (2, 'summary'), (2, 'user:8');",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();
        assert_eq!(audit.updated_threads, 3);
        assert_eq!(audit.updated_memories, 4);
        assert_eq!(audit.raw_memories, 2);
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT memory_kind FROM thread WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap(),
            MemoryKind::ThreadSummary as i32
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT memory_kind FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            MemoryKind::ThreadSummary as i32
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT memory_kind FROM memory WHERE id = 12")
                .fetch_one(&pool)
                .await
                .unwrap(),
            MemoryKind::Raw as i32
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "daily:1:2026-07-20:_all"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread")
                .fetch_one(&pool)
                .await
                .unwrap(),
            3
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory")
                .fetch_one(&pool)
                .await
                .unwrap(),
            4
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_resolves_period_summaries_through_legacy_summary_owner() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES \
               (1, 1, NULL), (2, 100000, NULL), (3, 100000, NULL), \
               (4, 100000, NULL), (5, 100000, NULL); \
             INSERT INTO memory (id, user_id, memory_kind, external_id, metadata) VALUES \
               (10, 1, NULL, 'raw:conversation', NULL), \
               (20, 100000, NULL, 'summary:legacy', '{\"summary_version\":1,\"source_thread_id\":1}'), \
               (30, 100000, NULL, 'daily:2026-07-20:_all', '{\"daily_date\":\"2026-07-20\",\"source_thread_ids\":[2]}'), \
               (40, 100000, NULL, 'weekly:2026-W30:_all', '{\"iso_week\":\"2026-W30\",\"source_memory_ids\":[30]}'), \
               (50, 100000, NULL, 'monthly:2026-07:_all', '{\"month\":\"2026-07\",\"source_memory_ids\":[40]}'); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES \
               (1, 10), (2, 20), (3, 30), (4, 40), (5, 50); \
             INSERT INTO thread_label (thread_id, label) VALUES \
               (2, 'summary'), (3, 'daily_summary'), (4, 'weekly_summary'), (5, 'monthly_summary');",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();
        assert!(audit.failures.is_empty());
        for (thread_id, memory_id, kind) in [
            (2, 20, MemoryKind::ThreadSummary),
            (3, 30, MemoryKind::DailySummary),
            (4, 40, MemoryKind::WeeklySummary),
            (5, 50, MemoryKind::MonthlySummary),
        ] {
            assert_eq!(
                sqlx::query_as::<_, (i64, i32)>(
                    "SELECT user_id, memory_kind FROM thread WHERE id = ?",
                )
                .bind(thread_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
                (1, kind as i32)
            );
            assert_eq!(
                sqlx::query_as::<_, (i64, i32)>(
                    "SELECT user_id, memory_kind FROM memory WHERE id = ?",
                )
                .bind(memory_id)
                .fetch_one(&pool)
                .await
                .unwrap(),
                (1, kind as i32)
            );
        }
        for (memory_id, external_id) in [
            (30, "daily:1:2026-07-20:_all"),
            (40, "weekly:1:2026-W30:_all"),
            (50, "monthly:1:2026-07:_all"),
        ] {
            assert_eq!(
                sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = ?")
                    .bind(memory_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                external_id
            );
        }
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_keeps_a_high_id_normal_source_thread_owner() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 100000, NULL), (2, 100000, NULL); \
             INSERT INTO memory (id, user_id, memory_kind, metadata) VALUES \
               (10, 100000, NULL, NULL), \
               (20, 100000, NULL, '{\"summary_version\":1,\"source_thread_id\":1}'); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10), (2, 20); \
             INSERT INTO thread_label (thread_id, label) VALUES (2, 'summary');",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();
        assert!(audit.warnings.is_empty());
        assert_eq!(
            sqlx::query_as::<_, (i64, i32)>(
                "SELECT user_id, memory_kind FROM thread WHERE id = 2",
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            (100_000, MemoryKind::ThreadSummary as i32)
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_leaves_cyclic_legacy_summary_provenance_unresolved() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 100000, NULL); \
             INSERT INTO memory (id, user_id, memory_kind, metadata) VALUES \
               (10, 100000, NULL, '{\"summary_version\":1,\"source_thread_id\":1}'); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10); \
             INSERT INTO thread_label (thread_id, label) VALUES (1, 'summary');",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();
        assert!(audit.warnings.iter().any(|warning| {
            warning.entity == "thread"
                && warning.id == Some(1)
                && warning.check == "unresolved_preflight"
        }));
        assert_eq!(
            sqlx::query_scalar::<_, Option<i32>>("SELECT memory_kind FROM thread WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap(),
            None
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_records_dangling_memberships_without_failing() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 7, 0), (2, 8, NULL); \
             INSERT INTO memory (id, memory_kind, external_id, metadata) VALUES (10, 0, NULL, NULL), (11, NULL, NULL, NULL); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 99), (99, 10), (2, 11);",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();

        assert_eq!(audit.updated_threads, 2);
        assert_eq!(audit.updated_memories, 1);
        assert_eq!(
            audit
                .warnings
                .iter()
                .filter(|warning| warning.check == "unresolved_preflight")
                .count(),
            2
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_dumps_and_removes_a_memory_and_its_memberships() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO memory (id, parent_ids, user_id, media_object_id, memory_kind) VALUES (10, '[1,2]', 100000, 42, NULL);
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (99, 10);
             INSERT INTO memory_rating (id, memory_id, user_id, rating) VALUES (20, 10, 1, 0.5);",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let mut audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();
        audit.status = "completed".into();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let dump = prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap();

        assert_eq!(dump.memory_ids, vec![10]);
        assert_eq!(dump.deleted_memory_rows, 1);
        assert_eq!(dump.deleted_membership_rows, 1);
        assert_eq!(dump.deleted_thread_rows, 0);
        assert_eq!(dump.status, "completed");
        assert!(dump_path.exists());
        let dump_json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&dump_path).unwrap()).unwrap();
        assert_eq!(dump_json["deleted_memory_rows"], 1);
        assert_eq!(dump_json["deleted_membership_rows"], 1);
        assert_eq!(dump_json["deleted_thread_rows"], 0);
        assert_eq!(dump_json["status"], "completed");
        assert_eq!(dump_json["records"]["memory"][0]["parent_ids"], "[1,2]");
        assert_eq!(dump_json["records"]["memory"][0]["media_object_id"], 42);
        assert_eq!(dump_json["records"]["memory_rating"][0]["id"], 20);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory_rating WHERE memory_id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        std::fs::remove_file(dump_path).unwrap();
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_rejects_memory_referenced_by_a_retained_thread() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, default_system_memory_id, user_id, memory_kind) VALUES (1, 10, 1, 0);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 1, NULL);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let error = prune_client_unresolved_with_pool(
            &pool,
            &completed_unresolved_memory_audit(10),
            &dump_path,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("default_system_memory_id"));
        assert!(!dump_path.exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_memory WHERE memory_id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn forced_client_prune_removes_a_memory_referenced_by_a_retained_thread() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, default_system_memory_id, user_id, memory_kind) VALUES (1, 10, 1, 0);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 1, NULL);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let dump = prune_client_unresolved_with_pool_force(
            &pool,
            &completed_unresolved_memory_audit(10),
            &dump_path,
            true,
        )
        .await
        .unwrap();

        assert_eq!(dump.deleted_memory_rows, 1);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        std::fs::remove_file(dump_path).unwrap();
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_rejects_memory_referenced_by_retained_parent_ids() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO memory (id, parent_ids, user_id, memory_kind) VALUES (10, NULL, 1, NULL), (20, '[10]', 1, 0);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let error = prune_client_unresolved_with_pool(
            &pool,
            &completed_unresolved_memory_audit(10),
            &dump_path,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("parent_ids"));
        assert!(!dump_path.exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_rejects_memory_shared_with_a_retained_thread() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 1, NULL), (2, 2, 0);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 1, NULL);
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10), (2, 10);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut audit = completed_unresolved_memory_audit(1);
        audit.warnings[0].entity = "thread".into();
        audit.warnings.push(client_warning(
            "memory",
            Some(10),
            "unresolved_preflight",
            "test unresolved memory",
        ));

        let error = prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("retained thread"));
        assert!(!dump_path.exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread_memory WHERE memory_id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            2
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_rejects_memory_used_by_a_retained_reflection_fact() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 1, NULL), (20, 1, 7);
             INSERT INTO reflection_fact (memory_id, fact_memory_id, fact_kind, turn_index) VALUES (20, 10, 1, 0);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let error = prune_client_unresolved_with_pool(
            &pool,
            &completed_unresolved_memory_audit(10),
            &dump_path,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("reflection_fact"));
        assert!(!dump_path.exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_rejects_thread_referenced_by_retained_reflection() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 1, NULL), (2, 300000, 7);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (20, 300000, 7);
             INSERT INTO thread_reflection_index (memory_id, thread_id, origin_thread_id, origin_user_id) VALUES (20, 2, 1, 1);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut audit = completed_unresolved_memory_audit(1);
        audit.warnings[0].entity = "thread".into();

        let error = prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("origin_thread_id"));
        assert!(!dump_path.exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM thread_reflection_index WHERE memory_id = 20"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_rejects_thread_that_aggregates_retained_reflection() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 300000, NULL);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (20, 300000, 7);
             INSERT INTO thread_reflection_index (memory_id, thread_id, origin_thread_id, origin_user_id) VALUES (20, 1, 2, 1);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut audit = completed_unresolved_memory_audit(1);
        audit.warnings[0].entity = "thread".into();

        let error = prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap_err();

        assert!(error.to_string().contains("thread_id"));
        assert!(!dump_path.exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM thread_reflection_index WHERE memory_id = 20"
            )
            .fetch_one(&pool)
            .await
            .unwrap(),
            1
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_rejects_memory_referenced_by_retained_reflection_previous_id()
    {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 300000, NULL), (20, 300000, 7);
             INSERT INTO thread_reflection_index (memory_id, thread_id, origin_thread_id, origin_user_id, previous_reflection_id) VALUES (20, 2, 2, 1, 10);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));

        let error = prune_client_unresolved_with_pool(
            &pool,
            &completed_unresolved_memory_audit(10),
            &dump_path,
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("previous_reflection_id"));
        assert!(!dump_path.exists());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_dumps_all_reflection_index_columns() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 1, NULL);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 1, NULL);
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10);
             INSERT INTO thread_reflection_index (memory_id, thread_id, origin_thread_id, origin_user_id, origin_channel, outcome, score, score_self, score_heuristic, task_category, reflection_aspect, dataset_quality, summary_embedding_status, summary_embedding_error, intent_embedding_status, intent_embedding_error, prompt_version, target_model_version, experiment_id, experiment_variant, previous_reflection_id, pinned, is_recurrence, mitigation_fingerprint, created_at, updated_at) VALUES (10, 1, 1, 1, 'channel', 2, 0.1, 0.2, 0.3, 4, 5, 6, 7, 'summary error', 8, 'intent error', 'prompt', 'model', 'experiment', 'variant', 9, 1, 1, 'fingerprint', 10, 11);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut audit = completed_unresolved_memory_audit(1);
        audit.warnings[0].entity = "thread".into();
        audit.warnings.push(client_warning(
            "memory",
            Some(10),
            "unresolved_preflight",
            "test unresolved memory",
        ));

        prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap();

        let row = &serde_json::from_slice::<serde_json::Value>(&std::fs::read(&dump_path).unwrap())
            .unwrap()["records"]["thread_reflection_index"][0];
        for field in [
            "origin_channel",
            "outcome",
            "score",
            "score_self",
            "score_heuristic",
            "task_category",
            "reflection_aspect",
            "dataset_quality",
            "summary_embedding_status",
            "summary_embedding_error",
            "intent_embedding_status",
            "intent_embedding_error",
            "prompt_version",
            "target_model_version",
            "experiment_id",
            "experiment_variant",
            "previous_reflection_id",
            "pinned",
            "is_recurrence",
            "mitigation_fingerprint",
            "created_at",
            "updated_at",
        ] {
            assert!(row.get(field).is_some(), "missing {field} from prune dump");
        }
        std::fs::remove_file(dump_path).unwrap();
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_dumps_thread_embedding() {
        let pool = client_fixture_pool().await;
        sqlx::query("INSERT INTO thread (id, user_id, embedding, memory_kind) VALUES (?, ?, ?, ?)")
            .bind(1_i64)
            .bind(1_i64)
            .bind(vec![0x01_u8, 0x02, 0xff])
            .bind(0_i32)
            .execute(&pool)
            .await
            .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut audit = completed_unresolved_memory_audit(1);
        audit.warnings[0].entity = "thread".into();
        audit.warnings.push(client_warning(
            "memory",
            Some(10),
            "unresolved_preflight",
            "test unresolved memory",
        ));

        prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap();

        let dump_json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&dump_path).unwrap()).unwrap();
        assert_eq!(dump_json["records"]["thread"][0]["embedding_hex"], "0102FF");
        std::fs::remove_file(dump_path).unwrap();
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_dumps_and_deletes_reflection_children() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 1, NULL);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 1, NULL), (99, 1, 0);
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10);
             INSERT INTO reflection_failure_mode (memory_id, mode) VALUES (10, 'timeout');
             INSERT INTO reflection_tool (memory_id, tool) VALUES (10, 'search');
             INSERT INTO reflection_tool_outcome (memory_id, tool, contribution, error_kind) VALUES (10, 'search', 1, '');
             INSERT INTO reflection_fact (memory_id, fact_memory_id, fact_kind, turn_index, weight, note, links_json) VALUES (10, 99, 1, 2, 0.5, 'note', '[]');
             INSERT INTO reflection_applied_target (memory_id, target, mitigation_fingerprint, applied_at) VALUES (10, 'target', 'fingerprint', 3);
             INSERT INTO reflection_few_shot_usage (memory_id, used_in_thread_id, used_at) VALUES (10, 99, 4), (99, 1, 5);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut audit = completed_unresolved_memory_audit(1);
        audit.warnings[0].entity = "thread".into();
        audit.warnings.push(client_warning(
            "memory",
            Some(10),
            "unresolved_preflight",
            "test unresolved memory",
        ));

        prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap();

        for table in [
            "reflection_failure_mode",
            "reflection_tool",
            "reflection_tool_outcome",
            "reflection_fact",
            "reflection_applied_target",
            "reflection_few_shot_usage",
        ] {
            let sql = format!("SELECT COUNT(*) FROM {table}");
            assert_eq!(
                sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql))
                    .fetch_one(&pool)
                    .await
                    .unwrap(),
                0,
                "{table} was not deleted"
            );
        }
        let dump_json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&dump_path).unwrap()).unwrap();
        for table in [
            "reflection_failure_mode",
            "reflection_tool",
            "reflection_tool_outcome",
            "reflection_fact",
            "reflection_applied_target",
        ] {
            assert_eq!(dump_json["records"][table].as_array().unwrap().len(), 1);
        }
        assert_eq!(
            dump_json["records"]["reflection_few_shot_usage"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        std::fs::remove_file(dump_path).unwrap();
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_preserves_default_system_memory_owner() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, default_system_memory_id, user_id, memory_kind) VALUES (1, 10, 1, NULL); \
             INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 200000, NULL); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10);",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();

        assert!(audit.failures.is_empty());
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT user_id FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            200000
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT memory_kind FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            MemoryKind::Raw as i32
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_continues_after_an_individual_update_failure() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 7, NULL), (2, 7, NULL); \
             INSERT INTO memory (id, memory_kind) VALUES (10, NULL), (20, NULL); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10), (2, 20); \
             CREATE TRIGGER reject_thread_two BEFORE UPDATE OF memory_kind ON thread \
             WHEN NEW.id = 2 BEGIN SELECT RAISE(ABORT, 'fixture rejection'); END;",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();

        assert_eq!(audit.updated_threads, 1);
        assert_eq!(audit.failures.len(), 1);
        assert_eq!(audit.failures[0].entity, "thread");
        assert_eq!(audit.failures[0].id, 2);
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT memory_kind FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            MemoryKind::Raw as i32
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<i32>>("SELECT memory_kind FROM memory WHERE id = 20")
                .fetch_one(&pool)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT memory_kind FROM thread WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap(),
            MemoryKind::Raw as i32
        );
        assert_eq!(
            sqlx::query_scalar::<_, Option<i32>>("SELECT memory_kind FROM thread WHERE id = 2")
                .fetch_one(&pool)
                .await
                .unwrap(),
            None
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_owner_scopes_external_ids_with_server_rules() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 42, NULL), (2, 42, NULL), (3, 42, NULL), (4, 42, NULL); \
             INSERT INTO memory (id, memory_kind, external_id, metadata) VALUES \
               (10, NULL, 'codex:session:entry', '{\"source\":\"codex\"}'), \
               (11, NULL, 'daily:2026-07-20:_all', NULL), \
               (12, NULL, 'weekly:42:2026-W30:_all', NULL), \
               (13, NULL, 'custom:entry', '{\"source\":\"codex\"}'), \
               (14, NULL, 'daily:2026-07-20:shared', NULL); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10), (2, 11), (3, 12), (4, 13), (1, 14), (2, 14); \
             INSERT INTO thread_label (thread_id, label) VALUES (2, 'daily_summary'), (2, 'user:42'), (3, 'weekly_summary'), (3, 'user:42'), (4, 'daily_summary'), (4, 'user:42');",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();

        assert_eq!(audit.updated_external_ids, 1);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "codex:42:session:entry"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 11")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "daily:2026-07-20:_all"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 12")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "weekly:42:2026-W30:_all"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 13")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "custom:entry"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 14")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "daily:2026-07-20:shared"
        );
        assert!(
            audit
                .warnings
                .iter()
                .any(|warning| warning.check == "external_id_not_converted")
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_keeps_external_id_when_owner_scoped_id_collides() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 42, NULL); \
             INSERT INTO memory (id, memory_kind, external_id, metadata) VALUES \
               (10, NULL, 'codex:session:entry', '{\"source\":\"codex\"}'), \
               (11, 1, 'codex:42:session:entry', NULL); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10);",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();

        assert_eq!(audit.updated_external_ids, 0);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "codex:session:entry"
        );
        assert!(
            audit
                .warnings
                .iter()
                .any(|warning| warning.id == Some(10)
                    && warning.check == "external_id_not_converted")
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_warns_when_aggregate_external_id_conflicts_with_kind() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 42, NULL); \
             INSERT INTO memory (id, memory_kind, external_id, metadata) VALUES \
               (10, NULL, 'weekly:2026-W30:_all', NULL); \
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10); \
             INSERT INTO thread_label (thread_id, label) VALUES (1, 'daily_summary');",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();

        assert_eq!(audit.updated_external_ids, 0);
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT external_id FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            "weekly:2026-W30:_all"
        );
        assert!(audit.warnings.iter().any(|warning| {
            warning.id == Some(10) && warning.check == "external_id_not_converted"
        }));
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_apply_replaces_a_reflection_aggregate_like_the_server() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, created_at, updated_at, memory_kind) VALUES \
               (1, 300000, 10, 20, NULL), (2, 1, 10, 20, NULL); \
             INSERT INTO memory (id, user_id, role, content, content_type, created_at, updated_at, memory_kind) VALUES \
               (10, 300000, 6, 'reflection', 0, 11, 30, NULL); \
             INSERT INTO thread_memory (thread_id, memory_id, position, created_at) VALUES (1, 10, 3, 11); \
             INSERT INTO thread_reflection_index (memory_id, thread_id, origin_thread_id, origin_user_id) VALUES (10, 1, 2, 1);",
        ))
        .execute(&pool)
        .await
        .unwrap();

        let audit = apply_client_mapping_with_pool(&pool, b"{}", None)
            .await
            .unwrap();

        assert!(audit.failures.is_empty());
        assert!(
            !audit
                .warnings
                .iter()
                .any(|warning| warning.check == "reflection_split_not_applied")
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap(),
            0
        );
        let replacement: i64 = sqlx::query_scalar(
            "SELECT thread_id FROM thread_reflection_index WHERE memory_id = 10",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT user_id FROM thread WHERE id = ?")
                .bind(replacement)
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT memory_kind FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            MemoryKind::Reflection as i32
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT user_id FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn client_prune_unresolved_records_actual_thread_and_membership_deletions() {
        let pool = client_fixture_pool().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "INSERT INTO thread (id, user_id, memory_kind) VALUES (1, 1, NULL);
             INSERT INTO memory (id, user_id, memory_kind) VALUES (10, 1, NULL);
             INSERT INTO thread_memory (thread_id, memory_id) VALUES (1, 10);
             INSERT INTO thread_label (thread_id, label) VALUES (1, 'label');",
        ))
        .execute(&pool)
        .await
        .unwrap();
        let dump_path = std::env::temp_dir().join(format!(
            "memory-kind-prune-test-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut audit = completed_unresolved_memory_audit(1);
        audit.warnings[0].entity = "thread".into();

        let dump = prune_client_unresolved_with_pool(&pool, &audit, &dump_path)
            .await
            .unwrap();

        assert_eq!(dump.deleted_memory_rows, 0);
        assert_eq!(dump.deleted_thread_rows, 1);
        assert_eq!(dump.deleted_membership_rows, 1);
        let dump_json: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&dump_path).unwrap()).unwrap();
        assert_eq!(dump_json["deleted_memory_rows"], 0);
        assert_eq!(dump_json["deleted_thread_rows"], 1);
        assert_eq!(dump_json["deleted_membership_rows"], 1);
        assert_eq!(
            dump_json["records"]["thread_memory"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(dump_json["records"]["thread_memory"][0]["thread_id"], 1);
        assert_eq!(dump_json["records"]["thread_memory"][0]["memory_id"], 10);
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM memory WHERE id = 10")
                .fetch_one(&pool)
                .await
                .unwrap(),
            1
        );
        std::fs::remove_file(dump_path).unwrap();
    }

    #[test]
    fn raw_memberships_with_different_owners_conflict() {
        let conflicts = conflicting_memory_ids(
            &[
                MembershipRow {
                    thread_id: 1,
                    memory_id: 42,
                },
                MembershipRow {
                    thread_id: 2,
                    memory_id: 42,
                },
            ],
            &BTreeMap::from([(1, (10, 1)), (2, (20, 1))]),
            &BTreeSet::new(),
            &BTreeSet::new(),
        );

        assert_eq!(conflicts, vec![42]);
    }

    #[test]
    fn shared_memory_with_unresolved_thread_conflicts() {
        let conflicts = conflicting_memory_ids(
            &[
                MembershipRow {
                    thread_id: 1,
                    memory_id: 42,
                },
                MembershipRow {
                    thread_id: 2,
                    memory_id: 42,
                },
            ],
            &BTreeMap::from([(1, (10, 1))]),
            &BTreeSet::from([2]),
            &BTreeSet::new(),
        );

        assert_eq!(conflicts, vec![42]);
    }

    #[test]
    fn shared_default_system_memory_is_not_a_payload_conflict() {
        let conflicts = conflicting_memory_ids(
            &[
                MembershipRow {
                    thread_id: 1,
                    memory_id: 42,
                },
                MembershipRow {
                    thread_id: 2,
                    memory_id: 42,
                },
            ],
            &BTreeMap::from([(1, (10, MemoryKind::Raw as i32))]),
            &BTreeSet::from([2]),
            &BTreeSet::from([42]),
        );

        assert!(conflicts.is_empty());
    }

    #[test]
    fn metadata_with_multiple_aggregate_tiers_conflicts() {
        let rows = [MetadataRow {
            metadata: Some(r#"{"daily_date":"2026-07-18","iso_week":"2026-W29"}"#.to_string()),
        }];

        let (_, _, aggregate_kind, has_conflict, has_invalid_metadata) = metadata_evidence(&rows);

        assert_eq!(aggregate_kind, None);
        assert!(has_conflict);
        assert!(!has_invalid_metadata);
    }

    #[test]
    fn malformed_metadata_is_reported() {
        let rows = [MetadataRow {
            metadata: Some("{not json}".to_string()),
        }];

        let (_, _, _, _, has_invalid_metadata) = metadata_evidence(&rows);

        assert!(has_invalid_metadata);
    }

    #[test]
    fn non_numeric_metadata_owner_or_source_id_is_reported() {
        let rows = [MetadataRow {
            metadata: Some(
                r#"{"source_user_id":"not-a-user","source_thread_ids":[10,"missing"]}"#.to_string(),
            ),
        }];

        let (_, _, _, _, has_invalid_metadata) = metadata_evidence(&rows);

        assert!(has_invalid_metadata);
    }

    #[test]
    fn invalid_aggregate_metadata_value_is_reported() {
        let rows = [MetadataRow {
            metadata: Some(r#"{"daily_date":null}"#.to_string()),
        }];

        let (_, _, aggregate_kind, _, has_invalid_metadata) = metadata_evidence(&rows);

        assert_eq!(aggregate_kind, Some(MemoryKind::DailySummary as i32));
        assert!(has_invalid_metadata);
    }

    #[test]
    fn aggregate_daily_date_must_exist_in_calendar() {
        assert!(!aggregate_metadata_value_is_valid(
            "daily_date",
            &serde_json::json!("2026-02-30"),
        ));
        assert!(aggregate_metadata_value_is_valid(
            "daily_date",
            &serde_json::json!("2024-02-29"),
        ));
    }

    #[test]
    fn owner_candidates_are_preserved_for_audit() {
        assert_eq!(owner_candidate_ids(&BTreeSet::from([20, 10])), vec![10, 20]);
    }

    #[test]
    fn reflection_split_details_are_grouped_by_owner() {
        let details = reflection_split_owner_audit(vec![
            ReflectionSplitRow {
                origin_user_id: 20,
                memory_id: 3,
                position: 4,
            },
            ReflectionSplitRow {
                origin_user_id: 10,
                memory_id: 1,
                position: 2,
            },
            ReflectionSplitRow {
                origin_user_id: 10,
                memory_id: 2,
                position: 3,
            },
        ]);

        assert_eq!(details.len(), 2);
        assert_eq!(details[0].origin_user_id, 10);
        assert_eq!(details[0].memory_count, 2);
        assert_eq!(
            details[0].memories,
            vec![
                ReflectionSplitMemoryAudit {
                    memory_id: 1,
                    position: 2,
                },
                ReflectionSplitMemoryAudit {
                    memory_id: 2,
                    position: 3,
                },
            ]
        );
    }

    #[test]
    fn preflight_snapshot_uses_backend_appropriate_settings() {
        #[cfg(feature = "postgres")]
        assert_eq!(
            preflight_snapshot_sql(),
            Some("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        );
        #[cfg(not(feature = "postgres"))]
        assert_eq!(preflight_snapshot_sql(), None);
    }

    #[test]
    fn preflight_records_mapping_hash_and_counts() {
        let mapping = br#"{"summary_labels":[{"label":"daily","memory_kind":"DAILY_SUMMARY"}],"explicit_owners":[{"thread_id":1,"owner_user_id":2}]}"#;
        let parsed = parse_mapping_json(std::str::from_utf8(mapping).unwrap()).unwrap();
        let hash = Sha256::digest(mapping)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(parsed.summary_labels.len(), 5);
        assert_eq!(parsed.explicit_owners_by_thread_id.len(), 1);
        assert_eq!(hash.len(), 64);
    }

    #[test]
    fn resolved_target_keeps_kind_and_owner_as_typed_values() {
        assert_eq!(resolved_target("resolved:kind=6,owner=42"), Some((42, 6)));
        assert_eq!(resolved_target("unresolved:missing_owner"), None);
    }

    #[test]
    fn source_thread_owner_query_casts_legacy_postgres_int_to_bigint() {
        assert!(thread_owner_sql().contains("CAST(user_id AS BIGINT)"));
    }

    #[test]
    fn source_namespace_normalizes_legacy_hyphen_to_external_id_underscore() {
        assert_eq!(
            namespace_for_external_id(
                "news-aggregator",
                "news_aggregator:https://example.test/article",
            ),
            Some("news_aggregator".to_string())
        );
        assert_eq!(
            namespace_for_external_id("news-aggregator", "codex:session:entry"),
            None
        );
    }

    #[test]
    fn reflection_key_uses_the_online_canonical_labels() {
        let labels = compose_aggregate_labels(&[
            "team:a".to_string(),
            "reflection".to_string(),
            "team:a".to_string(),
        ]);
        assert_eq!(labels, vec!["reflection", "team:a"]);
        assert_eq!(sha256_join_pipe(&labels).len(), 64);
    }

    #[test]
    fn replacement_timestamp_keeps_the_latest_member_timestamp() {
        assert_eq!(replacement_updated_at(100, [80, 120, 110]), 120);
        assert_eq!(replacement_updated_at(100, [80, 90]), 100);
    }

    #[test]
    fn single_owner_reflection_is_still_a_replacement_target() {
        assert!(should_replace_reflection_thread(
            MemoryKind::Reflection as i32,
            true
        ));
        assert!(!should_replace_reflection_thread(
            MemoryKind::Reflection as i32,
            false
        ));
    }

    #[test]
    fn referenced_reflection_aggregate_cannot_be_deleted() {
        assert!(!has_reference_count(0));
        assert!(has_reference_count(1));
    }

    #[test]
    fn reverse_reference_reason_prefers_origin_over_few_shot_usage() {
        assert_eq!(reverse_reference_unresolved_reason(0, 0), None);
        assert_eq!(
            reverse_reference_unresolved_reason(1, 0),
            Some("unresolved:reflection_thread_referenced_as_origin")
        );
        assert_eq!(
            reverse_reference_unresolved_reason(0, 1),
            Some("unresolved:reflection_thread_referenced_by_few_shot_usage")
        );
        assert_eq!(
            reverse_reference_unresolved_reason(1, 1),
            Some("unresolved:reflection_thread_referenced_as_origin")
        );
    }

    #[test]
    fn unknown_commit_outcome_keeps_the_apply_audit() {
        assert!(should_retain_apply_audit_after_error(true));
        assert!(!should_retain_apply_audit_after_error(false));
    }

    #[test]
    fn empty_client_audit_reservation_is_not_retained() {
        assert!(!should_retain_client_journal_after_error(0));
        assert!(should_retain_client_journal_after_error(1));
    }

    #[test]
    fn matching_owner_scoped_key_marks_reflection_as_already_migrated() {
        assert!(reflection_aggregate_key_matches(10, Some(10)));
        assert!(!reflection_aggregate_key_matches(10, Some(11)));
        assert!(!reflection_aggregate_key_matches(10, None));
    }

    #[test]
    fn partial_existing_reflection_is_not_treated_as_complete() {
        assert_eq!(
            reflection_membership_state(2, 2, 2),
            ExistingReflectionState::Complete
        );
        assert_eq!(
            reflection_membership_state(2, 1, 1),
            ExistingReflectionState::Incomplete
        );
        assert_eq!(
            reflection_membership_state(0, 0, 0),
            ExistingReflectionState::Incomplete
        );
    }

    #[test]
    fn existing_reflection_membership_must_belong_to_the_same_thread() {
        assert!(reflection_thread_membership_sql().contains("tri.thread_id = tm.thread_id"));
    }

    #[test]
    fn replacement_reflection_has_no_default_system_memory() {
        assert_eq!(reflection_default_system_memory_id(), None);
    }

    #[test]
    fn reflection_membership_requires_matching_sidecar_owner() {
        assert!(reflection_membership_matches(Some((4, 10)), 4, 10));
        assert!(!reflection_membership_matches(Some((4, 11)), 4, 10));
    }

    #[test]
    fn expected_thread_ids_replace_only_audited_aggregate_threads() {
        let moves = vec![ReflectionMoveAudit {
            old_thread_id: 2,
            new_thread_id: 20,
            origin_user_id: 10,
            labels: vec![],
            labels_hash: "a".repeat(64),
            description: None,
            channel: None,
            metadata: None,
            created_at: 1,
            updated_at: 1,
            memories: vec![],
            memory_timestamps: vec![],
        }];
        assert_eq!(
            expected_thread_ids_after_moves(&[1, 2, 3], &moves),
            BTreeSet::from([1, 3, 20])
        );
    }

    #[test]
    fn unknown_thread_reference_column_is_rejected() {
        assert!(is_known_thread_reference_column(
            "thread_memory",
            "thread_id"
        ));
        assert!(!is_known_thread_reference_column(
            "future_reflection_link",
            "aggregate_thread_id"
        ));
    }

    #[test]
    fn old_aggregate_key_is_not_a_conflict_but_another_target_is() {
        assert!(!aggregate_key_conflicts(Some(10), 10));
        assert!(aggregate_key_conflicts(Some(11), 10));
        assert!(!aggregate_key_conflicts(None, 10));
    }

    #[test]
    fn thread_ownership_audit_records_expected_target_without_changing_timestamps() {
        let rows = vec![ImmutableRow {
            id: 7,
            user_id: 17,
            created_at: 100,
            updated_at: 200,
            external_id: Some("stable-id".to_string()),
        }];
        let audit = thread_ownership_audit(rows, &BTreeMap::from([(7, (42, 6))]));
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].expected_user_id, 42);
        assert_eq!(audit[0].expected_memory_kind, 6);
        assert_eq!((audit[0].created_at, audit[0].updated_at), (100, 200));
        assert_eq!(audit[0].external_id.as_deref(), Some("stable-id"));
    }

    #[test]
    fn memory_ownership_audit_reassigns_generated_memory_to_resolved_owner() {
        let rows = vec![ImmutableRow {
            id: 8,
            user_id: 200_000,
            created_at: 100,
            updated_at: 200,
            external_id: Some("author-stable-id".to_string()),
        }];
        let audit = memory_ownership_audit(rows, &BTreeMap::from([(8, (42, 6))]));
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].expected_user_id, 42);
        assert_eq!(audit[0].expected_memory_kind, 6);
    }

    #[test]
    fn memory_ownership_audit_preserves_high_non_legacy_author() {
        let rows = vec![ImmutableRow {
            id: 10,
            user_id: 900_000,
            created_at: 100,
            updated_at: 200,
            external_id: None,
        }];
        let audit = memory_ownership_audit(rows, &BTreeMap::from([(10, (42, 6))]));
        assert_eq!(audit[0].expected_user_id, 900_000);
    }

    #[test]
    fn memory_ownership_audit_preserves_raw_memory_author() {
        let rows = vec![ImmutableRow {
            id: 9,
            user_id: 17,
            created_at: 100,
            updated_at: 200,
            external_id: Some("author-stable-id".to_string()),
        }];
        let audit =
            memory_ownership_audit(rows, &BTreeMap::from([(9, (42, MemoryKind::Raw as i32))]));
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].expected_user_id, 17);
        assert_eq!(audit[0].expected_memory_kind, MemoryKind::Raw as i32);
    }

    #[test]
    fn reflection_timestamp_audit_accepts_v7_author_user_id() {
        let audit: ReflectionMemoryTimestampAudit = serde_json::from_str(
            r#"{
                "memory_id": 9,
                "author_user_id": 17,
                "created_at": 100,
                "updated_at": 200,
                "external_id": null
            }"#,
        )
        .unwrap();
        assert_eq!(audit.expected_user_id, 17);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_reflection_sql_uses_distinct_parameters() {
        assert!(AGGREGATE_KEY_LOOKUP_SQL.contains("user_id = $1 AND labels_hash = $2"));
        assert!(REFLECTION_MEMBERSHIP_SQL.contains("thread_id = $1"));
        assert!(REFLECTION_MEMBERSHIP_SQL.contains("memory_id = $2"));
        assert!(REFLECTION_MEMBERSHIP_SQL.contains("tri.thread_id = $3"));
    }

    #[test]
    fn apply_audit_rejects_unknown_format_before_database_access() {
        let audit = ApplyAudit {
            format_version: 7,
            mapping_sha256: "0".repeat(64),
            initial_thread_ids: vec![],
            initial_memory_ids: vec![],
            threads: vec![],
            memories: vec![],
            reflection_moves: vec![],
            external_id_moves: vec![],
            updated_threads: 0,
            updated_memories: 0,
        };
        assert_ne!(audit.format_version, 1);
    }

    #[test]
    fn external_id_update_is_counted_as_a_memory_update() {
        assert_eq!(require_single_external_id_update(1, 7).unwrap(), 1);
    }

    #[test]
    fn external_id_update_rejects_a_missing_or_duplicate_row() {
        assert!(require_single_external_id_update(0, 7).is_err());
        assert!(require_single_external_id_update(2, 7).is_err());
    }

    #[test]
    fn postgres_reflection_thread_clone_inserts_metadata_as_jsonb() {
        assert!(POSTGRES_REFLECTION_THREAD_INSERT_SQL.contains("$10::jsonb"));
    }

    #[test]
    fn postgres_fresh_and_contract_schema_define_not_null_memory_kind_without_a_range_check() {
        let fresh_schema = include_str!("../../../infra/sql/postgres/001_init_postgres.sql");
        let contract_schema =
            include_str!("../../../infra/sql/postgres/manual/011_contract_memory_kind.sql");

        assert!(!fresh_schema.contains("memory_kind INT NOT NULL DEFAULT"));
        assert!(!fresh_schema.contains("CHECK (memory_kind BETWEEN 1 AND 7)"));
        assert!(contract_schema.contains("ALTER COLUMN memory_kind DROP DEFAULT"));
        assert!(contract_schema.contains("ALTER COLUMN memory_kind SET NOT NULL"));
        assert!(!contract_schema.contains("CHECK (memory_kind BETWEEN 1 AND 7)"));
        assert!(contract_schema.contains("DROP CONSTRAINT IF EXISTS thread_memory_kind_range"));
        assert!(contract_schema.contains("DROP CONSTRAINT IF EXISTS memory_memory_kind_range"));
    }

    #[test]
    fn contract_migrations_do_not_embed_procedural_database_logic() {
        let postgres_contract =
            include_str!("../../../infra/sql/postgres/manual/011_contract_memory_kind.sql");
        let sqlite_contract =
            include_str!("../../../infra/sql/sqlite/manual/012_contract_memory_kind.sql");

        for schema in [postgres_contract, sqlite_contract] {
            let normalized = schema.to_ascii_lowercase();
            assert!(!normalized.contains("do $$"));
            assert!(!normalized.contains("raise exception"));
            assert!(!normalized.contains("create temporary table"));
        }
    }

    #[test]
    fn raw_memory_kind_preserves_the_existing_stored_value() {
        assert_eq!(MemoryKind::Raw as i32, 1);
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn preflight_moves_only_legacy_external_ids_to_creator_scoped_ids() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE memory (id INTEGER PRIMARY KEY, external_id TEXT, metadata TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO memory (id, external_id, metadata) VALUES (1, 'codex:session:entry', '{\"source\":\"codex\"}')")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO memory (id, external_id, metadata) VALUES (2, 'daily:2026-07-19:_all', NULL)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO memory (id, external_id, metadata) VALUES (3, 'custom:foo', NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO memory (id, external_id, metadata) VALUES (4, 'other:foo', '{\"source\":\"custom\"}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let (moves, unresolved) = build_external_id_moves(
            &mut tx,
            &[
                MembershipRow {
                    thread_id: 10,
                    memory_id: 1,
                },
                MembershipRow {
                    thread_id: 11,
                    memory_id: 2,
                },
                MembershipRow {
                    thread_id: 12,
                    memory_id: 3,
                },
                MembershipRow {
                    thread_id: 13,
                    memory_id: 4,
                },
            ],
            &BTreeMap::from([
                (10, (42, MemoryKind::Raw as i32)),
                (11, (42, MemoryKind::DailySummary as i32)),
                (12, (42, MemoryKind::DailySummary as i32)),
                (13, (42, MemoryKind::Raw as i32)),
            ]),
        )
        .await
        .unwrap();

        assert_eq!(unresolved, vec![4]);
        assert_eq!(moves.len(), 2);
        assert_eq!(moves[0].new_external_id, "codex:42:session:entry");
        assert_eq!(moves[1].new_external_id, "daily:42:2026-07-19:_all");
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn preflight_rejects_a_legacy_external_id_with_multiple_memberships() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE memory (id INTEGER PRIMARY KEY, external_id TEXT, metadata TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO memory (id, external_id, metadata) VALUES (1, 'codex:session:entry', '{\"source\":\"codex\"}')")
            .execute(&pool)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        let (_, unresolved) = build_external_id_moves(
            &mut tx,
            &[
                MembershipRow {
                    thread_id: 10,
                    memory_id: 1,
                },
                MembershipRow {
                    thread_id: 11,
                    memory_id: 1,
                },
            ],
            &BTreeMap::from([(10, (42, MemoryKind::Raw as i32))]),
        )
        .await
        .unwrap();

        assert_eq!(unresolved, vec![1]);
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn aggregate_source_owners_follow_weekly_to_daily_chain_without_daily_raw_inputs() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE memory (id INTEGER PRIMARY KEY, metadata TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE thread (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO thread (id, user_id) VALUES (30, 10)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO memory (id, metadata) VALUES \
             (1, '{\"iso_week\":\"2026-W29\",\"source_memory_ids\":[2]}'), \
             (2, '{\"daily_date\":\"2026-07-18\",\"source_thread_ids\":[30]}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut tx = pool.begin().await.unwrap();

        let evidence = aggregate_source_evidence(
            &mut tx,
            &ClassificationMapping::default(),
            MemoryKind::MonthlySummary as i32,
            &BTreeSet::from([1]),
        )
        .await
        .unwrap();

        assert_eq!(evidence.owners, BTreeSet::from([10]));
        assert!(!evidence.has_invalid_hierarchy);
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn aggregate_source_hierarchy_accepts_daily_summaries_of_raw_memories() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE memory (id INTEGER PRIMARY KEY, metadata TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE thread (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO thread (id, user_id) VALUES (30, 10)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO memory (id, metadata) VALUES \
             (1, '{\"daily_date\":\"2026-07-18\",\"source_memory_ids\":[2],\"source_thread_ids\":[30]}'), \
             (2, '{}')",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut tx = pool.begin().await.unwrap();

        let evidence = aggregate_source_evidence(
            &mut tx,
            &ClassificationMapping::default(),
            MemoryKind::WeeklySummary as i32,
            &BTreeSet::from([1]),
        )
        .await
        .unwrap();

        assert_eq!(evidence.owners, BTreeSet::from([10]));
        assert!(!evidence.has_invalid_hierarchy);
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn daily_summary_rejects_a_missing_raw_input_memory() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE memory (id INTEGER PRIMARY KEY, metadata TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();

        let evidence = aggregate_source_evidence(
            &mut tx,
            &ClassificationMapping::default(),
            MemoryKind::DailySummary as i32,
            &BTreeSet::from([2]),
        )
        .await
        .unwrap();

        assert!(evidence.has_invalid_hierarchy);
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn aggregate_source_hierarchy_rejects_wrong_direct_tier() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query("CREATE TABLE memory (id INTEGER PRIMARY KEY, metadata TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO memory (id, metadata) VALUES (1, '{\"month\":\"2026-07\"}')")
            .execute(&pool)
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();

        let evidence = aggregate_source_evidence(
            &mut tx,
            &ClassificationMapping::default(),
            MemoryKind::WeeklySummary as i32,
            &BTreeSet::from([1]),
        )
        .await
        .unwrap();

        assert!(evidence.has_invalid_hierarchy);
    }

    #[cfg(not(feature = "postgres"))]
    async fn contract_fixture_pool(kind: Option<i32>) -> sqlx::SqlitePool {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(
            "CREATE TABLE thread (id INTEGER PRIMARY KEY, default_system_memory_id INTEGER, user_id INTEGER NOT NULL, description TEXT, channel TEXT, embedding BLOB, embedding_dim INTEGER, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, metadata JSON, memory_kind INTEGER); CREATE TABLE memory (id INTEGER PRIMARY KEY, parent_ids JSON, user_id INTEGER NOT NULL, content TEXT NOT NULL, content_type INTEGER NOT NULL, params JSON, metadata JSON, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, role INTEGER NOT NULL DEFAULT 0, external_id VARCHAR(512), media_object_id INTEGER, memory_kind INTEGER);",
        ))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO thread (id, user_id, created_at, updated_at, memory_kind) VALUES (1, 10, 100, 100, ?)")
            .bind(kind)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO memory (id, user_id, content, content_type, created_at, updated_at, memory_kind) VALUES (1, 10, 'content', 0, 100, 100, ?)")
            .bind(kind)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    #[cfg(not(feature = "postgres"))]
    async fn run_sqlite_contract(pool: &sqlx::SqlitePool) -> Result<()> {
        sqlx::raw_sql(sqlx::AssertSqlSafe(include_str!(
            "../../../infra/sql/sqlite/manual/012_contract_memory_kind.sql"
        )))
        .execute(pool)
        .await
        .context("run SQLite memory_kind contract fixture")?;
        Ok(())
    }

    #[cfg(not(feature = "postgres"))]
    async fn run_sqlite_contract_on_connection(
        connection: &mut sqlx::SqliteConnection,
    ) -> Result<()> {
        sqlx::raw_sql(sqlx::AssertSqlSafe(include_str!(
            "../../../infra/sql/sqlite/manual/012_contract_memory_kind.sql"
        )))
        .execute(&mut *connection)
        .await
        .context("run SQLite memory_kind contract fixture")?;
        Ok(())
    }

    #[cfg(not(feature = "postgres"))]
    async fn assert_sqlite_final_memory_kind_schema(pool: &sqlx::SqlitePool) {
        for table in ["thread", "memory"] {
            let rows = sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA table_info(`{table}`)")))
                .fetch_all(pool)
                .await
                .unwrap();
            let kind = rows
                .iter()
                .find(|row| row.get::<String, _>("name") == "memory_kind")
                .unwrap();
            assert_eq!(kind.get::<i64, _>("notnull"), 1);
            assert!(
                kind.try_get::<Option<String>, _>("dflt_value")
                    .unwrap()
                    .is_none()
            );
            let schema: String = sqlx::query_scalar(
                "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            )
            .bind(table)
            .fetch_one(pool)
            .await
            .unwrap();
            assert!(!schema.contains("CHECK (`memory_kind` BETWEEN 1 AND 7)"));
        }

        let index_names = sqlx::query_scalar::<_, String>(
            "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name",
        )
        .fetch_all(pool)
        .await
        .unwrap();
        assert!(index_names.contains(&"thread_user_memory_kind_updated_at".to_string()));
        assert!(index_names.contains(&"memory_user_memory_kind_updated_at".to_string()));
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn sqlite_contract_migration_enforces_final_schema_without_a_default() {
        let pool = contract_fixture_pool(Some(MemoryKind::Raw as i32)).await;
        run_sqlite_contract(&pool).await.unwrap();
        assert_sqlite_final_memory_kind_schema(&pool).await;
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn sqlite_fresh_schema_matches_the_memory_kind_contract() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(include_str!(
            "../../../infra/sql/sqlite/001_schema.sql"
        )))
        .execute(&pool)
        .await
        .unwrap();

        assert_sqlite_final_memory_kind_schema(&pool).await;
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn sqlite_contract_migration_rejects_null_without_changing_fixture() {
        let pool = contract_fixture_pool(None).await;
        let mut connection = pool.acquire().await.unwrap();
        assert!(
            run_sqlite_contract_on_connection(&mut connection)
                .await
                .is_err()
        );
        sqlx::query("ROLLBACK")
            .execute(&mut *connection)
            .await
            .unwrap();

        let stored: Option<i32> = sqlx::query_scalar("SELECT memory_kind FROM thread WHERE id = 1")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(stored, None);
        let kind_not_null: i64 = sqlx::query_scalar(
            "SELECT \"notnull\" FROM pragma_table_info('thread') WHERE name = 'memory_kind'",
        )
        .fetch_one(&mut *connection)
        .await
        .unwrap();
        assert_eq!(kind_not_null, 0);
    }

    #[cfg(not(feature = "postgres"))]
    #[tokio::test]
    async fn sqlite_contract_migration_accepts_unspecified_at_the_database_layer() {
        let pool = contract_fixture_pool(Some(MemoryKind::Unspecified as i32)).await;
        run_sqlite_contract(&pool).await.unwrap();
        assert_sqlite_final_memory_kind_schema(&pool).await;
    }

    #[test]
    fn standalone_memory_keeps_its_author_and_becomes_raw() {
        assert_eq!(
            standalone_memory_target(99999),
            (99999, MemoryKind::Raw as i32)
        );
    }
}
