//! `thread-groups-user-ids-v1@3`: conservative canonical membership repair.

use super::{DataMigrationTask, catalog::TaskCatalogEntry, state};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use common::thread_group_key::{
    IdentityScope, SourceIdentity, parse_legacy_owner_scope, reconciler_group_canonical_key,
    source_thread_canonical_key,
};
use infra_utils::infra::rdb::{RdbPool, RdbTransaction};
use serde::Serialize;
use sqlx::FromRow;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

const TASK_IDENTITY: &str = "thread-groups-user-ids-v1@3";
const CANONICAL_KEYS_IDENTITY: &str = "thread-groups-canonical-keys-v1@2";
const DEFAULT_LEASE_MS: i64 = 120_000;

#[cfg(feature = "postgres")]
const MEMBER_REPAIR_SQL: &str = "UPDATE thread_group_member SET thread_canonical_key = $1 \
    WHERE group_id = $2 AND thread_id = $3 AND thread_canonical_key = $4 AND state = 'active'";
#[cfg(not(feature = "postgres"))]
const MEMBER_REPAIR_SQL: &str = "UPDATE thread_group_member SET thread_canonical_key = ? \
    WHERE group_id = ? AND thread_id = ? AND thread_canonical_key = ? AND state = 'active'";

#[cfg(feature = "postgres")]
const PARENT_RELATION_REPAIR_SQL: &str = "UPDATE thread_relation SET parent_thread_canonical_key = $1 \
    WHERE id = $2 AND state = 'active' AND parent_thread_canonical_key = $3";
#[cfg(not(feature = "postgres"))]
const PARENT_RELATION_REPAIR_SQL: &str = "UPDATE thread_relation SET parent_thread_canonical_key = ? \
    WHERE id = ? AND state = 'active' AND parent_thread_canonical_key = ?";

#[cfg(feature = "postgres")]
const CHILD_RELATION_REPAIR_SQL: &str = "UPDATE thread_relation SET child_thread_canonical_key = $1 \
    WHERE id = $2 AND state = 'active' AND child_thread_canonical_key = $3";
#[cfg(not(feature = "postgres"))]
const CHILD_RELATION_REPAIR_SQL: &str = "UPDATE thread_relation SET child_thread_canonical_key = ? \
    WHERE id = ? AND state = 'active' AND child_thread_canonical_key = ?";

#[cfg(feature = "postgres")]
const EVENT_REFS_SQL: &str = "SELECT event_type, source, identity_scope, owner_scope, user_id, native_id_ref, \
    group_id, thread_id, payload::text AS payload, event_id FROM thread_group_event_outbox";
#[cfg(not(feature = "postgres"))]
const EVENT_REFS_SQL: &str = "SELECT event_type, source, identity_scope, owner_scope, user_id, native_id_ref, \
    group_id, thread_id, payload, event_id FROM thread_group_event_outbox";

#[cfg(feature = "postgres")]
const MUTATION_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RepairReport {
    pub status: String,
    pub mismatch_rows: u64,
    pub distinct_threads: u64,
    pub mismatch_by_key_origin: BTreeMap<String, u64>,
    pub mismatch_by_membership_state: BTreeMap<String, u64>,
    pub mismatch_by_role: BTreeMap<String, u64>,
    pub mismatch_by_provenance: BTreeMap<String, u64>,
    pub safe_candidate_count: u64,
    pub unsafe_candidate_count: u64,
    pub stop_reasons: BTreeMap<String, u64>,
    pub canonical_keys_pending_threads: u64,
    pub pending_typed_fields: BTreeMap<String, u64>,
    pub typed_id_fields_backfilled: BTreeMap<String, u64>,
    pub affected_group_count: u64,
    pub affected_component_count: u64,
    pub regeneration_group_ids: Vec<i64>,
    pub summary_policy: String,
    pub prerequisite_status: String,
    pub task_state: Option<String>,
    pub repaired_memberships: u64,
}

pub struct ThreadGroupsUserIdsV3Task {
    pool: RdbPool,
    catalog: TaskCatalogEntry,
    lease_duration_ms: i64,
    #[cfg(test)]
    fail_after_repair: bool,
}

impl ThreadGroupsUserIdsV3Task {
    pub fn new(pool: RdbPool, catalog: TaskCatalogEntry) -> Result<Self> {
        catalog.validate()?;
        if catalog.identity() != TASK_IDENTITY {
            bail!(
                "unexpected replacement task catalog identity: {}",
                catalog.identity()
            );
        }
        Ok(Self {
            pool,
            catalog,
            lease_duration_ms: DEFAULT_LEASE_MS,
            #[cfg(test)]
            fail_after_repair: false,
        })
    }

    #[cfg(test)]
    fn with_fail_after_repair(mut self) -> Self {
        self.fail_after_repair = true;
        self
    }

    pub async fn inspect(&self) -> Result<RepairReport> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning canonical mismatch inspection")?;
        let mut analysis = analyze_tx(&mut tx).await?;
        tx.rollback()
            .await
            .context("ending canonical mismatch inspection")?;
        analysis.report.prerequisite_status = prerequisite_status(&self.pool).await?;
        if analysis.report.prerequisite_status == "completed"
            && analysis.report.canonical_keys_pending_threads > 0
        {
            analysis.report.prerequisite_status = "keys_pending".to_string();
        }
        analysis.report.task_state = state::load(&self.pool, &self.catalog.identity())
            .await?
            .map(|row| row.state);
        set_report_status(&mut analysis.report);
        Ok(analysis.report)
    }

    pub async fn dry_run(&self) -> Result<RepairReport> {
        self.inspect().await
    }

    pub async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<RepairReport> {
        if let Some(existing) = state::load(&self.pool, &self.catalog.identity()).await?
            && existing.kind()? == state::TaskStateKind::Completed
        {
            if existing.canonical_definition_digest != self.catalog.canonical_definition_digest {
                bail!("task state definition digest does not match the fixed registry");
            }
            let mut report = self.inspect().await?;
            ensure_verified(&report)?;
            report.status = "already_completed".to_string();
            return Ok(report);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning atomic canonical mismatch repair")?;
        let run = async {
            acquire_group_membership_mutation_lock_tx(&mut tx).await?;
            let prerequisite = canonical_prerequisite_tx(&mut tx).await?;
            let analysis = analyze_tx(&mut tx).await?;
            if prerequisite != "completed" {
                bail!(
                    "thread-groups-user-ids-v1@3 requires completed thread-groups-canonical-keys-v1@2; run `memories-db-migrate post-migrate run --id thread-groups-canonical-keys-v1 --generation 2 --maintenance-window-ack` first"
                );
            }
            if analysis.report.canonical_keys_pending_threads != 0 {
                bail!(
                    "canonical key prerequisite has {} Thread(s) without persisted keys; run or verify `memories-db-migrate post-migrate run --id thread-groups-canonical-keys-v1 --generation 2 --maintenance-window-ack` before re-inspecting",
                    analysis.report.canonical_keys_pending_threads
                );
            }
            if !analysis.report.stop_reasons.is_empty() {
                bail!(
                    "canonical membership repair refused: {} unsafe candidate(s), stop_reasons={:?}; inspect with `memories-db-migrate post-migrate status`",
                    analysis.report.unsafe_candidate_count,
                    analysis.report.stop_reasons
                );
            }
            let lease = state::claim_tx(
                &mut tx,
                &self.catalog.identity(),
                &self.catalog.canonical_definition_digest,
                execution_id,
                holder_id,
                command_utils::util::datetime::now_millis(),
                self.lease_duration_ms,
            )
            .await?;
            if analysis.report.mismatch_rows != analysis.report.safe_candidate_count {
                bail!("canonical membership repair plan is incomplete");
            }

            for repair in &analysis.repairs {
                update_membership_key_tx(&mut tx, repair).await?;
                for relation in &repair.relations {
                    update_relation_key_tx(&mut tx, repair, relation).await?;
                }
            }
            let pending_typed_fields =
                super::thread_groups_user_ids_v1::backfill_typed_ids_v3_tx(&mut tx).await?;
            #[cfg(test)]
            if self.fail_after_repair {
                bail!("injected failure after repair and typed-ID backfill");
            }
            let after = analyze_tx(&mut tx).await?;
            if after.report.mismatch_rows != 0 {
                bail!(
                    "canonical membership verification found {} remaining mismatch row(s)",
                    after.report.mismatch_rows
                );
            }
            if !after.report.stop_reasons.is_empty() {
                bail!("post-repair reference verification found unsafe references");
            }
            if pending_typed_fields.values().sum::<u64>() != 0 {
                bail!("typed owner ID verification found pending fields");
            }
            let mut report = analysis.report;
            report.typed_id_fields_backfilled = report.pending_typed_fields.clone();
            report.pending_typed_fields = pending_typed_fields;
            report.repaired_memberships = report.mismatch_rows;
            report.status = if report.repaired_memberships == 0 {
                "no_op".to_string()
            } else {
                "repaired".to_string()
            };
            report.prerequisite_status = prerequisite;
            report.task_state = Some("completed".to_string());
            state::complete_tx(
                &mut tx,
                &lease,
                command_utils::util::datetime::now_millis(),
            )
            .await?;
            Ok(report)
        }
        .await;

        match run {
            Ok(report) => {
                tx.commit()
                    .await
                    .context("committing atomic canonical mismatch repair")?;
                Ok(report)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    pub async fn verify(&self) -> Result<()> {
        let report = self.inspect().await?;
        ensure_verified(&report)
    }
}

#[async_trait]
impl DataMigrationTask for ThreadGroupsUserIdsV3Task {
    fn task_identity(&self) -> String {
        self.catalog.identity()
    }

    async fn inspect(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV3Task::inspect(self).await?,
        )?)
    }

    async fn dry_run(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV3Task::dry_run(self).await?,
        )?)
    }

    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV3Task::apply(self, execution_id, holder_id).await?,
        )?)
    }

    async fn verify(&self) -> Result<()> {
        ThreadGroupsUserIdsV3Task::verify(self).await
    }
}

#[derive(Debug, Clone, FromRow)]
struct MismatchRow {
    group_id: i64,
    member_thread_id: Option<i64>,
    thread_id: i64,
    old_key: String,
    saved_key: String,
    owner_scope: String,
    source: Option<String>,
    identity_scope: Option<String>,
    native_id: Option<String>,
    role: String,
    membership_state: String,
    provenance: String,
    member_user_id: Option<i64>,
    thread_user_id: i64,
    canonical_owner_scope: String,
    canonical_user_id: Option<i64>,
    key_origin: String,
    group_status: Option<String>,
    group_user_id: Option<i64>,
    grouping_authority: Option<String>,
    group_canonical_key: Option<String>,
}

#[derive(Debug, Clone, FromRow)]
struct MemberRow {
    group_id: i64,
    thread_id: Option<i64>,
    key: String,
    role: String,
    state: String,
}

#[derive(Debug, Clone, FromRow)]
struct IdentityRow {
    owner_scope: String,
    source: String,
    identity_scope: String,
    native_id: String,
    thread_id: i64,
    user_id: Option<i64>,
}

#[derive(Debug, Clone, FromRow)]
struct CanonicalRow {
    thread_id: i64,
    key: String,
}

#[derive(Debug, Clone, FromRow)]
struct ThreadRow {
    id: i64,
    user_id: i64,
}

#[derive(Debug, Clone, FromRow)]
struct GroupRow {
    id: i64,
    user_id: i64,
    status: String,
    grouping_authority: String,
    group_canonical_key: String,
}

#[derive(Debug, Clone, FromRow)]
struct RelationRow {
    id: i64,
    parent_thread_id: Option<i64>,
    child_thread_id: Option<i64>,
    parent_key: String,
    child_key: String,
    parent_owner_scope: String,
    parent_user_id: Option<i64>,
    parent_source: Option<String>,
    parent_identity_scope: Option<String>,
    parent_native_id: Option<String>,
    child_owner_scope: String,
    child_user_id: Option<i64>,
    child_source: Option<String>,
    child_identity_scope: Option<String>,
    child_native_id: Option<String>,
    state: String,
}

#[derive(Debug, Clone, FromRow)]
struct AuditRow {
    source_group_id: i64,
    target_group_id: Option<i64>,
}

#[derive(Debug, Clone, FromRow)]
struct EventRefRow {
    event_id: String,
    event_type: String,
    source: Option<String>,
    identity_scope: Option<String>,
    owner_scope: Option<String>,
    user_id: Option<i64>,
    native_id_ref: Option<String>,
    group_id: Option<i64>,
    thread_id: Option<i64>,
    payload: String,
}

#[derive(Debug, Clone, Default)]
struct EventPayloadFacts {
    parseable: bool,
    known_contract: bool,
    canonical_key_values: Vec<String>,
}

#[derive(Debug, Clone, FromRow)]
struct MarkerRow {
    owner_scope: String,
    source: String,
    identity_scope: String,
    native_id: String,
    thread_canonical_key: Option<String>,
}

#[derive(Debug, Clone)]
struct RelationRepair {
    id: i64,
    side: RelationSide,
}

#[derive(Debug, Clone, Copy)]
enum RelationSide {
    Parent,
    Child,
}

#[derive(Debug, Clone)]
struct Repair {
    group_id: i64,
    thread_id: i64,
    old_key: String,
    saved_key: String,
    relations: Vec<RelationRepair>,
}

#[derive(Debug, Clone, Default)]
struct CandidateReasons {
    reasons: BTreeSet<String>,
    relation_repairs: Vec<RelationRepair>,
}

struct Analysis {
    report: RepairReport,
    repairs: Vec<Repair>,
}

async fn analyze_tx(tx: &mut RdbTransaction<'_>) -> Result<Analysis> {
    let typed_inspection = super::thread_groups_user_ids_v1::inspect_for_v3_tx(tx).await;
    let (pending_typed_fields, global_preflight_failure) = match typed_inspection {
        Ok(pending_fields) => (pending_fields, false),
        Err(error) if error.chain().any(|cause| cause.is::<sqlx::Error>()) => return Err(error),
        Err(_) => (BTreeMap::new(), true),
    };
    let mismatches: Vec<MismatchRow> = sqlx::query_as(
        "SELECT member.group_id, member.thread_id AS member_thread_id, thread.id AS thread_id, \
                member.thread_canonical_key AS old_key, canonical.key AS saved_key, \
                member.owner_scope, member.source, member.identity_scope, member.native_id, \
                member.role, member.state AS membership_state, member.provenance, member.user_id AS member_user_id, \
                thread.user_id AS thread_user_id, canonical.owner_scope AS canonical_owner_scope, \
                canonical.user_id AS canonical_user_id, canonical.origin AS key_origin, \
                group_row.status AS group_status, group_row.user_id AS group_user_id, \
                group_row.grouping_authority, group_row.group_canonical_key \
         FROM thread_group_member member \
         JOIN thread ON thread.id = member.thread_id \
         JOIN thread_canonical_key canonical ON canonical.thread_id = thread.id \
         LEFT JOIN thread_group group_row ON group_row.id = member.group_id \
         WHERE member.state = 'active' AND member.thread_canonical_key <> canonical.key \
         ORDER BY member.group_id, thread.id, member.thread_canonical_key",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading active ThreadGroup canonical-key mismatches")?;

    let members: Vec<MemberRow> = sqlx::query_as(
        "SELECT group_id, thread_id, thread_canonical_key AS key, role, state \
         FROM thread_group_member",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading ThreadGroup membership references")?;
    let identities: Vec<IdentityRow> = sqlx::query_as(
        "SELECT owner_scope, source, identity_scope, native_id, thread_id, user_id \
         FROM source_thread_identity",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading resolved source identity ownership")?;
    let canonical_rows: Vec<CanonicalRow> =
        sqlx::query_as("SELECT thread_id, key FROM thread_canonical_key")
            .fetch_all(&mut **tx)
            .await
            .context("reading persisted Thread canonical keys")?;
    let threads: Vec<ThreadRow> = sqlx::query_as("SELECT id, user_id FROM thread")
        .fetch_all(&mut **tx)
        .await
        .context("reading live Thread owners")?;
    let groups: Vec<GroupRow> = sqlx::query_as(
        "SELECT id, user_id, status, grouping_authority, group_canonical_key FROM thread_group",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading ThreadGroup identity rows")?;
    let relations: Vec<RelationRow> = sqlx::query_as(
        "SELECT id, parent_thread_id, child_thread_id, \
                parent_thread_canonical_key AS parent_key, child_thread_canonical_key AS child_key, \
                parent_owner_scope, parent_user_id, parent_source, parent_identity_scope, parent_native_id, \
                child_owner_scope, child_user_id, child_source, child_identity_scope, child_native_id, state \
         FROM thread_relation",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading Thread relation key references")?;
    let audits: Vec<AuditRow> =
        sqlx::query_as("SELECT source_group_id, target_group_id FROM thread_group_audit")
            .fetch_all(&mut **tx)
            .await
            .context("reading immutable ThreadGroup audit references")?;
    let events: Vec<EventRefRow> = sqlx::query_as(EVENT_REFS_SQL)
        .fetch_all(&mut **tx)
        .await
        .context("reading immutable ThreadGroup event references")?;
    let event_payload_facts = events
        .iter()
        .map(|event| inspect_outbox_payload(&event.event_type, &event.payload))
        .collect::<Vec<_>>();
    let markers: Vec<MarkerRow> = sqlx::query_as(
        "SELECT owner_scope, source, identity_scope, native_id, thread_canonical_key \
         FROM thread_deletion_marker",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading immutable deletion marker references")?;
    let canonical_keys_pending_threads: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM thread t LEFT JOIN thread_canonical_key k ON k.thread_id = t.id \
         WHERE k.thread_id IS NULL",
    )
    .fetch_one(&mut **tx)
    .await
    .context("counting Threads without a persisted canonical key")?;

    let identity_index = identities.iter().fold(
        HashMap::<(String, String, String, String), Vec<&IdentityRow>>::new(),
        |mut map, row| {
            map.entry((
                row.owner_scope.clone(),
                row.source.clone(),
                row.identity_scope.clone(),
                row.native_id.clone(),
            ))
            .or_default()
            .push(row);
            map
        },
    );
    let canonical_by_thread = canonical_rows
        .iter()
        .map(|row| (row.thread_id, row))
        .collect::<HashMap<_, _>>();
    let canonical_by_key = canonical_rows.iter().fold(
        HashMap::<&str, Vec<&CanonicalRow>>::new(),
        |mut map, row| {
            map.entry(row.key.as_str()).or_default().push(row);
            map
        },
    );
    let threads_by_id = threads
        .iter()
        .map(|row| (row.id, row))
        .collect::<HashMap<_, _>>();
    let groups_by_id = groups
        .iter()
        .map(|row| (row.id, row))
        .collect::<HashMap<_, _>>();
    let mut report = RepairReport {
        status: String::new(),
        mismatch_rows: mismatches.len() as u64,
        pending_typed_fields,
        canonical_keys_pending_threads: canonical_keys_pending_threads.max(0) as u64,
        summary_policy: "membership_snapshot_changed_regeneration_required_no_summary_rewrite"
            .to_string(),
        ..RepairReport::default()
    };
    let mut distinct_threads = BTreeSet::new();
    let mut reasons = vec![CandidateReasons::default(); mismatches.len()];
    for (index, mismatch) in mismatches.iter().enumerate() {
        distinct_threads.insert(mismatch.thread_id);
        increment(&mut report.mismatch_by_key_origin, &mismatch.key_origin);
        increment(
            &mut report.mismatch_by_membership_state,
            &mismatch.membership_state,
        );
        increment(&mut report.mismatch_by_role, &mismatch.role);
        increment(&mut report.mismatch_by_provenance, &mismatch.provenance);

        if mismatch.member_thread_id != Some(mismatch.thread_id) {
            reasons[index]
                .reasons
                .insert("membership_thread_mismatch".to_string());
        }
        if mismatch.membership_state != "active" {
            reasons[index]
                .reasons
                .insert("non_current_membership".to_string());
        }
        if parse_legacy_owner_scope(&mismatch.owner_scope) != Some(mismatch.thread_user_id)
            || parse_legacy_owner_scope(&mismatch.canonical_owner_scope)
                != Some(mismatch.thread_user_id)
            || mismatch
                .member_user_id
                .is_some_and(|id| id != mismatch.thread_user_id)
            || mismatch
                .canonical_user_id
                .is_some_and(|id| id != mismatch.thread_user_id)
            || mismatch
                .group_user_id
                .is_some_and(|id| id != mismatch.thread_user_id)
        {
            reasons[index].reasons.insert("owner_mismatch".to_string());
        }
        if mismatch.group_status.as_deref() != Some("active") {
            reasons[index]
                .reasons
                .insert("group_not_active_or_missing".to_string());
        }

        match (
            mismatch.source.as_deref(),
            mismatch.identity_scope.as_deref(),
            mismatch.native_id.as_deref(),
        ) {
            (Some(source), Some(scope), Some(native_id)) => {
                let identity_key = (
                    mismatch.owner_scope.clone(),
                    source.to_string(),
                    scope.to_string(),
                    native_id.to_string(),
                );
                match identity_index.get(&identity_key).map(Vec::as_slice) {
                    Some([identity]) if identity.thread_id == mismatch.thread_id => {
                        if identity
                            .user_id
                            .is_some_and(|id| id != mismatch.thread_user_id)
                        {
                            reasons[index].reasons.insert("owner_mismatch".to_string());
                        }
                        let derived_old_key = source_thread_canonical_key(&SourceIdentity::new(
                            mismatch.thread_user_id,
                            source,
                            IdentityScope::known(scope),
                            native_id,
                        ));
                        if derived_old_key.as_deref() != Some(mismatch.old_key.as_str()) {
                            reasons[index]
                                .reasons
                                .insert("old_key_not_identity_derived".to_string());
                        }
                    }
                    Some([_]) => {
                        reasons[index]
                            .reasons
                            .insert("identity_rebound".to_string());
                    }
                    Some(_) => {
                        reasons[index]
                            .reasons
                            .insert("identity_ambiguous".to_string());
                    }
                    None => {
                        reasons[index]
                            .reasons
                            .insert("identity_unproven".to_string());
                    }
                }
            }
            _ => {
                reasons[index]
                    .reasons
                    .insert("identity_unproven".to_string());
            }
        }

        let Some(group) = groups_by_id.get(&mismatch.group_id) else {
            reasons[index]
                .reasons
                .insert("group_identity_unknown".to_string());
            continue;
        };
        if group.status != "active"
            || group.user_id != mismatch.thread_user_id
            || mismatch.grouping_authority.as_deref() != Some(group.grouping_authority.as_str())
            || mismatch.group_canonical_key.as_deref() != Some(group.group_canonical_key.as_str())
        {
            reasons[index]
                .reasons
                .insert("group_identity_unknown".to_string());
        }
        if group.grouping_authority != "reconciler" {
            reasons[index]
                .reasons
                .insert("group_identity_unknown".to_string());
        }
        let roots = members
            .iter()
            .filter(|member| {
                member.group_id == group.id && member.state == "active" && member.role == "root"
            })
            .collect::<Vec<_>>();
        if let [root] = roots.as_slice() {
            if let Some(root_thread_id) = root.thread_id {
                let root_owner_matches = threads_by_id
                    .get(&root_thread_id)
                    .is_some_and(|thread| thread.user_id == group.user_id);
                let root_key_matches = canonical_by_thread
                    .get(&root_thread_id)
                    .is_some_and(|canonical| canonical.key == root.key);
                if !root_owner_matches || !root_key_matches {
                    reasons[index]
                        .reasons
                        .insert("group_root_or_identity_unverified".to_string());
                }
                if reconciler_group_canonical_key(&root.key) != group.group_canonical_key {
                    reasons[index]
                        .reasons
                        .insert("group_identity_unknown".to_string());
                }
            } else {
                reasons[index]
                    .reasons
                    .insert("group_root_or_identity_unverified".to_string());
            }
        } else {
            reasons[index]
                .reasons
                .insert("group_root_ambiguous".to_string());
        }

        for audit in &audits {
            if audit.source_group_id == group.id || audit.target_group_id == Some(group.id) {
                reasons[index]
                    .reasons
                    .insert("group_audit_reference".to_string());
                break;
            }
        }
        for (event, payload_facts) in events.iter().zip(&event_payload_facts) {
            if event.group_id == Some(group.id)
                || event.thread_id == Some(mismatch.thread_id)
                || event_identity_refers_to_candidate(event, mismatch)
            {
                reasons[index]
                    .reasons
                    .insert("event_history_reference".to_string());
            }
            if event_id_refers_to_candidate_key(event, &mismatch.old_key) {
                reasons[index]
                    .reasons
                    .insert("event_id_key_reference".to_string());
            }
            if !payload_facts.parseable {
                reasons[index]
                    .reasons
                    .insert("event_payload_unparseable".to_string());
            } else if !payload_facts.known_contract {
                reasons[index]
                    .reasons
                    .insert("event_payload_contract_unknown".to_string());
            }
            if payload_facts
                .canonical_key_values
                .iter()
                .any(|value| value == &mismatch.old_key)
            {
                reasons[index]
                    .reasons
                    .insert("event_payload_key_reference".to_string());
            }
        }
        for marker in &markers {
            let identity_refers_to_candidate = mismatch.source.as_deref()
                == Some(marker.source.as_str())
                && mismatch.identity_scope.as_deref() == Some(marker.identity_scope.as_str())
                && mismatch.native_id.as_deref() == Some(marker.native_id.as_str())
                && mismatch.owner_scope == marker.owner_scope;
            if identity_refers_to_candidate
                || marker.thread_canonical_key.as_deref() == Some(mismatch.old_key.as_str())
            {
                reasons[index]
                    .reasons
                    .insert("deletion_marker_reference".to_string());
                break;
            }
        }
        for member in &members {
            if member.key == mismatch.old_key
                && !(member.group_id == mismatch.group_id
                    && member.thread_id == Some(mismatch.thread_id)
                    && member.state == "active")
            {
                reasons[index]
                    .reasons
                    .insert("historical_membership_reference".to_string());
            }
            if member.key == mismatch.saved_key
                && !(member.group_id == mismatch.group_id
                    && member.thread_id == Some(mismatch.thread_id)
                    && member.state == "active")
            {
                if member.state == "active" || member.state == "deleted" {
                    reasons[index]
                        .reasons
                        .insert("target_current_key_collision".to_string());
                } else if member.thread_id != Some(mismatch.thread_id) {
                    reasons[index]
                        .reasons
                        .insert("target_historical_key_collision".to_string());
                }
            }
        }
        if canonical_by_key
            .get(mismatch.old_key.as_str())
            .is_some_and(|rows| rows.iter().any(|row| row.thread_id != mismatch.thread_id))
        {
            reasons[index]
                .reasons
                .insert("old_key_belongs_to_another_thread".to_string());
        }
        if canonical_by_key
            .get(mismatch.saved_key.as_str())
            .is_some_and(|rows| rows.iter().any(|row| row.thread_id != mismatch.thread_id))
        {
            reasons[index]
                .reasons
                .insert("saved_key_belongs_to_another_thread".to_string());
        }
        if !is_canonical_key(&mismatch.saved_key) {
            reasons[index]
                .reasons
                .insert("saved_key_malformed".to_string());
        }
    }

    let mut old_key_candidates = HashMap::<&str, Vec<usize>>::new();
    let mut saved_key_candidates = HashMap::<&str, Vec<usize>>::new();
    for (index, mismatch) in mismatches.iter().enumerate() {
        old_key_candidates
            .entry(&mismatch.old_key)
            .or_default()
            .push(index);
        saved_key_candidates
            .entry(&mismatch.saved_key)
            .or_default()
            .push(index);
    }
    for indices in old_key_candidates
        .values()
        .filter(|indices| indices.len() > 1)
    {
        for index in indices {
            reasons[*index]
                .reasons
                .insert("ambiguous_old_key_alias".to_string());
        }
    }
    for indices in saved_key_candidates
        .values()
        .filter(|indices| indices.len() > 1)
    {
        for index in indices {
            reasons[*index]
                .reasons
                .insert("target_key_collision".to_string());
        }
    }

    for relation in &relations {
        inspect_relation_side(
            relation,
            RelationSide::Parent,
            &mismatches,
            &old_key_candidates,
            &identity_index,
            &threads_by_id,
            &mut reasons,
        );
        inspect_relation_side(
            relation,
            RelationSide::Child,
            &mismatches,
            &old_key_candidates,
            &identity_index,
            &threads_by_id,
            &mut reasons,
        );
        inspect_target_relation_side(
            relation,
            RelationSide::Parent,
            &mismatches,
            &saved_key_candidates,
            &mut reasons,
        );
        inspect_target_relation_side(
            relation,
            RelationSide::Child,
            &mismatches,
            &saved_key_candidates,
            &mut reasons,
        );
    }
    inspect_projected_relation_graph(&relations, &mismatches, &mut reasons);
    inspect_projected_display_roots(&members, &relations, &mismatches, &mut reasons);

    let mut repairs = Vec::new();
    let mut affected_groups = BTreeSet::new();
    for (index, mismatch) in mismatches.iter().enumerate() {
        affected_groups.insert(mismatch.group_id);
        if reasons[index].reasons.is_empty() {
            report.safe_candidate_count += 1;
            repairs.push(Repair {
                group_id: mismatch.group_id,
                thread_id: mismatch.thread_id,
                old_key: mismatch.old_key.clone(),
                saved_key: mismatch.saved_key.clone(),
                relations: reasons[index].relation_repairs.clone(),
            });
        } else {
            report.unsafe_candidate_count += 1;
            for reason in &reasons[index].reasons {
                increment(&mut report.stop_reasons, reason);
            }
        }
    }
    if global_preflight_failure {
        increment(&mut report.stop_reasons, "typed_owner_or_marker_preflight");
    }
    report.distinct_threads = distinct_threads.len() as u64;
    report.affected_group_count = affected_groups.len() as u64;
    report.regeneration_group_ids = affected_groups.into_iter().collect();
    report.affected_component_count = count_affected_components(&mismatches, &relations);
    if report.safe_candidate_count == 0 {
        report.summary_policy = "no_membership_snapshot_change_no_summary_regeneration".to_string();
    }
    Ok(Analysis { report, repairs })
}

fn count_affected_components(mismatches: &[MismatchRow], relations: &[RelationRow]) -> u64 {
    if mismatches.is_empty() {
        return 0;
    }
    let mut adjacency = HashMap::<&str, Vec<&str>>::new();
    for relation in relations {
        adjacency
            .entry(&relation.parent_key)
            .or_default()
            .push(&relation.child_key);
        adjacency
            .entry(&relation.child_key)
            .or_default()
            .push(&relation.parent_key);
    }
    let mut candidate_keys = HashMap::<&str, Vec<usize>>::new();
    for (index, mismatch) in mismatches.iter().enumerate() {
        candidate_keys
            .entry(&mismatch.old_key)
            .or_default()
            .push(index);
        candidate_keys
            .entry(&mismatch.saved_key)
            .or_default()
            .push(index);
    }
    let mut parents = (0..mismatches.len()).collect::<Vec<_>>();
    fn find(parents: &mut [usize], index: usize) -> usize {
        if parents[index] != index {
            parents[index] = find(parents, parents[index]);
        }
        parents[index]
    }
    fn union(parents: &mut [usize], left: usize, right: usize) {
        let left_root = find(parents, left);
        let right_root = find(parents, right);
        if left_root != right_root {
            parents[right_root] = left_root;
        }
    }

    let mut group_candidates = HashMap::<i64, Vec<usize>>::new();
    for (index, mismatch) in mismatches.iter().enumerate() {
        group_candidates
            .entry(mismatch.group_id)
            .or_default()
            .push(index);
        union(&mut parents, index, index);
    }
    for indices in group_candidates.values() {
        if let Some(first) = indices.first() {
            for index in &indices[1..] {
                union(&mut parents, *first, *index);
            }
        }
    }

    let mut visited = HashSet::<&str>::new();
    for start in adjacency.keys().copied().collect::<Vec<_>>() {
        if !visited.insert(start) {
            continue;
        }
        let mut stack = vec![start];
        let mut connected_candidates = Vec::new();
        while let Some(key) = stack.pop() {
            if let Some(indices) = candidate_keys.get(key) {
                connected_candidates.extend(indices.iter().copied());
            }
            if let Some(neighbors) = adjacency.get(key) {
                for neighbor in neighbors {
                    if visited.insert(neighbor) {
                        stack.push(neighbor);
                    }
                }
            }
        }
        if let Some(first) = connected_candidates.first() {
            for index in &connected_candidates[1..] {
                union(&mut parents, *first, *index);
            }
        }
    }
    (0..mismatches.len())
        .map(|index| find(&mut parents, index))
        .collect::<HashSet<_>>()
        .len() as u64
}

fn inspect_relation_side(
    relation: &RelationRow,
    side: RelationSide,
    mismatches: &[MismatchRow],
    old_key_candidates: &HashMap<&str, Vec<usize>>,
    identity_index: &HashMap<(String, String, String, String), Vec<&IdentityRow>>,
    threads_by_id: &HashMap<i64, &ThreadRow>,
    reasons: &mut [CandidateReasons],
) {
    let (key, thread_id, owner_scope, user_id, source, scope, native_id) = match side {
        RelationSide::Parent => (
            relation.parent_key.as_str(),
            relation.parent_thread_id,
            relation.parent_owner_scope.as_str(),
            relation.parent_user_id,
            relation.parent_source.as_deref(),
            relation.parent_identity_scope.as_deref(),
            relation.parent_native_id.as_deref(),
        ),
        RelationSide::Child => (
            relation.child_key.as_str(),
            relation.child_thread_id,
            relation.child_owner_scope.as_str(),
            relation.child_user_id,
            relation.child_source.as_deref(),
            relation.child_identity_scope.as_deref(),
            relation.child_native_id.as_deref(),
        ),
    };
    let Some(indices) = old_key_candidates.get(key) else {
        return;
    };
    for index in indices {
        let mismatch = &mismatches[*index];
        let Some(thread) = threads_by_id.get(&mismatch.thread_id) else {
            reasons[*index]
                .reasons
                .insert("relation_thread_unproven".to_string());
            continue;
        };
        if relation.state != "active" {
            reasons[*index]
                .reasons
                .insert("historical_relation_reference".to_string());
            continue;
        }
        if thread_id != Some(mismatch.thread_id)
            || parse_legacy_owner_scope(owner_scope) != Some(thread.user_id)
            || user_id.is_some_and(|id| id != thread.user_id)
        {
            reasons[*index]
                .reasons
                .insert("relation_endpoint_mismatch".to_string());
            continue;
        }
        match (source, scope, native_id) {
            (None, None, None) => {}
            (Some(source), Some(scope), Some(native_id)) => {
                let identity_key = (
                    owner_scope.to_string(),
                    source.to_string(),
                    scope.to_string(),
                    native_id.to_string(),
                );
                match identity_index.get(&identity_key).map(Vec::as_slice) {
                    Some([identity]) if identity.thread_id == mismatch.thread_id => {}
                    Some([_]) => {
                        reasons[*index]
                            .reasons
                            .insert("relation_identity_rebound".to_string());
                        continue;
                    }
                    _ => {
                        reasons[*index]
                            .reasons
                            .insert("relation_identity_unproven".to_string());
                        continue;
                    }
                }
            }
            _ => {
                reasons[*index]
                    .reasons
                    .insert("relation_identity_ambiguous".to_string());
                continue;
            }
        }
        reasons[*index].relation_repairs.push(RelationRepair {
            id: relation.id,
            side,
        });
    }
}

fn inspect_target_relation_side(
    relation: &RelationRow,
    side: RelationSide,
    mismatches: &[MismatchRow],
    saved_key_candidates: &HashMap<&str, Vec<usize>>,
    reasons: &mut [CandidateReasons],
) {
    let (key, thread_id) = match side {
        RelationSide::Parent => (relation.parent_key.as_str(), relation.parent_thread_id),
        RelationSide::Child => (relation.child_key.as_str(), relation.child_thread_id),
    };
    let Some(indices) = saved_key_candidates.get(key) else {
        return;
    };
    for index in indices {
        if thread_id.is_some_and(|thread_id| thread_id != mismatches[*index].thread_id) {
            reasons[*index]
                .reasons
                .insert("canonical_relation_reference_ambiguous".to_string());
        }
    }
}

fn inspect_projected_relation_graph(
    relations: &[RelationRow],
    mismatches: &[MismatchRow],
    reasons: &mut [CandidateReasons],
) {
    let mut projection = HashMap::<&str, &str>::new();
    for mismatch in mismatches {
        projection.insert(&mismatch.old_key, &mismatch.saved_key);
    }
    let mut adjacency = HashMap::<String, Vec<String>>::new();
    let mut undirected = HashMap::<String, Vec<String>>::new();
    let mut incoming_parents = HashMap::<String, HashSet<String>>::new();
    let mut active_child_rows = HashMap::<String, usize>::new();
    let mut self_loop_nodes = HashSet::<String>::new();
    for relation in relations
        .iter()
        .filter(|relation| relation.state == "active")
    {
        let parent = projection
            .get(relation.parent_key.as_str())
            .copied()
            .unwrap_or(relation.parent_key.as_str())
            .to_string();
        let child = projection
            .get(relation.child_key.as_str())
            .copied()
            .unwrap_or(relation.child_key.as_str())
            .to_string();
        adjacency
            .entry(parent.clone())
            .or_default()
            .push(child.clone());
        adjacency.entry(child.clone()).or_default();
        undirected
            .entry(parent.clone())
            .or_default()
            .push(child.clone());
        undirected
            .entry(child.clone())
            .or_default()
            .push(parent.clone());
        incoming_parents
            .entry(child.clone())
            .or_default()
            .insert(parent.clone());
        *active_child_rows.entry(child.clone()).or_default() += 1;
        if parent == child {
            self_loop_nodes.insert(parent);
        }
    }

    let cyclic_nodes = find_cycle_nodes(&adjacency);
    let mut node_reasons = HashMap::<String, BTreeSet<&'static str>>::new();
    let mut visited = HashSet::new();
    let mut component_starts = undirected.keys().cloned().collect::<Vec<_>>();
    component_starts.sort_unstable();
    for start in component_starts {
        if !visited.insert(start.clone()) {
            continue;
        }
        let mut component = Vec::new();
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            component.push(node.clone());
            if let Some(neighbors) = undirected.get(&node) {
                for neighbor in neighbors {
                    if visited.insert(neighbor.clone()) {
                        stack.push(neighbor.clone());
                    }
                }
            }
        }

        let has_self_edge = component.iter().any(|node| self_loop_nodes.contains(node));
        let has_multiple_parents = component.iter().any(|node| {
            incoming_parents
                .get(node)
                .is_some_and(|parents| parents.len() > 1)
        });
        let has_duplicate_child_rows = component
            .iter()
            .any(|node| active_child_rows.get(node).is_some_and(|rows| *rows > 1));
        let has_cycle = component.iter().any(|node| cyclic_nodes.contains(node));
        for node in component {
            let node_reasons = node_reasons.entry(node).or_default();
            if has_self_edge {
                node_reasons.insert("projected_self_edge");
            }
            if has_multiple_parents {
                node_reasons.insert("multiple_active_parents");
            }
            if has_duplicate_child_rows {
                node_reasons.insert("duplicate_projected_child_rows");
            }
            if has_cycle {
                node_reasons.insert("projected_relation_cycle");
            }
        }
    }

    for (index, mismatch) in mismatches.iter().enumerate() {
        for candidate_key in [&mismatch.old_key, &mismatch.saved_key] {
            if let Some(found) = node_reasons.get(candidate_key) {
                reasons[index]
                    .reasons
                    .extend(found.iter().map(|reason| (*reason).to_string()));
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DisplayRoot {
    thread_id: Option<i64>,
    key: String,
}

fn inspect_projected_display_roots(
    members: &[MemberRow],
    relations: &[RelationRow],
    mismatches: &[MismatchRow],
    reasons: &mut [CandidateReasons],
) {
    let projection = mismatches
        .iter()
        .map(|mismatch| (mismatch.old_key.clone(), mismatch.saved_key.clone()))
        .collect::<HashMap<_, _>>();
    let affected_groups = mismatches
        .iter()
        .map(|mismatch| mismatch.group_id)
        .collect::<BTreeSet<_>>();

    for group_id in affected_groups {
        let before = display_root_for_group(group_id, members, relations, &HashMap::new());
        let after = display_root_for_group(group_id, members, relations, &projection);
        let root_identity_is_stable = match (&before, &after) {
            (None, None) => true,
            (Some(before), Some(after)) => {
                before.thread_id == after.thread_id
                    && (before.thread_id.is_some() || before.key == after.key)
            }
            _ => false,
        };
        if !root_identity_is_stable {
            for (index, mismatch) in mismatches.iter().enumerate() {
                if mismatch.group_id == group_id {
                    reasons[index]
                        .reasons
                        .insert("display_root_changed".to_string());
                }
            }
        }
    }
}

fn display_root_for_group(
    group_id: i64,
    members: &[MemberRow],
    relations: &[RelationRow],
    projection: &HashMap<String, String>,
) -> Option<DisplayRoot> {
    let current_members = members
        .iter()
        .filter(|member| {
            member.group_id == group_id && matches!(member.state.as_str(), "active" | "deleted")
        })
        .map(|member| DisplayRoot {
            thread_id: member.thread_id,
            key: projection
                .get(&member.key)
                .cloned()
                .unwrap_or_else(|| member.key.clone()),
        })
        .collect::<Vec<_>>();
    if current_members.is_empty() {
        return None;
    }

    let group_keys = current_members
        .iter()
        .map(|member| member.key.as_str())
        .collect::<HashSet<_>>();
    let mut children_with_group_parent = HashSet::new();
    for relation in relations
        .iter()
        .filter(|relation| relation.state == "active")
    {
        let parent_key = projection
            .get(&relation.parent_key)
            .map(String::as_str)
            .unwrap_or(&relation.parent_key);
        if !group_keys.contains(parent_key) {
            continue;
        }
        let child_key = projection
            .get(&relation.child_key)
            .map(String::as_str)
            .unwrap_or(&relation.child_key);
        if group_keys.contains(child_key) {
            children_with_group_parent.insert(child_key.to_string());
        }
    }

    current_members
        .into_iter()
        .filter(|member| !children_with_group_parent.contains(&member.key))
        .min_by(|left, right| left.key.cmp(&right.key))
}

fn find_cycle_nodes(adjacency: &HashMap<String, Vec<String>>) -> HashSet<String> {
    let edges = adjacency
        .iter()
        .flat_map(|(parent, children)| children.iter().map(|child| (parent.clone(), child.clone())))
        .collect::<Vec<_>>();
    find_cycle_nodes_from_edges(&edges)
}

fn find_cycle_nodes_from_edges(edges: &[(String, String)]) -> HashSet<String> {
    let mut adjacency = HashMap::<String, Vec<String>>::new();
    let mut reverse = HashMap::<String, Vec<String>>::new();
    for (parent, child) in edges {
        adjacency
            .entry(parent.clone())
            .or_default()
            .push(child.clone());
        adjacency.entry(child.clone()).or_default();
        reverse
            .entry(child.clone())
            .or_default()
            .push(parent.clone());
        reverse.entry(parent.clone()).or_default();
    }
    for children in adjacency.values_mut() {
        children.sort_unstable();
        children.dedup();
    }
    for parents in reverse.values_mut() {
        parents.sort_unstable();
        parents.dedup();
    }

    let mut nodes = adjacency.keys().cloned().collect::<Vec<_>>();
    nodes.sort_unstable();
    let mut visited = HashSet::new();
    let mut finished = Vec::with_capacity(nodes.len());
    for start in nodes {
        if visited.contains(&start) {
            continue;
        }
        let mut stack = vec![(start, false)];
        while let Some((node, expanded)) = stack.pop() {
            if expanded {
                finished.push(node);
                continue;
            }
            if !visited.insert(node.clone()) {
                continue;
            }
            stack.push((node.clone(), true));
            if let Some(children) = adjacency.get(&node) {
                for child in children.iter().rev() {
                    if !visited.contains(child) {
                        stack.push((child.clone(), false));
                    }
                }
            }
        }
    }

    let mut assigned = HashSet::new();
    let mut cyclic = HashSet::new();
    while let Some(start) = finished.pop() {
        if !assigned.insert(start.clone()) {
            continue;
        }
        let mut component = Vec::new();
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            component.push(node.clone());
            if let Some(parents) = reverse.get(&node) {
                for parent in parents {
                    if assigned.insert(parent.clone()) {
                        stack.push(parent.clone());
                    }
                }
            }
        }
        let has_self_loop = component.iter().any(|node| {
            adjacency
                .get(node)
                .is_some_and(|children| children.iter().any(|child| child == node))
        });
        if component.len() > 1 || has_self_loop {
            cyclic.extend(component);
        }
    }
    cyclic
}

async fn update_membership_key_tx(tx: &mut RdbTransaction<'_>, repair: &Repair) -> Result<()> {
    let changed = sqlx::query(MEMBER_REPAIR_SQL)
        .bind(&repair.saved_key)
        .bind(repair.group_id)
        .bind(repair.thread_id)
        .bind(&repair.old_key)
        .execute(&mut **tx)
        .await
        .context("updating a proven live membership canonical-key copy")?
        .rows_affected();
    if changed != 1 {
        bail!("live membership changed after canonical repair analysis");
    }
    Ok(())
}

async fn update_relation_key_tx(
    tx: &mut RdbTransaction<'_>,
    repair: &Repair,
    relation: &RelationRepair,
) -> Result<()> {
    let sql = match relation.side {
        RelationSide::Parent => PARENT_RELATION_REPAIR_SQL,
        RelationSide::Child => CHILD_RELATION_REPAIR_SQL,
    };
    if sqlx::query(sql)
        .bind(&repair.saved_key)
        .bind(relation.id)
        .bind(&repair.old_key)
        .execute(&mut **tx)
        .await
        .context("updating a proven live relation endpoint key")?
        .rows_affected()
        != 1
    {
        bail!("live relation changed after canonical repair analysis");
    }
    Ok(())
}

async fn acquire_group_membership_mutation_lock_tx(tx: &mut RdbTransaction<'_>) -> Result<()> {
    #[cfg(feature = "postgres")]
    // This key is identical to the shared lock repository's group-membership namespace.
    sqlx::query(MUTATION_LOCK_SQL)
        .bind("thread_group_membership_mutation")
        .execute(&mut **tx)
        .await
        .context("acquiring the shared ThreadGroup membership mutation lock")?;
    #[cfg(not(feature = "postgres"))]
    let _ = tx;
    Ok(())
}

async fn canonical_prerequisite_tx(tx: &mut RdbTransaction<'_>) -> Result<String> {
    let Some(row) = state::load_tx(tx, CANONICAL_KEYS_IDENTITY).await? else {
        return Ok("pending".to_string());
    };
    let expected = super::catalog::thread_groups_canonical_keys_v2()?.canonical_definition_digest;
    if row.canonical_definition_digest != expected {
        return Ok("definition_mismatch".to_string());
    }
    Ok(if row.kind()? == state::TaskStateKind::Completed {
        "completed".to_string()
    } else {
        row.state
    })
}

async fn prerequisite_status(pool: &RdbPool) -> Result<String> {
    let Some(row) = state::load(pool, CANONICAL_KEYS_IDENTITY).await? else {
        return Ok("pending".to_string());
    };
    let expected = super::catalog::thread_groups_canonical_keys_v2()?.canonical_definition_digest;
    if row.canonical_definition_digest != expected {
        return Ok("definition_mismatch".to_string());
    }
    Ok(if row.kind()? == state::TaskStateKind::Completed {
        "completed".to_string()
    } else {
        row.state
    })
}

fn set_report_status(report: &mut RepairReport) {
    report.status = if report.prerequisite_status != "completed" {
        "prerequisite_pending".to_string()
    } else if !report.stop_reasons.is_empty() {
        "blocked".to_string()
    } else {
        "ready".to_string()
    };
}

fn ensure_verified(report: &RepairReport) -> Result<()> {
    if report.prerequisite_status != "completed" {
        bail!("canonical-key generation @2 prerequisite is not completed");
    }
    if report.mismatch_rows != 0 {
        bail!(
            "post-repair verification found {} active canonical membership mismatch row(s)",
            report.mismatch_rows
        );
    }
    if !report.pending_typed_fields.is_empty() {
        bail!("post-repair verification found pending typed owner IDs");
    }
    if !report.stop_reasons.is_empty() {
        bail!("post-repair verification found unsafe ThreadGroup references");
    }
    Ok(())
}

fn increment(map: &mut BTreeMap<String, u64>, value: &str) {
    *map.entry(value.to_string()).or_default() += 1;
}

fn is_canonical_key(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn event_identity_refers_to_candidate(event: &EventRefRow, mismatch: &MismatchRow) -> bool {
    let has_identity_reference = event.source.is_some()
        || event.identity_scope.is_some()
        || event.owner_scope.is_some()
        || event.user_id.is_some()
        || event.native_id_ref.is_some();
    if !has_identity_reference {
        return false;
    }
    if event
        .source
        .as_deref()
        .is_some_and(|value| mismatch.source.as_deref() != Some(value))
        || event
            .identity_scope
            .as_deref()
            .is_some_and(|value| mismatch.identity_scope.as_deref() != Some(value))
        || event
            .owner_scope
            .as_deref()
            .is_some_and(|value| value != mismatch.owner_scope)
        || event
            .user_id
            .is_some_and(|value| value != mismatch.thread_user_id)
        || event
            .native_id_ref
            .as_deref()
            .is_some_and(|value| mismatch.native_id.as_deref() != Some(value))
    {
        return false;
    }
    true
}

fn event_id_refers_to_candidate_key(event: &EventRefRow, canonical_key: &str) -> bool {
    use app::app::thread_group::{ThreadGroupEventType, event_id_for};

    let derived_id = match event.event_type.as_str() {
        "thread_group_relation_selected" => {
            event_id_for(ThreadGroupEventType::RelationSelected, canonical_key)
        }
        "thread_group_conflict_detected" => {
            event_id_for(ThreadGroupEventType::ConflictDetected, canonical_key)
        }
        _ => return false,
    };
    event.event_id == derived_id
}

fn inspect_outbox_payload(event_type: &str, payload: &str) -> EventPayloadFacts {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(payload) else {
        return EventPayloadFacts::default();
    };
    let mut facts = EventPayloadFacts {
        parseable: true,
        known_contract: event_payload_contract_is_known(event_type, &value),
        canonical_key_values: Vec::new(),
    };
    collect_canonical_key_values(&value, &mut facts.canonical_key_values);
    facts
}

fn event_payload_contract_is_known(event_type: &str, payload: &serde_json::Value) -> bool {
    let Some(object) = payload.as_object() else {
        return false;
    };
    let expected_fields: &[&str] = match event_type {
        "thread_group_observation_recorded" => &["state", "adapter_version", "evidence_kind"],
        "thread_group_relation_selected" => &[
            "parent_thread_canonical_key",
            "relation_type",
            "selection_basis",
        ],
        "thread_group_conflict_detected" => {
            if !object
                .get("reason")
                .is_some_and(serde_json::Value::is_string)
            {
                return false;
            }
            return match object.get("retracted_relation_id") {
                None => object.len() == 1,
                Some(value) => object.len() == 2 && value.as_i64().is_some_and(|id| id > 0),
            };
        }
        _ => return false,
    };
    object.len() == expected_fields.len()
        && expected_fields
            .iter()
            .all(|field| object.get(*field).is_some_and(serde_json::Value::is_string))
}

fn collect_canonical_key_values(value: &serde_json::Value, values: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(object) => {
            for (field, value) in object {
                if field.to_ascii_lowercase().contains("canonical_key")
                    && let Some(key) = value.as_str()
                {
                    values.push(key.to_string());
                }
                collect_canonical_key_values(value, values);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                collect_canonical_key_values(item, values);
            }
        }
        _ => {}
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::{ThreadGroupsUserIdsV3Task, find_cycle_nodes_from_edges};
    use crate::db_migrate::{
        catalog,
        state::{self, TaskStateKind},
    };
    use common::thread_group_key::{
        IdentityScope, SourceIdentity, reconciler_group_canonical_key, source_thread_canonical_key,
    };
    use infra::infra::thread_group::test_support::{insert_thread, key};
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::TEST_RUNTIME;
    use sqlx::Row;
    use std::sync::atomic::{AtomicI64, Ordering};

    static FIXTURE_ID: AtomicI64 = AtomicI64::new(50_000_000);

    async fn test_pool() -> &'static RdbPool {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(include_str!("../../../infra/sql/sqlite/001_schema.sql"))
            .execute(&pool)
            .await
            .unwrap();
        for migration in [
            include_str!(
                "../../../infra/atlas/sqlite/migrations/20260920000001_thread_group_schema.sql"
            ),
            include_str!(
                "../../../infra/atlas/sqlite/migrations/20260926000001_thread_group_memory_relation.sql"
            ),
            include_str!(
                "../../../infra/atlas/sqlite/migrations/20260930000001_thread_group_user_ids.sql"
            ),
        ] {
            let ddl = migration
                .split("UPDATE memories_schema_contract")
                .next()
                .unwrap();
            sqlx::raw_sql(ddl).execute(&pool).await.unwrap();
        }
        Box::leak(Box::new(pool))
    }

    struct Seed {
        group_id: i64,
        thread_id: i64,
        root_thread_id: i64,
        old_key: String,
        saved_key: String,
        root_key: String,
    }

    async fn prepare_task_rows(pool: &RdbPool) {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS memories_data_migration_task_state (\
               task_identity TEXT PRIMARY KEY, canonical_definition_digest TEXT NOT NULL, state TEXT NOT NULL,\
               execution_id TEXT, holder_id TEXT, fencing_token BIGINT NOT NULL, heartbeat_at BIGINT,\
               lease_expires_at BIGINT, attempt_count BIGINT NOT NULL, checkpoint TEXT,\
               failure_classification TEXT, started_at BIGINT, updated_at BIGINT NOT NULL, completed_at BIGINT\
             )",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "DELETE FROM memories_data_migration_task_state \
             WHERE task_identity IN ('thread-groups-user-ids-v1@3', 'thread-groups-user-ids-v1@2', \
                                     'thread-groups-canonical-keys-v1@2')",
        )
        .execute(pool)
        .await
        .unwrap();
        let legacy_user_ids = catalog::thread_groups_user_ids_v2().unwrap();
        sqlx::query(
            "INSERT INTO memories_data_migration_task_state \
             (task_identity, canonical_definition_digest, state, execution_id, holder_id, fencing_token, \
              attempt_count, checkpoint, failure_classification, started_at, updated_at) \
             VALUES (?, ?, 'failed', 'legacy-execution', 'legacy-holder', 13, 4, \
                     'legacy-checkpoint', 'legacy-failure', 2, 3)",
        )
        .bind(legacy_user_ids.identity())
        .bind(legacy_user_ids.canonical_definition_digest)
        .execute(pool)
        .await
        .unwrap();
        let canonical = catalog::thread_groups_canonical_keys_v2().unwrap();
        sqlx::query(
            "INSERT INTO memories_data_migration_task_state \
             (task_identity, canonical_definition_digest, state, fencing_token, attempt_count, updated_at, completed_at) \
             VALUES (?, ?, 'completed', 7, 1, 1, 1)",
        )
        .bind(canonical.identity())
        .bind(canonical.canonical_definition_digest)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_membership(pool: &RdbPool, mismatch: bool) -> Seed {
        let n = FIXTURE_ID.fetch_add(10, Ordering::Relaxed);
        let group_id = n;
        let thread_id = n + 1;
        let root_thread_id = n + 2;
        let native_id = format!("thread-alias-{n}");
        let old_key = source_thread_canonical_key(&SourceIdentity::new(
            1,
            "codex",
            IdentityScope::known(""),
            &native_id,
        ))
        .unwrap();
        let saved_key = key((n + 1) as u64);
        let root_key = "0".repeat(64);

        insert_thread(pool, thread_id, None).await;
        insert_thread(pool, root_thread_id, None).await;
        let group_key = reconciler_group_canonical_key(&root_key);
        sqlx::query(
            "INSERT INTO thread_group \
             (id, user_id, group_canonical_key, title, status, grouping_authority, redirect_to_group_id, created_at, updated_at) \
             VALUES (?, 1, ?, NULL, 'active', 'reconciler', NULL, 10, 11)",
        )
        .bind(group_id)
        .bind(group_key)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO thread_group_member \
             (group_id, thread_id, thread_canonical_key, owner_scope, source, identity_scope, native_id, role, state, provenance, deleted_at, created_at, updated_at, user_id) \
             VALUES (?, ?, ?, 'user:1', 'codex', '', ?, 'root', 'active', 'reconciler', NULL, 20, 21, 1)",
        )
        .bind(group_id)
        .bind(root_thread_id)
        .bind(&root_key)
        .bind(format!("root-{n}"))
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO thread_group_member \
             (group_id, thread_id, thread_canonical_key, owner_scope, source, identity_scope, native_id, role, state, provenance, deleted_at, created_at, updated_at, user_id) \
             VALUES (?, ?, ?, 'user:1', 'codex', '', ?, 'member', 'active', 'reconciler', NULL, 30, 31, 1)",
        )
        .bind(group_id)
        .bind(thread_id)
        .bind(if mismatch { &old_key } else { &saved_key })
        .bind(&native_id)
        .execute(pool)
        .await
        .unwrap();
        for (canonical_thread_id, canonical_key, origin) in [
            (thread_id, &saved_key, "backfill_mapping"),
            (root_thread_id, &root_key, "source_identity"),
        ] {
            sqlx::query(
                "INSERT INTO thread_canonical_key \
                 (thread_id, owner_scope, key, origin, assigned_at, user_id) \
                 VALUES (?, 'user:1', ?, ?, 40, 1)",
            )
            .bind(canonical_thread_id)
            .bind(canonical_key)
            .bind(origin)
            .execute(pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO source_thread_identity \
             (owner_scope, source, identity_scope, native_id, thread_id, resolution_state, first_seen_at, last_seen_at, user_id) \
             VALUES ('user:1', 'codex', '', ?, ?, 'resolved', 1, 1, 1)",
        )
        .bind(&native_id)
        .bind(thread_id)
        .execute(pool)
        .await
        .unwrap();

        Seed {
            group_id,
            thread_id,
            root_thread_id,
            old_key,
            saved_key,
            root_key,
        }
    }

    async fn insert_live_child_relation(pool: &RdbPool, seed: &Seed) -> (i64, i64) {
        let relation_id = seed.group_id + 5_000_000;
        sqlx::query(
            "INSERT INTO thread_relation \
             (id, parent_thread_id, child_thread_id, parent_thread_canonical_key, child_thread_canonical_key, \
              parent_owner_scope, parent_source, parent_identity_scope, parent_native_id, \
              child_owner_scope, child_source, child_identity_scope, child_native_id, relation_type, state, \
              selection_basis, source_confidence, selected_observation_id, selected_operator_decision_id, \
              created_at, updated_at, parent_user_id, child_user_id) \
             VALUES (?, ?, ?, ?, ?, 'user:1', NULL, NULL, NULL, 'user:1', 'codex', '', ?, \
                     'delegated', 'active', 'source_exact', 'exact', ?, NULL, 40, 41, 1, 1)",
        )
        .bind(relation_id)
        .bind(seed.root_thread_id)
        .bind(seed.thread_id)
        .bind(&seed.root_key)
        .bind(&seed.old_key)
        .bind(format!("thread-alias-{}", seed.group_id))
        .bind(relation_id + 1)
        .execute(pool)
        .await
        .unwrap();
        (relation_id, 41)
    }

    async fn insert_deleted_placeholder(pool: &RdbPool, seed: &Seed) {
        sqlx::query(
            "INSERT INTO thread_group_member \
             (group_id, thread_id, thread_canonical_key, owner_scope, source, identity_scope, native_id, \
              role, state, provenance, deleted_at, created_at, updated_at, user_id) \
             VALUES (?, NULL, ?, 'user:1', NULL, NULL, NULL, 'member', 'deleted', 'operator', 50, 50, 51, 1)",
        )
        .bind(seed.group_id + 100)
        .bind(&seed.saved_key)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_deleted_placeholder_in_group(
        pool: &RdbPool,
        group_id: i64,
        canonical_key: &str,
    ) {
        sqlx::query(
            "INSERT INTO thread_group_member \
             (group_id, thread_id, thread_canonical_key, owner_scope, source, identity_scope, native_id, \
              role, state, provenance, deleted_at, created_at, updated_at, user_id) \
             VALUES (?, NULL, ?, 'user:1', NULL, NULL, NULL, 'member', 'deleted', 'operator', 50, 50, 51, 1)",
        )
        .bind(group_id)
        .bind(canonical_key)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_outbox_payload(
        pool: &RdbPool,
        event_id: &str,
        event_type: &str,
        payload: &str,
    ) {
        sqlx::query(
            "INSERT INTO thread_group_event_outbox \
             (event_id, event_type, operation_id, policy_version, payload, created_at) \
             VALUES (?, ?, 'fixture-operation', 'fixture-policy', ?, 60)",
        )
        .bind(event_id)
        .bind(event_type)
        .bind(payload)
        .execute(pool)
        .await
        .unwrap();
    }

    fn hex_successor(value: &str) -> String {
        let mut bytes = value.as_bytes().to_vec();
        for byte in bytes.iter_mut().rev() {
            if *byte != b'f' {
                *byte = if *byte == b'9' { b'a' } else { *byte + 1 };
                return String::from_utf8(bytes).unwrap();
            }
            *byte = b'0';
        }
        panic!("fixture key must have a hexadecimal successor");
    }

    fn hex_predecessor(value: &str) -> String {
        let mut bytes = value.as_bytes().to_vec();
        for byte in bytes.iter_mut().rev() {
            if *byte != b'0' {
                *byte = if *byte == b'a' { b'9' } else { *byte - 1 };
                return String::from_utf8(bytes).unwrap();
            }
            *byte = b'f';
        }
        panic!("fixture key must have a hexadecimal predecessor");
    }

    async fn insert_projected_self_edge(pool: &RdbPool, seed: &Seed) {
        sqlx::query(
            "INSERT INTO thread_relation \
             (id, parent_thread_id, child_thread_id, parent_thread_canonical_key, child_thread_canonical_key, \
              parent_owner_scope, parent_source, parent_identity_scope, parent_native_id, \
              child_owner_scope, child_source, child_identity_scope, child_native_id, relation_type, state, \
              selection_basis, source_confidence, selected_observation_id, selected_operator_decision_id, \
              created_at, updated_at, parent_user_id, child_user_id) \
             VALUES (?, ?, ?, ?, ?, 'user:1', 'codex', '', ?, 'user:1', 'codex', '', ?, \
                     'delegated', 'active', 'source_exact', 'exact', ?, NULL, 40, 41, 1, 1)",
        )
        .bind(seed.group_id + 200)
        .bind(seed.thread_id)
        .bind(seed.thread_id)
        .bind(&seed.old_key)
        .bind(&seed.saved_key)
        .bind(format!("thread-alias-{}", seed.group_id))
        .bind(format!("thread-alias-{}", seed.group_id))
        .bind(seed.group_id + 201)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_relation_edge(
        pool: &RdbPool,
        relation_id: i64,
        parent_thread_id: i64,
        child_thread_id: i64,
        parent_key: &str,
        child_key: &str,
    ) {
        sqlx::query(
            "INSERT INTO thread_relation \
             (id, parent_thread_id, child_thread_id, parent_thread_canonical_key, child_thread_canonical_key, \
              parent_owner_scope, parent_source, parent_identity_scope, parent_native_id, \
              child_owner_scope, child_source, child_identity_scope, child_native_id, relation_type, state, \
              selection_basis, source_confidence, selected_observation_id, selected_operator_decision_id, \
              created_at, updated_at, parent_user_id, child_user_id) \
             VALUES (?, ?, ?, ?, ?, 'user:1', NULL, NULL, NULL, 'user:1', NULL, NULL, NULL, \
                     'delegated', 'active', 'operator_confirmation', NULL, NULL, ?, 40, 41, 1, 1)",
        )
        .bind(relation_id)
        .bind(parent_thread_id)
        .bind(child_thread_id)
        .bind(parent_key)
        .bind(child_key)
        .bind(relation_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_unrelated_legacy_deletion_marker(pool: &RdbPool, seed: &Seed) -> String {
        let thread_id = seed.thread_id + 300;
        let marker_key = key((seed.group_id + 300) as u64);
        let native_id = format!("unrelated-marker-{}", seed.group_id);
        insert_thread(pool, thread_id, None).await;
        sqlx::query(
            "INSERT INTO thread_canonical_key (thread_id, owner_scope, key, origin, assigned_at, user_id) \
             VALUES (?, 'user:1', ?, 'backfill_mapping', 40, NULL)",
        )
        .bind(thread_id)
        .bind(&marker_key)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO source_thread_identity \
             (owner_scope, source, identity_scope, native_id, thread_id, resolution_state, first_seen_at, last_seen_at, user_id) \
             VALUES ('user:1', 'codex', '', ?, ?, 'resolved', 1, 1, NULL)",
        )
        .bind(&native_id)
        .bind(thread_id)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO thread_deletion_marker \
             (owner_scope, source, identity_scope, native_id, forbid_reimport, recursive, actor_id, reason, deleted_at, user_id, thread_canonical_key) \
             VALUES ('user:1', 'codex', '', ?, TRUE, FALSE, 'fixture', NULL, 2, NULL, NULL)",
        )
        .bind(&native_id)
        .execute(pool)
        .await
        .unwrap();
        native_id
    }

    fn task(pool: &RdbPool) -> ThreadGroupsUserIdsV3Task {
        ThreadGroupsUserIdsV3Task::new(pool.clone(), catalog::thread_groups_user_ids_v3().unwrap())
            .unwrap()
    }

    async fn business_table_snapshot(pool: &RdbPool) -> Vec<(String, String)> {
        const TABLES: &[&str] = &[
            "thread",
            "thread_group",
            "thread_group_member",
            "thread_relation",
            "thread_observation",
            "thread_group_candidate_association",
            "source_thread_identity",
            "thread_canonical_key",
            "thread_deletion_marker",
            "operator_decision",
            "manual_collection",
            "manual_collection_member",
            "thread_group_event_outbox",
            "thread_group_audit",
            "thread_group_memory_relation",
        ];
        let mut snapshots = Vec::with_capacity(TABLES.len());
        for table in TABLES {
            let table_identifier = format!("\"{}\"", table.replace('\"', "\"\""));
            let pragma_sql = format!("PRAGMA table_info({table_identifier})");
            let columns = sqlx::query(sqlx::AssertSqlSafe(pragma_sql.as_str()))
                .fetch_all(pool)
                .await
                .unwrap()
                .into_iter()
                .map(|row| row.get::<String, _>("name"))
                .collect::<Vec<_>>();
            assert!(!columns.is_empty(), "missing fixture table {table}");
            let array_values = columns
                .iter()
                .map(|column| {
                    let identifier = format!("\"{}\"", column.replace('\"', "\"\""));
                    format!(
                        "CASE WHEN typeof({identifier}) = 'blob' THEN hex({identifier}) ELSE {identifier} END"
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let snapshot_sql = format!(
                "SELECT COALESCE(json_group_array(json(row_json)), '[]') \
                 FROM (SELECT json_array({array_values}) AS row_json \
                       FROM {table_identifier} ORDER BY row_json)"
            );
            let snapshot: String = sqlx::query_scalar(sqlx::AssertSqlSafe(snapshot_sql.as_str()))
                .fetch_one(pool)
                .await
                .unwrap();
            snapshots.push(((*table).to_string(), snapshot));
        }
        snapshots
    }

    #[test]
    fn overlapping_cycle_nodes_are_complete_and_edge_order_independent() {
        let edges = vec![
            ("A".to_string(), "B".to_string()),
            ("A".to_string(), "C".to_string()),
            ("B".to_string(), "A".to_string()),
            ("C".to_string(), "B".to_string()),
        ];
        let expected: std::collections::HashSet<String> =
            ["A", "B", "C"].into_iter().map(str::to_string).collect();
        assert_eq!(find_cycle_nodes_from_edges(&edges), expected);
        assert_eq!(
            find_cycle_nodes_from_edges(&edges.into_iter().rev().collect::<Vec<_>>()),
            expected
        );
    }

    #[test]
    fn overlapping_projection_cycles_block_the_candidate_in_both_relation_orders() {
        TEST_RUNTIME.block_on(async {
            for reverse in [false, true] {
                let pool = test_pool().await;
                prepare_task_rows(pool).await;
                let seed = seed_membership(pool, true).await;
                let root_key = "0".repeat(64);
                sqlx::query("UPDATE thread_canonical_key SET key = ? WHERE thread_id = ?")
                    .bind(&root_key)
                    .bind(seed.root_thread_id)
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query(
                    "UPDATE thread_group_member SET thread_canonical_key = ? \
                     WHERE group_id = ? AND thread_id = ?",
                )
                .bind(&root_key)
                .bind(seed.group_id)
                .bind(seed.root_thread_id)
                .execute(pool)
                .await
                .unwrap();
                sqlx::query("UPDATE thread_group SET group_canonical_key = ? WHERE id = ?")
                    .bind(reconciler_group_canonical_key(&root_key))
                    .bind(seed.group_id)
                    .execute(pool)
                    .await
                    .unwrap();

                let other_thread = seed.thread_id + 3_000;
                let other_key = key((seed.group_id + 3_000) as u64);
                insert_thread(pool, other_thread, None).await;
                sqlx::query(
                    "INSERT INTO thread_canonical_key \
                     (thread_id, owner_scope, key, origin, assigned_at, user_id) \
                     VALUES (?, 'user:1', ?, 'backfill_mapping', 40, 1)",
                )
                .bind(other_thread)
                .bind(&other_key)
                .execute(pool)
                .await
                .unwrap();
                sqlx::query("DROP INDEX thread_relation_active_child_canonical_key")
                    .execute(pool)
                    .await
                    .unwrap();
                let edge_id = seed.group_id + 3_100;
                let edges = [
                    (
                        edge_id,
                        seed.root_thread_id,
                        other_thread,
                        root_key.clone(),
                        other_key.clone(),
                    ),
                    (
                        edge_id + 1,
                        seed.root_thread_id,
                        seed.thread_id,
                        root_key.clone(),
                        seed.old_key.clone(),
                    ),
                    (
                        edge_id + 2,
                        other_thread,
                        seed.root_thread_id,
                        other_key.clone(),
                        root_key.clone(),
                    ),
                    (
                        edge_id + 3,
                        seed.thread_id,
                        other_thread,
                        seed.old_key.clone(),
                        other_key.clone(),
                    ),
                ];
                let ordered_edges = if reverse {
                    edges.iter().rev().collect::<Vec<_>>()
                } else {
                    edges.iter().collect::<Vec<_>>()
                };
                for edge in ordered_edges {
                    insert_relation_edge(pool, edge.0, edge.1, edge.2, &edge.3, &edge.4).await;
                }

                let report = task(pool).inspect().await.unwrap();
                assert_eq!(report.safe_candidate_count, 0);
                assert_eq!(
                    report.stop_reasons.get("projected_relation_cycle"),
                    Some(&1)
                );
                assert_eq!(report.stop_reasons.get("multiple_active_parents"), Some(&1));
                assert!(
                    task(pool)
                        .apply("overlapping-cycle", "test-holder")
                        .await
                        .is_err()
                );
                assert!(
                    state::load(pool, "thread-groups-user-ids-v1@3")
                        .await
                        .unwrap()
                        .is_none()
                );
            }
        });
    }

    #[test]
    fn repair_updates_only_proven_live_alias_references_and_reports_summary_regeneration() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let (relation_id, relation_updated_at) = insert_live_child_relation(pool, &seed).await;
            let marker_native_id = insert_unrelated_legacy_deletion_marker(pool, &seed).await;
            let saved_group_key: String = sqlx::query_scalar(
                "SELECT group_canonical_key FROM thread_group WHERE id = ?",
            )
            .bind(seed.group_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let read_service = app::app::thread_group::ThreadGroupReadService::new(pool);
            let runtime_view_before = read_service
                .list_groups(false, None, None, Some(1))
                .await
                .unwrap()
                .into_iter()
                .find(|view| view.id == seed.group_id)
                .unwrap();
            assert_eq!(runtime_view_before.root_thread_id, Some(seed.root_thread_id));
            assert_eq!(
                runtime_view_before.root_thread_canonical_key.as_deref(),
                Some(seed.root_key.as_str())
            );
            let before_member_updated_at: i64 = sqlx::query_scalar(
                "SELECT updated_at FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let canonical_state_before = state::load(pool, "thread-groups-canonical-keys-v1@2")
                .await
                .unwrap()
                .unwrap();
            let legacy_user_ids_state_before =
                state::load(pool, "thread-groups-user-ids-v1@2").await.unwrap();
            assert!(legacy_user_ids_state_before.is_some());

            let inspection = task(pool).inspect().await.unwrap();
            assert_eq!(inspection.mismatch_rows, 1);
            assert_eq!(inspection.distinct_threads, 1);
            assert_eq!(inspection.mismatch_by_key_origin.get("backfill_mapping"), Some(&1));
            assert_eq!(inspection.safe_candidate_count, 1);
            assert_eq!(inspection.unsafe_candidate_count, 0);
            assert!(!inspection.stop_reasons.contains_key("display_root_changed"));
            assert_eq!(inspection.affected_component_count, 1);
            assert_eq!(inspection.regeneration_group_ids, vec![seed.group_id]);

            let preview = task(pool).dry_run().await.unwrap();
            assert_eq!(preview, inspection);
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let key_before_apply: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(key_before_apply, seed.old_key);

            let result = task(pool)
                .apply("repair-execution", "test-holder")
                .await
                .unwrap();
            assert_eq!(result.repaired_memberships, 1);
            assert_eq!(result.regeneration_group_ids, vec![seed.group_id]);
            task(pool).verify().await.unwrap();

            let repaired_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let member_updated_at: i64 = sqlx::query_scalar(
                "SELECT updated_at FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let group_key_after: String = sqlx::query_scalar(
                "SELECT group_canonical_key FROM thread_group WHERE id = ?",
            )
            .bind(seed.group_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let relation_key_after: String = sqlx::query_scalar(
                "SELECT child_thread_canonical_key FROM thread_relation WHERE id = ?",
            )
            .bind(relation_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let relation_updated_at_after: i64 = sqlx::query_scalar(
                "SELECT updated_at FROM thread_relation WHERE id = ?",
            )
            .bind(relation_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let marker_key_after: Option<String> = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_deletion_marker WHERE native_id = ?",
            )
            .bind(&marker_native_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let marker_typed_user_id: Option<i64> = sqlx::query_scalar(
                "SELECT user_id FROM thread_deletion_marker WHERE native_id = ?",
            )
            .bind(&marker_native_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let marker_forbid_reimport: bool = sqlx::query_scalar(
                "SELECT forbid_reimport FROM thread_deletion_marker WHERE native_id = ?",
            )
            .bind(&marker_native_id)
            .fetch_one(pool)
            .await
            .unwrap();
            use app::app::thread_group::{
                MembershipSnapshotEntry, membership_snapshot_digest,
            };
            let snapshot_digest = |thread_canonical_key: String| {
                membership_snapshot_digest(&[MembershipSnapshotEntry {
                    thread_canonical_key,
                    state: "active".to_string(),
                    role: "member".to_string(),
                    deleted_at: None,
                    updated_at: Some(before_member_updated_at),
                    last_message_at: None,
                }])
            };
            assert_eq!(repaired_key, seed.saved_key);
            assert_eq!(relation_key_after, seed.saved_key);
            assert_eq!(member_updated_at, before_member_updated_at);
            assert_eq!(relation_updated_at_after, relation_updated_at);
            assert_eq!(marker_key_after, None);
            assert_eq!(marker_typed_user_id, Some(1));
            assert!(marker_forbid_reimport);
            assert_eq!(group_key_after, saved_group_key);
            let runtime_view_after = read_service
                .list_groups(false, None, None, Some(1))
                .await
                .unwrap()
                .into_iter()
                .find(|view| view.id == seed.group_id)
                .unwrap();
            assert_eq!(runtime_view_after.root_thread_id, runtime_view_before.root_thread_id);
            assert_eq!(
                runtime_view_after.root_thread_canonical_key,
                runtime_view_before.root_thread_canonical_key
            );
            assert_ne!(
                runtime_view_after.membership_snapshot_digest,
                runtime_view_before.membership_snapshot_digest
            );
            assert_ne!(
                snapshot_digest(seed.old_key.clone()),
                snapshot_digest(seed.saved_key.clone())
            );
            let canonical_state_after = state::load(pool, "thread-groups-canonical-keys-v1@2")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                canonical_state_after.canonical_definition_digest,
                canonical_state_before.canonical_definition_digest
            );
            assert_eq!(canonical_state_after.state, canonical_state_before.state);
            assert_eq!(canonical_state_after.fencing_token, canonical_state_before.fencing_token);
            assert_eq!(canonical_state_after.attempt_count, canonical_state_before.attempt_count);
            assert_eq!(canonical_state_after.updated_at, canonical_state_before.updated_at);
            assert_eq!(canonical_state_after.checkpoint, canonical_state_before.checkpoint);
            let legacy_user_ids_state_after =
                state::load(pool, "thread-groups-user-ids-v1@2").await.unwrap();
            assert_eq!(
                legacy_user_ids_state_after.as_ref().map(|row| (&row.state, row.fencing_token, row.attempt_count, &row.checkpoint, &row.failure_classification, row.updated_at)),
                legacy_user_ids_state_before.as_ref().map(|row| (&row.state, row.fencing_token, row.attempt_count, &row.checkpoint, &row.failure_classification, row.updated_at))
            );
            let serialized = serde_json::to_string(&result).unwrap();
            assert!(!serialized.contains(&seed.old_key));
            assert!(!serialized.contains(&seed.saved_key));
            assert!(!serialized.contains(&format!("thread-alias-{}", seed.group_id)));
            assert_eq!(
                state::load(pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .unwrap()
                    .kind()
                    .unwrap(),
                TaskStateKind::Completed
            );
        });
    }

    #[test]
    fn unsafe_identity_rebinding_blocks_every_write_and_reports_a_reason() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let other_thread_id = seed.thread_id + 1_000_000;
            insert_thread(pool, other_thread_id, None).await;
            sqlx::query("UPDATE source_thread_identity SET thread_id = ? WHERE native_id = ?")
                .bind(other_thread_id)
                .bind(format!("thread-alias-{}", seed.group_id))
                .execute(pool)
                .await
                .unwrap();

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(report.unsafe_candidate_count, 1);
            assert_eq!(report.stop_reasons.get("identity_rebound"), Some(&1));
            let result = task(pool)
                .apply("unsafe-execution", "test-holder")
                .await;
            assert!(result.is_err());
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let current_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(current_key, seed.old_key);

            let unproven_pool = test_pool().await;
            prepare_task_rows(unproven_pool).await;
            let unproven_seed = seed_membership(unproven_pool, true).await;
            sqlx::query("DELETE FROM source_thread_identity WHERE native_id = ?")
                .bind(format!("thread-alias-{}", unproven_seed.group_id))
                .execute(unproven_pool)
                .await
                .unwrap();
            let unproven_report = task(unproven_pool).inspect().await.unwrap();
            assert_eq!(unproven_report.stop_reasons.get("identity_unproven"), Some(&1));
            assert!(task(unproven_pool)
                .apply("unproven", "test-holder")
                .await
                .is_err());
            assert!(state::load(unproven_pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());

            let owner_pool = test_pool().await;
            prepare_task_rows(owner_pool).await;
            let owner_seed = seed_membership(owner_pool, true).await;
            sqlx::query(
                "UPDATE thread_canonical_key SET owner_scope = 'user:2', user_id = 2 WHERE thread_id = ?",
            )
            .bind(owner_seed.thread_id)
            .execute(owner_pool)
            .await
            .unwrap();
            let owner_report = task(owner_pool).inspect().await.unwrap();
            assert_eq!(owner_report.stop_reasons.get("owner_mismatch"), Some(&1));
            assert!(task(owner_pool)
                .apply("owner-mismatch", "test-holder")
                .await
                .is_err());
            assert!(state::load(owner_pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
        });
    }

    #[test]
    fn unrelated_old_key_is_not_proven_by_a_matching_source_identity_alone() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let unrelated_old_key = key((seed.group_id + 8_000) as u64);
            sqlx::query(
                "UPDATE thread_group_member SET thread_canonical_key = ? \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(&unrelated_old_key)
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .execute(pool)
            .await
            .unwrap();

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(report.unsafe_candidate_count, 1);
            assert_eq!(
                report.stop_reasons.get("old_key_not_identity_derived"),
                Some(&1)
            );
            assert!(task(pool)
                .apply("unrelated-old-key", "test-holder")
                .await
                .is_err());
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let current_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(current_key, unrelated_old_key);
        });
    }

    #[test]
    fn deleted_placeholder_and_root_identity_changes_are_not_repaired() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            insert_deleted_placeholder(pool, &seed).await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(
                report.stop_reasons.get("target_current_key_collision"),
                Some(&1)
            );
            assert!(task(pool).apply("collision", "test-holder").await.is_err());
            assert!(
                state::load(pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );

            let second_pool = test_pool().await;
            prepare_task_rows(second_pool).await;
            let root_seed = seed_membership(second_pool, true).await;
            sqlx::query(
                "UPDATE thread_group_member SET role = 'member' \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(root_seed.group_id)
            .bind(root_seed.root_thread_id)
            .execute(second_pool)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE thread_group_member SET role = 'root' WHERE group_id = ? AND thread_id = ?",
            )
            .bind(root_seed.group_id)
            .bind(root_seed.thread_id)
            .execute(second_pool)
            .await
            .unwrap();
            sqlx::query("UPDATE thread_group SET group_canonical_key = ? WHERE id = ?")
                .bind(reconciler_group_canonical_key(&root_seed.old_key))
                .bind(root_seed.group_id)
                .execute(second_pool)
                .await
                .unwrap();
            insert_live_child_relation(second_pool, &root_seed).await;
            let root_reader = app::app::thread_group::ThreadGroupReadService::new(second_pool);
            let displayed_root = root_reader
                .root_member(root_seed.group_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(displayed_root.thread_id, Some(root_seed.root_thread_id));
            let root_report = task(second_pool).inspect().await.unwrap();
            assert_eq!(root_report.safe_candidate_count, 0);
            assert_eq!(
                root_report
                    .stop_reasons
                    .get("group_root_or_identity_unverified"),
                Some(&1)
            );
            assert!(
                !root_report
                    .stop_reasons
                    .contains_key("display_root_changed")
            );
            assert!(
                task(second_pool)
                    .apply("root-change", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(second_pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );

            let identity_pool = test_pool().await;
            prepare_task_rows(identity_pool).await;
            let identity_seed = seed_membership(identity_pool, true).await;
            sqlx::query("UPDATE thread_group SET group_canonical_key = ? WHERE id = ?")
                .bind(key((identity_seed.group_id + 9_000) as u64))
                .bind(identity_seed.group_id)
                .execute(identity_pool)
                .await
                .unwrap();
            let identity_report = task(identity_pool).inspect().await.unwrap();
            assert_eq!(
                identity_report.stop_reasons.get("group_identity_unknown"),
                Some(&1)
            );
            assert!(
                task(identity_pool)
                    .apply("unknown-group-identity", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(identity_pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn projected_display_root_change_is_rejected_for_active_and_deleted_roots() {
        TEST_RUNTIME.block_on(async {
            let active_pool = test_pool().await;
            prepare_task_rows(active_pool).await;
            let active_seed = seed_membership(active_pool, true).await;
            let next_role_root_key = hex_successor(&active_seed.old_key);
            let upper_key = "f".repeat(64);
            sqlx::query(
                "UPDATE thread_canonical_key SET key = ? WHERE thread_id = ?",
            )
            .bind(&next_role_root_key)
            .bind(active_seed.root_thread_id)
            .execute(active_pool)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE thread_group_member SET thread_canonical_key = ? \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(&next_role_root_key)
            .bind(active_seed.group_id)
            .bind(active_seed.root_thread_id)
            .execute(active_pool)
            .await
            .unwrap();
            sqlx::query("UPDATE thread_group SET group_canonical_key = ? WHERE id = ?")
                .bind(reconciler_group_canonical_key(&next_role_root_key))
                .bind(active_seed.group_id)
                .execute(active_pool)
                .await
                .unwrap();
            sqlx::query("UPDATE thread_canonical_key SET key = ? WHERE thread_id = ?")
                .bind(&upper_key)
                .bind(active_seed.thread_id)
                .execute(active_pool)
                .await
                .unwrap();

            let active_reader =
                app::app::thread_group::ThreadGroupReadService::new(active_pool);
            let active_root_before = active_reader
                .root_member(active_seed.group_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(active_root_before.thread_id, Some(active_seed.thread_id));
            assert_eq!(active_root_before.thread_canonical_key, active_seed.old_key);
            let active_report = task(active_pool).inspect().await.unwrap();
            assert_eq!(active_report.safe_candidate_count, 0);
            assert_eq!(
                active_report.stop_reasons.get("display_root_changed"),
                Some(&1)
            );
            assert!(task(active_pool)
                .apply("display-root-active", "test-holder")
                .await
                .is_err());
            assert!(state::load(active_pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let active_key_after_refusal: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(active_seed.group_id)
            .bind(active_seed.thread_id)
            .fetch_one(active_pool)
            .await
            .unwrap();
            assert_eq!(active_key_after_refusal, active_seed.old_key);
            sqlx::query(
                "UPDATE thread_group_member SET thread_canonical_key = ? \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(&upper_key)
            .bind(active_seed.group_id)
            .bind(active_seed.thread_id)
            .execute(active_pool)
            .await
            .unwrap();
            let active_root_after_projection = active_reader
                .root_member(active_seed.group_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                active_root_after_projection.thread_id,
                Some(active_seed.root_thread_id)
            );

            let deleted_pool = test_pool().await;
            prepare_task_rows(deleted_pool).await;
            let deleted_seed = seed_membership(deleted_pool, true).await;
            let placeholder_key = hex_predecessor(&deleted_seed.old_key);
            let lower_key = "0".repeat(64);
            assert!(lower_key < placeholder_key);
            insert_deleted_placeholder_in_group(
                deleted_pool,
                deleted_seed.group_id,
                &placeholder_key,
            )
            .await;
            let upper_role_root_key = "f".repeat(64);
            sqlx::query(
                "UPDATE thread_canonical_key SET key = ? WHERE thread_id = ?",
            )
            .bind(&upper_role_root_key)
            .bind(deleted_seed.root_thread_id)
            .execute(deleted_pool)
            .await
            .unwrap();
            sqlx::query(
                "UPDATE thread_group_member SET thread_canonical_key = ? \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(&upper_role_root_key)
            .bind(deleted_seed.group_id)
            .bind(deleted_seed.root_thread_id)
            .execute(deleted_pool)
            .await
            .unwrap();
            sqlx::query("UPDATE thread_group SET group_canonical_key = ? WHERE id = ?")
                .bind(reconciler_group_canonical_key(&upper_role_root_key))
                .bind(deleted_seed.group_id)
                .execute(deleted_pool)
                .await
                .unwrap();
            sqlx::query("UPDATE thread_canonical_key SET key = ? WHERE thread_id = ?")
                .bind(&lower_key)
                .bind(deleted_seed.thread_id)
                .execute(deleted_pool)
                .await
                .unwrap();

            let deleted_reader =
                app::app::thread_group::ThreadGroupReadService::new(deleted_pool);
            let deleted_root_before = deleted_reader
                .root_member(deleted_seed.group_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(deleted_root_before.thread_id, None);
            assert_eq!(deleted_root_before.thread_canonical_key, placeholder_key);
            let deleted_report = task(deleted_pool).inspect().await.unwrap();
            assert_eq!(deleted_report.safe_candidate_count, 0);
            assert_eq!(
                deleted_report.stop_reasons.get("display_root_changed"),
                Some(&1)
            );
            assert!(task(deleted_pool)
                .apply("display-root-deleted-placeholder", "test-holder")
                .await
                .is_err());
            assert!(state::load(deleted_pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let deleted_key_after_refusal: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(deleted_seed.group_id)
            .bind(deleted_seed.thread_id)
            .fetch_one(deleted_pool)
            .await
            .unwrap();
            assert_eq!(deleted_key_after_refusal, deleted_seed.old_key);
            sqlx::query(
                "UPDATE thread_group_member SET thread_canonical_key = ? \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(&lower_key)
            .bind(deleted_seed.group_id)
            .bind(deleted_seed.thread_id)
            .execute(deleted_pool)
            .await
            .unwrap();
            let deleted_root_after_projection = deleted_reader
                .root_member(deleted_seed.group_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                deleted_root_after_projection.thread_id,
                Some(deleted_seed.thread_id)
            );
        });
    }

    #[test]
    fn projected_self_relation_is_reported_and_rejected_without_writes() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            insert_projected_self_edge(pool, &seed).await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(report.stop_reasons.get("projected_self_edge"), Some(&1));
            assert!(task(pool).apply("self-edge", "test-holder").await.is_err());
            let member_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(member_key, seed.old_key);
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
        });
    }

    #[test]
    fn projected_cycle_and_multiple_parent_edges_are_blocked() {
        TEST_RUNTIME.block_on(async {
            let cycle_pool = test_pool().await;
            prepare_task_rows(cycle_pool).await;
            let cycle_seed = seed_membership(cycle_pool, true).await;
            let other_thread = cycle_seed.thread_id + 1_000;
            let other_key = key((cycle_seed.group_id + 1_000) as u64);
            insert_thread(cycle_pool, other_thread, None).await;
            sqlx::query(
                "INSERT INTO thread_canonical_key (thread_id, owner_scope, key, origin, assigned_at, user_id) \
                 VALUES (?, 'user:1', ?, 'backfill_mapping', 40, 1)",
            )
            .bind(other_thread)
            .bind(&other_key)
            .execute(cycle_pool)
            .await
            .unwrap();
            insert_relation_edge(
                cycle_pool,
                cycle_seed.group_id + 500,
                cycle_seed.thread_id,
                other_thread,
                &cycle_seed.old_key,
                &other_key,
            )
            .await;
            insert_relation_edge(
                cycle_pool,
                cycle_seed.group_id + 501,
                other_thread,
                cycle_seed.thread_id,
                &other_key,
                &cycle_seed.old_key,
            )
            .await;
            let cycle_report = task(cycle_pool).inspect().await.unwrap();
            assert_eq!(cycle_report.stop_reasons.get("projected_relation_cycle"), Some(&1));
            assert!(task(cycle_pool)
                .apply("cycle", "test-holder")
                .await
                .is_err());
            assert!(state::load(cycle_pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());

            let parents_pool = test_pool().await;
            prepare_task_rows(parents_pool).await;
            let parents_seed = seed_membership(parents_pool, true).await;
            let other_parent = parents_seed.thread_id + 2_000;
            let other_parent_key = key((parents_seed.group_id + 2_000) as u64);
            insert_thread(parents_pool, other_parent, None).await;
            sqlx::query(
                "INSERT INTO thread_canonical_key (thread_id, owner_scope, key, origin, assigned_at, user_id) \
                 VALUES (?, 'user:1', ?, 'backfill_mapping', 40, 1)",
            )
            .bind(other_parent)
            .bind(&other_parent_key)
            .execute(parents_pool)
            .await
            .unwrap();
            insert_relation_edge(
                parents_pool,
                parents_seed.group_id + 600,
                parents_seed.root_thread_id,
                parents_seed.thread_id,
                &parents_seed.root_key,
                &parents_seed.old_key,
            )
            .await;
            insert_relation_edge(
                parents_pool,
                parents_seed.group_id + 601,
                other_parent,
                parents_seed.thread_id,
                &other_parent_key,
                &parents_seed.saved_key,
            )
            .await;
            let parents_report = task(parents_pool).inspect().await.unwrap();
            assert_eq!(parents_report.stop_reasons.get("multiple_active_parents"), Some(&1));
            assert!(task(parents_pool)
                .apply("multiple-parents", "test-holder")
                .await
                .is_err());
        });
    }

    #[test]
    fn multiple_parents_elsewhere_in_an_affected_relation_component_block_repair() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let stable_root_key = "0".repeat(64);
            sqlx::query("UPDATE thread_canonical_key SET key = ? WHERE thread_id = ?")
                .bind(&stable_root_key)
                .bind(seed.root_thread_id)
                .execute(pool)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE thread_group_member SET thread_canonical_key = ? \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(&stable_root_key)
            .bind(seed.group_id)
            .bind(seed.root_thread_id)
            .execute(pool)
            .await
            .unwrap();
            sqlx::query("UPDATE thread_group SET group_canonical_key = ? WHERE id = ?")
                .bind(reconciler_group_canonical_key(&stable_root_key))
                .bind(seed.group_id)
                .execute(pool)
                .await
                .unwrap();

            let child_thread_id = seed.thread_id + 4_000;
            let first_parent_id = seed.thread_id + 4_001;
            let second_parent_id = seed.thread_id + 4_002;
            let child_key = key((seed.group_id + 4_000) as u64);
            let first_parent_key = key((seed.group_id + 4_001) as u64);
            let second_parent_key = key((seed.group_id + 4_002) as u64);
            for (thread_id, canonical_key) in [
                (child_thread_id, &child_key),
                (first_parent_id, &first_parent_key),
                (second_parent_id, &second_parent_key),
            ] {
                insert_thread(pool, thread_id, None).await;
                sqlx::query(
                    "INSERT INTO thread_canonical_key \
                     (thread_id, owner_scope, key, origin, assigned_at, user_id) \
                     VALUES (?, 'user:1', ?, 'backfill_mapping', 40, 1)",
                )
                .bind(thread_id)
                .bind(canonical_key)
                .execute(pool)
                .await
                .unwrap();
            }
            sqlx::query("DROP INDEX thread_relation_active_child_canonical_key")
                .execute(pool)
                .await
                .unwrap();
            insert_relation_edge(
                pool,
                seed.group_id + 4_100,
                seed.thread_id,
                child_thread_id,
                &seed.old_key,
                &child_key,
            )
            .await;
            insert_relation_edge(
                pool,
                seed.group_id + 4_101,
                first_parent_id,
                child_thread_id,
                &first_parent_key,
                &child_key,
            )
            .await;
            insert_relation_edge(
                pool,
                seed.group_id + 4_102,
                second_parent_id,
                child_thread_id,
                &second_parent_key,
                &child_key,
            )
            .await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(report.unsafe_candidate_count, 1);
            assert_eq!(report.stop_reasons.get("multiple_active_parents"), Some(&1));
            assert!(
                task(pool)
                    .apply("component-multiple-parents", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn duplicate_projected_child_rows_from_the_same_parent_block_repair() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let root_relation_id = seed.group_id + 4_200;
            insert_relation_edge(
                pool,
                root_relation_id,
                seed.root_thread_id,
                seed.thread_id,
                &seed.root_key,
                &seed.old_key,
            )
            .await;
            insert_relation_edge(
                pool,
                root_relation_id + 1,
                seed.root_thread_id,
                seed.thread_id,
                &seed.root_key,
                &seed.saved_key,
            )
            .await;

            let inspection = task(pool).inspect().await.unwrap();
            assert_eq!(inspection.safe_candidate_count, 0);
            assert_eq!(inspection.unsafe_candidate_count, 1);
            assert_eq!(
                inspection.stop_reasons.get("duplicate_projected_child_rows"),
                Some(&1)
            );
            let dry_run = task(pool).dry_run().await.unwrap();
            assert_eq!(dry_run.safe_candidate_count, 0);
            assert_eq!(
                dry_run.stop_reasons.get("duplicate_projected_child_rows"),
                Some(&1)
            );
            assert!(task(pool)
                .apply("duplicate-projected-child", "test-holder")
                .await
                .is_err());
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let keys: Vec<String> = sqlx::query_scalar(
                "SELECT child_thread_canonical_key FROM thread_relation WHERE id IN (?, ?) ORDER BY id",
            )
            .bind(root_relation_id)
            .bind(root_relation_id + 1)
            .fetch_all(pool)
            .await
            .unwrap();
            assert_eq!(keys, vec![seed.old_key, seed.saved_key]);
        });
    }

    #[test]
    fn audit_outbox_and_deletion_marker_key_references_are_stops() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let native_id = format!("thread-alias-{}", seed.group_id);
            sqlx::query(
                "INSERT INTO thread_group_audit \
                 (id, audit_type, source_group_id, target_group_id, actor_id, reason, canonical_partition, successor_group_ids, created_at) \
                 VALUES (?, 'merge', ?, ?, 'fixture', 'fixture', NULL, NULL, 1)",
            )
            .bind(seed.group_id + 700)
            .bind(seed.group_id)
            .bind(seed.group_id + 1)
            .execute(pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO thread_group_event_outbox \
                 (event_id, event_type, operation_id, policy_version, group_id, thread_id, payload, created_at) \
                 VALUES ('fixture-event', 'thread_group_reconciliation_completed', 'op', 'policy', ?, ?, '{}', 1)",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .execute(pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO thread_deletion_marker \
                 (owner_scope, source, identity_scope, native_id, forbid_reimport, recursive, actor_id, reason, deleted_at, user_id, thread_canonical_key) \
                 VALUES ('user:1', 'codex', '', ?, TRUE, FALSE, 'fixture', NULL, 2, 1, ?)",
            )
            .bind(native_id)
            .bind(&seed.old_key)
            .execute(pool)
            .await
            .unwrap();

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.stop_reasons.get("group_audit_reference"), Some(&1));
            assert_eq!(report.stop_reasons.get("event_history_reference"), Some(&1));
            assert_eq!(report.stop_reasons.get("deletion_marker_reference"), Some(&1));
            assert_eq!(report.regeneration_group_ids, vec![seed.group_id]);
            assert!(task(pool)
                .apply("historical-reference", "test-holder")
                .await
                .is_err());
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
        });
    }

    #[test]
    fn split_partition_audit_blocks_repair_and_remains_immutable() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let partition = serde_json::json!([seed.old_key, seed.saved_key]).to_string();
            let successors = serde_json::json!([seed.group_id + 900]).to_string();
            sqlx::query(
                "INSERT INTO thread_group_audit \
                 (id, audit_type, source_group_id, target_group_id, actor_id, reason, \
                  canonical_partition, successor_group_ids, created_at) \
                 VALUES (?, 'split', ?, NULL, 'fixture', 'split boundary', ?, ?, 70)",
            )
            .bind(seed.group_id + 901)
            .bind(seed.group_id)
            .bind(&partition)
            .bind(&successors)
            .execute(pool)
            .await
            .unwrap();

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.stop_reasons.get("group_audit_reference"), Some(&1));
            assert!(task(pool)
                .apply("split-history-reference", "test-holder")
                .await
                .is_err());
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let stored_partition: (Option<String>, String) = sqlx::query_as(
                "SELECT canonical_partition, successor_group_ids \
                 FROM thread_group_audit WHERE id = ?",
            )
            .bind(seed.group_id + 901)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(stored_partition, (Some(partition), successors));
            let member_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(member_key, seed.old_key);
        });
    }

    #[test]
    fn payload_only_old_key_reference_blocks_repair_without_rewriting_event() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let payload = serde_json::json!({
                "nested": {
                    "parent_thread_canonical_key": seed.old_key,
                },
            })
            .to_string();
            insert_outbox_payload(
                pool,
                "payload-only-reference",
                "thread_group_relation_selected",
                &payload,
            )
            .await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(
                report.stop_reasons.get("event_payload_key_reference"),
                Some(&1)
            );
            assert!(task(pool)
                .apply("payload-only-event", "test-holder")
                .await
                .is_err());
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
            let stored_payload: String = sqlx::query_scalar(
                "SELECT payload FROM thread_group_event_outbox WHERE event_id = ?",
            )
            .bind("payload-only-reference")
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(stored_payload, payload);
            let member_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(member_key, seed.old_key);
        });
    }

    #[test]
    fn subject_derived_event_id_blocks_even_when_payload_only_has_parent_key() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let event_id = app::app::thread_group::event_id_for(
                app::app::thread_group::ThreadGroupEventType::RelationSelected,
                &seed.old_key,
            );
            let payload = serde_json::json!({
                "parent_thread_canonical_key": seed.root_key,
                "relation_type": "delegated",
                "selection_basis": "source_exact",
            })
            .to_string();
            insert_outbox_payload(pool, &event_id, "thread_group_relation_selected", &payload)
                .await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(report.unsafe_candidate_count, 1);
            assert_eq!(report.stop_reasons.get("event_id_key_reference"), Some(&1));
            assert!(
                task(pool)
                    .apply("subject-event-id-reference", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );
            let stored: String = sqlx::query_scalar(
                "SELECT payload FROM thread_group_event_outbox WHERE event_id = ?",
            )
            .bind(event_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(stored, payload);
        });
    }

    #[test]
    fn subject_derived_conflict_event_id_is_a_history_reference() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let event_id = app::app::thread_group::event_id_for(
                app::app::thread_group::ThreadGroupEventType::ConflictDetected,
                &seed.old_key,
            );
            let payload = serde_json::json!({"reason": "equal_ranked_parents"}).to_string();
            insert_outbox_payload(pool, &event_id, "thread_group_conflict_detected", &payload)
                .await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(report.stop_reasons.get("event_id_key_reference"), Some(&1));
            assert!(
                task(pool)
                    .apply("subject-conflict-event-id-reference", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn conflicting_parent_payload_with_typed_relation_id_is_recognized() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let payload = serde_json::json!({
                "reason": "conflicting_parent",
                "retracted_relation_id": seed.group_id + 4_300,
            })
            .to_string();
            insert_outbox_payload(
                pool,
                "unrelated-conflict-event",
                "thread_group_conflict_detected",
                &payload,
            )
            .await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(
                report.stop_reasons.get("event_payload_contract_unknown"),
                None
            );
            assert_eq!(report.safe_candidate_count, 1);
            let applied = task(pool)
                .apply("unrelated-conflict-event", "test-holder")
                .await
                .unwrap();
            assert_eq!(applied.repaired_memberships, 1);
            let stored: String = sqlx::query_scalar(
                "SELECT payload FROM thread_group_event_outbox WHERE event_id = ?",
            )
            .bind("unrelated-conflict-event")
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(stored, payload);
        });
    }

    #[test]
    fn malformed_and_unknown_outbox_payload_contracts_fail_closed() {
        TEST_RUNTIME.block_on(async {
            let malformed_pool = test_pool().await;
            prepare_task_rows(malformed_pool).await;
            let _malformed_seed = seed_membership(malformed_pool, true).await;
            insert_outbox_payload(
                malformed_pool,
                "malformed-event-payload",
                "thread_group_relation_selected",
                "not-json",
            )
            .await;
            let malformed_report = task(malformed_pool).inspect().await.unwrap();
            assert_eq!(
                malformed_report
                    .stop_reasons
                    .get("event_payload_unparseable"),
                Some(&1)
            );
            assert!(
                task(malformed_pool)
                    .apply("malformed-event-payload", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(malformed_pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );

            let invalid_conflict_pool = test_pool().await;
            prepare_task_rows(invalid_conflict_pool).await;
            let _invalid_conflict_seed = seed_membership(invalid_conflict_pool, true).await;
            insert_outbox_payload(
                invalid_conflict_pool,
                "invalid-conflict-relation-id",
                "thread_group_conflict_detected",
                r#"{"reason":"conflicting_parent","retracted_relation_id":"42"}"#,
            )
            .await;
            let invalid_conflict_report = task(invalid_conflict_pool).inspect().await.unwrap();
            assert_eq!(
                invalid_conflict_report
                    .stop_reasons
                    .get("event_payload_contract_unknown"),
                Some(&1)
            );
            assert!(
                task(invalid_conflict_pool)
                    .apply("invalid-conflict-relation-id", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(invalid_conflict_pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );

            let unknown_pool = test_pool().await;
            prepare_task_rows(unknown_pool).await;
            let _unknown_seed = seed_membership(unknown_pool, true).await;
            insert_outbox_payload(
                unknown_pool,
                "unknown-event-payload-contract",
                "thread_group_reconciliation_completed",
                "{}",
            )
            .await;
            let unknown_report = task(unknown_pool).inspect().await.unwrap();
            assert_eq!(
                unknown_report
                    .stop_reasons
                    .get("event_payload_contract_unknown"),
                Some(&1)
            );
            assert!(
                task(unknown_pool)
                    .apply("unknown-event-payload-contract", "test-holder")
                    .await
                    .is_err()
            );
            assert!(
                state::load(unknown_pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn outbox_identity_columns_are_checked_without_group_or_thread_ids() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let native_id = format!("thread-alias-{}", seed.group_id);
            let payload = serde_json::json!({
                "state": "candidate",
                "adapter_version": "fixture@1",
                "evidence_kind": "source_event",
            })
            .to_string();
            sqlx::query(
                "INSERT INTO thread_group_event_outbox \
                 (event_id, event_type, operation_id, policy_version, source, identity_scope, \
                  owner_scope, native_id_ref, payload, created_at, user_id) \
                 VALUES ('identity-only-event', 'thread_group_observation_recorded', \
                         'fixture-operation', 'fixture-policy', 'codex', '', 'user:1', ?, ?, 60, 1)",
            )
            .bind(native_id)
            .bind(payload)
            .execute(pool)
            .await
            .unwrap();

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.safe_candidate_count, 0);
            assert_eq!(report.stop_reasons.get("event_history_reference"), Some(&1));
            assert!(task(pool)
                .apply("identity-only-event", "test-holder")
                .await
                .is_err());
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
        });
    }

    #[test]
    fn transaction_failure_rolls_back_membership_relation_typed_ids_and_task_state() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let (relation_id, _) = insert_live_child_relation(pool, &seed).await;
            sqlx::query(
                "UPDATE thread_group_member SET user_id = NULL WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .execute(pool)
            .await
            .unwrap();

            let error = task(pool)
                .with_fail_after_repair()
                .apply("interrupted", "test-holder")
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("injected failure"));
            let member: (String, Option<i64>) = sqlx::query_as(
                "SELECT thread_canonical_key, user_id FROM thread_group_member \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let relation_key: String = sqlx::query_scalar(
                "SELECT child_thread_canonical_key FROM thread_relation WHERE id = ?",
            )
            .bind(relation_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(member, (seed.old_key.clone(), None));
            assert_eq!(relation_key, seed.old_key);
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());
        });
    }

    #[test]
    fn active_lease_conflict_is_unchanged_and_expired_lease_is_fenced() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            let entry = catalog::thread_groups_user_ids_v3().unwrap();
            sqlx::query(
                "INSERT INTO memories_data_migration_task_state \
                 (task_identity, canonical_definition_digest, state, execution_id, holder_id, fencing_token, \
                  heartbeat_at, lease_expires_at, attempt_count, checkpoint, updated_at) \
                 VALUES (?, ?, 'running', 'owner', 'holder', 8, 10, 9223372036854775807, 2, 'checkpoint', 10)",
            )
            .bind(entry.identity())
            .bind(&entry.canonical_definition_digest)
            .execute(pool)
            .await
            .unwrap();
            let before = state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .unwrap();
            assert!(task(pool).apply("contender", "test-holder").await.is_err());
            let after = state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(after.fencing_token, before.fencing_token);
            assert_eq!(after.attempt_count, before.attempt_count);
            assert_eq!(after.checkpoint, before.checkpoint);
            let member_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(member_key, seed.old_key);

            let expired_pool = test_pool().await;
            prepare_task_rows(expired_pool).await;
            let expired_seed = seed_membership(expired_pool, true).await;
            sqlx::query(
                "INSERT INTO memories_data_migration_task_state \
                 (task_identity, canonical_definition_digest, state, execution_id, holder_id, fencing_token, \
                  heartbeat_at, lease_expires_at, attempt_count, checkpoint, updated_at) \
                 VALUES (?, ?, 'running', 'stale-owner', 'stale-holder', 8, 10, 1, 2, 'stale-checkpoint', 10)",
            )
            .bind(entry.identity())
            .bind(&entry.canonical_definition_digest)
            .execute(expired_pool)
            .await
            .unwrap();
            let result = task(expired_pool)
                .apply("reclaimer", "test-holder")
                .await
                .unwrap();
            assert_eq!(result.repaired_memberships, 1);
            let completed = state::load(expired_pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(completed.kind().unwrap(), TaskStateKind::Completed);
            assert_eq!(completed.fencing_token, 9);
            assert_eq!(completed.checkpoint.as_deref(), Some("stale-checkpoint"));
            let repaired_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(expired_seed.group_id)
            .bind(expired_seed.thread_id)
            .fetch_one(expired_pool)
            .await
            .unwrap();
            assert_eq!(repaired_key, expired_seed.saved_key);
        });
    }

    #[test]
    fn canonical_key_prerequisite_is_actionable_and_late_mismatches_are_not_hidden() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let _seed = seed_membership(pool, true).await;
            sqlx::query("DELETE FROM memories_data_migration_task_state WHERE task_identity = ?")
                .bind("thread-groups-canonical-keys-v1@2")
                .execute(pool)
                .await
                .unwrap();
            let report = task(pool).inspect().await.unwrap();
            assert_eq!(report.status, "prerequisite_pending");
            assert_eq!(report.prerequisite_status, "pending");
            let error = task(pool)
                .apply("before-prerequisite", "test-holder")
                .await
                .unwrap_err();
            assert!(format!("{error:#}").contains("--generation 2"));
            assert!(state::load(pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .is_none());

            let repaired_pool = test_pool().await;
            prepare_task_rows(repaired_pool).await;
            let repaired_seed = seed_membership(repaired_pool, true).await;
            task(repaired_pool)
                .apply("first-run", "test-holder")
                .await
                .unwrap();
            sqlx::query(
                "UPDATE thread_group_member SET thread_canonical_key = ? \
                 WHERE group_id = ? AND thread_id = ?",
            )
            .bind(&repaired_seed.old_key)
            .bind(repaired_seed.group_id)
            .bind(repaired_seed.thread_id)
            .execute(repaired_pool)
            .await
            .unwrap();
            assert!(task(repaired_pool).verify().await.is_err());
            assert!(task(repaired_pool)
                .apply("repeat-after-mismatch", "test-holder")
                .await
                .is_err());
            let state_after = state::load(repaired_pool, "thread-groups-user-ids-v1@3")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(state_after.kind().unwrap(), TaskStateKind::Completed);
            let late_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(repaired_seed.group_id)
            .bind(repaired_seed.thread_id)
            .fetch_one(repaired_pool)
            .await
            .unwrap();
            assert_eq!(late_key, repaired_seed.old_key);
        });
    }

    #[test]
    fn zero_mismatch_is_a_business_data_noop_but_records_new_task_completion() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, false).await;
            insert_relation_edge(
                pool,
                seed.group_id + 5_100,
                seed.root_thread_id,
                seed.thread_id,
                &seed.root_key,
                &seed.saved_key,
            )
            .await;
            let event_payload = serde_json::json!({
                "parent_thread_canonical_key": seed.root_key,
                "relation_type": "delegated",
                "selection_basis": "source_exact",
            })
            .to_string();
            insert_outbox_payload(
                pool,
                "no-op-relation-event",
                "thread_group_relation_selected",
                &event_payload,
            )
            .await;
            sqlx::query(
                "INSERT INTO thread_group_audit \
                 (id, audit_type, source_group_id, target_group_id, actor_id, reason, \
                  canonical_partition, successor_group_ids, created_at) \
                 VALUES (?, 'merge', ?, ?, 'fixture', 'unrelated history', NULL, NULL, 61)",
            )
            .bind(seed.group_id + 5_200)
            .bind(seed.group_id + 5_201)
            .bind(seed.group_id + 5_202)
            .execute(pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO thread_deletion_marker \
                 (owner_scope, source, identity_scope, native_id, forbid_reimport, recursive, \
                  actor_id, reason, deleted_at, user_id, thread_canonical_key) \
                 VALUES ('user:1', 'codex', '', 'unrelated-noop-marker', TRUE, FALSE, \
                         'fixture', NULL, 62, 1, NULL)",
            )
            .execute(pool)
            .await
            .unwrap();
            let business_before = business_table_snapshot(pool).await;
            let read_service = app::app::thread_group::ThreadGroupReadService::new(pool);
            let runtime_view_before = read_service
                .list_groups(false, None, None, Some(1))
                .await
                .unwrap()
                .into_iter()
                .find(|view| view.id == seed.group_id)
                .unwrap();
            let updated_before: i64 = sqlx::query_scalar(
                "SELECT updated_at FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            use app::app::thread_group::{MembershipSnapshotEntry, membership_snapshot_digest};
            let digest_before = membership_snapshot_digest(&[MembershipSnapshotEntry {
                thread_canonical_key: seed.saved_key.clone(),
                state: "active".to_string(),
                role: "member".to_string(),
                deleted_at: None,
                updated_at: Some(updated_before),
                last_message_at: None,
            }]);

            let result = task(pool)
                .apply("noop-execution", "test-holder")
                .await
                .unwrap();
            let business_after = business_table_snapshot(pool).await;
            assert_eq!(business_after, business_before);
            let runtime_view_after = read_service
                .list_groups(false, None, None, Some(1))
                .await
                .unwrap()
                .into_iter()
                .find(|view| view.id == seed.group_id)
                .unwrap();
            assert_eq!(
                runtime_view_after.root_thread_id,
                runtime_view_before.root_thread_id
            );
            assert_eq!(
                runtime_view_after.root_thread_canonical_key,
                runtime_view_before.root_thread_canonical_key
            );
            assert_eq!(
                runtime_view_after.membership_snapshot_digest,
                runtime_view_before.membership_snapshot_digest
            );
            assert_eq!(result.repaired_memberships, 0);
            assert!(result.regeneration_group_ids.is_empty());
            assert_eq!(result.affected_component_count, 0);
            let updated_after: i64 = sqlx::query_scalar(
                "SELECT updated_at FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(updated_after, updated_before);
            assert_eq!(
                digest_before,
                membership_snapshot_digest(&[MembershipSnapshotEntry {
                    thread_canonical_key: seed.saved_key,
                    state: "active".to_string(),
                    role: "member".to_string(),
                    deleted_at: None,
                    updated_at: Some(updated_after),
                    last_message_at: None,
                }])
            );
            assert_eq!(
                state::load(pool, "thread-groups-user-ids-v1@3")
                    .await
                    .unwrap()
                    .unwrap()
                    .kind()
                    .unwrap(),
                TaskStateKind::Completed
            );
        });
    }
}

#[cfg(all(test, feature = "postgres"))]
mod postgres_tests {
    use super::ThreadGroupsUserIdsV3Task;
    use crate::db_migrate::{
        catalog,
        state::{self, TaskStateKind},
    };
    use common::thread_group_key::{
        IdentityScope, SourceIdentity, reconciler_group_canonical_key, source_thread_canonical_key,
    };
    use sqlx::postgres::PgConnectOptions;
    use std::{
        str::FromStr,
        sync::atomic::{AtomicI64, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    static FIXTURE_ID: AtomicI64 = AtomicI64::new(700_000_000);

    struct PgTestDatabase {
        admin: sqlx::PgPool,
        pool: sqlx::PgPool,
        schema: String,
    }

    impl PgTestDatabase {
        async fn new() -> Self {
            use sqlx::postgres::PgPoolOptions;

            let url = std::env::var("TEST_POSTGRES_URL")
                .expect("opt-in PostgreSQL tests require TEST_POSTGRES_URL");
            let parsed_url =
                url::Url::parse(&url).expect("TEST_POSTGRES_URL must be a PostgreSQL URL");
            let local_host = matches!(
                parsed_url.host_str(),
                Some("localhost" | "127.0.0.1" | "::1")
            );
            assert!(
                !(local_host && parsed_url.port().unwrap_or(5432) == 5432),
                "refusing the shared default localhost:5432 PostgreSQL test database"
            );
            assert!(
                !parsed_url.query_pairs().any(|(key, _)| {
                    key == "search_path" || key == "options" || key == "options[search_path]"
                }),
                "TEST_POSTGRES_URL must not set a search_path; the test creates its own schema"
            );

            let admin = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .expect("connect to explicitly supplied disposable TEST_POSTGRES_URL");
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock must be after the Unix epoch")
                .as_nanos();
            let schema = format!(
                "memories_pg_v3_{}_{}_{}",
                std::process::id(),
                FIXTURE_ID.fetch_add(1, Ordering::Relaxed),
                nanos
            );
            let create_schema = format!("CREATE SCHEMA {schema}");
            sqlx::raw_sql(sqlx::AssertSqlSafe(create_schema.as_str()))
                .execute(&admin)
                .await
                .expect("create isolated PostgreSQL test schema");

            let connect_options = PgConnectOptions::from_str(&url)
                .expect("parse TEST_POSTGRES_URL")
                .options([("search_path", schema.as_str())]);
            let pool = PgPoolOptions::new()
                .max_connections(6)
                .connect_with(connect_options)
                .await
                .expect("connect to isolated PostgreSQL test schema");
            for migration in [
                include_str!(
                    "../../../infra/atlas/postgres/migrations/20260803000001_adoption_baseline.sql"
                ),
                include_str!(
                    "../../../infra/atlas/postgres/migrations/20260803000002_thread_message_times_schema.sql"
                ),
                include_str!(
                    "../../../infra/atlas/postgres/migrations/20260803000003_schema_contract.sql"
                ),
                include_str!(
                    "../../../infra/atlas/postgres/migrations/20260920000001_thread_group_schema.sql"
                ),
                include_str!(
                    "../../../infra/atlas/postgres/migrations/20260926000001_thread_group_memory_relation.sql"
                ),
                include_str!(
                    "../../../infra/atlas/postgres/migrations/20260930000001_thread_group_user_ids.sql"
                ),
            ] {
                sqlx::raw_sql(migration)
                    .execute(&pool)
                    .await
                    .expect("apply fixed PostgreSQL test schema migration");
            }

            Self {
                admin,
                pool,
                schema,
            }
        }

        async fn close(self) {
            self.pool.close().await;
            let drop_schema = format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema);
            sqlx::raw_sql(sqlx::AssertSqlSafe(drop_schema.as_str()))
                .execute(&self.admin)
                .await
                .expect("drop isolated PostgreSQL test schema");
            self.admin.close().await;
        }
    }

    struct Seed {
        group_id: i64,
        thread_id: i64,
        old_key: String,
        saved_key: String,
    }

    async fn seed_membership(pool: &sqlx::PgPool, mismatch: bool) -> Seed {
        let group_id = FIXTURE_ID.fetch_add(10, Ordering::Relaxed);
        let thread_id = group_id + 1;
        let root_thread_id = group_id + 2;
        let native_id = format!("pg-v3-thread-{group_id}");
        let old_key = source_thread_canonical_key(&SourceIdentity::new(
            1,
            "codex",
            IdentityScope::known(""),
            &native_id,
        ))
        .expect("known source identity scope");
        let saved_key = format!("{:064}", group_id + 1);
        let root_key = "0".repeat(64);

        for id in [thread_id, root_thread_id] {
            sqlx::query(
                "INSERT INTO thread (id, user_id, created_at, updated_at, memory_kind) \
                 VALUES ($1, 1, 10, 11, 1)",
            )
            .bind(id)
            .execute(pool)
            .await
            .expect("insert fixture thread");
        }
        sqlx::query(
            "INSERT INTO thread_group \
             (id, user_id, group_canonical_key, title, status, grouping_authority, \
              redirect_to_group_id, created_at, updated_at) \
             VALUES ($1, 1, $2, NULL, 'active', 'reconciler', NULL, 10, 11)",
        )
        .bind(group_id)
        .bind(reconciler_group_canonical_key(&root_key))
        .execute(pool)
        .await
        .expect("insert fixture group");

        for (member_thread_id, member_key, role, member_native_id) in [
            (root_thread_id, root_key.as_str(), "root", None),
            (
                thread_id,
                if mismatch { &old_key } else { &saved_key },
                "member",
                Some(native_id.as_str()),
            ),
        ] {
            sqlx::query(
                "INSERT INTO thread_group_member \
                 (group_id, thread_id, thread_canonical_key, user_id, owner_scope, source, \
                  identity_scope, native_id, role, state, provenance, deleted_at, created_at, updated_at) \
                 VALUES ($1, $2, $3, 1, 'user:1', $4, $5, $6, $7, 'active', 'reconciler', NULL, 20, 21)",
            )
            .bind(group_id)
            .bind(member_thread_id)
            .bind(member_key)
            .bind(member_native_id.map(|_| "codex"))
            .bind(member_native_id.map(|_| ""))
            .bind(member_native_id)
            .bind(role)
            .execute(pool)
            .await
            .expect("insert fixture membership");
        }

        for (id, canonical_key, origin) in [
            (thread_id, saved_key.as_str(), "backfill_mapping"),
            (root_thread_id, root_key.as_str(), "source_identity"),
        ] {
            sqlx::query(
                "INSERT INTO thread_canonical_key (thread_id, owner_scope, key, origin, assigned_at, user_id) \
                 VALUES ($1, 'user:1', $2, $3, 40, 1)",
            )
            .bind(id)
            .bind(canonical_key)
            .bind(origin)
            .execute(pool)
            .await
            .expect("insert fixture canonical key");
        }
        sqlx::query(
            "INSERT INTO source_thread_identity \
             (owner_scope, source, identity_scope, native_id, thread_id, resolution_state, \
              first_seen_at, last_seen_at, user_id) \
             VALUES ('user:1', 'codex', '', $1, $2, 'resolved', 1, 1, 1)",
        )
        .bind(&native_id)
        .bind(thread_id)
        .execute(pool)
        .await
        .expect("insert fixture source identity");

        Seed {
            group_id,
            thread_id,
            old_key,
            saved_key,
        }
    }

    async fn prepare_canonical_key_prerequisite(pool: &sqlx::PgPool) {
        let entry = catalog::thread_groups_canonical_keys_v2().expect("fixed @2 catalog entry");
        sqlx::query(
            "INSERT INTO memories_data_migration_task_state \
             (task_identity, canonical_definition_digest, state, fencing_token, attempt_count, updated_at, completed_at) \
             VALUES ($1, $2, 'completed', 1, 1, 1, 1)",
        )
        .bind(entry.identity())
        .bind(entry.canonical_definition_digest)
        .execute(pool)
        .await
        .expect("seed completed @2 prerequisite");
    }

    fn task(pool: &sqlx::PgPool) -> ThreadGroupsUserIdsV3Task {
        ThreadGroupsUserIdsV3Task::new(
            pool.clone(),
            catalog::thread_groups_user_ids_v3().expect("fixed @3 catalog entry"),
        )
        .expect("construct fixed @3 task")
    }

    #[test]
    #[ignore = "requires TEST_POSTGRES_URL for disposable PostgreSQL; creates an isolated schema"]
    fn pg_v3_analyzer_apply_and_failure_rollback_are_atomic() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let db = PgTestDatabase::new().await;
            prepare_canonical_key_prerequisite(&db.pool).await;
            let seed = seed_membership(&db.pool, true).await;

            let report = task(&db.pool).inspect().await.expect("inspect mismatch");
            assert_eq!(report.safe_candidate_count, 1);
            assert_eq!(report.unsafe_candidate_count, 0);

            let failure = task(&db.pool)
                .with_fail_after_repair()
                .apply("pg-failure", "pg-test")
                .await
                .expect_err("test failpoint aborts the transactional repair");
            assert!(format!("{failure:#}").contains("injected failure"));
            let key_after_rollback: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = $1 AND thread_id = $2",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(&db.pool)
            .await
            .expect("read membership after rollback");
            assert_eq!(key_after_rollback.trim(), seed.old_key);
            assert!(state::load(&db.pool, "thread-groups-user-ids-v1@3")
                .await
                .expect("read rollback state")
                .is_none());

            let repaired = task(&db.pool)
                .apply("pg-repair", "pg-test")
                .await
                .expect("apply proven repair");
            assert_eq!(repaired.repaired_memberships, 1);
            let repaired_key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = $1 AND thread_id = $2",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(&db.pool)
            .await
            .expect("read repaired membership");
            assert_eq!(repaired_key.trim(), seed.saved_key);
            assert_eq!(
                state::load(&db.pool, "thread-groups-user-ids-v1@3")
                    .await
                    .expect("read completed state")
                    .expect("completed @3 state")
                    .kind()
                    .expect("valid task state"),
                TaskStateKind::Completed
            );
            db.close().await;
        });
    }

    #[test]
    #[ignore = "requires TEST_POSTGRES_URL for disposable PostgreSQL; creates an isolated schema"]
    fn pg_v3_noop_completes_without_changing_membership_or_audit_time() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let db = PgTestDatabase::new().await;
            prepare_canonical_key_prerequisite(&db.pool).await;
            let seed = seed_membership(&db.pool, false).await;
            let updated_before: i64 = sqlx::query_scalar(
                "SELECT updated_at FROM thread_group_member WHERE group_id = $1 AND thread_id = $2",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(&db.pool)
            .await
            .expect("read initial audit timestamp");

            let report = task(&db.pool)
                .apply("pg-noop", "pg-test")
                .await
                .expect("run no-op task");
            assert_eq!(report.status, "no_op");
            assert_eq!(report.repaired_memberships, 0);
            let current: (String, i64) = sqlx::query_as(
                "SELECT thread_canonical_key, updated_at FROM thread_group_member WHERE group_id = $1 AND thread_id = $2",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(&db.pool)
            .await
            .expect("read no-op membership");
            assert_eq!(current.0.trim(), seed.saved_key);
            assert_eq!(current.1, updated_before);
            assert_eq!(
                state::load(&db.pool, "thread-groups-user-ids-v1@3")
                    .await
                    .expect("read no-op state")
                    .expect("completed no-op state")
                    .kind()
                    .expect("valid task state"),
                TaskStateKind::Completed
            );
            db.close().await;
        });
    }

    #[test]
    #[ignore = "requires TEST_POSTGRES_URL for disposable PostgreSQL; creates an isolated schema"]
    fn pg_task_state_transactions_rollback_and_fence_waiting_claims() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use tokio::{
                sync::oneshot,
                time::{sleep, timeout},
            };

            let db = PgTestDatabase::new().await;
            let mut rollback_tx = db.pool.begin().await.expect("begin rollback transaction");
            let rolled_back_lease = state::claim_tx(
                &mut rollback_tx,
                "pg-atomic-rollback@1",
                "digest",
                "execution",
                "holder",
                10,
                20,
            )
            .await
            .expect("claim in transaction");
            state::complete_tx(&mut rollback_tx, &rolled_back_lease, 11)
                .await
                .expect("complete in transaction");
            rollback_tx.rollback().await.expect("rollback completion");
            assert!(
                state::load(&db.pool, "pg-atomic-rollback@1")
                    .await
                    .expect("load rolled-back task")
                    .is_none()
            );

            let mut failure_tx = db.pool.begin().await.expect("begin failure transaction");
            let failed_lease = state::claim_tx(
                &mut failure_tx,
                "pg-atomic-failure@1",
                "digest",
                "execution",
                "holder",
                10,
                20,
            )
            .await
            .expect("claim before injected failure");
            state::fail_tx(&mut failure_tx, &failed_lease, "injected", 11)
                .await
                .expect("write failure classification");
            failure_tx.rollback().await.expect("rollback failure state");
            assert!(
                state::load(&db.pool, "pg-atomic-failure@1")
                    .await
                    .expect("load rolled-back failure")
                    .is_none()
            );

            let mut owner_tx = db.pool.begin().await.expect("begin lock owner transaction");
            let owner_lease = state::claim_tx(
                &mut owner_tx,
                "pg-row-lock@1",
                "digest",
                "owner-execution",
                "owner-holder",
                10,
                10,
            )
            .await
            .expect("claim row lock");
            let contender_pool = db.pool.clone();
            let (started_tx, started_rx) = oneshot::channel();
            let contender = tokio::spawn(async move {
                let mut tx = contender_pool
                    .begin()
                    .await
                    .expect("begin contender transaction");
                let backend_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                    .fetch_one(&mut *tx)
                    .await
                    .expect("identify contender PostgreSQL backend");
                let _ = started_tx.send(backend_pid);
                let claimed = state::claim_tx(
                    &mut tx,
                    "pg-row-lock@1",
                    "digest",
                    "contender-execution",
                    "contender-holder",
                    15,
                    10,
                )
                .await;
                match claimed {
                    Ok(lease) => {
                        tx.commit().await.expect("commit contender claim");
                        Ok(lease)
                    }
                    Err(error) => {
                        tx.rollback().await.expect("rollback contender claim");
                        Err(error)
                    }
                }
            });
            let contender_pid = started_rx.await.expect("contender entered transaction");
            timeout(Duration::from_secs(2), async {
                loop {
                    let wait_event: Option<String> = sqlx::query_scalar(
                        "SELECT wait_event_type FROM pg_stat_activity WHERE pid = $1",
                    )
                    .bind(contender_pid)
                    .fetch_one(&db.pool)
                    .await
                    .expect("inspect contender lock wait");
                    if wait_event.as_deref() == Some("Lock") {
                        break;
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("contender must wait behind the transaction-held PostgreSQL row lock");

            state::complete_tx(&mut owner_tx, &owner_lease, 21)
                .await
                .expect("complete after nominal lease expiry under row lock");
            owner_tx.commit().await.expect("commit owner completion");
            let contender_result = contender.await.expect("join contender");
            assert!(
                contender_result.is_err(),
                "completed task cannot be reclaimed"
            );
            let completed = state::load(&db.pool, "pg-row-lock@1")
                .await
                .expect("load completed row-lock state")
                .expect("state row remains");
            assert_eq!(
                completed.kind().expect("valid state"),
                TaskStateKind::Completed
            );
            assert_eq!(completed.fencing_token, owner_lease.fencing_token);
            assert_eq!(completed.heartbeat_at, None);
            assert_eq!(completed.lease_expires_at, None);
            assert_eq!(completed.completed_at, Some(21));
            db.close().await;
        });
    }
}
