//! Pure classification logic (and its operator-config parsing counterpart)
//! for the one-time `memory_kind` backfill migration. Deliberately kept
//! dependency-free (no SQL/RDB access): `grpc-admin`'s `migrate-memory-kind`
//! binary collects `ThreadEvidence` from the database and calls
//! `classify_thread` here, so the decision logic stays unit-testable
//! without a database and is shared between `plan`/`apply`/`verify`
//! subcommands. See docs/memory-kind-implementation-plan_ja.md for why
//! this lives in `infra` alongside the evidence/audit/mapping types
//! rather than in `app`.

use protobuf::llm_memory::data::MemoryKind;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// Evidence collected from a legacy thread before migration writes anything.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThreadEvidence {
    pub thread_id: i64,
    pub legacy_user_id: i64,
    pub labels: Vec<String>,
    pub metadata_source_user_id: Option<i64>,
    pub has_invalid_metadata: bool,
    pub has_missing_summary_source_thread: bool,
    pub has_metadata_owner_conflict: bool,
    pub has_summary_metadata: bool,
    pub aggregate_metadata_kind: Option<i32>,
    pub has_aggregate_metadata_conflict: bool,
    pub has_invalid_aggregate_source_hierarchy: bool,
    pub has_reflection_sidecar: bool,
    pub has_sidecar_thread_mismatch: bool,
    pub has_reflection_origin_owner_mismatch: bool,
    pub reflection_origin_user_ids: BTreeSet<i64>,
    pub has_non_sidecar_memory: bool,
    pub role_reflection_without_sidecar: bool,
}

/// Operator-provided, exact-match classification rules.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassificationMapping {
    pub summary_labels: BTreeMap<String, i32>,
    pub explicit_owners_by_thread_id: BTreeMap<i64, i64>,
}

impl Default for ClassificationMapping {
    // Matches the `memory_thread_label_prefix` defaults the summary
    // workflows actually emit (see `agent-chat-import/workers/*/
    // *-summary-single.yaml`), and the canonical default set fixed by
    // docs/memory-kind-migration-plan_ja.md §5.2. Deployments that
    // changed the label prefix must pass an explicit mapping to `plan`
    // rather than relying on guessed variants here.
    fn default() -> Self {
        Self {
            summary_labels: BTreeMap::from([
                ("summary".to_string(), MemoryKind::ThreadSummary as i32),
                ("daily_summary".to_string(), MemoryKind::DailySummary as i32),
                (
                    "weekly_summary".to_string(),
                    MemoryKind::WeeklySummary as i32,
                ),
                (
                    "monthly_summary".to_string(),
                    MemoryKind::MonthlySummary as i32,
                ),
            ]),
            explicit_owners_by_thread_id: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MappingFile {
    #[serde(default)]
    summary_labels: Vec<SummaryLabelMapping>,
    #[serde(default)]
    explicit_owners: Vec<ExplicitOwnerMapping>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SummaryLabelMapping {
    label: String,
    memory_kind: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExplicitOwnerMapping {
    thread_id: i64,
    owner_user_id: i64,
}

/// Parse the operator JSON mapping without accepting numeric enum values.
/// Named kinds prevent a typo from silently changing a generated category.
pub fn parse_mapping_json(input: &str) -> anyhow::Result<ClassificationMapping> {
    let file: MappingFile = serde_json::from_str(input)?;
    let mut mapping = ClassificationMapping::default();
    let mut configured_summary_labels = BTreeSet::new();
    for entry in file.summary_labels {
        let kind = match entry.memory_kind.as_str() {
            "THREAD_SUMMARY" => MemoryKind::ThreadSummary as i32,
            "DAILY_SUMMARY" => MemoryKind::DailySummary as i32,
            "WEEKLY_SUMMARY" => MemoryKind::WeeklySummary as i32,
            "MONTHLY_SUMMARY" => MemoryKind::MonthlySummary as i32,
            other => anyhow::bail!("invalid summary memory_kind: {other}"),
        };
        if entry.label.is_empty() || !configured_summary_labels.insert(entry.label.clone()) {
            anyhow::bail!("duplicate or empty summary label: {}", entry.label);
        }
        mapping.summary_labels.insert(entry.label, kind);
    }
    for entry in file.explicit_owners {
        if entry.thread_id <= 0
            || entry.owner_user_id <= 0
            || mapping
                .explicit_owners_by_thread_id
                .insert(entry.thread_id, entry.owner_user_id)
                .is_some()
        {
            anyhow::bail!("duplicate or invalid explicit owner mapping");
        }
    }
    Ok(mapping)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Classification {
    Resolved {
        memory_kind: i32,
        owner_user_id: i64,
    },
    Unresolved {
        reason: String,
    },
    ReflectionSplitRequired {
        origin_user_ids: BTreeSet<i64>,
    },
}

/// The deliberately narrow evidence used by the client-side backfill.
/// It is separate from `Classification`: Lookback data is migrated without
/// ownership inference, reference repair, or reflection splitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientKindDecision {
    pub memory_kind: i32,
    pub label_kind: Option<i32>,
    pub external_id_kind: Option<i32>,
}

impl ClientKindDecision {
    pub fn has_conflict(self) -> bool {
        self.label_kind
            .zip(self.external_id_kind)
            .is_some_and(|(label, external_id)| label != external_id)
    }
}

/// Classify one or more labels using the client migration's fixed precedence.
/// A higher lifecycle tier wins when old data has more than one marker.
pub fn client_kind_from_labels(labels: impl IntoIterator<Item = impl AsRef<str>>) -> Option<i32> {
    labels
        .into_iter()
        .filter_map(|label| client_kind_from_label(label.as_ref()))
        .max_by_key(|kind| client_kind_priority(*kind))
}

/// Classify one or more external IDs using their stable producer prefixes.
pub fn client_kind_from_external_ids(
    external_ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> Option<i32> {
    external_ids
        .into_iter()
        .filter_map(|external_id| client_kind_from_external_id(external_id.as_ref()))
        .max_by_key(|kind| client_kind_priority(*kind))
}

/// Resolve client-side evidence. Labels intentionally override external IDs:
/// labels are the explicit thread classification marker while external IDs are
/// a fallback for older generated records.
pub fn classify_client_memory_kind(
    labels: impl IntoIterator<Item = impl AsRef<str>>,
    external_ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> ClientKindDecision {
    let label_kind = client_kind_from_labels(labels);
    let external_id_kind = client_kind_from_external_ids(external_ids);
    ClientKindDecision {
        memory_kind: label_kind
            .or(external_id_kind)
            .unwrap_or(MemoryKind::Raw as i32),
        label_kind,
        external_id_kind,
    }
}

fn client_kind_from_label(label: &str) -> Option<i32> {
    match label {
        "reflection" => Some(MemoryKind::Reflection as i32),
        "personality" | "personality_profile" => Some(MemoryKind::Personality as i32),
        "monthly_summary" => Some(MemoryKind::MonthlySummary as i32),
        "weekly_summary" => Some(MemoryKind::WeeklySummary as i32),
        "daily_summary" => Some(MemoryKind::DailySummary as i32),
        "summary" => Some(MemoryKind::ThreadSummary as i32),
        _ => None,
    }
}

fn client_kind_from_external_id(external_id: &str) -> Option<i32> {
    [
        ("reflection:", MemoryKind::Reflection as i32),
        ("personality:", MemoryKind::Personality as i32),
        ("personality_profile:", MemoryKind::Personality as i32),
        ("monthly:", MemoryKind::MonthlySummary as i32),
        ("weekly:", MemoryKind::WeeklySummary as i32),
        ("daily:", MemoryKind::DailySummary as i32),
        ("summary:", MemoryKind::ThreadSummary as i32),
    ]
    .into_iter()
    .find_map(|(prefix, kind)| external_id.starts_with(prefix).then_some(kind))
}

/// `MemoryKind`'s discriminants are already ordered by lifecycle tier
/// (Raw < ThreadSummary < ... < Personality < Reflection), so the
/// discriminant value itself is the precedence used to pick a winner
/// when old data carries more than one marker.
fn client_kind_priority(kind: i32) -> i32 {
    kind
}

fn user_label_owner_ids(labels: &[String]) -> Result<BTreeSet<i64>, String> {
    labels
        .iter()
        .filter_map(|label| label.strip_prefix("user:"))
        .map(|value| {
            value
                .parse::<i64>()
                .map_err(|_| "invalid_user_owner_label".to_string())
        })
        .collect()
}

/// Collapse a set of candidate owner ids gathered from independent
/// evidence sources (metadata, explicit mapping, `user:` labels) down to
/// a single owner. More than one distinct id means the evidence
/// conflicts; `empty_reason` distinguishes "no evidence at all" from
/// that conflict for callers where the two need different audit reasons.
fn resolve_single_owner(candidates: BTreeSet<i64>, empty_reason: &str) -> Result<i64, String> {
    match candidates.len() {
        1 => Ok(*candidates.iter().next().expect("len == 1")),
        0 => Err(empty_reason.to_string()),
        _ => Err("conflicting_owner_evidence".to_string()),
    }
}

/// Reject evidence that is internally inconsistent regardless of the
/// operator mapping — each of these signals means the legacy row(s)
/// don't fit the migration's invariants and need a human to resolve
/// them rather than being auto-classified.
fn reject_inconsistent_evidence(evidence: &ThreadEvidence) -> Option<Classification> {
    if evidence.role_reflection_without_sidecar {
        return Some(Classification::Unresolved {
            reason: "reflection_role_without_sidecar".to_string(),
        });
    }
    if evidence.has_metadata_owner_conflict {
        return Some(Classification::Unresolved {
            reason: "conflicting_owner_evidence".to_string(),
        });
    }
    if evidence.has_invalid_metadata {
        return Some(Classification::Unresolved {
            reason: "invalid_metadata".to_string(),
        });
    }
    if evidence.has_aggregate_metadata_conflict {
        return Some(Classification::Unresolved {
            reason: "conflicting_summary_metadata".to_string(),
        });
    }
    if evidence.has_invalid_aggregate_source_hierarchy {
        return Some(Classification::Unresolved {
            reason: "invalid_aggregate_source_hierarchy".to_string(),
        });
    }
    if evidence.has_sidecar_thread_mismatch {
        return Some(Classification::Unresolved {
            reason: "reflection_sidecar_thread_mismatch".to_string(),
        });
    }
    if evidence.has_reflection_origin_owner_mismatch {
        return Some(Classification::Unresolved {
            reason: "reflection_origin_owner_mismatch".to_string(),
        });
    }
    None
}

/// Classify only from explicit evidence. A reserved legacy owner is never a
/// classification signal because it can be a legitimate normal user.
pub fn classify_thread(
    evidence: &ThreadEvidence,
    mapping: &ClassificationMapping,
) -> Classification {
    if let Some(rejected) = reject_inconsistent_evidence(evidence) {
        return rejected;
    }
    // Operator-supplied label->kind rules may only target the summary
    // tiers; Personality/Reflection are derived from evidence, not from
    // operator mapping, so any other value here is a config error.
    let summary_kind_range = MemoryKind::ThreadSummary as i32..=MemoryKind::MonthlySummary as i32;
    if mapping
        .summary_labels
        .values()
        .any(|kind| !summary_kind_range.contains(kind))
    {
        return Classification::Unresolved {
            reason: "invalid_summary_mapping_kind".to_string(),
        };
    }
    // A reflection thread must contain only the reflection memory (plus
    // its sidecar); any other memory sharing the thread means the legacy
    // data doesn't fit the reflection invariant and needs a human to
    // resolve it rather than being auto-split.
    if evidence.has_reflection_sidecar && evidence.has_non_sidecar_memory {
        return Classification::Unresolved {
            reason: "reflection_thread_with_non_sidecar_memory".to_string(),
        };
    }
    let summary_kinds: Vec<i32> = evidence
        .labels
        .iter()
        .filter_map(|label| mapping.summary_labels.get(label).copied())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let has_personality_label = evidence
        .labels
        .iter()
        .any(|label| label == "personality" || label == "personality_profile");
    if !has_personality_label && evidence.has_missing_summary_source_thread {
        return Classification::Unresolved {
            reason: "missing_summary_source_thread".to_string(),
        };
    }
    // Personality metadata may legitimately retain source-thread provenance,
    // but aggregate tier metadata is a competing generated-artifact signal.
    if has_personality_label && evidence.aggregate_metadata_kind.is_some() {
        return Classification::Unresolved {
            reason: "conflicting_generated_kinds".to_string(),
        };
    }
    if summary_kinds.is_empty()
        && !has_personality_label
        && (evidence.has_summary_metadata || evidence.aggregate_metadata_kind.is_some())
    {
        return Classification::Unresolved {
            reason: "summary_metadata_without_mapped_label".to_string(),
        };
    }

    let label_owner_ids = if summary_kinds.is_empty() && !has_personality_label {
        BTreeSet::new()
    } else {
        match user_label_owner_ids(&evidence.labels) {
            Ok(ids) => ids,
            Err(reason) => return Classification::Unresolved { reason },
        }
    };

    let mut generated_kinds = summary_kinds;
    if has_personality_label {
        generated_kinds.push(MemoryKind::Personality as i32);
    }
    if evidence.has_reflection_sidecar {
        generated_kinds.push(MemoryKind::Reflection as i32);
    }
    generated_kinds.sort_unstable();
    generated_kinds.dedup();

    const PERSONALITY: i32 = MemoryKind::Personality as i32;
    const REFLECTION: i32 = MemoryKind::Reflection as i32;
    match generated_kinds.as_slice() {
        [] => Classification::Resolved {
            memory_kind: MemoryKind::Raw as i32,
            owner_user_id: evidence.legacy_user_id,
        },
        // Unlike Personality/summary owner conflicts (which are just
        // Unresolved below), multiple reflection origin owners sharing
        // one legacy aggregate thread is an expected, mechanically
        // recoverable shape: the aggregate thread must be split into
        // one thread per origin user, so it gets its own outcome
        // instead of being reported as a plain conflict.
        [REFLECTION] => match evidence.reflection_origin_user_ids.len() {
            0 => Classification::Unresolved {
                reason: "reflection_sidecar_without_origin_owner".to_string(),
            },
            1 => Classification::Resolved {
                memory_kind: MemoryKind::Reflection as i32,
                owner_user_id: *evidence
                    .reflection_origin_user_ids
                    .first()
                    .expect("len == 1"),
            },
            _ => Classification::ReflectionSplitRequired {
                origin_user_ids: evidence.reflection_origin_user_ids.clone(),
            },
        },
        [PERSONALITY] => {
            let owners = label_owner_ids
                .iter()
                .copied()
                .chain(evidence.metadata_source_user_id)
                .collect::<BTreeSet<_>>();
            match resolve_single_owner(owners, "personality_label_without_owner") {
                Ok(owner_user_id) => Classification::Resolved {
                    memory_kind: MemoryKind::Personality as i32,
                    owner_user_id,
                },
                Err(reason) => Classification::Unresolved { reason },
            }
        }
        [memory_kind] => {
            if evidence
                .aggregate_metadata_kind
                .is_some_and(|metadata_kind| metadata_kind != *memory_kind)
            {
                return Classification::Unresolved {
                    reason: "conflicting_summary_metadata".to_string(),
                };
            }
            let metadata_matches = if *memory_kind == MemoryKind::ThreadSummary as i32 {
                evidence.has_summary_metadata && evidence.aggregate_metadata_kind.is_none()
            } else if summary_kind_range.contains(memory_kind) {
                evidence.aggregate_metadata_kind == Some(*memory_kind)
            } else {
                false
            };
            if !metadata_matches {
                return Classification::Unresolved {
                    reason: "summary_label_without_matching_metadata".to_string(),
                };
            }
            let owners = [
                evidence.metadata_source_user_id,
                mapping
                    .explicit_owners_by_thread_id
                    .get(&evidence.thread_id)
                    .copied(),
            ]
            .into_iter()
            .flatten()
            .chain(label_owner_ids.iter().copied())
            .collect::<BTreeSet<_>>();
            match resolve_single_owner(owners, "generated_label_without_owner") {
                Ok(owner_user_id) => Classification::Resolved {
                    memory_kind: *memory_kind,
                    owner_user_id,
                },
                Err(reason) => Classification::Unresolved { reason },
            }
        }
        _ => Classification::Unresolved {
            reason: "conflicting_generated_kinds".to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_kind_priority_follows_lifecycle_tier_order() {
        let tiers = [
            MemoryKind::Raw as i32,
            MemoryKind::ThreadSummary as i32,
            MemoryKind::DailySummary as i32,
            MemoryKind::WeeklySummary as i32,
            MemoryKind::MonthlySummary as i32,
            MemoryKind::Personality as i32,
            MemoryKind::Reflection as i32,
        ];
        for pair in tiers.windows(2) {
            assert!(
                client_kind_priority(pair[0]) < client_kind_priority(pair[1]),
                "expected priority({:?}) < priority({:?})",
                pair[0],
                pair[1]
            );
        }
    }

    #[test]
    fn client_classification_prefers_labels_over_external_ids() {
        let decision = classify_client_memory_kind(["summary"], ["daily:10:2026-07-20:_all"]);

        assert_eq!(decision.memory_kind, MemoryKind::ThreadSummary as i32);
        assert_eq!(decision.label_kind, Some(MemoryKind::ThreadSummary as i32));
        assert_eq!(
            decision.external_id_kind,
            Some(MemoryKind::DailySummary as i32)
        );
        assert!(decision.has_conflict());
    }

    #[test]
    fn client_classification_uses_fixed_precedence_within_each_source() {
        let decision = classify_client_memory_kind(
            ["summary", "monthly_summary", "reflection"],
            ["daily:10:2026-07-20:_all", "weekly:10:2026-W30:_all"],
        );

        assert_eq!(decision.memory_kind, MemoryKind::Reflection as i32);
        assert_eq!(decision.label_kind, Some(MemoryKind::Reflection as i32));
        assert_eq!(
            decision.external_id_kind,
            Some(MemoryKind::WeeklySummary as i32)
        );
    }

    #[test]
    fn client_classification_falls_back_to_raw_without_markers() {
        let decision = classify_client_memory_kind(["project:example"], ["import:42"]);

        assert_eq!(decision.memory_kind, MemoryKind::Raw as i32);
        assert_eq!(decision.label_kind, None);
        assert_eq!(decision.external_id_kind, None);
    }

    #[test]
    fn parses_named_mapping_json() {
        let mapping = parse_mapping_json(
            r#"{"summary_labels":[{"label":"daily","memory_kind":"DAILY_SUMMARY"}],"explicit_owners":[{"thread_id":10,"owner_user_id":20}]}"#,
        )
        .unwrap();
        assert_eq!(
            mapping.summary_labels["daily"],
            MemoryKind::DailySummary as i32
        );
        assert_eq!(mapping.explicit_owners_by_thread_id[&10], 20);
    }

    #[test]
    fn uses_standard_summary_labels_without_operator_mapping() {
        let mapping = parse_mapping_json("{}").unwrap();

        assert_eq!(
            mapping.summary_labels["summary"],
            MemoryKind::ThreadSummary as i32
        );
        assert_eq!(
            mapping.summary_labels["daily_summary"],
            MemoryKind::DailySummary as i32
        );
        assert_eq!(
            mapping.summary_labels["weekly_summary"],
            MemoryKind::WeeklySummary as i32
        );
        assert_eq!(
            mapping.summary_labels["monthly_summary"],
            MemoryKind::MonthlySummary as i32
        );
    }

    #[test]
    fn classifies_workflow_default_summary_label_without_operator_mapping() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["summary".to_string()],
                has_summary_metadata: true,
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &parse_mapping_json("{}").unwrap(),
        );

        assert_eq!(
            result,
            Classification::Resolved {
                memory_kind: MemoryKind::ThreadSummary as i32,
                owner_user_id: 10,
            }
        );
    }

    #[test]
    fn operator_mapping_can_override_a_standard_label() {
        let mapping = parse_mapping_json(
            r#"{"summary_labels":[{"label":"summary","memory_kind":"DAILY_SUMMARY"}]}"#,
        )
        .unwrap();

        assert_eq!(
            mapping.summary_labels["summary"],
            MemoryKind::DailySummary as i32
        );
    }

    #[test]
    fn rejects_unknown_mapping_fields() {
        assert!(parse_mapping_json(r#"{"summary_lables":[]}"#).is_err());
        assert!(
            parse_mapping_json(
                r#"{"summary_labels":[{"label":"custom","memory_kind":"THREAD_SUMMARY","kind":"bad"}]}"#,
            )
            .is_err()
        );
        assert!(
            parse_mapping_json(
                r#"{"explicit_owners":[{"thread_id":1,"owner_user_id":2,"owner":3}]}"#
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_non_summary_mapping_json_kind() {
        assert!(
            parse_mapping_json(
                r#"{"summary_labels":[{"label":"bad","memory_kind":"PERSONALITY"}]}"#
            )
            .is_err()
        );
    }

    #[test]
    fn reserved_owner_alone_remains_raw() {
        let result = classify_thread(
            &ThreadEvidence {
                legacy_user_id: 100_000,
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Resolved {
                memory_kind: MemoryKind::Raw as i32,
                owner_user_id: 100_000,
            }
        );
    }

    #[test]
    fn reflection_role_without_sidecar_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                legacy_user_id: 300_000,
                role_reflection_without_sidecar: true,
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "reflection_role_without_sidecar".to_string(),
            }
        );
    }

    #[test]
    fn reflection_origin_owner_mismatch_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                has_reflection_sidecar: true,
                has_reflection_origin_owner_mismatch: true,
                reflection_origin_user_ids: BTreeSet::from([10]),
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "reflection_origin_owner_mismatch".to_string(),
            }
        );
    }

    #[test]
    fn summary_label_needs_owner_evidence() {
        let result = classify_thread(
            &ThreadEvidence {
                legacy_user_id: 100_000,
                labels: vec!["thread-summary".to_string()],
                has_summary_metadata: true,
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([("thread-summary".to_string(), 2)]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "generated_label_without_owner".to_string(),
            }
        );
    }

    #[test]
    fn reflection_and_summary_evidence_conflict_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["daily-summary".to_string()],
                has_reflection_sidecar: true,
                reflection_origin_user_ids: BTreeSet::from([10]),
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([("daily-summary".to_string(), 3)]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "conflicting_generated_kinds".to_string(),
            }
        );
    }

    #[test]
    fn aggregate_summary_without_matching_metadata_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["daily-summary".to_string()],
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([("daily-summary".to_string(), 3)]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "summary_label_without_matching_metadata".to_string(),
            }
        );
    }

    #[test]
    fn invalid_aggregate_source_hierarchy_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["weekly-summary".to_string()],
                metadata_source_user_id: Some(10),
                aggregate_metadata_kind: Some(MemoryKind::WeeklySummary as i32),
                has_invalid_aggregate_source_hierarchy: true,
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([(
                    "weekly-summary".to_string(),
                    MemoryKind::WeeklySummary as i32,
                )]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "invalid_aggregate_source_hierarchy".to_string(),
            }
        );
    }

    #[test]
    fn invalid_metadata_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                has_invalid_metadata: true,
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "invalid_metadata".to_string(),
            }
        );
    }

    #[test]
    fn missing_summary_source_thread_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["thread-summary".to_string()],
                has_summary_metadata: true,
                metadata_source_user_id: Some(10),
                has_missing_summary_source_thread: true,
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([(
                    "thread-summary".to_string(),
                    MemoryKind::ThreadSummary as i32,
                )]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "missing_summary_source_thread".to_string(),
            }
        );
    }

    #[test]
    fn personality_label_with_owner_metadata_is_personality() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["personality_profile".to_string()],
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Resolved {
                memory_kind: MemoryKind::Personality as i32,
                owner_user_id: 10,
            }
        );
    }

    #[test]
    fn personality_label_with_user_label_is_personality_without_metadata_owner() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["personality".to_string(), "user:10".to_string()],
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );

        assert_eq!(
            result,
            Classification::Resolved {
                memory_kind: MemoryKind::Personality as i32,
                owner_user_id: 10,
            }
        );
    }

    #[test]
    fn personality_source_thread_metadata_is_not_summary_evidence() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["personality".to_string()],
                metadata_source_user_id: Some(10),
                has_summary_metadata: true,
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Resolved {
                memory_kind: MemoryKind::Personality as i32,
                owner_user_id: 10,
            }
        );
    }

    #[test]
    fn personality_with_missing_source_thread_remains_resolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["personality".to_string()],
                metadata_source_user_id: Some(10),
                has_missing_summary_source_thread: true,
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Resolved {
                memory_kind: MemoryKind::Personality as i32,
                owner_user_id: 10,
            }
        );
    }

    #[test]
    fn personality_with_aggregate_metadata_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["personality".to_string()],
                metadata_source_user_id: Some(10),
                aggregate_metadata_kind: Some(MemoryKind::DailySummary as i32),
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "conflicting_generated_kinds".to_string(),
            }
        );
    }

    #[test]
    fn conflicting_owner_evidence_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                legacy_user_id: 100_000,
                labels: vec!["thread-summary".to_string()],
                metadata_source_user_id: Some(10),
                has_summary_metadata: true,
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([("thread-summary".to_string(), 2)]),
                explicit_owners_by_thread_id: BTreeMap::from([(0, 20)]),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "conflicting_owner_evidence".to_string(),
            }
        );
    }

    #[test]
    fn explicit_owner_mapping_applies_only_to_its_thread() {
        let mapping = ClassificationMapping {
            summary_labels: BTreeMap::from([("thread-summary".to_string(), 2)]),
            explicit_owners_by_thread_id: BTreeMap::from([(101, 10)]),
        };
        let mapped = classify_thread(
            &ThreadEvidence {
                thread_id: 101,
                legacy_user_id: 100_000,
                labels: vec!["thread-summary".to_string()],
                has_summary_metadata: true,
                ..Default::default()
            },
            &mapping,
        );
        let unmapped = classify_thread(
            &ThreadEvidence {
                thread_id: 102,
                legacy_user_id: 100_000,
                labels: vec!["thread-summary".to_string()],
                has_summary_metadata: true,
                ..Default::default()
            },
            &mapping,
        );
        assert_eq!(
            mapped,
            Classification::Resolved {
                memory_kind: MemoryKind::ThreadSummary as i32,
                owner_user_id: 10,
            }
        );
        assert_eq!(
            unmapped,
            Classification::Unresolved {
                reason: "generated_label_without_owner".to_string(),
            }
        );
    }

    #[test]
    fn thread_summary_conflicting_with_aggregate_metadata_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["thread-summary".to_string()],
                has_summary_metadata: true,
                aggregate_metadata_kind: Some(3),
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([("thread-summary".to_string(), 2)]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "conflicting_summary_metadata".to_string(),
            }
        );
    }

    #[test]
    fn non_summary_mapping_kind_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["bad-summary-label".to_string()],
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([("bad-summary-label".to_string(), 6)]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "invalid_summary_mapping_kind".to_string(),
            }
        );
    }

    #[test]
    fn unmapped_summary_metadata_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["custom-summary".to_string()],
                has_summary_metadata: true,
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "summary_metadata_without_mapped_label".to_string(),
            }
        );
    }

    #[test]
    fn personality_conflicting_user_label_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["personality".to_string(), "user:20".to_string()],
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "conflicting_owner_evidence".to_string(),
            }
        );
    }

    #[test]
    fn summary_conflicting_user_label_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                labels: vec!["thread-summary".to_string(), "user:20".to_string()],
                has_summary_metadata: true,
                metadata_source_user_id: Some(10),
                ..Default::default()
            },
            &ClassificationMapping {
                summary_labels: BTreeMap::from([("thread-summary".to_string(), 2)]),
                explicit_owners_by_thread_id: BTreeMap::new(),
            },
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "conflicting_owner_evidence".to_string(),
            }
        );
    }

    #[test]
    fn raw_memory_with_non_numeric_user_label_remains_raw() {
        let result = classify_thread(
            &ThreadEvidence {
                legacy_user_id: 10,
                labels: vec!["user:admin".to_string()],
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Resolved {
                memory_kind: MemoryKind::Raw as i32,
                owner_user_id: 10,
            }
        );
    }

    #[test]
    fn reflection_with_multiple_origin_owners_requires_split_plan() {
        let result = classify_thread(
            &ThreadEvidence {
                has_reflection_sidecar: true,
                reflection_origin_user_ids: BTreeSet::from([10, 20]),
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::ReflectionSplitRequired {
                origin_user_ids: BTreeSet::from([10, 20]),
            }
        );
    }

    #[test]
    fn reflection_thread_with_non_sidecar_memory_is_unresolved() {
        let result = classify_thread(
            &ThreadEvidence {
                has_reflection_sidecar: true,
                reflection_origin_user_ids: BTreeSet::from([10]),
                has_non_sidecar_memory: true,
                ..Default::default()
            },
            &ClassificationMapping::default(),
        );
        assert_eq!(
            result,
            Classification::Unresolved {
                reason: "reflection_thread_with_non_sidecar_memory".to_string(),
            }
        );
    }
}
