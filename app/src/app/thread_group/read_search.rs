//! Read projections, search cursors, and delegated member search.

use super::prelude::*;

/// Search target for one raw member predicate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupSearchTarget {
    Thread,
    Memory,
}

/// Search mode delegated to the existing ThreadVector / MemoryVector paths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupSearchMode {
    Keyword,
    Semantic,
    Hybrid,
}

/// One raw member predicate. The app layer owns the same-member and
/// cross-member semantics; the transport only maps proto values into this
/// type.
#[derive(Clone, Debug, PartialEq)]
pub struct ThreadGroupMemberSearchQuery {
    pub target: GroupSearchTarget,
    pub mode: GroupSearchMode,
    pub query_text: String,
    pub query_vectors: Vec<Vec<f32>>,
    pub thread_filter: Option<protobuf::llm_memory::data::ThreadSearchFilter>,
    pub memory_filter: Option<protobuf::llm_memory::data::MemorySearchFilter>,
    pub hybrid_options: Option<protobuf::llm_memory::data::HybridSearchOptions>,
}

/// One member-level relevance result returned by a delegated search path.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ThreadGroupMemberSearchHit {
    pub thread_id: i64,
    pub score: f32,
}

/// Search adapter seam. A provider searches exactly one current member so
/// the read model can enforce same-member predicates without approximating
/// the result from a group-wide query.
#[async_trait]
pub trait ThreadGroupMemberSearchProvider: Send + Sync {
    async fn search_member(
        &self,
        query: &ThreadGroupMemberSearchQuery,
        group_id: i64,
        thread_id: i64,
    ) -> anyhow::Result<Option<ThreadGroupMemberSearchHit>>;
}

/// Inputs that participate in the current-membership snapshot digest.
/// Conversation timestamps are included so a changed live Thread makes a
/// previously generated group summary stale without exposing its content.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MembershipSnapshotEntry {
    pub thread_canonical_key: String,
    pub state: String,
    pub role: String,
    pub deleted_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub last_message_at: Option<i64>,
}

/// Build the stable digest used to compare a saved group-summary input with
/// the current membership read model. Sorting here makes database row order
/// irrelevant to the digest.
pub fn membership_snapshot_digest(entries: &[MembershipSnapshotEntry]) -> String {
    let mut sorted = entries.to_vec();
    sorted.sort_by(|left, right| {
        left.thread_canonical_key
            .cmp(&right.thread_canonical_key)
            .then_with(|| left.state.cmp(&right.state))
            .then_with(|| left.role.cmp(&right.role))
    });

    let mut encoded = common::thread_group_key::canonical_serialize_v2(&[
        Some("thread-group-membership-snapshot-v1"),
        Some(&sorted.len().to_string()),
    ]);
    for entry in sorted {
        let deleted_at = entry.deleted_at.map(|value| value.to_string());
        let updated_at = entry.updated_at.map(|value| value.to_string());
        let last_message_at = entry.last_message_at.map(|value| value.to_string());
        encoded.extend(common::thread_group_key::canonical_serialize_v2(&[
            Some(&entry.thread_canonical_key),
            Some(&entry.state),
            Some(&entry.role),
            deleted_at.as_deref(),
            updated_at.as_deref(),
            last_message_at.as_deref(),
        ]));
    }
    sha256_hex(&encoded)
}

/// Safe display projection for a live Thread or a deleted placeholder.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadDisplayView {
    pub thread_id: Option<i64>,
    pub thread_canonical_key: String,
    pub description: Option<String>,
    pub source: Option<String>,
    pub created_at: Option<i64>,
    pub last_message_at: Option<i64>,
    pub deleted_at: Option<i64>,
}

#[cfg(test)]
mod snapshot_digest_tests {
    use super::*;

    #[test]
    fn membership_snapshot_digest_is_order_independent_but_changes_with_membership_time() {
        let first = vec![
            MembershipSnapshotEntry {
                thread_canonical_key: "b".into(),
                state: "active".into(),
                role: "member".into(),
                deleted_at: None,
                updated_at: Some(20),
                last_message_at: Some(19),
            },
            MembershipSnapshotEntry {
                thread_canonical_key: "a".into(),
                state: "deleted".into(),
                role: "root".into(),
                deleted_at: Some(30),
                updated_at: None,
                last_message_at: None,
            },
        ];
        let mut reversed = first.clone();
        reversed.reverse();

        let digest = membership_snapshot_digest(&first);
        assert_eq!(digest, membership_snapshot_digest(&reversed));

        reversed[0].last_message_at = Some(21);
        assert_ne!(digest, membership_snapshot_digest(&reversed));
    }

    fn search_query(
        target: GroupSearchTarget,
        mode: GroupSearchMode,
        text: &str,
        vectors: usize,
    ) -> ThreadGroupMemberSearchQuery {
        ThreadGroupMemberSearchQuery {
            target,
            mode,
            query_text: text.to_string(),
            query_vectors: (0..vectors).map(|_| vec![0.0_f32]).collect(),
            thread_filter: None,
            memory_filter: None,
            hybrid_options: None,
        }
    }

    #[test]
    fn validation_rejects_memory_semantic_without_query_text() {
        // Memory semantic embeds query_text server-side: an empty text must
        // be rejected before the vector path runs.
        let empty = search_query(
            GroupSearchTarget::Memory,
            GroupSearchMode::Semantic,
            "   ",
            0,
        );
        assert!(
            validate_group_search_request(std::slice::from_ref(&empty), None, 10).is_err(),
            "empty memory-semantic query_text must be InvalidArgument"
        );

        let with_text = search_query(
            GroupSearchTarget::Memory,
            GroupSearchMode::Semantic,
            "hello",
            0,
        );
        assert!(
            validate_group_search_request(std::slice::from_ref(&with_text), None, 10).is_ok(),
            "memory semantic only needs query_text"
        );
    }

    #[test]
    fn validation_keeps_thread_semantic_vector_behavior() {
        // Thread semantic uses the caller's vector; empty text is fine.
        let thread_vector =
            search_query(GroupSearchTarget::Thread, GroupSearchMode::Semantic, "", 1);
        assert!(
            validate_group_search_request(std::slice::from_ref(&thread_vector), None, 10).is_ok()
        );

        let missing_vector =
            search_query(GroupSearchTarget::Thread, GroupSearchMode::Semantic, "", 0);
        assert!(
            validate_group_search_request(std::slice::from_ref(&missing_vector), None, 10).is_err()
        );
    }

    #[test]
    fn validation_requires_both_inputs_for_hybrid() {
        let hybrid_missing_text =
            search_query(GroupSearchTarget::Memory, GroupSearchMode::Hybrid, "", 1);
        assert!(
            validate_group_search_request(std::slice::from_ref(&hybrid_missing_text), None, 10)
                .is_err()
        );
        let hybrid_complete = search_query(
            GroupSearchTarget::Memory,
            GroupSearchMode::Hybrid,
            "hello",
            1,
        );
        assert!(
            validate_group_search_request(std::slice::from_ref(&hybrid_complete), None, 10).is_ok()
        );
    }

    #[test]
    fn browse_filter_allows_cursor_search_without_a_text_predicate() {
        let filter = protobuf::llm_memory::data::ThreadSearchFilter {
            labels: vec!["project:lookback".into()],
            thread_created_after: Some(10),
            ..Default::default()
        };
        assert!(validate_group_search_request(&[], Some(&filter), 10).is_ok());
    }

    #[test]
    fn relation_endpoint_resolution_returns_group_and_display_for_cross_group_keys() {
        let relations = vec![ThreadRelationRow {
            id: 91,
            parent_thread_id: Some(11),
            child_thread_id: Some(22),
            parent_thread_canonical_key: "parent".into(),
            child_thread_canonical_key: "child".into(),
            parent_user_id: 1,
            parent_source: Some("codex".into()),
            parent_identity_scope: None,
            parent_native_id: None,
            child_user_id: 1,
            child_source: Some("codex".into()),
            child_identity_scope: None,
            child_native_id: None,
            relation_type: "continuation".into(),
            state: "active".into(),
            selection_basis: "source_exact".into(),
            source_confidence: Some("exact".into()),
            selected_observation_id: None,
            selected_operator_decision_id: None,
            created_at: 0,
            updated_at: 0,
        }];
        let members = vec![
            ThreadGroupMemberRow {
                group_id: 7,
                thread_id: Some(11),
                thread_canonical_key: "parent".into(),
                user_id: 1,
                source: Some("codex".into()),
                identity_scope: None,
                native_id: None,
                role: "root".into(),
                state: "active".into(),
                provenance: "reconciler".into(),
                deleted_at: None,
                created_at: 0,
                updated_at: 0,
            },
            ThreadGroupMemberRow {
                group_id: 8,
                thread_id: Some(22),
                thread_canonical_key: "child".into(),
                user_id: 1,
                source: Some("codex".into()),
                identity_scope: None,
                native_id: None,
                role: "member".into(),
                state: "active".into(),
                provenance: "reconciler".into(),
                deleted_at: None,
                created_at: 0,
                updated_at: 0,
            },
        ];
        let displays = vec![
            ThreadDisplayView {
                thread_id: Some(11),
                thread_canonical_key: "parent".into(),
                description: Some("Parent".into()),
                ..Default::default()
            },
            ThreadDisplayView {
                thread_id: Some(22),
                thread_canonical_key: "child".into(),
                description: Some("Child".into()),
                ..Default::default()
            },
        ];
        let endpoints = resolve_relation_endpoints(&relations, &members, &displays, &[7, 8]);
        assert_eq!(endpoints[0].parent_group_id, Some(7));
        assert_eq!(endpoints[0].child_group_id, Some(8));
        assert_eq!(
            endpoints[0]
                .parent_display
                .as_ref()
                .unwrap()
                .description
                .as_deref(),
            Some("Parent")
        );
        assert_eq!(
            endpoints[0]
                .child_display
                .as_ref()
                .unwrap()
                .description
                .as_deref(),
            Some("Child")
        );
    }
}

/// Read-model projection of one ThreadGroup with its derived
/// `latest_activity_at`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ThreadGroupView {
    pub id: i64,
    pub user_id: i64,
    pub group_canonical_key: String,
    pub title: Option<String>,
    pub status: String,
    pub grouping_authority: String,
    pub redirect_to_group_id: Option<i64>,
    pub latest_activity_at: Option<i64>,
    /// Display root: a current member with no active parent inside the
    /// group, tie-broken by canonical key ascending. Placeholder
    /// (deleted) members are eligible.
    pub root_thread_id: Option<i64>,
    pub root_thread_canonical_key: Option<String>,
    pub root_display: Option<ThreadDisplayView>,
    pub active_member_count: i64,
    pub deleted_member_count: i64,
    pub unresolved_count: i64,
    pub membership_snapshot_digest: String,
}

/// Operational snapshot for monitoring / alerting.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadGroupReconciliationReport {
    pub active_groups: i64,
    pub redirected_groups: i64,
    pub split_groups: i64,
    pub pending_candidates: i64,
    pub ambiguous_candidates: i64,
    pub conflict_candidates: i64,
    pub unsupported_observations: i64,
}

/// Read-model projection of one group's lineage.
#[derive(Clone, Debug)]
pub struct ThreadGroupLineageView {
    pub group: ThreadGroupView,
    pub members: Vec<ThreadGroupMemberRow>,
    pub member_displays: Vec<ThreadDisplayView>,
    pub relations: Vec<ThreadRelationRow>,
    pub unresolved: Vec<ThreadGroupCandidateAssociationRow>,
    pub observations: Vec<ThreadObservationRow>,
    pub relation_endpoints: Vec<ThreadRelationEndpointView>,
}

/// Batch-resolved endpoint projection for relation displays. The group IDs
/// are optional because an endpoint can remain only as historical evidence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ThreadRelationEndpointView {
    pub relation_id: i64,
    pub parent_group_id: Option<i64>,
    pub child_group_id: Option<i64>,
    pub parent_display: Option<ThreadDisplayView>,
    pub child_display: Option<ThreadDisplayView>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadGroupSearchWitness {
    pub predicate_index: usize,
    pub target: GroupSearchTarget,
    pub member: ThreadDisplayView,
    pub score: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadGroupSearchResult {
    pub group: ThreadGroupView,
    pub relevance_score: f32,
    pub witnesses: Vec<ThreadGroupSearchWitness>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ThreadGroupSearchPage {
    pub results: Vec<ThreadGroupSearchResult>,
    pub next_page_token: Option<String>,
}

#[derive(Clone, Debug)]
struct ThreadGroupSearchCursor {
    version: u8,
    snapshot: String,
    query_fingerprint: String,
    relevance_score_bits: u32,
    latest_activity_at: Option<i64>,
    group_canonical_key: String,
}

fn append_snapshot_fields(encoded: &mut Vec<u8>, fields: &[String]) {
    for field in fields {
        encoded.extend(common::thread_group_key::canonical_serialize_v2(&[Some(
            field.as_str(),
        )]));
    }
}

fn validate_group_search_request(
    queries: &[ThreadGroupMemberSearchQuery],
    browse_filter: Option<&ThreadSearchFilter>,
    page_size: usize,
) -> anyhow::Result<()> {
    if queries.is_empty() && browse_filter.is_none() {
        return Err(anyhow::Error::new(
            infra::error::LlmMemoryError::InvalidArgument(
                "thread-group search requires a predicate or browse_filter".to_string(),
            ),
        ));
    }
    if !queries.is_empty() && browse_filter.is_some() {
        return Err(anyhow::Error::new(
            infra::error::LlmMemoryError::InvalidArgument(
                "browse_filter cannot be combined with predicates".to_string(),
            ),
        ));
    }
    if page_size == 0 {
        return Err(anyhow::Error::new(
            infra::error::LlmMemoryError::InvalidArgument(
                "thread-group search page_size must be greater than zero".to_string(),
            ),
        ));
    }
    for query in queries {
        // Memory semantic search embeds `query_text` server-side (like
        // MemoryService.SearchSemantic), while thread semantic search uses
        // the caller's pre-computed vector. Hybrid uses both. Validate the
        // target-specific requirement before any vector execution.
        let is_memory = matches!(query.target, GroupSearchTarget::Memory);
        let requires_text = matches!(
            query.mode,
            GroupSearchMode::Keyword | GroupSearchMode::Hybrid
        ) || (matches!(query.mode, GroupSearchMode::Semantic) && is_memory);
        if requires_text && query.query_text.trim().is_empty() {
            return Err(anyhow::Error::new(
                infra::error::LlmMemoryError::InvalidArgument(
                    "thread-group keyword, hybrid, and memory-semantic queries require \
                     non-empty query_text"
                        .to_string(),
                ),
            ));
        }
        let requires_vector = matches!(query.mode, GroupSearchMode::Hybrid)
            || (matches!(query.mode, GroupSearchMode::Semantic) && !is_memory);
        if requires_vector && query.query_vectors.len() != 1 {
            return Err(anyhow::Error::new(
                infra::error::LlmMemoryError::InvalidArgument(
                    "thread-group thread-semantic and hybrid queries require exactly one \
                     query vector"
                        .to_string(),
                ),
            ));
        }
    }
    Ok(())
}

fn group_search_query_fingerprint(
    queries: &[ThreadGroupMemberSearchQuery],
    allow_cross_member: bool,
    browse_filter: Option<&ThreadSearchFilter>,
    owner_user_id: Option<i64>,
) -> String {
    let owner_user_id = owner_user_id.map(|user_id| user_id.to_string());
    let mut encoded = common::thread_group_key::canonical_serialize_v2(&[
        Some("group-owner"),
        owner_user_id.as_deref(),
        Some(if allow_cross_member { "cross" } else { "same" }),
    ]);
    for query in queries {
        let target = match query.target {
            GroupSearchTarget::Thread => "thread",
            GroupSearchTarget::Memory => "memory",
        };
        let mode = match query.mode {
            GroupSearchMode::Keyword => "keyword",
            GroupSearchMode::Semantic => "semantic",
            GroupSearchMode::Hybrid => "hybrid",
        };
        let vectors = query
            .query_vectors
            .iter()
            .flat_map(|vector| vector.iter().map(|value| value.to_bits().to_string()))
            .collect::<Vec<_>>()
            .join(",");
        let thread_filter = query
            .thread_filter
            .as_ref()
            .map(|filter| format!("{filter:?}"))
            .unwrap_or_default();
        let memory_filter = query
            .memory_filter
            .as_ref()
            .map(|filter| format!("{filter:?}"))
            .unwrap_or_default();
        let hybrid_options = query
            .hybrid_options
            .as_ref()
            .map(|options| format!("{options:?}"))
            .unwrap_or_default();
        for field in [
            target.to_string(),
            mode.to_string(),
            query.query_text.clone(),
            vectors,
            thread_filter,
            memory_filter,
            hybrid_options,
        ] {
            encoded.extend(common::thread_group_key::canonical_serialize_v2(&[Some(
                field.as_str(),
            )]));
        }
    }
    if let Some(filter) = browse_filter {
        encoded.extend(common::thread_group_key::canonical_serialize_v2(&[
            Some("browse_filter"),
            Some(&format!("{filter:?}")),
        ]));
    }
    sha256_hex(&encoded)
}

fn encode_group_search_cursor(cursor: &ThreadGroupSearchCursor) -> String {
    let value = serde_json::json!({
        "v": cursor.version,
        "s": cursor.snapshot,
        "q": cursor.query_fingerprint,
        "r": cursor.relevance_score_bits,
        "l": cursor.latest_activity_at,
        "k": cursor.group_canonical_key,
    });
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(value.to_string())
}

fn decode_group_search_cursor(token: &str) -> anyhow::Result<ThreadGroupSearchCursor> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|_| {
            anyhow::Error::new(infra::error::LlmMemoryError::InvalidArgument(
                "invalid thread-group search page token".to_string(),
            ))
        })?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| {
        anyhow::Error::new(infra::error::LlmMemoryError::InvalidArgument(
            "invalid thread-group search page token".to_string(),
        ))
    })?;
    let version = value
        .get("v")
        .and_then(serde_json::Value::as_u64)
        .filter(|version| *version == 1)
        .ok_or_else(|| {
            anyhow::Error::new(infra::error::LlmMemoryError::InvalidArgument(
                "unsupported thread-group search page token".to_string(),
            ))
        })? as u8;
    let snapshot = value
        .get("s")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::Error::new(infra::error::LlmMemoryError::InvalidArgument(
                "invalid thread-group search page token".to_string(),
            ))
        })?
        .to_string();
    let query_fingerprint = value
        .get("q")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::Error::new(infra::error::LlmMemoryError::InvalidArgument(
                "invalid thread-group search page token".to_string(),
            ))
        })?
        .to_string();
    let relevance_score_bits = value
        .get("r")
        .and_then(serde_json::Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .ok_or_else(|| {
            anyhow::Error::new(infra::error::LlmMemoryError::InvalidArgument(
                "invalid thread-group search page token".to_string(),
            ))
        })?;
    let latest_activity_at = value
        .get("l")
        .and_then(|value| (!value.is_null()).then(|| value.as_i64()))
        .flatten();
    let group_canonical_key = value
        .get("k")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow::Error::new(infra::error::LlmMemoryError::InvalidArgument(
                "invalid thread-group search page token".to_string(),
            ))
        })?
        .to_string();
    Ok(ThreadGroupSearchCursor {
        version,
        snapshot,
        query_fingerprint,
        relevance_score_bits,
        latest_activity_at,
        group_canonical_key,
    })
}

fn compare_group_search_results(
    left: &ThreadGroupSearchResult,
    right: &ThreadGroupSearchResult,
) -> std::cmp::Ordering {
    right
        .relevance_score
        .total_cmp(&left.relevance_score)
        .then_with(|| {
            compare_optional_desc(
                left.group.latest_activity_at,
                right.group.latest_activity_at,
            )
        })
        .then_with(|| {
            left.group
                .group_canonical_key
                .cmp(&right.group.group_canonical_key)
        })
}

fn compare_optional_desc(left: Option<i64>, right: Option<i64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => right.cmp(&left),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn resolve_relation_endpoints(
    relations: &[ThreadRelationRow],
    members: &[ThreadGroupMemberRow],
    displays: &[ThreadDisplayView],
    active_group_ids: &[i64],
) -> Vec<ThreadRelationEndpointView> {
    let active_groups = active_group_ids.iter().copied().collect::<HashSet<_>>();
    let member_for = |key: &str| {
        members
            .iter()
            .find(|member| member.thread_canonical_key == key)
    };
    let display_for = |key: &str| {
        displays
            .iter()
            .find(|display| display.thread_canonical_key == key)
    };
    relations
        .iter()
        .map(|relation| {
            let parent = member_for(&relation.parent_thread_canonical_key);
            let child = member_for(&relation.child_thread_canonical_key);
            ThreadRelationEndpointView {
                relation_id: relation.id,
                parent_group_id: parent
                    .filter(|member| active_groups.contains(&member.group_id))
                    .map(|member| member.group_id),
                child_group_id: child
                    .filter(|member| active_groups.contains(&member.group_id))
                    .map(|member| member.group_id),
                parent_display: display_for(&relation.parent_thread_canonical_key).cloned(),
                child_display: display_for(&relation.child_thread_canonical_key).cloned(),
            }
        })
        .collect()
}

fn cursor_is_before(cursor: &ThreadGroupSearchCursor, result: &ThreadGroupSearchResult) -> bool {
    let cursor_score = f32::from_bits(cursor.relevance_score_bits);
    match compare_group_search_results(
        result,
        &ThreadGroupSearchResult {
            group: ThreadGroupView {
                id: 0,
                user_id: 0,
                group_canonical_key: cursor.group_canonical_key.clone(),
                title: None,
                status: String::new(),
                grouping_authority: String::new(),
                redirect_to_group_id: None,
                latest_activity_at: cursor.latest_activity_at,
                root_thread_id: None,
                root_thread_canonical_key: None,
                root_display: None,
                active_member_count: 0,
                deleted_member_count: 0,
                unresolved_count: 0,
                membership_snapshot_digest: String::new(),
            },
            relevance_score: cursor_score,
            witnesses: Vec::new(),
        },
    ) {
        std::cmp::Ordering::Greater => true,
        std::cmp::Ordering::Equal => result.group.group_canonical_key > cursor.group_canonical_key,
        std::cmp::Ordering::Less => false,
    }
}

pub const THREAD_GROUP_SUMMARY_EXTERNAL_ID_PREFIX: &str = "thread-group-summary:";

pub fn thread_group_summary_external_id(group_id: i64) -> String {
    format!("{THREAD_GROUP_SUMMARY_EXTERNAL_ID_PREFIX}{group_id}")
}

/// Read-only ThreadGroup queries. No transaction is opened; each method
/// is a consistent-enough snapshot for display (the mutation paths own
/// the invariants).
pub struct ThreadGroupReadService {
    pool: &'static RdbPool,
    groups: ThreadGroupRepositoryImpl,
    members: ThreadGroupMemberRepositoryImpl,
    threads: ThreadRepositoryImpl,
    thread_labels: ThreadLabelRepositoryImpl,
    memories: MemoryRepositoryImpl,
    relations: ThreadRelationRepositoryImpl,
    candidates: ThreadGroupCandidateAssociationRepositoryImpl,
    observations: ThreadObservationRepositoryImpl,
    revision: ThreadGroupReadModelRevisionRepositoryImpl,
}

impl ThreadGroupReadService {
    pub fn new(pool: &'static RdbPool) -> Self {
        let id_generator = IdGeneratorWrapper::new();
        Self {
            pool,
            groups: ThreadGroupRepositoryImpl::new(id_generator.clone(), pool),
            members: ThreadGroupMemberRepositoryImpl::new(pool),
            threads: ThreadRepositoryImpl::new(id_generator.clone(), pool),
            thread_labels: ThreadLabelRepositoryImpl::new(pool),
            memories: MemoryRepositoryImpl::new(id_generator.clone(), pool),
            relations: ThreadRelationRepositoryImpl::new(id_generator.clone(), pool),
            candidates: ThreadGroupCandidateAssociationRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            observations: ThreadObservationRepositoryImpl::new(id_generator, pool),
            revision: ThreadGroupReadModelRevisionRepositoryImpl::new(pool),
        }
    }

    /// Backend-side aggregate revision of the tables a ThreadGroup search
    /// reads. O(1) row transfer (aggregates run in the database) rather
    /// than materialising every Memory / Group row.
    pub async fn read_model_revision(&self) -> anyhow::Result<ThreadGroupReadModelRevision> {
        self.revision.read_model_revision().await
    }

    /// Search current groups through one-member raw Thread / Memory queries.
    /// The returned cursor is an opaque keyset token containing the query
    /// fingerprint, sort boundary, and read-model snapshot digest.
    #[allow(clippy::too_many_arguments)]
    pub async fn search_groups<P: ThreadGroupMemberSearchProvider>(
        &self,
        provider: &P,
        queries: &[ThreadGroupMemberSearchQuery],
        allow_cross_member: bool,
        page_size: usize,
        page_token: Option<&str>,
        browse_filter: Option<&ThreadSearchFilter>,
        owner_user_id: Option<i64>,
    ) -> anyhow::Result<ThreadGroupSearchPage> {
        validate_group_search_request(queries, browse_filter, page_size)?;
        let page_size = page_size.clamp(1, 100);
        let query_fingerprint = group_search_query_fingerprint(
            queries,
            allow_cross_member,
            browse_filter,
            owner_user_id,
        );
        let snapshot = self.search_snapshot().await?;
        let cursor = page_token
            .map(decode_group_search_cursor)
            .transpose()?
            .map(|cursor| {
                if cursor.query_fingerprint != query_fingerprint {
                    return Err(anyhow::Error::new(
                        infra::error::LlmMemoryError::InvalidArgument(
                            "thread-group search cursor does not match the query".to_string(),
                        ),
                    ));
                }
                if cursor.snapshot != snapshot {
                    return Err(anyhow::Error::new(
                        infra::error::LlmMemoryError::SnapshotChanged(
                            "thread-group search snapshot changed; restart from the first page"
                                .to_string(),
                        ),
                    ));
                }
                Ok(cursor)
            })
            .transpose()?;

        let rows = self
            .groups
            .list_by_status(values::group_status::ACTIVE, None, None)
            .await?;
        let mut matches = Vec::new();
        for row in rows {
            if owner_user_id.is_some_and(|user_id| row.user_id != user_id) {
                continue;
            }
            let members = self.members.list_current_by_group_id(row.id).await?;
            let displays = self.member_displays(&members).await?;
            let (relevance_score, witnesses) = if queries.is_empty() {
                if !self
                    .group_matches_filter(row.id, &members, browse_filter.unwrap())
                    .await?
                {
                    continue;
                }
                (0.0, Vec::new())
            } else {
                let Some(result) = self
                    .match_group(
                        row.id,
                        &members,
                        &displays,
                        provider,
                        queries,
                        allow_cross_member,
                    )
                    .await?
                else {
                    continue;
                };
                result
            };
            matches.push(ThreadGroupSearchResult {
                group: self.view_of(row).await?,
                relevance_score,
                witnesses,
            });
        }
        matches.sort_by(compare_group_search_results);

        // A mutation during the expensive delegated searches must not create
        // a cursor that claims to represent the earlier result set.
        if self.search_snapshot().await? != snapshot {
            return Err(anyhow::Error::new(
                infra::error::LlmMemoryError::SnapshotChanged(
                    "thread-group search snapshot changed while reading results; retry".to_string(),
                ),
            ));
        }

        let start = cursor
            .as_ref()
            .map(|cursor| {
                matches
                    .iter()
                    .position(|result| cursor_is_before(cursor, result))
                    .unwrap_or(matches.len())
            })
            .unwrap_or(0);
        let end = (start + page_size).min(matches.len());
        let results = matches[start..end].to_vec();
        let next_page_token = (end < matches.len()).then(|| {
            encode_group_search_cursor(&ThreadGroupSearchCursor {
                version: 1,
                snapshot: snapshot.clone(),
                query_fingerprint: query_fingerprint.clone(),
                relevance_score_bits: results
                    .last()
                    .map(|result| result.relevance_score.to_bits())
                    .unwrap_or_default(),
                latest_activity_at: results
                    .last()
                    .and_then(|result| result.group.latest_activity_at),
                group_canonical_key: results
                    .last()
                    .map(|result| result.group.group_canonical_key.clone())
                    .unwrap_or_default(),
            })
        });

        Ok(ThreadGroupSearchPage {
            results,
            next_page_token,
        })
    }

    async fn group_matches_filter(
        &self,
        group_id: i64,
        members: &[ThreadGroupMemberRow],
        filter: &ThreadSearchFilter,
    ) -> anyhow::Result<bool> {
        if filter
            .thread_group_id
            .is_some_and(|value| value != group_id)
        {
            return Ok(false);
        }
        let thread_ids = members
            .iter()
            .filter_map(|member| member.thread_id)
            .collect::<Vec<_>>();
        let threads = self.threads.find_by_ids(&thread_ids).await?;
        let labels = self
            .thread_labels
            .find_labels_by_thread_ids(&thread_ids)
            .await?;
        let labels_by_thread =
            labels
                .into_iter()
                .fold(BTreeMap::<i64, Vec<String>>::new(), |mut map, row| {
                    map.entry(row.thread_id).or_default().push(row.label);
                    map
                });
        Ok(threads.into_iter().any(|thread| {
            let Some(data) = thread.data else {
                return false;
            };
            if filter
                .user_id
                .is_some_and(|value| data.user_id.as_ref().map(|id| id.value) != Some(value))
                || filter
                    .channel
                    .as_deref()
                    .is_some_and(|value| data.channel.as_deref() != Some(value))
                || filter
                    .thread_id
                    .is_some_and(|value| thread.id.as_ref().map(|id| id.value) != Some(value))
                || filter
                    .thread_created_after
                    .is_some_and(|value| data.created_at <= value)
                || filter
                    .thread_created_before
                    .is_some_and(|value| data.created_at >= value)
                || filter
                    .thread_updated_after
                    .is_some_and(|value| data.updated_at <= value)
                || filter
                    .thread_updated_before
                    .is_some_and(|value| data.updated_at >= value)
                || filter
                    .first_message_after
                    .is_some_and(|value| data.first_message_at.unwrap_or(i64::MIN) <= value)
                || filter
                    .first_message_before
                    .is_some_and(|value| data.first_message_at.unwrap_or(i64::MAX) >= value)
                || filter
                    .last_message_after
                    .is_some_and(|value| data.last_message_at.unwrap_or(i64::MIN) <= value)
                || filter
                    .last_message_before
                    .is_some_and(|value| data.last_message_at.unwrap_or(i64::MAX) >= value)
            {
                return false;
            }
            if !filter.memory_kinds.is_empty()
                && !filter.memory_kinds.contains(&data.memory_kind)
                && !(filter.memory_kinds.contains(&(MemoryKind::Raw as i32))
                    && data.memory_kind == 0)
            {
                return false;
            }
            if filter.labels.is_empty() {
                return true;
            }
            let thread_labels =
                labels_by_thread.get(&thread.id.map(|id| id.value).unwrap_or_default());
            let has_label =
                |label: &String| thread_labels.is_some_and(|values| values.contains(label));
            match filter.label_match_mode() {
                LabelMatchMode::LabelAll => filter.labels.iter().all(has_label),
                _ => filter.labels.iter().any(has_label),
            }
        }))
    }

    async fn match_group<P: ThreadGroupMemberSearchProvider>(
        &self,
        group_id: i64,
        members: &[ThreadGroupMemberRow],
        displays: &[ThreadDisplayView],
        provider: &P,
        queries: &[ThreadGroupMemberSearchQuery],
        allow_cross_member: bool,
    ) -> anyhow::Result<Option<(f32, Vec<ThreadGroupSearchWitness>)>> {
        let live_members = members
            .iter()
            .filter_map(|member| {
                member.thread_id.map(|thread_id| {
                    let display = displays
                        .iter()
                        .find(|display| display.thread_id == Some(thread_id))
                        .cloned()
                        .unwrap_or_else(|| ThreadDisplayView {
                            thread_id: Some(thread_id),
                            thread_canonical_key: member.thread_canonical_key.clone(),
                            description: None,
                            source: member.source.clone(),
                            created_at: None,
                            last_message_at: None,
                            deleted_at: member.deleted_at,
                        });
                    (thread_id, display)
                })
            })
            .collect::<Vec<_>>();

        if allow_cross_member {
            let mut witnesses = Vec::with_capacity(queries.len());
            for (predicate_index, query) in queries.iter().enumerate() {
                let mut best = None;
                for (thread_id, display) in &live_members {
                    if let Some(hit) = provider.search_member(query, group_id, *thread_id).await? {
                        let witness = ThreadGroupSearchWitness {
                            predicate_index,
                            target: query.target,
                            member: display.clone(),
                            score: hit.score,
                        };
                        if best
                            .as_ref()
                            .is_none_or(|current: &ThreadGroupSearchWitness| {
                                witness.score > current.score
                                    || (witness.score == current.score
                                        && witness.member.thread_canonical_key
                                            < current.member.thread_canonical_key)
                            })
                        {
                            best = Some(witness);
                        }
                    }
                }
                let Some(best) = best else {
                    return Ok(None);
                };
                witnesses.push(best);
            }
            let score = witnesses
                .iter()
                .map(|witness| witness.score)
                .fold(f32::NEG_INFINITY, f32::max);
            return Ok(Some((score, witnesses)));
        }

        let mut best_same_member = None;
        for (thread_id, display) in &live_members {
            let mut witnesses = Vec::with_capacity(queries.len());
            for (predicate_index, query) in queries.iter().enumerate() {
                let Some(hit) = provider.search_member(query, group_id, *thread_id).await? else {
                    witnesses.clear();
                    break;
                };
                witnesses.push(ThreadGroupSearchWitness {
                    predicate_index,
                    target: query.target,
                    member: display.clone(),
                    score: hit.score,
                });
            }
            if witnesses.len() != queries.len() {
                continue;
            }
            let score = witnesses
                .iter()
                .map(|witness| witness.score)
                .fold(f32::NEG_INFINITY, f32::max);
            if best_same_member.as_ref().is_none_or(
                |(current_score, current_witnesses): &(f32, Vec<ThreadGroupSearchWitness>)| {
                    score > *current_score
                        || (score == *current_score
                            && witnesses[0].member.thread_canonical_key
                                < current_witnesses[0].member.thread_canonical_key)
                },
            ) {
                best_same_member = Some((score, witnesses));
            }
        }
        Ok(best_same_member)
    }

    async fn search_snapshot(&self) -> anyhow::Result<String> {
        // The cursor only needs to detect a read-model change. Use the
        // backend aggregates so the cost is independent of row count and
        // no Memory / Group row is materialised in Rust.
        let revision = self.read_model_revision().await?;
        let mut encoded = Vec::new();
        for table in [
            revision.groups,
            revision.members,
            revision.relations,
            revision.candidate_associations,
            revision.threads,
            revision.memories,
            revision.thread_labels,
        ] {
            append_snapshot_fields(
                &mut encoded,
                &[
                    table.row_count.to_string(),
                    table.max_updated_at.to_string(),
                    table.updated_at_checksum.to_string(),
                ],
            );
        }
        Ok(sha256_hex(&encoded))
    }

    /// Active groups by default. The caller applies the documented
    /// `latest_activity_at DESC, group_canonical_key ASC` sort after
    /// enriching; this returns rows in stable key order.
    pub async fn list_groups(
        &self,
        include_inactive: bool,
        limit: Option<i64>,
        offset: Option<i64>,
        owner_user_id: Option<i64>,
    ) -> anyhow::Result<Vec<ThreadGroupView>> {
        let statuses: &[&str] = if include_inactive {
            &[
                values::group_status::ACTIVE,
                values::group_status::REDIRECTED,
                values::group_status::SPLIT,
            ]
        } else {
            &[values::group_status::ACTIVE]
        };
        let mut views = Vec::new();
        for status in statuses {
            for row in self.groups.list_by_status(status, None, None).await? {
                if owner_user_id.is_some_and(|user_id| row.user_id != user_id) {
                    continue;
                }
                views.push(self.view_of(row).await?);
            }
        }
        views.sort_by(|a, b| {
            b.latest_activity_at
                .cmp(&a.latest_activity_at)
                .then_with(|| a.group_canonical_key.cmp(&b.group_canonical_key))
        });
        let offset = offset.unwrap_or_default().max(0) as usize;
        let views = views.into_iter().skip(offset);
        let views = match limit {
            Some(limit) => views.take(limit.max(0) as usize).collect(),
            None => views.collect(),
        };
        Ok(views)
    }

    pub async fn get_lineage(
        &self,
        group_id: i64,
        owner_user_id: Option<i64>,
    ) -> anyhow::Result<Option<ThreadGroupLineageView>> {
        let Some(group) = self.groups.find_by_id(group_id).await? else {
            return Ok(None);
        };
        if owner_user_id.is_some_and(|user_id| group.user_id != user_id) {
            return Ok(None);
        }
        let members = self.members.list_current_by_group_id(group_id).await?;
        let member_displays = self.member_displays(&members).await?;
        let member_keys: HashSet<&str> = members
            .iter()
            .map(|member| member.thread_canonical_key.as_str())
            .collect();
        let mut relations = Vec::new();
        let mut seen = HashSet::new();
        for &key in &member_keys {
            for relation in self
                .relations
                .list_by_parent_canonical_key(key, Some(values::relation_state::ACTIVE))
                .await?
                .into_iter()
                .chain(
                    self.relations
                        .list_by_child_canonical_key(key, Some(values::relation_state::ACTIVE))
                        .await?,
                )
            {
                if seen.insert(relation.id) {
                    relations.push(relation);
                }
            }
        }
        relations.sort_by_key(|relation| relation.id);

        let mut unresolved = Vec::new();
        self.for_each_unresolved_candidate(group_id, &members, |candidate| {
            unresolved.push(candidate);
        })
        .await?;

        // Evidence for each member's owner-local identity. A member
        // without source identity (manual thread) contributes none.
        let mut observations = Vec::new();
        let mut seen_observations = HashSet::new();
        for member in &members {
            let Some(source) = member.source.as_deref() else {
                continue;
            };
            let known = member.identity_scope.is_some();
            let scope = member.identity_scope.as_deref().unwrap_or_default();
            let Some(native_id) = member.native_id.as_deref() else {
                continue;
            };
            for observation in self
                .observations
                .list_by_subject(member.user_id, source, known, scope, native_id)
                .await?
            {
                if seen_observations.insert(observation.id) {
                    observations.push(observation);
                }
            }
        }
        observations.sort_by_key(|observation| observation.id);

        let relation_keys = relations
            .iter()
            .flat_map(|relation| {
                [
                    relation.parent_thread_canonical_key.clone(),
                    relation.child_thread_canonical_key.clone(),
                ]
            })
            .collect::<HashSet<_>>();
        let relation_keys = relation_keys.into_iter().collect::<Vec<_>>();
        let relation_members = self
            .members
            .find_current_by_thread_canonical_keys(&relation_keys)
            .await?;
        let relation_displays = self.member_displays(&relation_members).await?;
        let relation_group_ids = relation_members
            .iter()
            .map(|member| member.group_id)
            .collect::<HashSet<_>>();
        let active_group_ids = self
            .groups
            .find_by_ids(&relation_group_ids.into_iter().collect::<Vec<_>>())
            .await?
            .into_iter()
            .filter(|group| group.status == values::group_status::ACTIVE)
            .map(|group| group.id)
            .collect::<Vec<_>>();
        let relation_endpoints = resolve_relation_endpoints(
            &relations,
            &relation_members,
            &relation_displays,
            &active_group_ids,
        );

        Ok(Some(ThreadGroupLineageView {
            group: self.view_of(group).await?,
            members,
            member_displays,
            relations,
            unresolved,
            observations,
            relation_endpoints,
        }))
    }

    /// Find the dedicated group-summary Memory. The kind check is part of
    /// the lookup so a legacy per-thread summary cannot masquerade as a
    /// group summary during migration.
    pub async fn find_group_summary(
        &self,
        group_id: i64,
        owner_user_id: Option<i64>,
    ) -> anyhow::Result<Option<protobuf::llm_memory::data::Memory>> {
        let Some(group) = self.groups.find_by_id(group_id).await? else {
            return Ok(None);
        };
        if group.status != values::group_status::ACTIVE
            || owner_user_id.is_some_and(|user_id| group.user_id != user_id)
        {
            return Ok(None);
        }
        self.memories
            .find_by_external_id_and_kind(
                &thread_group_summary_external_id(group_id),
                MemoryKind::DerivedSummary,
            )
            .await
    }

    /// Count saved summaries for active groups that still have at least one
    /// live member. Deleted-placeholder-only groups are intentionally absent.
    pub async fn count_group_summaries(&self, owner_user_id: Option<i64>) -> anyhow::Result<i64> {
        let groups = self
            .groups
            .list_by_status(values::group_status::ACTIVE, None, None)
            .await?;
        let mut count = 0;
        for group in groups {
            if owner_user_id.is_some_and(|user_id| group.user_id != user_id) {
                continue;
            }
            let members = self.members.list_current_by_group_id(group.id).await?;
            if !members.iter().any(|member| {
                member.state == values::member_state::ACTIVE && member.thread_id.is_some()
            }) {
                continue;
            }
            if self
                .find_group_summary(group.id, owner_user_id)
                .await?
                .is_some()
            {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Operational counts for monitoring / alerts (design 9.3). A
    /// snapshot, not a transactionally consistent set.
    pub async fn reconciliation_report(&self) -> anyhow::Result<ThreadGroupReconciliationReport> {
        let count_groups = async |status: &str| -> anyhow::Result<i64> {
            Ok(self.groups.list_by_status(status, None, None).await?.len() as i64)
        };
        let count_candidates = async |state: &str| -> anyhow::Result<i64> {
            Ok(self
                .candidates
                .list_by_state(state, None, None)
                .await?
                .len() as i64)
        };
        let count_observations = async |state: &str| -> anyhow::Result<i64> {
            Ok(self
                .observations
                .list_by_state(state, None, None)
                .await?
                .len() as i64)
        };
        Ok(ThreadGroupReconciliationReport {
            active_groups: count_groups(values::group_status::ACTIVE).await?,
            redirected_groups: count_groups(values::group_status::REDIRECTED).await?,
            split_groups: count_groups(values::group_status::SPLIT).await?,
            pending_candidates: count_candidates(values::candidate_state::PENDING).await?,
            ambiguous_candidates: count_candidates(values::candidate_state::AMBIGUOUS).await?,
            conflict_candidates: count_candidates(values::candidate_state::CONFLICT).await?,
            unsupported_observations: count_observations(values::observation_state::UNSUPPORTED)
                .await?,
        })
    }

    async fn view_of(&self, row: ThreadGroupRow) -> anyhow::Result<ThreadGroupView> {
        let latest_activity_at = self
            .members
            .find_latest_activity_at_tx(self.pool, row.id)
            .await?;
        let members = self.members.list_current_by_group_id(row.id).await?;
        let root = self.root_member(row.id).await?;
        let displays = self.member_displays(&members).await?;
        let root_display = root.as_ref().and_then(|root| {
            displays
                .iter()
                .find(|display| display.thread_canonical_key == root.thread_canonical_key)
                .cloned()
        });
        let snapshot_entries = self.snapshot_entries(&members).await?;
        Ok(ThreadGroupView {
            id: row.id,
            user_id: row.user_id,
            group_canonical_key: row.group_canonical_key,
            title: row.title,
            status: row.status,
            grouping_authority: row.grouping_authority,
            redirect_to_group_id: row.redirect_to_group_id,
            latest_activity_at,
            root_thread_id: root.as_ref().and_then(|member| member.thread_id),
            root_thread_canonical_key: root.map(|member| member.thread_canonical_key),
            root_display,
            active_member_count: members
                .iter()
                .filter(|member| member.state == values::member_state::ACTIVE)
                .count() as i64,
            deleted_member_count: members
                .iter()
                .filter(|member| member.state == values::member_state::DELETED)
                .count() as i64,
            unresolved_count: self.unresolved_count(row.id, &members).await?,
            membership_snapshot_digest: membership_snapshot_digest(&snapshot_entries),
        })
    }

    async fn unresolved_count(
        &self,
        group_id: i64,
        members: &[ThreadGroupMemberRow],
    ) -> anyhow::Result<i64> {
        let mut count = 0;
        self.for_each_unresolved_candidate(group_id, members, |_| count += 1)
            .await?;
        Ok(count)
    }

    async fn for_each_unresolved_candidate(
        &self,
        group_id: i64,
        members: &[ThreadGroupMemberRow],
        mut visit: impl FnMut(ThreadGroupCandidateAssociationRow),
    ) -> anyhow::Result<()> {
        let member_thread_ids: HashSet<i64> = members
            .iter()
            .filter_map(|member| member.thread_id)
            .collect();
        for state in [
            values::candidate_state::PENDING,
            values::candidate_state::AMBIGUOUS,
            values::candidate_state::CONFLICT,
        ] {
            for candidate in self.candidates.list_by_state(state, None, None).await? {
                if candidate.candidate_group_id == Some(group_id)
                    || candidate
                        .subject_thread_id
                        .is_some_and(|id| member_thread_ids.contains(&id))
                {
                    visit(candidate);
                }
            }
        }
        Ok(())
    }

    async fn member_displays(
        &self,
        members: &[ThreadGroupMemberRow],
    ) -> anyhow::Result<Vec<ThreadDisplayView>> {
        let by_id = self.member_threads_by_id(members).await?;
        Ok(members
            .iter()
            .map(|member| {
                let data = member
                    .thread_id
                    .and_then(|thread_id| by_id.get(&thread_id))
                    .and_then(|thread| thread.data.as_ref());
                ThreadDisplayView {
                    thread_id: member.thread_id,
                    thread_canonical_key: member.thread_canonical_key.clone(),
                    description: data.and_then(|data| data.description.clone()),
                    source: member.source.clone(),
                    created_at: data.map(|data| data.created_at),
                    last_message_at: data.and_then(|data| data.last_message_at),
                    deleted_at: member.deleted_at,
                }
            })
            .collect())
    }

    async fn snapshot_entries(
        &self,
        members: &[ThreadGroupMemberRow],
    ) -> anyhow::Result<Vec<MembershipSnapshotEntry>> {
        let by_id = self.member_threads_by_id(members).await?;
        Ok(members
            .iter()
            .map(|member| {
                let data = member
                    .thread_id
                    .and_then(|thread_id| by_id.get(&thread_id))
                    .and_then(|thread| thread.data.as_ref());
                MembershipSnapshotEntry {
                    thread_canonical_key: member.thread_canonical_key.clone(),
                    state: member.state.clone(),
                    role: member.role.clone(),
                    deleted_at: member.deleted_at,
                    updated_at: data.map(|data| data.updated_at),
                    last_message_at: data.and_then(|data| data.last_message_at),
                }
            })
            .collect())
    }

    async fn member_threads_by_id(
        &self,
        members: &[ThreadGroupMemberRow],
    ) -> anyhow::Result<std::collections::HashMap<i64, protobuf::llm_memory::data::Thread>> {
        let thread_ids = members
            .iter()
            .filter_map(|member| member.thread_id)
            .collect::<Vec<_>>();
        let threads = self.threads.find_by_ids(&thread_ids).await?;
        Ok(threads
            .into_iter()
            .filter_map(|thread| thread.id.map(|id| (id.value, thread)))
            .collect())
    }

    /// Display root of a group: the lexicographically smallest current
    /// member that has no active parent among the group's own members.
    pub async fn root_member(&self, group_id: i64) -> anyhow::Result<Option<ThreadGroupMemberRow>> {
        let members = self.members.list_current_by_group_id(group_id).await?;
        if members.is_empty() {
            return Ok(None);
        }
        let keys: HashSet<&str> = members
            .iter()
            .map(|member| member.thread_canonical_key.as_str())
            .collect();
        let mut has_parent: HashSet<String> = HashSet::new();
        for key in &keys {
            for relation in self
                .relations
                .list_by_parent_canonical_key(key, Some(values::relation_state::ACTIVE))
                .await?
            {
                if keys.contains(relation.child_thread_canonical_key.as_str()) {
                    has_parent.insert(relation.child_thread_canonical_key);
                }
            }
        }
        let mut candidates: Vec<&ThreadGroupMemberRow> = members
            .iter()
            .filter(|member| !has_parent.contains(&member.thread_canonical_key))
            .collect();
        candidates.sort_by(|a, b| a.thread_canonical_key.cmp(&b.thread_canonical_key));
        Ok(candidates.first().map(|member| (*member).clone()))
    }
}
