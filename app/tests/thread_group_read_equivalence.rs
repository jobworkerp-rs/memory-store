//! Pins the observable output of the ThreadGroup read model on a fixture
//! that exercises ties, NULL activity, empty / deleted-only groups,
//! inactive history, non-trivial roots, and every unresolved-candidate
//! matching path. The golden file was produced by the per-group
//! implementation, so batched read paths must reproduce it byte for byte.
//!
//! Regenerate only when an intentional behavior change is reviewed:
//! `UPDATE_THREAD_GROUP_GOLDEN=1 cargo test -p app --test thread_group_read_equivalence`.
#![cfg(not(feature = "postgres"))]

use anyhow::Result;
use app::app::thread_group::{
    GroupSearchMode, GroupSearchTarget, ThreadGroupMemberSearchHit,
    ThreadGroupMemberSearchProvider, ThreadGroupMemberSearchQuery, ThreadGroupReadService,
    ThreadGroupSearchResult, ThreadGroupView,
};
use async_trait::async_trait;
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use infra::infra::thread_group::candidate::{
    ThreadGroupCandidateAssociationRepository, ThreadGroupCandidateAssociationRepositoryImpl,
};
use infra::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
use infra::infra::thread_group::member::{
    ThreadGroupMemberRepository, ThreadGroupMemberRepositoryImpl,
};
use infra::infra::thread_group::relation::{
    ThreadRelationRepository, ThreadRelationRepositoryImpl,
};
use infra::infra::thread_group::rows::values;
use infra::infra::thread_group::test_support::{
    T0, insert_thread, key, new_candidate, new_group, new_member, new_relation,
    setup_thread_group_pool,
};
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::test::TEST_RUNTIME;
use protobuf::llm_memory::data::{
    ContentType, LabelMatchMode, MemoryData, MemoryKind, ThreadSearchFilter, UserId,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Write as _;

const GOLDEN_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/thread_group_read_equivalence.txt"
);

/// Thread ids double as canonical-key seeds so displays are deterministic.
const T: i64 = 800_000;

#[derive(Default)]
struct FakeSearchProvider {
    scores: HashMap<(String, i64), f32>,
}

#[async_trait]
impl ThreadGroupMemberSearchProvider for FakeSearchProvider {
    async fn search_member(
        &self,
        query: &ThreadGroupMemberSearchQuery,
        _group_id: i64,
        thread_id: i64,
    ) -> Result<Option<ThreadGroupMemberSearchHit>> {
        Ok(self
            .scores
            .get(&(query.query_text.clone(), thread_id))
            .copied()
            .map(|score| ThreadGroupMemberSearchHit { thread_id, score }))
    }
}

struct Fixture {
    pool: &'static RdbPool,
    /// Generated group id -> stable fixture label.
    labels: HashMap<i64, String>,
    ids: BTreeMap<String, i64>,
}

impl Fixture {
    fn label(&self, id: i64) -> String {
        self.labels
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("<unknown {id}>"))
    }
}

async fn thread(pool: &RdbPool, n: i64, last_message_at: Option<i64>) -> i64 {
    let id = T + n;
    insert_thread(pool, id, last_message_at).await;
    id
}

async fn build_fixture() -> Fixture {
    let pool = setup_thread_group_pool().await;
    let id_gen = infra::test_helper::shared_id_generator();
    let groups = ThreadGroupRepositoryImpl::new(id_gen.clone(), pool);
    let members = ThreadGroupMemberRepositoryImpl::new(pool);
    let relations = ThreadRelationRepositoryImpl::new(id_gen.clone(), pool);
    let candidates = ThreadGroupCandidateAssociationRepositoryImpl::new(id_gen.clone(), pool);
    let labels_repo = ThreadLabelRepositoryImpl::new(pool);
    let memories = MemoryRepositoryImpl::new(id_gen, pool);

    let mut labels = HashMap::new();
    let mut ids = BTreeMap::new();
    let mut create_group = async |label: &str, group_key: u64, user_id: i64| -> i64 {
        let mut group = new_group(group_key);
        group.user_id = user_id;
        let id = groups.create_tx(pool, &group).await.expect("group");
        labels.insert(id, label.to_string());
        ids.insert(label.to_string(), id);
        id
    };

    // Owner 1.
    let a = create_group("A", 810_001, 1).await;
    let b = create_group("B", 810_002, 1).await;
    let c = create_group("C", 810_003, 1).await;
    let _d = create_group("D", 810_004, 1).await;
    let e = create_group("E", 810_005, 1).await;
    let f = create_group("F", 810_006, 1).await;
    let g = create_group("G", 810_007, 1).await;
    let mut paging = Vec::new();
    for (index, label) in ["H", "I", "J", "K", "L"].iter().enumerate() {
        paging.push(create_group(label, 810_008 + index as u64, 1).await);
    }
    // Owner 2.
    let m = create_group("M", 810_020, 2).await;
    let n = create_group("N", 810_021, 2).await;

    let attach = async |group_id: i64, thread_id: i64| {
        members
            .insert_tx(
                pool,
                &new_member(group_id, Some(thread_id), key(thread_id as u64)),
            )
            .await
            .expect("member");
    };
    let attach_deleted = async |group_id: i64, n: i64| {
        let mut member = new_member(group_id, None, key((T + n) as u64));
        member.state = values::member_state::DELETED.to_string();
        member.deleted_at = Some(T0 + n);
        members
            .insert_tx(pool, &member)
            .await
            .expect("deleted member");
    };

    // A: two members; t2 is the parent of t1, so the root is t2 even though
    // t1 has the smaller canonical key.
    let t1 = thread(pool, 1, Some(100)).await;
    let t2 = thread(pool, 2, Some(300)).await;
    attach(a, t1).await;
    attach(a, t2).await;
    let mut rel = new_relation((T + 2) as u64, (T + 1) as u64);
    rel.parent_thread_id = Some(t2);
    rel.child_thread_id = Some(t1);
    relations.insert_tx(pool, &rel).await.expect("relation a");

    // B ties A on latest activity; canonical key breaks the tie.
    let t3 = thread(pool, 3, Some(300)).await;
    attach(b, t3).await;
    // C: live member without conversation time (NULL activity).
    let t4 = thread(pool, 4, None).await;
    attach(c, t4).await;
    // D: no members. E: deleted placeholder only.
    attach_deleted(e, 5).await;
    // F: two live members plus a deleted placeholder; t6 -> t7.
    let t6 = thread(pool, 6, Some(200)).await;
    let t7 = thread(pool, 7, Some(250)).await;
    attach(f, t6).await;
    attach(f, t7).await;
    attach_deleted(f, 8).await;
    let mut rel = new_relation((T + 6) as u64, (T + 7) as u64);
    rel.parent_thread_id = Some(t6);
    rel.child_thread_id = Some(t7);
    relations.insert_tx(pool, &rel).await.expect("relation f");
    // G: labelled, legacy unspecified memory kind, own channel.
    let t9 = thread(pool, 9, Some(50)).await;
    attach(g, t9).await;
    sqlx::query("UPDATE thread SET memory_kind = 0, channel = 'slack' WHERE id = ?")
        .bind(t9)
        .execute(pool)
        .await
        .expect("legacy thread");
    labels_repo
        .add_labels(t9, &["project:x".into(), "team:y".into()], T0)
        .await
        .expect("labels");
    // H..L: paging volume with distinct activity.
    for (index, group_id) in paging.iter().enumerate() {
        let t = thread(pool, 10 + index as i64, Some(400 + index as i64 * 10)).await;
        attach(*group_id, t).await;
    }
    let t10 = T + 10;
    labels_repo
        .add_labels(t10, &["project:x".into()], T0)
        .await
        .expect("labels");
    // Owner 2 groups.
    let t15 = thread(pool, 15, Some(500)).await;
    let t16 = thread(pool, 16, Some(300)).await;
    attach(m, t15).await;
    attach(n, t16).await;
    sqlx::query("UPDATE thread SET user_id = 2 WHERE id IN (?, ?)")
        .bind(t15)
        .bind(t16)
        .execute(pool)
        .await
        .expect("owner two threads");

    // Inactive history: a redirected group and a split group that shares
    // B's canonical key (allowed because the key is unique only while active).
    let mut redirected = new_group(810_030);
    redirected.status = values::group_status::REDIRECTED.to_string();
    redirected.redirect_to_group_id = Some(a);
    let r = groups
        .create_tx(pool, &redirected)
        .await
        .expect("redirected");
    labels.insert(r, "R".into());
    ids.insert("R".into(), r);
    let mut split = new_group(810_002);
    split.status = values::group_status::SPLIT.to_string();
    let s = groups.create_tx(pool, &split).await.expect("split");
    labels.insert(s, "S".into());
    ids.insert("S".into(), s);
    let mut history = new_member(r, Some(t3), key((T + 3) as u64));
    history.state = values::member_state::REDIRECTED.to_string();
    members.insert_tx(pool, &history).await.expect("history");

    // Unresolved candidates: by group, by subject thread, by both, in each
    // unresolved state, plus resolved-like states that must be ignored.
    let add_candidate =
        async |n: u64, state: &str, group: Option<i64>, subject: Option<i64>, updated_at: i64| {
            let mut candidate = new_candidate(n);
            candidate.state = state.to_string();
            candidate.candidate_group_id = group;
            candidate.subject_thread_id = subject;
            candidate.updated_at = updated_at;
            candidates
                .insert_tx(pool, &candidate)
                .await
                .expect("candidate");
        };
    use values::candidate_state::*;
    add_candidate(1, PENDING, Some(a), None, T0 + 10).await;
    add_candidate(2, AMBIGUOUS, None, Some(t3), T0 + 11).await;
    add_candidate(3, CONFLICT, Some(f), Some(t6), T0 + 12).await;
    add_candidate(4, PENDING, Some(b), Some(t1), T0 + 13).await;
    add_candidate(5, CANDIDATE, Some(a), None, T0 + 14).await;
    add_candidate(6, SUPERSEDED, None, Some(t3), T0 + 15).await;
    add_candidate(7, PENDING, Some(a), None, T0 + 5).await;
    add_candidate(8, AMBIGUOUS, Some(m), None, T0 + 6).await;
    add_candidate(9, UNSUPPORTED, None, Some(t7), T0 + 7).await;

    // Group summaries: counted only for active groups with a live member and
    // the DerivedSummary kind.
    let summary = async |label: &str, group_id: i64, kind: MemoryKind, user_id: i64| {
        memories
            .create(
                pool,
                &MemoryData {
                    user_id: Some(UserId { value: user_id }),
                    content: format!("summary {label}"),
                    content_type: ContentType::Text as i32,
                    external_id: Some(format!("thread-group-summary:{group_id}")),
                    memory_kind: kind as i32,
                    ..Default::default()
                },
            )
            .await
            .expect("summary");
    };
    summary("A", a, MemoryKind::DerivedSummary, 1).await;
    summary("B", b, MemoryKind::ThreadSummary, 1).await;
    summary("C", c, MemoryKind::DerivedSummary, 1).await;
    summary("D", ids["D"], MemoryKind::DerivedSummary, 1).await;
    summary("E", e, MemoryKind::DerivedSummary, 1).await;
    summary("M", m, MemoryKind::DerivedSummary, 2).await;
    summary("R", r, MemoryKind::DerivedSummary, 1).await;

    Fixture { pool, labels, ids }
}

fn render_view(fixture: &Fixture, view: &ThreadGroupView) -> String {
    format!(
        "{} user={} key={} title={:?} status={} authority={} redirect={:?} latest={:?} \
         root=({:?},{:?}) root_display={:?} active={} deleted={} unresolved={} digest={}",
        fixture.label(view.id),
        view.user_id,
        &view.group_canonical_key[58..],
        view.title,
        view.status,
        view.grouping_authority,
        view.redirect_to_group_id.map(|id| fixture.label(id)),
        view.latest_activity_at,
        view.root_thread_id,
        view.root_thread_canonical_key
            .as_deref()
            .map(|key| &key[58..]),
        view.root_display,
        view.active_member_count,
        view.deleted_member_count,
        view.unresolved_count,
        view.membership_snapshot_digest,
    )
}

fn render_result(fixture: &Fixture, result: &ThreadGroupSearchResult) -> String {
    let mut out = format!(
        "  score={} {}\n",
        result.relevance_score,
        render_view(fixture, &result.group)
    );
    for witness in &result.witnesses {
        let _ = writeln!(
            out,
            "    witness p={} target={:?} thread={:?} score={}",
            witness.predicate_index, witness.target, witness.member.thread_id, witness.score
        );
    }
    out
}

fn keyword(target: GroupSearchTarget, text: &str) -> ThreadGroupMemberSearchQuery {
    ThreadGroupMemberSearchQuery {
        target,
        mode: GroupSearchMode::Keyword,
        query_text: text.to_owned(),
        query_vectors: Vec::new(),
        thread_filter: None,
        memory_filter: None,
        hybrid_options: None,
    }
}

#[allow(clippy::too_many_arguments)]
async fn render_search(
    out: &mut String,
    fixture: &Fixture,
    read: &ThreadGroupReadService,
    provider: &FakeSearchProvider,
    name: &str,
    queries: &[ThreadGroupMemberSearchQuery],
    allow_cross_member: bool,
    browse: Option<&ThreadSearchFilter>,
    owner: Option<i64>,
) {
    let full = read
        .search_groups(
            provider,
            queries,
            allow_cross_member,
            100,
            None,
            browse,
            owner,
        )
        .await
        .expect("full search");
    let _ = writeln!(out, "search {name} owner={owner:?} full:");
    for result in &full.results {
        out.push_str(&render_result(fixture, result));
    }
    assert!(full.next_page_token.is_none() || full.results.len() == 100);
    for page_size in [1, 2, 3] {
        let mut paged = Vec::new();
        let mut token = None;
        let mut pages = 0;
        loop {
            let page = read
                .search_groups(
                    provider,
                    queries,
                    allow_cross_member,
                    page_size,
                    token.as_deref(),
                    browse,
                    owner,
                )
                .await
                .expect("paged search");
            pages += 1;
            assert!(page.results.len() <= page_size);
            paged.extend(page.results);
            token = page.next_page_token;
            if token.is_none() {
                break;
            }
            assert!(pages < 100, "pagination must terminate");
        }
        assert_eq!(
            paged, full.results,
            "{name}: page_size {page_size} must concatenate to the full result"
        );
        let _ = writeln!(out, "  page_size={page_size} pages={pages}");
    }
}

async fn render_everything(fixture: &Fixture) -> String {
    let read = ThreadGroupReadService::new(fixture.pool);
    let mut out = String::new();

    for include_inactive in [false, true] {
        for owner in [None, Some(1), Some(2)] {
            for (limit, offset) in [
                (None, None),
                (Some(0), None),
                (Some(1), None),
                (Some(3), Some(2)),
                (Some(100), Some(0)),
                (Some(2), Some(100)),
                (Some(-1), Some(-5)),
                (None, Some(4)),
            ] {
                let views = read
                    .list_groups(include_inactive, limit, offset, owner)
                    .await
                    .expect("list groups");
                let _ = writeln!(
                    out,
                    "list inactive={include_inactive} owner={owner:?} limit={limit:?} offset={offset:?}:"
                );
                for view in &views {
                    let _ = writeln!(out, "  {}", render_view(fixture, view));
                }
            }
        }
    }

    let mut provider = FakeSearchProvider::default();
    for (thread, score) in [
        (1, 0.5),
        (2, 0.9),
        (3, 0.9),
        (6, 0.7),
        (10, 0.9),
        (15, 0.95),
        (16, 0.9),
    ] {
        provider.scores.insert(("q".into(), T + thread), score);
    }
    for (thread, score) in [(1, 0.8), (7, 0.6), (10, 0.2), (3, 0.9)] {
        provider.scores.insert(("r".into(), T + thread), score);
    }
    let q = [keyword(GroupSearchTarget::Thread, "q")];
    let qr = [
        keyword(GroupSearchTarget::Thread, "q"),
        keyword(GroupSearchTarget::Memory, "r"),
    ];
    for owner in [None, Some(1), Some(2)] {
        render_search(
            &mut out, fixture, &read, &provider, "q-same", &q, false, None, owner,
        )
        .await;
        render_search(
            &mut out, fixture, &read, &provider, "qr-same", &qr, false, None, owner,
        )
        .await;
        render_search(
            &mut out, fixture, &read, &provider, "qr-cross", &qr, true, None, owner,
        )
        .await;
    }
    let browse_filters = [
        (
            "browse-user1",
            ThreadSearchFilter {
                user_id: Some(1),
                ..Default::default()
            },
        ),
        (
            "browse-label-any",
            ThreadSearchFilter {
                labels: vec!["project:x".into(), "missing".into()],
                ..Default::default()
            },
        ),
        (
            "browse-label-all",
            ThreadSearchFilter {
                labels: vec!["project:x".into(), "team:y".into()],
                label_match_mode: Some(LabelMatchMode::LabelAll as i32),
                ..Default::default()
            },
        ),
        (
            "browse-raw-kind",
            ThreadSearchFilter {
                memory_kinds: vec![MemoryKind::Raw as i32],
                ..Default::default()
            },
        ),
        (
            "browse-channel",
            ThreadSearchFilter {
                channel: Some("slack".into()),
                ..Default::default()
            },
        ),
        (
            "browse-last-message",
            ThreadSearchFilter {
                last_message_after: Some(250),
                last_message_before: Some(450),
                ..Default::default()
            },
        ),
        (
            "browse-group-id",
            ThreadSearchFilter {
                thread_group_id: Some(fixture.ids["F"]),
                ..Default::default()
            },
        ),
    ];
    for (name, filter) in &browse_filters {
        for owner in [None, Some(1)] {
            render_search(
                &mut out,
                fixture,
                &read,
                &provider,
                name,
                &[],
                false,
                Some(filter),
                owner,
            )
            .await;
        }
    }

    for (label, id) in &fixture.ids {
        for owner in [None, Some(1), Some(2)] {
            let lineage = read.get_lineage(*id, owner).await.expect("lineage");
            let Some(lineage) = lineage else {
                let _ = writeln!(out, "lineage {label} owner={owner:?}: none");
                continue;
            };
            let unresolved = lineage
                .unresolved
                .iter()
                .map(|candidate| candidate.subject_native_id.clone())
                .collect::<Vec<_>>();
            let _ = writeln!(
                out,
                "lineage {label} owner={owner:?}: {} unresolved={unresolved:?}",
                render_view(fixture, &lineage.group)
            );
        }
        for owner in [None, Some(1), Some(2)] {
            let summary = read
                .find_group_summary(*id, owner)
                .await
                .expect("summary")
                .map(|memory| memory.data.map(|data| data.content));
            let _ = writeln!(out, "summary {label} owner={owner:?}: {summary:?}");
        }
    }

    for owner in [None, Some(1), Some(2)] {
        let count = read
            .count_group_summaries(owner)
            .await
            .expect("count summaries");
        let _ = writeln!(out, "count_group_summaries owner={owner:?}: {count}");
    }
    for owner in [None, Some(1), Some(2), Some(3)] {
        assert_eq!(
            read.count_groups(owner).await.expect("count groups"),
            read.list_groups(false, None, None, owner)
                .await
                .expect("list groups")
                .len() as i64,
            "count_groups must match the active listing for owner {owner:?}"
        );
    }
    let mut requested = fixture.ids.values().copied().collect::<Vec<_>>();
    requested.push(-1);
    requested.push(fixture.ids["A"]);
    for owner in [None, Some(1), Some(2)] {
        let batch = read
            .find_group_summaries(&requested, owner)
            .await
            .expect("batch summaries");
        assert_eq!(batch.len(), requested.len());
        for (group_id, summary) in requested.iter().zip(batch) {
            assert_eq!(
                summary,
                read.find_group_summary(*group_id, owner)
                    .await
                    .expect("single summary"),
                "group {group_id} owner {owner:?}"
            );
        }
    }
    for invalid in [Vec::new(), vec![fixture.ids["A"]; 101]] {
        let error = read
            .find_group_summaries(&invalid, None)
            .await
            .expect_err("batch size outside 1..=100 is rejected");
        assert!(error.to_string().contains("group_ids"), "{error}");
    }
    let report = read.reconciliation_report().await.expect("report");
    let _ = writeln!(out, "report: {report:?}");
    out
}

#[test]
fn thread_group_read_model_matches_golden() {
    TEST_RUNTIME.block_on(async {
        let fixture = build_fixture().await;
        let actual = render_everything(&fixture).await;
        if std::env::var_os("UPDATE_THREAD_GROUP_GOLDEN").is_some() {
            std::fs::create_dir_all(std::path::Path::new(GOLDEN_PATH).parent().unwrap())
                .expect("golden dir");
            std::fs::write(GOLDEN_PATH, &actual).expect("write golden");
            return;
        }
        let expected = std::fs::read_to_string(GOLDEN_PATH).expect("golden file");
        if actual != expected {
            let first_diff = actual
                .lines()
                .zip(expected.lines())
                .position(|(a, e)| a != e)
                .unwrap_or_else(|| actual.lines().count().min(expected.lines().count()));
            panic!(
                "read model output diverged from golden at line {}:\n  actual:   {:?}\n  expected: {:?}",
                first_diff + 1,
                actual.lines().nth(first_diff),
                expected.lines().nth(first_diff)
            );
        }
    });
}
