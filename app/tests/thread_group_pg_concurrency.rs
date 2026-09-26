//! PostgreSQL-only concurrency test for the ThreadGroup advisory locks.
//!
//! Runs against the `docker-compose.yaml` postgres service
//! (`postgres://postgres:postgres@127.0.0.1:5432/test`, override with
//! `TEST_POSTGRES_URL`). The SQLite integration suite is gated the other
//! way (`not(postgres)`), so this file is the real advisory-lock proof.
#![cfg(feature = "postgres")]

use std::sync::Arc;

use app::app::thread_group::{
    ObservationInput, ObservedEndpoint, ThreadGroupReconciliationService,
};
use common::thread_group_key::IdentityScope;
use infra::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
use infra::infra::thread_group::lock::{ThreadGroupLockRepository, ThreadGroupLockRepositoryImpl};
use infra::infra::thread_group::member::{
    ThreadGroupMemberRepository, ThreadGroupMemberRepositoryImpl,
};
use infra::infra::thread_group::relation::{
    ThreadRelationRepository, ThreadRelationRepositoryImpl,
};
use infra::infra::thread_group::rows::{NewThreadGroupMember, values};
use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};

fn endpoint(native_id: &str) -> ObservedEndpoint {
    ObservedEndpoint {
        source: "codex".to_string(),
        identity_scope: IdentityScope::known(String::new()),
        owner_scope: "user:1".to_string(),
        native_id: native_id.to_string(),
    }
}

fn key_of(native_id: &str) -> String {
    common::thread_group_key::source_thread_canonical_key(
        &common::thread_group_key::SourceIdentity::new(
            "user:1",
            "codex",
            IdentityScope::known(String::new()),
            native_id,
        ),
    )
    .expect("known scope")
}

fn observation(child: &str, parent: &str) -> ObservationInput {
    ObservationInput {
        subject: endpoint(child),
        candidate_parent: Some(endpoint(parent)),
        relation_kind: Some("delegated".to_string()),
        evidence_kind: "source_field".to_string(),
        polarity: "supports".to_string(),
        source_confidence: Some("exact".to_string()),
        adapter_version: "codex-adapter@1".to_string(),
        source_record_ref: "test:fixture".to_string(),
        import_run_id: None,
        observed_at: 1_000,
    }
}

async fn insert_thread(pool: &sqlx::PgPool, id: i64) {
    sqlx::query(
        "INSERT INTO thread (id, user_id, created_at, updated_at, memory_kind, last_message_at) \
         VALUES ($1, 1, 1, 1, 1, NULL) ON CONFLICT (id) DO NOTHING",
    )
    .bind(id)
    .execute(pool)
    .await
    .expect("insert thread");
}

#[test]
fn pg_concurrent_same_parent_converges_to_one_edge() {
    TEST_RUNTIME.block_on(async {
        let pool = setup_test_rdb_from("../infra/sql/postgres").await;
        let reconcile = Arc::new(ThreadGroupReconciliationService::new(pool));
        let parent = endpoint("pg-cc-parent");
        let child = endpoint("pg-cc-child");
        insert_thread(pool, 41_001).await;
        insert_thread(pool, 41_002).await;
        reconcile
            .reconcile_subject(41_001, &parent, &[], "op", 1_000)
            .await
            .expect("parent reconcile");

        let evidence = observation("pg-cc-child", "pg-cc-parent");
        let first = reconcile.clone();
        let second = reconcile.clone();
        let (child_a, child_b) = (child.clone(), child.clone());
        let (evidence_a, evidence_b) = (evidence.clone(), evidence.clone());
        let (a, b) = tokio::join!(
            async move {
                first
                    .reconcile_subject(41_002, &child_a, &[evidence_a], "op", 1_001)
                    .await
            },
            async move {
                second
                    .reconcile_subject(41_002, &child_b, &[evidence_b], "op", 1_002)
                    .await
            },
        );
        // The advisory lock serialises the two sections, so neither loses
        // to a UNIQUE violation.
        assert!(a.is_ok(), "first concurrent reconcile failed: {a:?}");
        assert!(b.is_ok(), "second concurrent reconcile failed: {b:?}");

        let child_key = common::thread_group_key::source_thread_canonical_key(
            &common::thread_group_key::SourceIdentity::new(
                "user:1",
                "codex",
                IdentityScope::known(String::new()),
                "pg-cc-child",
            ),
        )
        .expect("known scope");
        let active =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .list_by_child_canonical_key(&child_key, Some(values::relation_state::ACTIVE))
                .await
                .expect("list relations");
        assert_eq!(active.len(), 1, "exactly one active edge must survive");
    });
}

#[test]
fn pg_concurrent_distinct_parents_never_leave_two_active_parents() {
    TEST_RUNTIME.block_on(async {
        let pool = setup_test_rdb_from("../infra/sql/postgres").await;
        let reconcile = Arc::new(ThreadGroupReconciliationService::new(pool));
        let parent_a = endpoint("pg-cd-parent-a");
        let parent_b = endpoint("pg-cd-parent-b");
        let child = endpoint("pg-cd-child");
        insert_thread(pool, 41_101).await;
        insert_thread(pool, 41_102).await;
        insert_thread(pool, 41_103).await;
        reconcile
            .reconcile_subject(41_101, &parent_a, &[], "op", 1_000)
            .await
            .expect("parent a reconcile");
        reconcile
            .reconcile_subject(41_102, &parent_b, &[], "op", 1_000)
            .await
            .expect("parent b reconcile");

        let evidence_a = observation("pg-cd-child", "pg-cd-parent-a");
        let evidence_b = observation("pg-cd-child", "pg-cd-parent-b");
        let first = reconcile.clone();
        let second = reconcile.clone();
        let (child_a, child_b) = (child.clone(), child.clone());
        let (_a, _b) = tokio::join!(
            async move {
                first
                    .reconcile_subject(41_103, &child_a, &[evidence_a], "op", 1_001)
                    .await
            },
            async move {
                second
                    .reconcile_subject(41_103, &child_b, &[evidence_b], "op", 1_002)
                    .await
            },
        );

        let child_key = common::thread_group_key::source_thread_canonical_key(
            &common::thread_group_key::SourceIdentity::new(
                "user:1",
                "codex",
                IdentityScope::known(String::new()),
                "pg-cd-child",
            ),
        )
        .expect("known scope");
        let active =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .list_by_child_canonical_key(&child_key, Some(values::relation_state::ACTIVE))
                .await
                .expect("list relations");
        assert!(
            active.len() <= 1,
            "a child must never have two active canonical parents (got {})",
            active.len()
        );
    });
}

#[test]
fn pg_concurrent_siblings_do_not_leave_an_empty_active_group() {
    TEST_RUNTIME.block_on(async {
        let pool = setup_test_rdb_from("../infra/sql/postgres").await;
        let reconcile = Arc::new(ThreadGroupReconciliationService::new(pool));
        let generator = infra::test_helper::shared_id_generator();
        let parent_id = generator.generate_id().unwrap();
        let a_id = generator.generate_id().unwrap();
        let b_id = generator.generate_id().unwrap();
        for id in [parent_id, a_id, b_id] {
            insert_thread(pool, id).await;
        }
        let parent_name = format!("pg-redirect-parent-{parent_id}");
        let a_name = format!("pg-redirect-child-{a_id}");
        let b_name = format!("pg-redirect-child-{b_id}");
        let parent = endpoint(&parent_name);
        let a = endpoint(&a_name);
        let b = endpoint(&b_name);
        let target_group = reconcile
            .reconcile_subject(parent_id, &parent, &[], "pg-redirect", 1_000)
            .await
            .unwrap()
            .group_id
            .unwrap();
        let old_group = reconcile
            .reconcile_subject(a_id, &a, &[], "pg-redirect", 1_001)
            .await
            .unwrap()
            .group_id
            .unwrap();
        let b_group = reconcile
            .reconcile_subject(b_id, &b, &[], "pg-redirect", 1_002)
            .await
            .unwrap()
            .group_id
            .unwrap();
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        let b_member = members
            .find_current_by_thread_canonical_key(&key_of(&b_name))
            .await
            .unwrap()
            .unwrap();
        assert!(
            members
                .redirect_current_tx(pool, &b_member.thread_canonical_key, b_group, 1_003)
                .await
                .unwrap()
        );
        members
            .insert_tx(
                pool,
                &NewThreadGroupMember {
                    group_id: old_group,
                    thread_id: b_member.thread_id,
                    thread_canonical_key: b_member.thread_canonical_key.clone(),
                    owner_scope: b_member.owner_scope,
                    source: b_member.source,
                    identity_scope: b_member.identity_scope,
                    native_id: b_member.native_id,
                    role: b_member.role,
                    state: values::member_state::ACTIVE.into(),
                    provenance: values::grouping_authority::RECONCILER.into(),
                    deleted_at: None,
                    created_at: 1_003,
                    updated_at: 1_003,
                },
            )
            .await
            .unwrap();

        let first = reconcile.clone();
        let second = reconcile.clone();
        let parent_a = parent_name.clone();
        let parent_b = parent_name;
        let (left, right) = tokio::join!(
            async move {
                first
                    .reconcile_subject(
                        a_id,
                        &a,
                        &[observation(&a_name, &parent_a)],
                        "pg-redirect",
                        1_010,
                    )
                    .await
            },
            async move {
                second
                    .reconcile_subject(
                        b_id,
                        &b,
                        &[observation(&b_name, &parent_b)],
                        "pg-redirect",
                        1_011,
                    )
                    .await
            },
        );
        assert!(left.is_ok(), "first sibling import failed: {left:?}");
        assert!(right.is_ok(), "second sibling import failed: {right:?}");
        let old = ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
            .find_by_id(old_group)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.status, values::group_status::REDIRECTED);
        assert_eq!(old.redirect_to_group_id, Some(target_group));
    });
}

#[test]
fn pg_group_membership_mutations_wait_for_the_current_transaction() {
    TEST_RUNTIME.block_on(async {
        let pool = setup_test_rdb_from("../infra/sql/postgres").await;
        let locks = ThreadGroupLockRepositoryImpl::new(pool);
        let mut first = pool.begin().await.unwrap();
        locks.lock_group_membership_tx(&mut *first).await.unwrap();
        let second = async {
            let mut tx = pool.begin().await.unwrap();
            locks.lock_group_membership_tx(&mut *tx).await.unwrap();
            tx.commit().await.unwrap();
        };
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), second)
                .await
                .is_err()
        );
        first.rollback().await.unwrap();
        let mut retry = pool.begin().await.unwrap();
        locks.lock_group_membership_tx(&mut *retry).await.unwrap();
        retry.commit().await.unwrap();
    });
}
