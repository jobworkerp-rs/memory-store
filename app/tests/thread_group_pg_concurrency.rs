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
use infra::infra::thread_group::relation::{
    ThreadRelationRepository, ThreadRelationRepositoryImpl,
};
use infra::infra::thread_group::rows::values;
use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};

fn endpoint(native_id: &str) -> ObservedEndpoint {
    ObservedEndpoint {
        source: "codex".to_string(),
        identity_scope: IdentityScope::known(String::new()),
        owner_scope: "user:1".to_string(),
        native_id: native_id.to_string(),
    }
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
