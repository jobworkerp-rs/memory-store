//! End-to-end coverage for the structured-startup-error path: open a
//! LanceDB table with one embedding dimension, then re-open with a
//! different dimension and confirm the resulting error round-trips
//! through `anyhow::Error::downcast_ref::<StartupError>` carrying the
//! `expected_dim` / `actual_dim` the agent-app side expects.
//!
//! This file is the regression guard against silent reversions of:
//!  - `verify_table_schema_or_fail` returning a plain `anyhow::Error`
//!  - `extract_embedding_dim_from_schema` returning `None` for the
//!    in-tree `memory_arrow_schema` / `thread_arrow_schema` shapes
//!
//! Both repos share the same `schema_fingerprint` machinery, so a dim
//! change is observed as a fingerprint mismatch, not as a column-count
//! mismatch. The dim pair fed to `StartupError::LancedbSchemaMismatch`
//! must reflect the actual on-disk size and the expected new size —
//! otherwise the agent-app UI would render `expected_dim=0` and lose
//! its ability to explain the root cause to the user.

use infra::infra::memory_vector::config::{
    DistanceType, FtsConfig, OptimizeConfig, VectorDBConfig, VectorIndexConfig,
};
use infra::infra::memory_vector::repository::MemoryVectorRepositoryImpl;
use infra::infra::startup_error::StartupError;
use infra::infra::thread_vector::config::ThreadVectorDBConfig;
use infra::infra::thread_vector::repository::ThreadVectorRepositoryImpl;

fn vector_db_config(uri: &str, vector_size: usize) -> VectorDBConfig {
    VectorDBConfig {
        uri: uri.to_string(),
        table_name: "memories".to_string(),
        vector_size,
        distance_type: DistanceType::Cosine,
        optimize: OptimizeConfig {
            prune_on_startup: false,
            ..Default::default()
        },
        fts: FtsConfig::default(),
        vector_index: VectorIndexConfig::default(),
    }
}

fn thread_db_config(uri: &str, vector_size: usize) -> ThreadVectorDBConfig {
    ThreadVectorDBConfig {
        uri: uri.to_string(),
        table_name: "thread_vectors".to_string(),
        vector_size,
        distance_type: DistanceType::Cosine,
        optimize: OptimizeConfig {
            prune_on_startup: false,
            ..Default::default()
        },
        fts: FtsConfig::default(),
        vector_index: VectorIndexConfig::default(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_vector_dim_mismatch_returns_structured_startup_error() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let uri = tmp.path().to_string_lossy().into_owned();

    // First open at dim=8 — creates the table on disk with that schema.
    MemoryVectorRepositoryImpl::new(vector_db_config(&uri, 8))
        .await
        .expect("first open creates the table at dim=8");

    // Second open at dim=16 — must fail with the structured error so
    // `module.rs` can route into `StartupError::fatal()` via downcast.
    let err = match MemoryVectorRepositoryImpl::new(vector_db_config(&uri, 16)).await {
        Ok(_) => panic!("reopening with a different dim must fail"),
        Err(e) => e,
    };
    let structured = err
        .downcast_ref::<StartupError>()
        .expect("error must wrap a StartupError so the parent can classify");
    match structured {
        StartupError::LancedbSchemaMismatch {
            table,
            expected_dim,
            actual_dim,
            ..
        } => {
            assert_eq!(table, "memories");
            // "expected" is what we asked for on this second open (16),
            // "actual" is what already lives on disk from the first open (8).
            assert_eq!(*expected_dim, 16);
            assert_eq!(*actual_dim, 8);
        }
        other => panic!("expected LancedbSchemaMismatch, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thread_vector_dim_mismatch_returns_structured_startup_error() {
    let tmp = tempfile::tempdir().expect("tmpdir");
    let uri = tmp.path().to_string_lossy().into_owned();

    ThreadVectorRepositoryImpl::new(thread_db_config(&uri, 8))
        .await
        .expect("first open creates the thread table at dim=8");

    let err = match ThreadVectorRepositoryImpl::new(thread_db_config(&uri, 16)).await {
        Ok(_) => panic!("reopening with a different dim must fail"),
        Err(e) => e,
    };
    let structured = err
        .downcast_ref::<StartupError>()
        .expect("error must wrap a StartupError so the parent can classify");
    match structured {
        StartupError::LancedbSchemaMismatch {
            table,
            expected_dim,
            actual_dim,
            ..
        } => {
            assert_eq!(table, "thread_vectors");
            assert_eq!(*expected_dim, 16);
            assert_eq!(*actual_dim, 8);
        }
        other => panic!("expected LancedbSchemaMismatch, got {other:?}"),
    }
}
