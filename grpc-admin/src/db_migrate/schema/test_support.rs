//! Disposable databases for schema-engine tests, empty or loaded from the
//! legacy-history fixtures (`grpc-admin/tests/fixtures/db-migrate`).

use super::history::{Connection, LegacyHistoryLocation};
use infra_utils::infra::rdb::RdbPool;

/// Fixtures of this build's backend.
#[cfg(not(feature = "postgres"))]
pub const LEGACY_FIXTURES: &[&str] = &[
    "fresh-full",
    "fresh-partial",
    "adopted-000001",
    "adopted-000002",
    "legacy-ddl-000001",
];
#[cfg(feature = "postgres")]
pub const LEGACY_FIXTURES: &[&str] = &[
    "dedicated-fresh-full",
    "dedicated-fresh-partial",
    "dedicated-adopted-000001",
    "dedicated-adopted-000002",
    "scoped-fresh-full",
    "scoped-fresh-partial",
    "scoped-adopted-000001",
    "scoped-adopted-000002",
];

/// The fixture migrated through the whole catalog without adoption.
#[cfg(not(feature = "postgres"))]
pub const FULL_FIXTURE: &str = "fresh-full";
#[cfg(feature = "postgres")]
pub const FULL_FIXTURE: &str = "scoped-fresh-full";

fn fixture_sql(name: &str) -> String {
    let backend = if cfg!(feature = "postgres") {
        "postgres"
    } else {
        "sqlite"
    };
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/db-migrate/legacy-history")
        .join(backend)
        .join(format!("{name}.sql"));
    std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("reading fixture {}: {error}", path.display()))
}

pub struct TestDatabase {
    pub pool: RdbPool,
    pub location: LegacyHistoryLocation,
    #[cfg(not(feature = "postgres"))]
    _directory: tempfile::TempDir,
    #[cfg(feature = "postgres")]
    database: String,
}

impl TestDatabase {
    pub async fn conn(&self) -> sqlx::pool::PoolConnection<infra_utils::infra::rdb::Rdb> {
        self.pool.acquire().await.unwrap()
    }

    pub async fn execute(&self, sql: &str) {
        let mut conn = self.conn().await;
        sqlx::raw_sql(sqlx::AssertSqlSafe(sql.to_string()))
            .execute(&mut *conn)
            .await
            .unwrap_or_else(|error| panic!("executing test SQL: {error}"));
    }

    #[cfg(feature = "postgres")]
    pub async fn scalar_bool(&self, sql: &str) -> bool {
        let mut conn = self.conn().await;
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_string()))
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    }

    pub async fn scalar_i64(&self, sql: &str) -> i64 {
        let mut conn = self.conn().await;
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql.to_string()))
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    }
}

pub async fn inspect(database: &TestDatabase) -> super::history::Inspection {
    let mut conn = database.conn().await;
    let conn: &mut Connection = &mut conn;
    super::history::inspect(conn, database.location)
        .await
        .unwrap()
}

#[cfg(not(feature = "postgres"))]
mod backend {
    use super::*;

    pub async fn empty() -> TestDatabase {
        let directory = tempfile::tempdir().unwrap();
        let url = format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("target.sqlite3").display()
        );
        let pool = crate::db_migrate::schema::target::connect_for_migration(&url)
            .await
            .unwrap();
        TestDatabase {
            pool,
            location: LegacyHistoryLocation::ApplicationSchema,
            _directory: directory,
        }
    }

    pub async fn from_fixture(name: &str) -> TestDatabase {
        let database = empty().await;
        database.execute(&fixture_sql(name)).await;
        database
    }

    impl TestDatabase {
        pub async fn close(self) {
            self.pool.close().await;
        }
    }
}

#[cfg(feature = "postgres")]
mod backend {
    use super::*;
    use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU32, Ordering};

    static NEXT_DATABASE: AtomicU32 = AtomicU32::new(0);

    fn service_url() -> String {
        std::env::var("TEST_POSTGRES_URL")
            .expect("PostgreSQL schema-engine tests require TEST_POSTGRES_URL")
    }

    async fn admin() -> sqlx::PgPool {
        PgPoolOptions::new()
            .max_connections(1)
            .connect(&service_url())
            .await
            .expect("connecting to TEST_POSTGRES_URL")
    }

    /// A fresh database; `schema` selects the application schema through
    /// `search_path`, as an operator's URL would.
    async fn create(schema: Option<&str>) -> TestDatabase {
        let database = format!(
            "mdm_schema_test_{}_{}",
            std::process::id(),
            NEXT_DATABASE.fetch_add(1, Ordering::Relaxed)
        );
        let admin = admin().await;
        // Separate statements: a multi-statement string runs as one
        // transaction block, which DROP/CREATE DATABASE refuse.
        for statement in [
            format!("DROP DATABASE IF EXISTS {database}"),
            format!("CREATE DATABASE {database}"),
        ] {
            sqlx::raw_sql(sqlx::AssertSqlSafe(statement))
                .execute(&admin)
                .await
                .unwrap();
        }
        admin.close().await;

        let mut url = url::Url::parse(&service_url()).unwrap();
        url.set_path(&database);
        let mut options = PgConnectOptions::from_str(url.as_str()).unwrap();
        let location = match schema {
            Some(schema) => {
                options = options.options([("search_path", schema)]);
                LegacyHistoryLocation::ApplicationSchema
            }
            None => LegacyHistoryLocation::DedicatedSchema,
        };
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .unwrap();
        TestDatabase {
            pool,
            location,
            database,
        }
    }

    pub async fn empty() -> TestDatabase {
        create(None).await
    }

    pub async fn from_fixture(name: &str) -> TestDatabase {
        let schema = name.starts_with("scoped-").then_some("memories");
        let database = create(schema).await;
        // The dump resets the session search_path, so it gets a connection
        // of its own.
        let mut url = url::Url::parse(&service_url()).unwrap();
        url.set_path(&database.database);
        let loader = PgPoolOptions::new()
            .max_connections(1)
            .connect(url.as_str())
            .await
            .unwrap();
        sqlx::raw_sql(sqlx::AssertSqlSafe(fixture_sql(name)))
            .execute(&loader)
            .await
            .unwrap_or_else(|error| panic!("loading fixture {name}: {error}"));
        loader.close().await;
        database
    }

    impl TestDatabase {
        pub async fn close(self) {
            self.pool.close().await;
            let admin = admin().await;
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                self.database
            )))
            .execute(&admin)
            .await
            .unwrap();
            admin.close().await;
        }
    }
}

pub use backend::{empty, from_fixture};
