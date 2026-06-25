use infra::error::LlmMemoryError;
use sqlx::error::DatabaseError;

// TODO map redis etc error
pub fn handle_error(err: &anyhow::Error) -> tonic::Status {
    // TODO search with err.chain()
    match err.downcast_ref::<LlmMemoryError>() {
        Some(LlmMemoryError::DBError(sqlx::Error::Database(e))) => map_db_error(e.as_ref()),
        Some(LlmMemoryError::DBError(sqlx::Error::RowNotFound)) => {
            tracing::warn!("row not found occurred: {:?}", err);
            tonic::Status::not_found(format!("not found: {:?}", err))
        }
        Some(LlmMemoryError::NotFound(msg)) => {
            tracing::warn!("not found: {}", msg);
            tonic::Status::not_found(msg.clone())
        }
        Some(LlmMemoryError::InvalidArgument(msg)) => {
            tracing::warn!("invalid argument: {}", msg);
            tonic::Status::invalid_argument(msg.clone())
        }
        Some(LlmMemoryError::FailedPrecondition(msg)) => {
            tracing::warn!("failed precondition: {}", msg);
            tonic::Status::failed_precondition(msg.clone())
        }
        Some(LlmMemoryError::ResourceExhausted(msg)) => {
            tracing::warn!("resource exhausted: {}", msg);
            tonic::Status::resource_exhausted(msg.clone())
        }
        Some(LlmMemoryError::Aborted(msg)) => {
            tracing::warn!("aborted (transient conflict, retryable): {}", msg);
            tonic::Status::aborted(msg.clone())
        }
        Some(LlmMemoryError::PermissionDenied(msg)) => {
            tracing::warn!("permission denied: {}", msg);
            tonic::Status::permission_denied(msg.clone())
        }
        Some(LlmMemoryError::AlreadyExists(msg)) => {
            tracing::warn!("already exists: {}", msg);
            tonic::Status::already_exists(msg.clone())
        }
        Some(LlmMemoryError::Unimplemented(msg)) => {
            tracing::info!("unimplemented mode requested: {}", msg);
            tonic::Status::unimplemented(msg.clone())
        }
        Some(e) => {
            tracing::warn!("unknown error occurred: {:?}", e);
            tonic::Status::internal(format!("unknown: {:?}", e))
        }
        None => {
            tracing::warn!("other error occurred: {:?}", err);
            tonic::Status::internal(format!("other error: {:?}", err))
        }
    }
}

// TODO あとでちゃんと実装する
fn map_db_error(err: &dyn DatabaseError) -> tonic::Status {
    tracing::warn!("database error: {:?}", err);
    #[cfg(not(feature = "postgres"))]
    {
        use sqlx::sqlite::SqliteError;
        if let Some(e) = err.try_downcast_ref::<SqliteError>() {
            if e.code().as_deref() == Some("2067") {
                // SQLITE_CONSTRAINT_UNIQUE
                return tonic::Status::already_exists(format!("{:?}", e));
            } else {
                tracing::warn!("sqlite error occurred: {:?}", e);
                return tonic::Status::unavailable(format!("db error: {:?}", e));
            }
        }
    }
    #[cfg(feature = "postgres")]
    {
        use sqlx::postgres::PgDatabaseError;
        if let Some(e) = err.try_downcast_ref::<PgDatabaseError>() {
            if e.code() == "23505" {
                // unique_violation
                return tonic::Status::already_exists(format!("{:?}", e));
            } else {
                tracing::warn!("postgres error occurred: {:?}", e);
                return tonic::Status::unavailable(format!("db error: {:?}", e));
            }
        }
    }
    tracing::warn!("unknown db error occurred: {:?}", err);
    tonic::Status::unavailable(format!("db error: {:?}", err))
}
