use prost::DecodeError;
use redis::RedisError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LlmMemoryError {
    #[error("TonicServerError({0:?})")]
    TonicServerError(tonic::transport::Error),
    //    #[error("ReqwestError({0:?})")]
    //    ReqwestError(reqwest::Error),
    #[error("RuntimeError({0})")]
    RuntimeError(String),
    #[error("InvalidArgument({0})")]
    InvalidArgument(String),
    /// Caller request was rejected because a precondition (size limit,
    /// configuration prerequisite, etc.) was not met. Mapped to gRPC
    /// `FAILED_PRECONDITION` (code 9) by `handle_error`.
    #[error("FailedPrecondition({0})")]
    FailedPrecondition(String),
    /// Caller-supplied payload exceeded a hard size cap (e.g. media
    /// Upload over `MEDIA_UPLOAD_MAX_BYTES`). Mapped to gRPC
    /// `RESOURCE_EXHAUSTED` (code 8) by `handle_error`.
    #[error("ResourceExhausted({0})")]
    ResourceExhausted(String),
    /// A transient concurrency conflict (e.g. a media Upload racing a
    /// concurrent reservation / promotion / deletion of the same
    /// sha256). The caller should retry — the conflict resolves once the
    /// other path finishes or the GC reclaims a crashed one. Mapped to
    /// gRPC `ABORTED` (code 10) by `handle_error`, distinct from
    /// `FAILED_PRECONDITION` (which means "fix state before retrying").
    #[error("Aborted({0})")]
    Aborted(String),
    /// Caller request was rejected because the requested mode is reserved
    /// but not yet implemented in this build (e.g. CountSearchMode::VECTOR
    /// in Phase 5-1). Mapped to gRPC `UNIMPLEMENTED` (code 12) by
    /// `handle_error`. Switching from Unimplemented to a real
    /// implementation is a non-breaking server-side change.
    #[error("Unimplemented({0})")]
    Unimplemented(String),
    /// Caller is authenticated but lacks the rights to operate on the
    /// requested resource (e.g. cross-user memory write attempt during
    /// batch import). Mapped to gRPC `PERMISSION_DENIED` (code 7) by
    /// `handle_error`.
    #[error("PermissionDenied({0})")]
    PermissionDenied(String),
    #[error("CodecError({0:?})")]
    CodecError(DecodeError),
    #[error("NotFound({0})")]
    NotFound(String),
    #[error("AlreadyExists({0})")]
    AlreadyExists(String),
    #[error("RedisError({0:?})")]
    RedisError(RedisError),
    #[error("DBError({0:?})")]
    DBError(sqlx::Error),
    #[error("GenerateIdError({0})")]
    GenerateIdError(String),
    #[error("ParseError({0})")]
    ParseError(String),
    //    #[error("serde_json error({0:?})")]
    //    SerdeJsonError(serde_json::error::Error),
    //    #[error("serde_yaml error({0:?})")]
    //    SerdeYamlError(serde_yaml::Error),
    //    #[error("docker error({0:?})")]
    //    DockerError(bollard::errors::Error),
    //    #[error("kube error({0:?})")]
    //    KubeError(kube::error::Error),
    #[error("OtherError({0})")]
    OtherError(String),
}
impl From<tonic::transport::Error> for LlmMemoryError {
    fn from(e: tonic::transport::Error) -> Self {
        LlmMemoryError::TonicServerError(e)
    }
}
impl From<RedisError> for LlmMemoryError {
    fn from(e: RedisError) -> Self {
        LlmMemoryError::RedisError(e)
    }
}
//impl From<serde_json::Error> for LlmMemoryError {
//    fn from(e: serde_json::Error) -> Self {
//        LlmMemoryError::SerdeJsonError(e)
//    }
//}
//impl From<serde_yaml::Error> for LlmMemoryError {
//    fn from(e: serde_yaml::Error) -> Self {
//        LlmMemoryError::SerdeYamlError(e)
//    }
//}
//impl From<kube::error::Error> for LlmMemoryError {
//    fn from(e: kube::error::Error) -> Self {
//        LlmMemoryError::KubeError(e)
//    }
//}
//impl From<bollard::errors::Error> for LlmMemoryError {
//    fn from(e: bollard::errors::Error) -> Self {
//        LlmMemoryError::DockerError(e)
//    }
//}
