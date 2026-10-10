//! `memories-db-migrate embedding`: status, planning, and (later) the
//! migration of the embedding space. Works with both RDB backends and
//! without a running memories server or jobworkerp.

pub mod attempt;
pub mod backup;
pub use infra::infra::embedding_index::counts;
pub mod finalize;
pub mod guard;
pub mod inspect;
pub mod lock;
pub mod observe;
pub mod output;
pub mod plan;
pub mod stage;

#[cfg(test)]
mod tests;
