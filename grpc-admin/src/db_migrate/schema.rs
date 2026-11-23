//! Schema migration engine of `memories-db-migrate`: the migration catalog
//! compiled into the binary, the migration history, the handover from the
//! legacy history, and the drift check.

pub mod drift;
pub mod engine;
pub mod handover;
pub mod history;
pub mod migrations;
pub mod seeds;
pub mod target;
#[cfg(test)]
mod test_support;
