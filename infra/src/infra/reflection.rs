//! Reflection RDB layer (Phase B of ai-docs/thread-reflection-spec.md
//! implementation).
//!
//! Submodules:
//! - `rdb`           — `thread_reflection_index` sidecar CRUD + filtered
//!   search / aggregate.
//! - `rows`          — sqlx FromRow structs for sidecar / child tables.
//! - `failure_mode`  — `reflection_failure_mode` child table.
//! - `tool`          — `reflection_tool` child table.
//! - `tool_outcome`  — `reflection_tool_outcome` child table.
//! - `fact`          — `reflection_fact` child table (links_json).
//! - `applied_target`   — `reflection_applied_target` (idempotent).
//! - `few_shot_usage`   — `reflection_few_shot_usage` (idempotent).
//! - `stats`         — `tool_outcome_stats` / `tool_contribution_stats`
//!   upsert + rebuild.
//! - `dictionary`    — `failure_mode_dictionary` (read-only at runtime;
//!   loaded into a DashMap on boot).
//! - `signature_norm` — `failure_signature_indicator_norm`.
//! - `aggregate_thread` — `thread_aggregate_key` UNIQUE-based
//!   retrieve-or-create for the aggregate reflection thread.
//!
//! All tables omit FK constraints by project convention; cascade
//! deletes flow from `app::ReflectionApp::delete`.

pub mod aggregate_thread;
pub mod applied_target;
pub mod dictionary;
pub mod fact;
pub mod failure_mode;
pub mod failure_mode_convert;
pub mod few_shot_usage;
pub mod rdb;
pub mod rows;
pub mod signature_norm;
pub mod stats;
pub mod tool;
pub mod tool_outcome;

#[cfg(test)]
pub(crate) mod test_support;
