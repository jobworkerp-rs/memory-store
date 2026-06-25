//! Intent-vector LanceDB store dedicated to reflections.
//!
//! Separate namespace from the existing `memory_vector` table so that
//! `task_intent` (pre-condition), `summary` (post hoc explanation), and
//! the original memory body can co-exist without the
//! "1 memory : 1 vector" assumption baked into `memory_vector`.
//!
//! Submodules:
//! - `config`     — env knobs (`REFLECTION_LANCEDB_URI`, vector size).
//! - `schema`     — Arrow schema. No FTS (no `content` column); pure
//!   vector search.
//! - `record`     — `ReflectionIntentVectorRecord` + serialisation.
//! - `repository` — `ReflectionIntentVectorRepository` (open / upsert /
//!   delete / vector search).
//! - `safe_filter` — small helper for `IN (..)` filter strings reused
//!   across the 2-stage RDB→IN list→Lance search path.

pub mod config;
pub mod record;
pub mod repository;
pub mod safe_filter;
pub mod schema;
