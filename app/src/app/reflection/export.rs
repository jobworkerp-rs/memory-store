//! F-E1 — keyset-paginated export over the sidecar.
//!
//! Sort is fixed to `memory_id DESC` (= `ReflectionSortKey::MemoryIdDesc`)
//! so the `cursor_after_memory_id` keyset filter (`memory_id < cursor`)
//! walks through pages without OFFSET drift under concurrent inserts.
//! Snowflake IDs are time-ordered to within ms resolution, so the
//! visible order tracks `created_at DESC` for natural inserts but is
//! not a guarantee — backfilled / imported rows whose generation time
//! diverged from `created_at` will surface in `memory_id` order, not
//! wall-clock order. A strict `(created_at DESC, memory_id DESC)`
//! contract would need a compound cursor (see design doc L675 /
//! `cursor_after_memory_id` proto comment).

use anyhow::Result;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::rows::ReflectionSortKey;
use protobuf::llm_memory::data::{Reflection, ReflectionSearchFilter};

use crate::app::reflection::ReflectionAppImpl;
use crate::app::reflection::search;

const DEFAULT_BATCH: u32 = 200;
const MAX_BATCH: u32 = 1000;

pub async fn export(
    app: &ReflectionAppImpl,
    filter: Option<&ReflectionSearchFilter>,
    cursor_after_memory_id: Option<i64>,
    batch_size: Option<u32>,
) -> Result<Vec<Reflection>> {
    let mut resolved = filter.map(search::resolve_filter).unwrap_or_default();
    if let Some(cursor) = cursor_after_memory_id {
        resolved.memory_id_lt = Some(cursor);
    }
    let limit = batch_size.unwrap_or(DEFAULT_BATCH).clamp(1, MAX_BATCH) as i64;
    let rows = app
        .index_repo
        .search_index(&resolved, ReflectionSortKey::MemoryIdDesc, limit, 0)
        .await?;
    let results = search::hydrate_rows(app, rows).await?;
    Ok(results.into_iter().filter_map(|r| r.reflection).collect())
}
