pub mod media;
pub mod memory;
pub mod memory_rating;
pub mod memory_vector;
pub mod reflection;
pub mod thread;
// Shared P5 thread_filter resolver. Used by both the RDB list/Count
// path (`MemoryApp::find_memory_list_by_condition`) and the LanceDB-
// bound vector/FTS/hybrid paths.
pub mod thread_filter_resolver;
pub mod thread_vector;

/// Shared cache key format for memory entries.
/// Used by both MemoryApp and ThreadApp (which share the same cache instance).
pub(crate) fn memory_cache_key(id: &i64) -> String {
    ["memory_id:", &id.to_string()].join("")
}

/// Reserved owner for reflection memories (spec §3.6, §9.1 fixpoint #1').
/// All reflection memories are stored under this user_id and attached
/// only to per-(owner, labels) aggregate threads — never to the origin
/// thread of the trajectory under analysis.
pub const REFLECTION_USER_ID: i64 = 300_000;
