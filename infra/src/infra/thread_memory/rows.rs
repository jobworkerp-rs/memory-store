// db row definitions for thread_memory junction table
#[derive(sqlx::FromRow)]
pub struct ThreadMemoryRow {
    pub thread_id: i64,
    pub memory_id: i64,
    pub position: i32,
    pub created_at: i64,
}
