// db row definitions for thread_label junction table

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadLabelRow {
    pub thread_id: i64,
    pub label: String,
    pub created_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct LabelWithCountRow {
    pub label: String,
    pub thread_count: i64,
}
