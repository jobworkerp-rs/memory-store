-- Run during the thread timestamp maintenance window before executing the
-- application backfill command. Existing audit values are intentionally kept.
ALTER TABLE thread ADD COLUMN IF NOT EXISTS first_message_at BIGINT;
ALTER TABLE thread ADD COLUMN IF NOT EXISTS last_message_at BIGINT;

CREATE INDEX IF NOT EXISTS thread_user_memory_kind_last_message_at_id
    ON thread (user_id, memory_kind, last_message_at DESC NULLS LAST, id DESC);
CREATE INDEX IF NOT EXISTS thread_user_last_message_at_id
    ON thread (user_id, last_message_at DESC NULLS LAST, id DESC);
CREATE INDEX IF NOT EXISTS thread_last_message_at_id
    ON thread (last_message_at DESC NULLS LAST, id DESC);
CREATE INDEX IF NOT EXISTS thread_user_memory_kind_first_message_at
    ON thread (user_id, memory_kind, first_message_at);
CREATE INDEX IF NOT EXISTS thread_user_first_message_at ON thread (user_id, first_message_at);
CREATE INDEX IF NOT EXISTS thread_first_message_at ON thread (first_message_at);

CREATE TABLE IF NOT EXISTS thread_time_migration_state (
    migration_key TEXT PRIMARY KEY,
    rdb_completed_at BIGINT NULL,
    vector_status TEXT NOT NULL,
    staging_table_name TEXT NULL,
    vector_completed_at BIGINT NULL,
    CHECK (migration_key = 'thread-time-fields-v1'),
    CHECK (vector_status IN ('PENDING', 'STAGED', 'SWITCHING', 'NOT_REQUIRED', 'COMPLETED'))
);
