-- Add the message-time fields without changing the thread audit timestamps.
ALTER TABLE thread ADD COLUMN first_message_at BIGINT;
ALTER TABLE thread ADD COLUMN last_message_at BIGINT;

CREATE INDEX thread_user_memory_kind_last_message_at_id
    ON thread (user_id, memory_kind, last_message_at DESC NULLS LAST, id DESC);
CREATE INDEX thread_user_last_message_at_id
    ON thread (user_id, last_message_at DESC NULLS LAST, id DESC);
CREATE INDEX thread_last_message_at_id ON thread (last_message_at DESC NULLS LAST, id DESC);
CREATE INDEX thread_user_memory_kind_first_message_at
    ON thread (user_id, memory_kind, first_message_at);
CREATE INDEX thread_user_first_message_at ON thread (user_id, first_message_at);
CREATE INDEX thread_first_message_at ON thread (first_message_at);

CREATE TABLE thread_time_migration_state (
    migration_key TEXT PRIMARY KEY,
    rdb_completed_at BIGINT NULL,
    vector_status TEXT NOT NULL,
    staging_table_name TEXT NULL,
    vector_completed_at BIGINT NULL,
    CHECK (migration_key = 'thread-time-fields-v1'),
    CHECK (vector_status IN ('PENDING', 'STAGED', 'SWITCHING', 'NOT_REQUIRED', 'COMPLETED'))
);
