-- Expand migration for MemoryKind.
-- Columns remain nullable until the contract migration has classified all
-- existing rows and populated an explicit kind.
ALTER TABLE thread ADD COLUMN memory_kind INT;
ALTER TABLE memory ADD COLUMN memory_kind INT;

CREATE INDEX IF NOT EXISTS thread_user_memory_kind_updated_at
    ON thread (user_id, memory_kind, updated_at);
CREATE INDEX IF NOT EXISTS memory_user_memory_kind_updated_at
    ON memory (user_id, memory_kind, updated_at);
