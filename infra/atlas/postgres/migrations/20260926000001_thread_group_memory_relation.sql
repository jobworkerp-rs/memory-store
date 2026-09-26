-- Group/Memory existence and exclusive ownership are checked by the app in
-- the same transaction as a write or a purge; no trigger or cascade is used.
CREATE TABLE thread_group_memory_relation (
    group_id BIGINT NOT NULL,
    memory_id BIGINT NOT NULL,
    purpose TEXT NOT NULL CHECK (length(purpose) > 0),
    on_group_delete TEXT NOT NULL CHECK (on_group_delete IN ('retain', 'delete')),
    created_at BIGINT NOT NULL,
    PRIMARY KEY (group_id, memory_id)
);
CREATE INDEX thread_group_memory_relation_memory_id
    ON thread_group_memory_relation (memory_id);

UPDATE memories_schema_contract SET version = '20260926000001' WHERE contract_key = 'rdb_schema';
