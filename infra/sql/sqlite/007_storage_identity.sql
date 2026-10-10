-- Mirrors infra/atlas/sqlite/migrations/20261009000001_embedding_storage_identity.sql
-- for the SQLx test schema.
CREATE TABLE memories_storage_identity (
    identity_key TEXT PRIMARY KEY CHECK (identity_key = 'rdb'),
    storage_id TEXT NOT NULL CHECK (length(storage_id) = 32)
);

INSERT INTO memories_storage_identity (identity_key, storage_id)
VALUES ('rdb', lower(hex(randomblob(16))));
