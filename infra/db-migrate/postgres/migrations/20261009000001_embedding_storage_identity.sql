-- Identifier of this RDB, matched against the identifiers recorded in the
-- LanceDB stores and the embedding state directory so a mismatched
-- storage set is detected before anything is served or migrated.
CREATE TABLE memories_storage_identity (
    identity_key TEXT PRIMARY KEY CHECK (identity_key = 'rdb'),
    storage_id TEXT NOT NULL CHECK (length(storage_id) = 32)
);

INSERT INTO memories_storage_identity (identity_key, storage_id)
VALUES ('rdb', replace(gen_random_uuid()::text, '-', ''));

UPDATE memories_schema_contract
SET version = '20261009000001'
WHERE contract_key = 'rdb_schema';
