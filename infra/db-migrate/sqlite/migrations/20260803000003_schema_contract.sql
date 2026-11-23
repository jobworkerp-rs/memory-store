CREATE TABLE memories_schema_contract (
    contract_key TEXT PRIMARY KEY CHECK (contract_key = 'rdb_schema'),
    version TEXT NOT NULL CHECK (length(version) = 14)
);

INSERT INTO memories_schema_contract (contract_key, version)
VALUES ('rdb_schema', '20260803000003');

CREATE TABLE memories_data_migration_task_state (
    task_identity TEXT PRIMARY KEY,
    canonical_definition_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'completed', 'failed')),
    execution_id TEXT,
    holder_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    heartbeat_at BIGINT,
    lease_expires_at BIGINT,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    checkpoint TEXT,
    failure_classification TEXT,
    started_at BIGINT,
    updated_at BIGINT NOT NULL,
    completed_at BIGINT
);
