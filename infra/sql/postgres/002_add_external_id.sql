ALTER TABLE memory ADD COLUMN IF NOT EXISTS external_id VARCHAR(512);
CREATE UNIQUE INDEX IF NOT EXISTS memory_external_id ON memory (external_id);
