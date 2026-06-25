-- Adds external_id column and UNIQUE index to the memory table.
--
-- New deployments: 001_schema.sql already includes external_id, so only the
-- idempotent CREATE INDEX runs here.
--
-- Existing deployments (pre-external_id): run the manual migration first:
--   infra/sql/sqlite/manual/007_add_external_id.sql
-- which performs ALTER TABLE ADD COLUMN, then apply this numbered migration
-- for the index.
CREATE UNIQUE INDEX IF NOT EXISTS `memory_external_id` ON `memory` (`external_id`);
