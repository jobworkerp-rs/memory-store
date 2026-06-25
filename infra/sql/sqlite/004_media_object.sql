-- Image memory feature: media_object table + memory.media_object_id.
--
-- New deployments: 001_schema.sql already includes the media_object table,
-- the memory.media_object_id column, and all three indexes, so only the
-- idempotent CREATE TABLE / CREATE INDEX statements below run (no-ops).
--
-- Existing deployments (pre-media_object): SQLite cannot ALTER TABLE ADD
-- COLUMN idempotently, so run the manual migration first:
--   infra/sql/sqlite/manual/010_add_media_object.sql
-- which performs `ALTER TABLE memory ADD COLUMN media_object_id BIGINT`,
-- then apply this numbered migration for the table and indexes.
--
-- gc_state is a 6-state machine (0=active / 1=orphan / 2=deleted-failed /
-- 3=unresolvable / 4=promoting / 5=deleting). See
-- ai-docs/image-memory-design.md 1/3 §3.1.1 for the state transitions.
CREATE TABLE IF NOT EXISTS `media_object` (
    `id`              BIGINT      NOT NULL PRIMARY KEY,
    `kind`            INT         NOT NULL,
    `media_type`      VARCHAR(64) NOT NULL,
    `byte_size`       BIGINT,
    `sha256`          CHAR(64),
    `width`           INT,
    `height`          INT,
    `duration_ms`     BIGINT,
    `storage_backend` VARCHAR(16) NOT NULL,
    `storage_uri`     TEXT,
    `alt`             TEXT,
    `ref_count`       BIGINT      NOT NULL DEFAULT 0,
    `gc_state`        INT         NOT NULL DEFAULT 0,
    `metadata`        JSON,
    `created_at`      BIGINT      NOT NULL,
    `updated_at`      BIGINT      NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS `media_object_sha256`   ON `media_object` (`sha256`);
CREATE INDEX IF NOT EXISTS `media_object_kind`            ON `media_object` (`kind`);
CREATE INDEX IF NOT EXISTS `media_object_gc_state`        ON `media_object` (`gc_state`);
CREATE INDEX IF NOT EXISTS `memory_media_object_id`       ON `memory` (`media_object_id`);
