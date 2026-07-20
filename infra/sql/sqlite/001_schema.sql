PRAGMA encoding = 'UTF-8';

-- Phase 3 of system-prompt-as-memory migration:
--   `system_prompt` is gone. System prompts are now plain `memory` rows with
--   `role = ROLE_SYSTEM (3)`, referenced from conversation messages via
--   `parent_ids` and from threads via `default_system_memory_id`.
--
-- Phase 4 of the same migration:
--   The legacy `memory.thread_id` column has been removed. Thread membership
--   is now resolved exclusively through the `thread_memory` junction table,
--   which also records the conversation `position` used by the parent_ids
--   traversal at execution time.
--
-- IMPORTANT: This revision assumes Phase 3 has already been applied on any
-- existing deployment. The Phase 3 data-migration binary
-- `migrate_sp_to_memory` and its supporting module
-- `infra::infra::migration::system_prompt_to_memory` were removed as part
-- of the Phase 4 cleanup because their INSERT statements still referenced
-- `memory.thread_id`, which this revision drops; leaving the binary in
-- place would be misleading (it would fail against a Phase-4 schema).
-- If you are upgrading a deployment that has NOT completed Phase 3, pin it
-- to a pre-Phase-4 tag first, run `migrate_sp_to_memory` + the Phase 3
-- manual SQL, and only then roll forward to this revision.
--
--   Phase-4 upgrade order for an already-Phase-3 deployment:
--     1. Apply `sql/sqlite/manual/005_phase4_drop_thread_id.sql` to
--        physically drop `memory.thread_id` and the `memory_thread_id`
--        index.
--     2. Deploy the new binary — startup will see the junction-only
--        schema below and run normally.
--
--   The Phase-1 + Phase-2 + Phase-3 manual SQL files
--   (`manual/004_phase3_drop_system_prompt.sql`, etc.) are kept under
--   `manual/` purely for historical reference.
CREATE TABLE IF NOT EXISTS `thread` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `default_system_memory_id` BIGINT,
    `user_id` BIGINT NOT NULL,
    `description` TEXT,
    `channel` TEXT,
    `embedding` BLOB,
    `embedding_dim` INT,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    `metadata` JSON,
    `memory_kind` INT NOT NULL
);

CREATE INDEX IF NOT EXISTS `thread_default_system_memory_id` ON `thread` (`default_system_memory_id`);
CREATE INDEX IF NOT EXISTS `thread_user_id` ON `thread` (`user_id`);
CREATE INDEX IF NOT EXISTS `thread_updated_at` ON `thread` (`updated_at`);
CREATE INDEX IF NOT EXISTS `thread_user_memory_kind_updated_at` ON `thread` (`user_id`, `memory_kind`, `updated_at`);

CREATE TABLE IF NOT EXISTS `memory` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `parent_ids` JSON, -- JSON type for parent_ids list
    `user_id` BIGINT NOT NULL,
    `content` TEXT NOT NULL,
    `content_type` INT NOT NULL, -- 0: text, 1: image, 2: tool, etc
    `params` JSON, -- llama inference params
    `metadata` JSON, -- workflow metadata, etc (experimental)
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    `role` INT NOT NULL DEFAULT 0,
    `external_id` VARCHAR(512),
    -- Optional reference to a media_object (image memory feature). NULL = no
    -- attached media. content_type-independent (any content_type may carry
    -- media). FK omitted per project convention; ref_count integrity is
    -- enforced in the app layer.
    `media_object_id` BIGINT,
    `memory_kind` INT NOT NULL
);
CREATE INDEX IF NOT EXISTS `memory_user_id` ON `memory` (`user_id`);
CREATE INDEX IF NOT EXISTS `memory_user_memory_kind_updated_at` ON `memory` (`user_id`, `memory_kind`, `updated_at`);
CREATE UNIQUE INDEX IF NOT EXISTS `memory_external_id` ON `memory` (`external_id`);
CREATE INDEX IF NOT EXISTS `memory_media_object_id` ON `memory` (`media_object_id`);

-- Media object: external-storage-backed image (and future audio/video) body.
-- The DB holds metadata only; bytes live in S3/file/url (inline backend is a
-- test-only exception that base64-encodes into `metadata`). Shared across
-- memories via sha256 dedup. FK omitted per project convention.
CREATE TABLE IF NOT EXISTS `media_object` (
    `id`              BIGINT      NOT NULL PRIMARY KEY,   -- snowflake (IdGeneratorWrapper)
    `kind`            INT         NOT NULL,                -- ContentType (IMAGE=2; AUDIO/VIDEO reserved, embedding unimplemented)
    `media_type`      VARCHAR(64) NOT NULL,                -- "image/png" etc.
    `byte_size`       BIGINT,                              -- NULL allowed (url backend size unknown)
    `sha256`          CHAR(64),                            -- dedup/integrity key. NULL allowed (url backend has no sha256)
    `width`           INT,
    `height`          INT,
    `duration_ms`     BIGINT,                              -- AUDIO/VIDEO (future)
    `storage_backend` VARCHAR(16) NOT NULL,                -- s3|file|url|inline|unresolvable
    `storage_uri`     TEXT,                                -- NULL = reservation/promoting in progress or unresolvable
    `alt`             TEXT,                                -- accessibility / caption
    `ref_count`       BIGINT      NOT NULL DEFAULT 0,      -- number of referencing memories
    -- 0=active / 1=orphan / 2=deleted-failed / 3=unresolvable / 4=promoting / 5=deleting
    -- (6-state machine; see ai-docs/image-memory-design.md 1/3 §3.1.1)
    `gc_state`        INT         NOT NULL DEFAULT 0,
    `metadata`        JSON,                                -- EXIF / inline base64 (inline backend only)
    `created_at`      BIGINT      NOT NULL,
    `updated_at`      BIGINT      NOT NULL
);
-- UNIQUE + NULL-allowed: SQLite/PostgreSQL exclude NULL from UNIQUE, so
-- multiple url rows (sha256 NULL) coexist. Same pattern as memory_external_id.
-- Empty string ('') would collide on UNIQUE, so callers MUST use NULL.
CREATE UNIQUE INDEX IF NOT EXISTS `media_object_sha256`   ON `media_object` (`sha256`);
CREATE INDEX IF NOT EXISTS `media_object_kind`            ON `media_object` (`kind`);
CREATE INDEX IF NOT EXISTS `media_object_gc_state`        ON `media_object` (`gc_state`);

CREATE TABLE IF NOT EXISTS `memory_rating` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `memory_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `rating` REAL NOT NULL,
    `metadata` JSON,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS `memory_rating_memory_user` ON `memory_rating` (`memory_id`, `user_id`);
CREATE INDEX IF NOT EXISTS `memory_rating_memory_id` ON `memory_rating` (`memory_id`);
CREATE INDEX IF NOT EXISTS `memory_rating_user_id` ON `memory_rating` (`user_id`);

-- Thread-Memory many-to-many junction table
-- NOTE: FK constraints are intentionally omitted across all tables in this project.
-- Cascade deletes are managed at the application layer (ThreadApp::delete_thread,
-- MemoryApp::delete_memory) to keep DDL portable and avoid SQLite FK enforcement
-- quirks. See app/src/app/thread.rs and app/src/app/memory.rs for delete logic.
CREATE TABLE IF NOT EXISTS `thread_memory` (
    `thread_id`  BIGINT NOT NULL,
    `memory_id`  BIGINT NOT NULL,
    `position`   INT NOT NULL,
    `created_at` BIGINT NOT NULL,
    PRIMARY KEY (`thread_id`, `memory_id`)
);

CREATE UNIQUE INDEX IF NOT EXISTS `thread_memory_thread_position`
    ON `thread_memory` (`thread_id`, `position`);
CREATE INDEX IF NOT EXISTS `thread_memory_memory_id`
    ON `thread_memory` (`memory_id`);

-- Thread labels: many-to-many junction table for thread classification.
-- Cascade deletes managed at the application layer (ThreadApp::delete_thread).
CREATE TABLE IF NOT EXISTS `thread_label` (
    `thread_id`  BIGINT NOT NULL,
    `label`      VARCHAR(512) NOT NULL,
    `created_at` BIGINT NOT NULL,
    PRIMARY KEY (`thread_id`, `label`)
);

CREATE INDEX IF NOT EXISTS `thread_label_thread_id`
    ON `thread_label` (`thread_id`);
CREATE INDEX IF NOT EXISTS `thread_label_label`
    ON `thread_label` (`label`);
