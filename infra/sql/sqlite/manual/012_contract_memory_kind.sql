-- Contract migration for MemoryKind.
--
-- Run only after `migrate-memory-kind plan`, `apply`, and `verify` have
-- completed with no unresolved rows. This script deliberately does not
-- normalize NULL: the NOT NULL constraint aborts the transaction if invalid
-- rows remain. Application input validation owns the accepted enum value set.
--
-- SQLite cannot add a NOT NULL constraint in place, so the two
-- tables are rebuilt in one transaction. If copying a row violates a
-- constraint, issue ROLLBACK; the original schema and data remain intact.
BEGIN TRANSACTION;

CREATE TABLE `thread_contract` (
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
INSERT INTO `thread_contract`
SELECT `id`, `default_system_memory_id`, `user_id`, `description`, `channel`,
       `embedding`, `embedding_dim`, `created_at`, `updated_at`, `metadata`, `memory_kind`
FROM `thread`;
DROP TABLE `thread`;
ALTER TABLE `thread_contract` RENAME TO `thread`;
CREATE INDEX `thread_default_system_memory_id` ON `thread` (`default_system_memory_id`);
CREATE INDEX `thread_user_id` ON `thread` (`user_id`);
CREATE INDEX `thread_updated_at` ON `thread` (`updated_at`);
CREATE INDEX `thread_user_memory_kind_updated_at` ON `thread` (`user_id`, `memory_kind`, `updated_at`);

CREATE TABLE `memory_contract` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `parent_ids` JSON,
    `user_id` BIGINT NOT NULL,
    `content` TEXT NOT NULL,
    `content_type` INT NOT NULL,
    `params` JSON,
    `metadata` JSON,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    `role` INT NOT NULL DEFAULT 0,
    `external_id` VARCHAR(512),
    `media_object_id` BIGINT,
    `memory_kind` INT NOT NULL
);
INSERT INTO `memory_contract`
SELECT `id`, `parent_ids`, `user_id`, `content`, `content_type`, `params`, `metadata`,
       `created_at`, `updated_at`, `role`, `external_id`, `media_object_id`, `memory_kind`
FROM `memory`;
DROP TABLE `memory`;
ALTER TABLE `memory_contract` RENAME TO `memory`;
CREATE INDEX `memory_user_id` ON `memory` (`user_id`);
CREATE INDEX `memory_user_memory_kind_updated_at` ON `memory` (`user_id`, `memory_kind`, `updated_at`);
CREATE UNIQUE INDEX `memory_external_id` ON `memory` (`external_id`);
CREATE INDEX `memory_media_object_id` ON `memory` (`media_object_id`);

COMMIT;
