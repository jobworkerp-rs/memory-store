-- Phase 2 of system-prompt-as-memory migration:
--   `thread.system_prompt_id BIGINT NOT NULL` is renamed to
--   `default_system_memory_id BIGINT NULL` and the matching index is renamed.
--   The column points at a ROLE_SYSTEM Memory id which AddMemory auto-injects
--   into `parent_ids` when the client does not supply an explicit ROLE_SYSTEM
--   parent.
--
--   SQLite does not support `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL`,
--   so the rename + nullable change requires a table rebuild. The general
--   recipe is:
--     1. PRAGMA foreign_keys=OFF;
--     2. BEGIN TRANSACTION;
--     3. CREATE TABLE thread_new (...);    -- new schema
--     4. INSERT INTO thread_new SELECT ... FROM thread;
--     5. DROP TABLE thread;
--     6. ALTER TABLE thread_new RENAME TO thread;
--     7. CREATE INDEX ... (recreate dropped indexes)
--     8. COMMIT; PRAGMA foreign_keys=ON;
--
--   This file is intentionally written as a single transaction. If any
--   statement fails, SQLite rolls the whole transaction back automatically,
--   so the `thread_new` scratch table will not be left behind on failure.
--   Re-running the script after a successful commit is a no-op because the
--   original `thread` table no longer exists under its old shape.

PRAGMA foreign_keys = OFF;

BEGIN TRANSACTION;

-- 1. New thread table with the renamed nullable column.
CREATE TABLE IF NOT EXISTS `thread_new` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `default_system_memory_id` BIGINT,
    `user_id` BIGINT NOT NULL,
    `description` TEXT,
    `channel` TEXT,
    `embedding` BLOB,
    `embedding_dim` INT,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL
);

-- 2. Copy existing rows. Treat the legacy sentinel value 0 (which the old
--    gRPC validator rejected on input but could survive direct DB writes) as
--    NULL so the new column carries clean Option semantics.
INSERT INTO `thread_new` (
    `id`, `default_system_memory_id`, `user_id`, `description`, `channel`,
    `embedding`, `embedding_dim`, `created_at`, `updated_at`
)
SELECT
    `id`,
    NULLIF(`system_prompt_id`, 0),
    `user_id`,
    `description`,
    `channel`,
    `embedding`,
    `embedding_dim`,
    `created_at`,
    `updated_at`
FROM `thread`;

-- 3. Drop the old table (and its indexes implicitly).
DROP TABLE `thread`;

-- 4. Rename the replacement into place.
ALTER TABLE `thread_new` RENAME TO `thread`;

-- 5. Recreate indexes under the new column name.
CREATE INDEX IF NOT EXISTS `thread_default_system_memory_id`
    ON `thread` (`default_system_memory_id`);
CREATE INDEX IF NOT EXISTS `thread_user_id` ON `thread` (`user_id`);
CREATE INDEX IF NOT EXISTS `thread_updated_at` ON `thread` (`updated_at`);

COMMIT;

PRAGMA foreign_keys = ON;
