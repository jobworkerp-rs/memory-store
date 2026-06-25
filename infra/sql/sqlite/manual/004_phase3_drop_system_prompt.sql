-- Phase 3 of system-prompt-as-memory migration (SQLite):
--   Physically drop `memory.system_id` and the `system_prompt` table. Run
--   this AFTER the Rust migration binary `migrate_sp_to_memory` has copied
--   every `system_prompt` row into a ROLE_SYSTEM `memory` row and rewritten
--   references through `_sp_migration_map`. The binary is idempotent so
--   running it twice is safe, but running this script BEFORE the binary is
--   catastrophic — the source data is destroyed here.
--
--   SQLite historically does not support `ALTER TABLE ... DROP COLUMN`, so
--   we rebuild the `memory` table from scratch. The recipe mirrors
--   `003_phase2_rename_thread_column.sql`:
--     1. PRAGMA foreign_keys = OFF;
--     2. BEGIN TRANSACTION;
--     3. CREATE TABLE memory_new (...);        -- new schema without system_id
--     4. INSERT INTO memory_new SELECT ... FROM memory;
--     5. DROP TABLE memory;
--     6. ALTER TABLE memory_new RENAME TO memory;
--     7. Recreate indexes under the new column set.
--     8. DROP the system_prompt table and its index.
--     9. COMMIT; PRAGMA foreign_keys = ON;
--
--   Rollback: abort the transaction (CTRL-C mid-run or any statement error
--   triggers an automatic ROLLBACK). If the script has already committed,
--   rely on the DB backup taken before the migration window.
--
-- `_sp_migration_map` is intentionally NOT dropped here. It is the only
-- surviving record of which memory rows were synthesised from a legacy
-- system_prompt row, so operators keep it around for at least one release
-- cycle before running `DROP TABLE _sp_migration_map;` manually.

PRAGMA foreign_keys = OFF;

BEGIN TRANSACTION;

-- 1. New memory table without the legacy `system_id` column.
CREATE TABLE IF NOT EXISTS `memory_new` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `parent_ids` JSON,
    `user_id` BIGINT NOT NULL,
    `content` TEXT NOT NULL,
    `content_type` INT NOT NULL,
    `params` JSON,
    `metadata` JSON,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    `thread_id` BIGINT,
    `role` INT NOT NULL DEFAULT 0
);

-- 2. Copy all rows, dropping system_id.
INSERT INTO `memory_new` (
    `id`, `parent_ids`, `user_id`, `content`, `content_type`,
    `params`, `metadata`, `created_at`, `updated_at`, `thread_id`, `role`
)
SELECT
    `id`, `parent_ids`, `user_id`, `content`, `content_type`,
    `params`, `metadata`, `created_at`, `updated_at`, `thread_id`, `role`
FROM `memory`;

-- 3. Swap tables.
DROP TABLE `memory`;
ALTER TABLE `memory_new` RENAME TO `memory`;

-- 4. Recreate indexes (memory_system_id is deliberately omitted).
CREATE INDEX IF NOT EXISTS `memory_user_id` ON `memory` (`user_id`);
CREATE INDEX IF NOT EXISTS `memory_thread_id` ON `memory` (`thread_id`, `created_at`);

-- 5. Drop the legacy system_prompt table and its index.
DROP INDEX IF EXISTS `system_prompt_user_id`;
DROP TABLE IF EXISTS `system_prompt`;

COMMIT;

PRAGMA foreign_keys = ON;
