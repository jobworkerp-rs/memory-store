-- Phase 4 of system-prompt-as-memory migration (SQLite):
--   Physically drop `memory.thread_id` and the `memory_thread_id` index.
--   Thread membership is now resolved exclusively through the `thread_memory`
--   junction table (introduced in Phase 2 and made the sole source of truth
--   for conversation ordering / parent_ids traversal).
--
--   Prerequisites: Phase 1-3 must already be applied — specifically, the
--   Rust migration binary `migrate_sp_to_memory` must have run and
--   `004_phase3_drop_system_prompt.sql` must have been applied so that the
--   `memory` table does not still carry `system_id`.
--
--   Data safety: the `memory.thread_id` column has been read-only (not
--   consulted by any query) since the Phase 2-3 patches moved reads to
--   `thread_memory`. Dropping it does not lose any information that is not
--   already duplicated in the junction table. ThreadApp::add_memory has been
--   registering memories into the junction since Phase 2, so any row created
--   via the public API is already represented there.
--
--   SQLite historically does not support `ALTER TABLE ... DROP COLUMN`, so
--   we rebuild the `memory` table. The recipe mirrors
--   `004_phase3_drop_system_prompt.sql`:
--     1. PRAGMA foreign_keys = OFF;
--     2. BEGIN TRANSACTION;
--     3. CREATE TABLE memory_new (...);        -- new schema without thread_id
--     4. INSERT INTO memory_new SELECT ... FROM memory;
--     5. DROP TABLE memory;
--     6. ALTER TABLE memory_new RENAME TO memory;
--     7. Recreate indexes under the new column set (memory_thread_id is
--        deliberately dropped — junction table carries equivalent lookups).
--     8. COMMIT; PRAGMA foreign_keys = ON;
--
--   Rollback: abort the transaction (CTRL-C mid-run or any statement error
--   triggers an automatic ROLLBACK). If the script has already committed,
--   rely on the DB backup taken before the migration window.

-- ============================================================
-- The entire migration (backfill + column drop) runs inside a
-- single transaction so a failure at any point rolls back
-- everything atomically. PRAGMA foreign_keys must be set outside
-- the transaction (SQLite requirement).
-- ============================================================

PRAGMA foreign_keys = OFF;

BEGIN TRANSACTION;

-- ============================================================
-- STEP 0: Backfill thread_memory from legacy data.
--   Runs inside the transaction so partial backfill cannot be
--   committed separately from the column drop. INSERT OR IGNORE
--   is idempotent — rows already registered via the public API
--   are skipped.
-- ============================================================

-- 0a. Memories that have a legacy thread_id but no junction row yet.
--     Each thread's backfilled rows get sequential positions starting
--     from MAX(existing position) + 1, ordered by created_at then id.
INSERT OR IGNORE INTO thread_memory (thread_id, memory_id, position, created_at)
SELECT
    ranked.thread_id,
    ranked.memory_id,
    ranked.base_position + ranked.rn,
    ranked.created_at
FROM (
    SELECT
        m.thread_id,
        m.id AS memory_id,
        m.created_at,
        COALESCE(
            (SELECT MAX(tm2.position) FROM thread_memory tm2 WHERE tm2.thread_id = m.thread_id),
            -1
        ) AS base_position,
        ROW_NUMBER() OVER (
            PARTITION BY m.thread_id ORDER BY m.created_at, m.id
        ) AS rn
    FROM memory m
    WHERE m.thread_id IS NOT NULL
      AND m.thread_id != 0
      AND NOT EXISTS (
        SELECT 1 FROM thread_memory tm
        WHERE tm.thread_id = m.thread_id AND tm.memory_id = m.id
      )
) ranked;

-- 0b. Threads whose default_system_memory_id is set but not yet anchored
--     in the junction. This prevents delete_thread from orphan-deleting a
--     shared ROLE_SYSTEM memory that other threads still reference.
INSERT OR IGNORE INTO thread_memory (thread_id, memory_id, position, created_at)
SELECT
    t.id,
    t.default_system_memory_id,
    COALESCE(
        (SELECT MIN(tm2.position) - 1 FROM thread_memory tm2 WHERE tm2.thread_id = t.id),
        0
    ),
    COALESCE(
        (SELECT mem.created_at FROM memory mem WHERE mem.id = t.default_system_memory_id),
        CAST(strftime('%s', 'now') * 1000 AS INTEGER)
    )
FROM thread t
WHERE t.default_system_memory_id IS NOT NULL
  AND t.default_system_memory_id != 0
  AND NOT EXISTS (
    SELECT 1 FROM thread_memory tm
    WHERE tm.thread_id = t.id AND tm.memory_id = t.default_system_memory_id
  );

-- ============================================================
-- STEP 1: Drop the legacy column (same transaction).
-- ============================================================

-- 1. New memory table without the legacy `thread_id` column.
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
    `role` INT NOT NULL DEFAULT 0
);

-- 2. Copy all rows, dropping thread_id.
INSERT INTO `memory_new` (
    `id`, `parent_ids`, `user_id`, `content`, `content_type`,
    `params`, `metadata`, `created_at`, `updated_at`, `role`
)
SELECT
    `id`, `parent_ids`, `user_id`, `content`, `content_type`,
    `params`, `metadata`, `created_at`, `updated_at`, `role`
FROM `memory`;

-- 3. Swap tables.
DROP TABLE `memory`;
ALTER TABLE `memory_new` RENAME TO `memory`;

-- 4. Recreate indexes (memory_thread_id is deliberately omitted — the
--    thread_memory junction table already provides efficient thread-based
--    lookups via thread_memory_thread_position / thread_memory_memory_id).
CREATE INDEX IF NOT EXISTS `memory_user_id` ON `memory` (`user_id`);

COMMIT;

PRAGMA foreign_keys = ON;
