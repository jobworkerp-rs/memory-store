-- Phase 4 of system-prompt-as-memory migration (PostgreSQL):
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
--   already duplicated in the junction table.
--
--   Rollback: abort the transaction mid-run and PostgreSQL will ROLLBACK the
--   schema change automatically. After a successful commit, restore from the
--   DB backup taken before the migration window.

-- ============================================================
-- The entire migration (backfill + column drop) runs inside a
-- single transaction so a failure at any point rolls back
-- everything atomically.
-- ============================================================

BEGIN;

-- ============================================================
-- STEP 0: Backfill thread_memory from legacy data.
--   Runs inside the transaction so partial backfill cannot be
--   committed separately from the column drop. ON CONFLICT DO
--   NOTHING makes this idempotent.
-- ============================================================

-- 0a. Memories that have a legacy thread_id but no junction row yet.
--     Each thread's backfilled rows get sequential positions starting
--     from MAX(existing position) + 1, ordered by created_at then id.
INSERT INTO thread_memory (thread_id, memory_id, position, created_at)
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
) ranked
ON CONFLICT DO NOTHING;

-- 0b. Threads whose default_system_memory_id is set but not yet anchored
--     in the junction.
INSERT INTO thread_memory (thread_id, memory_id, position, created_at)
SELECT
    t.id,
    t.default_system_memory_id,
    COALESCE(
        (SELECT MIN(tm2.position) - 1 FROM thread_memory tm2 WHERE tm2.thread_id = t.id),
        0
    ),
    COALESCE(
        (SELECT mem.created_at FROM memory mem WHERE mem.id = t.default_system_memory_id),
        EXTRACT(EPOCH FROM NOW())::BIGINT * 1000
    )
FROM thread t
WHERE t.default_system_memory_id IS NOT NULL
  AND t.default_system_memory_id != 0
  AND NOT EXISTS (
    SELECT 1 FROM thread_memory tm
    WHERE tm.thread_id = t.id AND tm.memory_id = t.default_system_memory_id
  )
ON CONFLICT DO NOTHING;

-- ============================================================
-- STEP 1: Drop the legacy column (same transaction).
-- ============================================================

DROP INDEX IF EXISTS memory_thread_id;
ALTER TABLE memory DROP COLUMN thread_id;

COMMIT;
