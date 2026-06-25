-- Phase 2 of system-prompt-as-memory migration:
--   `thread.system_prompt_id BIGINT NOT NULL` is renamed to
--   `default_system_memory_id BIGINT NULL` and the matching index is renamed.
--   The column points at a ROLE_SYSTEM Memory id which AddMemory auto-injects
--   into `parent_ids` when the client does not supply an explicit ROLE_SYSTEM
--   parent.
--
--   PostgreSQL supports `RENAME COLUMN`, `DROP NOT NULL`, and `ALTER INDEX
--   RENAME` directly, so the migration is a single transaction with no table
--   rebuild.

BEGIN;

-- 1. Drop the legacy NOT NULL constraint so 0/NULL is a valid "no default" state.
ALTER TABLE thread
    ALTER COLUMN system_prompt_id DROP NOT NULL;

-- 2. Treat the legacy sentinel 0 (which the old gRPC validator rejected on
--    input but could survive direct DB writes) as NULL.
UPDATE thread
SET system_prompt_id = NULL
WHERE system_prompt_id = 0;

-- 3. Rename the column.
ALTER TABLE thread
    RENAME COLUMN system_prompt_id TO default_system_memory_id;

-- 4. Rename the supporting index to match.
ALTER INDEX IF EXISTS thread_system_prompt_id
    RENAME TO thread_default_system_memory_id;

COMMIT;
