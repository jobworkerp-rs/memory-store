-- Manual migration for environments that already applied an earlier
-- version of `003_reflection_schema.sql` (i.e. before the
-- `origin_thread_id` column on `thread_reflection_index` and the
-- `turn_index` column on `reflection_fact` were introduced).
--
-- Run this file first, then re-apply `003_reflection_schema.sql` for
-- the new index definitions (`tri_origin_thread_id` /
-- `tri_origin_thread_created`) — both are guarded by `IF NOT EXISTS`
-- so re-running is safe.
--
-- New deployments: `003_reflection_schema.sql` already includes both
-- columns, so this manual migration can be skipped entirely.

-- =====================================================================
-- thread_reflection_index.origin_thread_id
-- =====================================================================
-- Existing rows in pre-migration databases stored the aggregate thread
-- id in `thread_id` only; there is no canonical record of the original
-- trajectory thread for those rows. We default the new column to 0 so
-- the NOT NULL constraint can be satisfied; downstream queries treat
-- `origin_thread_id = 0` as "unknown / pre-migration" and operators
-- can backfill from external state if needed. New finalize traffic
-- always populates origin_thread_id explicitly.
ALTER TABLE `thread_reflection_index`
    ADD COLUMN `origin_thread_id` BIGINT NOT NULL DEFAULT 0;

-- The `tri_origin_thread_id` and `tri_origin_thread_created` indexes
-- are created idempotently by `003_reflection_schema.sql` after this
-- migration runs.

-- =====================================================================
-- reflection_fact.turn_index
-- =====================================================================
-- Pre-migration rows have no recorded turn_index; default to 0 so
-- existing rows satisfy NOT NULL. proto `ReflectionFact.turn_index`
-- consumers should treat 0 as "unknown for legacy rows".
ALTER TABLE `reflection_fact`
    ADD COLUMN `turn_index` INT NOT NULL DEFAULT 0;
