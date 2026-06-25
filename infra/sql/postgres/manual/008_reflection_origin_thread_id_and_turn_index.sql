-- PostgreSQL mirror of
-- `infra/sql/sqlite/manual/009_reflection_origin_thread_id_and_turn_index.sql`.
-- Run before re-applying `003_reflection_schema.sql` on databases that
-- already carry the pre-migration shape of `thread_reflection_index`
-- and `reflection_fact`.
--
-- See the sqlite file for the rationale on the `DEFAULT 0` sentinel.

ALTER TABLE thread_reflection_index
    ADD COLUMN IF NOT EXISTS origin_thread_id BIGINT NOT NULL DEFAULT 0;

ALTER TABLE reflection_fact
    ADD COLUMN IF NOT EXISTS turn_index INT NOT NULL DEFAULT 0;
