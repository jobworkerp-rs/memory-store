-- Phase 3 of system-prompt-as-memory migration (PostgreSQL):
--   Physically drop `memory.system_id` and the `system_prompt` table. Run
--   this AFTER the Rust migration binary `migrate_sp_to_memory` has copied
--   every `system_prompt` row into a ROLE_SYSTEM `memory` row and rewritten
--   references through `_sp_migration_map`. The binary is idempotent so
--   running it twice is safe, but running this script BEFORE the binary is
--   catastrophic — the source data is destroyed here.
--
--   Rollback: abort the transaction mid-run and PostgreSQL will ROLLBACK the
--   schema change automatically. After a successful commit, restore from the
--   DB backup taken before the migration window.
--
-- `_sp_migration_map` is intentionally NOT dropped here. It is the only
-- surviving record of which memory rows were synthesised from a legacy
-- system_prompt row, so operators keep it around for at least one release
-- cycle before running `DROP TABLE _sp_migration_map;` manually.

BEGIN;

DROP INDEX IF EXISTS memory_system_id;
ALTER TABLE memory DROP COLUMN system_id;

DROP INDEX IF EXISTS system_prompt_user_id;
DROP TABLE IF EXISTS system_prompt;

COMMIT;
