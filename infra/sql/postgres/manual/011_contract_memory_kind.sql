-- Contract migration for MemoryKind.
--
-- Run only after `migrate-memory-kind plan`, `apply`, and `verify` have
-- completed with no unresolved rows. The declarative constraint rejects NULL;
-- application input validation owns the accepted enum value set. This
-- migration never changes data to make it fit the contract.
BEGIN;

ALTER TABLE thread ALTER COLUMN memory_kind DROP DEFAULT;
ALTER TABLE memory ALTER COLUMN memory_kind DROP DEFAULT;
ALTER TABLE thread DROP CONSTRAINT IF EXISTS thread_memory_kind_range;
ALTER TABLE memory DROP CONSTRAINT IF EXISTS memory_memory_kind_range;
ALTER TABLE thread ALTER COLUMN memory_kind SET NOT NULL;
ALTER TABLE memory ALTER COLUMN memory_kind SET NOT NULL;

COMMIT;
