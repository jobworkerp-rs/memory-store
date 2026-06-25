-- Manual migration: adds external_id column to the memory table.
-- Run this BEFORE applying the numbered migration 002_add_external_id.sql
-- on existing databases that were created without external_id.
--
-- New databases (created with the current 001_schema.sql) already have this
-- column and do NOT need this script.
ALTER TABLE `memory` ADD COLUMN `external_id` VARCHAR(512);
