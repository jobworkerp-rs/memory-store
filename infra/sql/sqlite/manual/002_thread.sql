CREATE TABLE IF NOT EXISTS `thread` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `system_prompt_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `description` TEXT,
    `channel` TEXT,
    `embedding` BLOB,
    `embedding_dim` INT,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS `thread_system_prompt_id` ON `thread` (`system_prompt_id`);
CREATE INDEX IF NOT EXISTS `thread_user_id` ON `thread` (`user_id`);
CREATE INDEX IF NOT EXISTS `thread_updated_at` ON `thread` (`updated_at`);

ALTER TABLE `memory` ADD COLUMN `thread_id` BIGINT;
ALTER TABLE `memory` ADD COLUMN `role` INT NOT NULL DEFAULT 0;
CREATE INDEX IF NOT EXISTS `memory_thread_id` ON `memory` (`thread_id`, `created_at`);

-- channel カラムの削除（SQLiteではALTER TABLE DROP COLUMNは3.35.0+で対応）
-- ALTER TABLE `memory` DROP COLUMN `channel`;
-- 対応していないSQLiteバージョンでは新テーブル作成＋データ移行が必要
