-- Thread label junction table for thread classification.
-- Adds many-to-many relationship between threads and labels.

CREATE TABLE IF NOT EXISTS `thread_label` (
    `thread_id`  BIGINT NOT NULL,
    `label`      VARCHAR(512) NOT NULL,
    `created_at` BIGINT NOT NULL,
    PRIMARY KEY (`thread_id`, `label`)
);

CREATE INDEX IF NOT EXISTS `thread_label_thread_id`
    ON `thread_label` (`thread_id`);
CREATE INDEX IF NOT EXISTS `thread_label_label`
    ON `thread_label` (`label`);
