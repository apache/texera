USE texera_db;

DROP TABLE IF EXISTS workflow_user_activity;

CREATE TABLE IF NOT EXISTS user_activity (
    `uid` INT UNSIGNED NOT NULL DEFAULT 0,
    `id` INT UNSIGNED NOT NULL,
    `type` VARCHAR(15) NOT NULL,
    `ip` VARCHAR(15) DEFAULT NULL,
    `activate` VARCHAR(10) NOT NULL,
    `activity_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    ) ENGINE = INNODB;
