USE `texera_db`;

-- Create new tables for dataset management
CREATE TABLE IF NOT EXISTS dataset
(
    `did`             INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `owner_uid`       INT UNSIGNED NOT NULL,
    `name`            VARCHAR(128) NOT NULL,
    `is_public`       TINYINT NOT NULL DEFAULT 1,
    `storage_path`    VARCHAR(512) NOT NULL,
    `description`     VARCHAR(512) NOT NULL,
    `creation_time`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(`did`),
    FOREIGN KEY (`owner_uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS dataset_user_access
(
    `did`             INT UNSIGNED NOT NULL,
    `uid`             INT UNSIGNED NOT NULL,
    `privilege`    ENUM('NONE', 'READ', 'WRITE') NOT NULL DEFAULT 'NONE',
    PRIMARY KEY(`did`, `uid`),
    FOREIGN KEY (`did`) REFERENCES `dataset` (`did`) ON DELETE CASCADE,
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS dataset_version
(
    `dvid`            INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `did`             INT UNSIGNED NOT NULL,
    `creator_uid`     INT UNSIGNED NOT NULL,
    `name`            VARCHAR(128) NOT NULL,
    `version_hash`    VARCHAR(64) NOT NULL,
    `creation_time`   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(`dvid`),
    FOREIGN KEY (`did`) REFERENCES `dataset` (`did`) ON DELETE CASCADE
) ENGINE = INNODB;

-- Create fulltext indexes for the new tables
CREATE FULLTEXT INDEX `idx_dataset_name_description` ON `texera_db`.`dataset` (name, description);
CREATE FULLTEXT INDEX `idx_dataset_version_name` ON `texera_db`.`dataset_version` (name);