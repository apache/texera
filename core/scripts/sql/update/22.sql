USE texera_db;

DROP TABLE IF EXISTS dataset_user_likes;
DROP TABLE IF EXISTS dataset_view_count;

CREATE TABLE IF NOT EXISTS dataset_user_likes
(
    `uid` INT UNSIGNED NOT NULL,
    `did` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`uid`, `did`),
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`did`) REFERENCES `dataset` (`did`) ON DELETE CASCADE
    ) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS `dataset_view_count` (
    `did` INT UNSIGNED NOT NULL,
    `view_count` INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (`did`),
    FOREIGN KEY (`did`) REFERENCES `dataset` (`did`) ON DELETE CASCADE
    ) ENGINE = INNODB;