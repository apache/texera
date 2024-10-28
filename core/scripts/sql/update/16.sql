USE texera_db;
CREATE TABLE IF NOT EXISTS workflow_user_likes
(
    `uid` INT UNSIGNED NOT NULL,
    `wid` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`uid`, `wid`),
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE
    ) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS workflow_user_clones
(
    `uid` INT UNSIGNED NOT NULL,
    `wid` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`uid`, `wid`),
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE
    ) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS workflow_user_activity (
    `uid` INT UNSIGNED NOT NULL DEFAULT 0,
    `wid` INT UNSIGNED NOT NULL,
    `ip` VARCHAR(15) DEFAULT NULL,
    `activate` VARCHAR(10) NOT NULL,
    `activity_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE = INNODB;
