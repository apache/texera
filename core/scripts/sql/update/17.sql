USE texera_db;
CREATE TABLE IF NOT EXISTS workflow_view_count
(
    `wid` INT UNSIGNED NOT NULL,
    `view_count` INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (`wid`),
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE
    ) ENGINE = INNODB;