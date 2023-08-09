USE `texera_db`;
CREATE TABLE IF NOT EXISTS public_project
(
    `pid`             INT UNSIGNED NOT NULL,
    PRIMARY KEY (`pid`),
    FOREIGN KEY (`pid`) REFERENCES `project` (`pid`) ON DELETE CASCADE
    ) ENGINE = INNODB;