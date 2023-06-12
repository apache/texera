USE `texera_db`;
CREATE TABLE IF NOT EXISTS project_user_access
(
    `uid`             INT UNSIGNED NOT NULL,
    `pid`             INT UNSIGNED NOT NULL,
    `privilege`          ENUM('NONE', 'READ', 'WRITE') NOT NULL DEFAULT 'NONE',
    PRIMARY KEY (`uid`, `pid`),
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`pid`) REFERENCES `project` (`pid`) ON DELETE CASCADE
) ENGINE = INNODB;
