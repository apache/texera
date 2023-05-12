USE `texera_db`;
CREATE TABLE IF NOT EXISTS file_of_workflow
(
    `fid`            INT UNSIGNED                     NOT NULL,
    `wid`            INT UNSIGNED                     NOT NULL,
    PRIMARY KEY (`fid`, `wid`),
    FOREIGN KEY (`fid`) REFERENCES `file` (`fid`)      ON DELETE CASCADE,
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`)  ON DELETE CASCADE
) ENGINE = INNODB;