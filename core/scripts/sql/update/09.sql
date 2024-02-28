USE `texera_db`;

CREATE TABLE IF NOT EXISTS environment
(
    `eid`              INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `owner_uid`        INT UNSIGNED NOT NULL,
    `name`			   VARCHAR(128) NOT NULL DEFAULT 'Untitled Environment',
    `description`      VARCHAR(1000),
    `creation_time`    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`eid`),
    FOREIGN KEY (`owner_uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS environment_of_workflow
(
    `eid`              INT UNSIGNED NOT NULL,
    `wid`              INT UNSIGNED NOT NULL,
    PRIMARY KEY (`eid`, `wid`),
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE,
    FOREIGN KEY (`eid`) REFERENCES `environment` (`eid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS dataset_of_environment
(
    `did`                   INT UNSIGNED NOT NULL,
    `eid`                   INT UNSIGNED NOT NULL,
    `dvid`                  INT UNSIGNED NOT NULL,
    PRIMARY KEY (`did`, `eid`),
    FOREIGN KEY (`eid`) REFERENCES `environment` (`eid`) ON DELETE CASCADE,
    FOREIGN KEY (`dvid`) REFERENCES `dataset_version` (`dvid`) ON DELETE CASCADE
) ENGINE = INNODB;

-- Add the `environment_eid` column to the `workflow_executions` table
ALTER TABLE workflow_executions
ADD COLUMN `environment_eid` INT UNSIGNED;

-- Add the foreign key constraint for `environment_eid`
ALTER TABLE workflow_executions
ADD CONSTRAINT fk_environment_eid
FOREIGN KEY (`environment_eid`) REFERENCES environment(`eid`) ON DELETE SET NULL;