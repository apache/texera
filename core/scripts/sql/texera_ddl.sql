CREATE SCHEMA IF NOT EXISTS `texera_db`;
USE `texera_db`;

DROP TABLE IF EXISTS `workflow_user_access`;
DROP TABLE IF EXISTS `workflow_of_user`;
DROP TABLE IF EXISTS `user_config`;
DROP TABLE IF EXISTS `user`;
DROP TABLE IF EXISTS `workflow`;
DROP TABLE IF EXISTS `workflow_version`;
DROP TABLE IF EXISTS `project`;
DROP TABLE IF EXISTS `workflow_of_project`;
DROP TABLE IF EXISTS `workflow_executions`;
DROP TABLE IF EXISTS `dataset`;
DROP TABLE IF EXISTS `dataset_user_access`;
DROP TABLE IF EXISTS `dataset_version`;
DROP TABLE IF EXISTS operator_executions;
DROP TABLE IF EXISTS operator_runtime_statistics;

SET PERSIST time_zone = '+00:00'; -- this line is mandatory
SET PERSIST sql_mode=(SELECT REPLACE(@@sql_mode,'ONLY_FULL_GROUP_BY',''));

CREATE TABLE IF NOT EXISTS user
(
    `uid`           INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `name`          VARCHAR(256)                NOT NULL,
    `email`         VARCHAR(256) UNIQUE,
    `password`      VARCHAR(256),
    `google_id`     VARCHAR(256) UNIQUE,
    `role`          ENUM('INACTIVE', 'RESTRICTED', 'REGULAR', 'ADMIN') NOT NULL DEFAULT 'INACTIVE',
    `google_avatar` VARCHAR(100) null,
    PRIMARY KEY (`uid`),
    CONSTRAINT CK_nulltest
        CHECK (`password` IS NOT NULL OR `google_id` IS NOT NULL)
) ENGINE = INNODB,
-- start auto increment userID from 1 because userID 0 means user not exists
  AUTO_INCREMENT = 1;

CREATE TABLE IF NOT EXISTS user_config
(
    `uid`   INT UNSIGNED NOT NULL,
    `key`   varchar(256) NOT NULL,
    `value` text         NOT NULL,
    PRIMARY KEY (`uid`, `key`),
    FOREIGN KEY (`uid`) REFERENCES user (`uid`) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS workflow
(
    `name`               VARCHAR(128)                NOT NULL,
	`description`        VARCHAR(500),
    `wid`                INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `content`            LONGTEXT                    NOT NULL,
    `creation_time`      TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `last_modified_time` TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`wid`)
) ENGINE = INNODB,
  AUTO_INCREMENT = 1;

CREATE TABLE IF NOT EXISTS workflow_of_user
(
    `uid` INT UNSIGNED NOT NULL,
    `wid` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`uid`, `wid`),
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS workflow_user_access
(
    `uid`             INT UNSIGNED NOT NULL,
    `wid`             INT UNSIGNED NOT NULL,
    `privilege`          ENUM('NONE', 'READ', 'WRITE') NOT NULL DEFAULT 'NONE',
    PRIMARY KEY (`uid`, `wid`),
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS workflow_version
(
    `vid`             INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `wid`             INT UNSIGNED NOT NULL,
    `content`            TEXT                        NOT NULL,
    `creation_time`      TIMESTAMP                   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`vid`),
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS project
(
    `pid`             INT UNSIGNED AUTO_INCREMENT      NOT NULL,
    `name`            VARCHAR(128)                     NOT NULL,
    `description`     VARCHAR(10000),
    `owner_id`        INT UNSIGNED                     NOT NULL,
    `creation_time`   TIMESTAMP                        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `color`           VARCHAR(6),
    UNIQUE(`owner_id`, `name`),
    PRIMARY KEY (`pid`),
    FOREIGN KEY (`owner_id`) REFERENCES user (`uid`)   ON DELETE CASCADE
) ENGINE = INNODB,
  AUTO_INCREMENT = 1;

CREATE TABLE IF NOT EXISTS workflow_of_project
(
     `wid`            INT UNSIGNED                     NOT NULL,
     `pid`            INT UNSIGNED                     NOT NULL,
     PRIMARY KEY (`wid`, `pid`),
     FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE,
     FOREIGN KEY (`pid`) REFERENCES `project` (`pid`)  ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS project_user_access
(
    `uid`             INT UNSIGNED NOT NULL,
    `pid`             INT UNSIGNED NOT NULL,
    `privilege`          ENUM('NONE', 'READ', 'WRITE') NOT NULL DEFAULT 'NONE',
    PRIMARY KEY (`uid`, `pid`),
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`pid`) REFERENCES `project` (`pid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS workflow_executions
(
    `eid`                    INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `vid`                    INT UNSIGNED NOT NULL,
    `uid`                    INT UNSIGNED NOT NULL,
    `status`                 TINYINT NOT NULL DEFAULT 1,
    `result`                 TEXT, /* pointer to volume */
    `starting_time`          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `last_update_time`       TIMESTAMP,
    `bookmarked`             BOOLEAN DEFAULT FALSE,
    `name`				     VARCHAR(128) NOT NULL DEFAULT 'Untitled Execution',
    `environment_version`    VARCHAR(128) NOT NULL,
    `log_location`           TEXT, /* uri to log storage */
    PRIMARY KEY (`eid`),
    FOREIGN KEY (`vid`) REFERENCES `workflow_version` (`vid`) ON DELETE CASCADE,
    FOREIGN KEY (`uid`) REFERENCES `user` (`uid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS public_project
(
    `pid`             INT UNSIGNED NOT NULL,
    `uid`             INT UNSIGNED,
    PRIMARY KEY (`pid`),
    FOREIGN KEY (`pid`) REFERENCES `project` (`pid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS dataset
(
    `did`             INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `owner_uid`       INT UNSIGNED NOT NULL,
    `name`            VARCHAR(128) NOT NULL,
    `is_public`       TINYINT NOT NULL DEFAULT 1,
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
    )  ENGINE = INNODB;


-- create fulltext search indexes

CREATE FULLTEXT INDEX `idx_workflow_name_description_content` ON `texera_db`.`workflow` (name, description, content);

CREATE FULLTEXT INDEX `idx_user_name` ON `texera_db`.`user` (name);

CREATE FULLTEXT INDEX `idx_user_project_name_description` ON `texera_db`.`project` (name, description);

CREATE FULLTEXT INDEX `idx_dataset_name_description` ON `texera_db`.`dataset` (name, description);

CREATE FULLTEXT INDEX `idx_dataset_version_name` ON `texera_db`.`dataset_version` (name);

ALTER TABLE workflow
ADD is_published BOOLEAN NOT NULL DEFAULT false;

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
    
CREATE TABLE IF NOT EXISTS workflow_view_count
(
    `wid` INT UNSIGNED NOT NULL,
    `view_count` INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (`wid`),
    FOREIGN KEY (`wid`) REFERENCES `workflow` (`wid`) ON DELETE CASCADE
    ) ENGINE = INNODB;

ALTER TABLE dataset
MODIFY COLUMN is_public BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE workflow
CHANGE COLUMN is_published is_public BOOLEAN NOT NULL DEFAULT false;

CREATE TABLE IF NOT EXISTS operator_executions (
    operator_execution_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY, 
    workflow_execution_id INT UNSIGNED NOT NULL, 
    operator_id VARCHAR(100) NOT NULL, 
    UNIQUE (workflow_execution_id, operator_id),
    FOREIGN KEY (workflow_execution_id) REFERENCES workflow_executions (eid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS operator_runtime_statistics (
    operator_execution_id BIGINT UNSIGNED NOT NULL, 
    time TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    input_tuple_cnt BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    output_tuple_cnt BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    status TINYINT NOT NULL DEFAULT 1, 
    data_processing_time BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    control_processing_time BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    idle_time BIGINT UNSIGNED NOT NULL DEFAULT 0, 
    num_workers INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (operator_execution_id, time),
    FOREIGN KEY (operator_execution_id) REFERENCES operator_executions (operator_execution_id) ON DELETE CASCADE
);
