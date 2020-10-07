CREATE SCHEMA IF NOT EXISTS `texera_db`;
USE `texera_db`;

-- DROP TABLE IF EXISTS `uploaded_file`;
-- DROP TABLE IF EXISTS `key_search_dict`;
-- DROP TABLE IF EXISTS `workflow_of_user`;
-- DROP TABLE IF EXISTS `user_account`;
DROP TABLE IF EXISTS workflow;

SET GLOBAL time_zone = '-8:00'; # this line is mandatory

CREATE TABLE IF NOT EXISTS user_account
(
    `name` VARCHAR(32)                 NOT NULL,
    `uid`  INT UNSIGNED AUTO_INCREMENT NOT NULL,
    UNIQUE (`name`),
    PRIMARY KEY (`uid`)
) ENGINE = INNODB,
-- start auto increment userID from 1 because userID 0 means user not exists
  AUTO_INCREMENT = 1;

CREATE TABLE IF NOT EXISTS uploaded_file
(
    `uid`         INT UNSIGNED                NOT NULL,
    `fid`         INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `size`        INT UNSIGNED                NOT NULL,
    `name`        VARCHAR(128)                NOT NULL,
    `path`        VARCHAR(512)                NOT NULL,
    `description` VARCHAR(512)                NOT NULL,
    UNIQUE (`uid`, `name`),
    PRIMARY KEY (`fid`),
    FOREIGN KEY (`uid`) REFERENCES user_account (`uid`) ON DELETE CASCADE
) ENGINE = INNODB;

CREATE TABLE IF NOT EXISTS key_search_dict
(
    `uid`         INT UNSIGNED                NOT NULL,
    `ksd_id`      INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `name`        VARCHAR(128)                NOT NULL,
    `content`     MEDIUMBLOB                  NOT NULL,
    `description` VARCHAR(512)                NOT NULL,
    UNIQUE (`uid`, `name`),
    PRIMARY KEY (`ksd_id`),
    FOREIGN KEY (`uid`) REFERENCES user_account (`uid`) ON DELETE CASCADE
) ENGINE = INNODB,
  AUTO_INCREMENT = 1;

CREATE TABLE IF NOT EXISTS workflow
(
    `name`    VARCHAR(128)                NOT NULL,
    `wf_id`   INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `content` TEXT                        NOT NULL,
    PRIMARY KEY (`wf_id`)
) ENGINE = INNODB,
  AUTO_INCREMENT = 1;

CREATE TABLE IF NOT EXISTS workflow_of_user
(
    `uid`   INT UNSIGNED NOT NULL,
    `wf_id` INT UNSIGNED NOT NULL,
    PRIMARY KEY (`uid`, `wf_id`),
    FOREIGN KEY (`uid`) REFERENCES `user_account` (`uid`) ON DELETE CASCADE,
    FOREIGN KEY (`wf_id`) REFERENCES `workflow` (`wf_id`) ON DELETE CASCADE
) ENGINE = INNODB;
-- at current design for the tobacco project, since the insertWorkflow handler is not yet implemented
-- UserWorkflow cannot be inserted with new record, but existing records can be updated
-- thus a tobacco-analysis-workflow need to be manually inserted here within this file

INSERT INTO workflow (`name`, `content`)
VALUES ('tobacco-analysis-workflow',
        '{"operators":[{"operatorID":"operator-13f95767-3b3d-4381-a832-0249d50a452c","operatorType":"MysqlSource","operatorProperties":{"host":"54.215.247.135","port":3306,"database":"tweets_readonly","table":"tobacco_superset","username":"team_member","password":"Summer2020@UCI","column":"text","boolean expression":"(+\"hemp wrap\")","limit":10},"inputPorts":[],"outputPorts":["output-0"],"showAdvanced":false},{"operatorID":"operator-98ab8c8e-71ef-4d62-af65-667c6d17fef5","operatorType":"ViewResults","operatorProperties":{"limit":10,"offset":0},"inputPorts":["input-0"],"outputPorts":[],"showAdvanced":false}],"operatorPositions":{"operator-13f95767-3b3d-4381-a832-0249d50a452c":{"x":246,"y":262},"operator-98ab8c8e-71ef-4d62-af65-667c6d17fef5":{"x":704,"y":258}},"links":[{"linkID":"669b6ec2-5d01-4ab1-9ed0-3038df0fc39c","source":{"operatorID":"operator-13f95767-3b3d-4381-a832-0249d50a452c","portID":"output-0"},"target":{"operatorID":"operator-98ab8c8e-71ef-4d62-af65-667c6d17fef5","portID":"input-0"}}]}');
