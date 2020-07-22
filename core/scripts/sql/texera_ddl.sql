CREATE SCHEMA IF NOT EXISTS `texera`;
USE `texera`;

-- DROP TABLE IF EXISTS `UserFile`;
-- DROP TABLE IF EXISTS `UserDict`;
-- DROP TABLE IF EXISTS `UserAccount`;
DROP TABLE IF EXISTS `UserWorkflow`;

SET GLOBAL time_zone = '-8:00'; # this line is mandatory

CREATE TABLE IF NOT EXISTS `UserAccount` (
    `userName` VARCHAR(32) NOT NULL,
    `userID` INT UNSIGNED AUTO_INCREMENT NOT NULL,
    UNIQUE (`userName`),
    PRIMARY KEY (`userID`)
)ENGINE=INNODB,
-- start auto increment userID from 1 because userID 0 means user not exists
AUTO_INCREMENT=1;

CREATE TABLE IF NOT EXISTS `UserFile` (
	`userID` INT UNSIGNED NOT NULL,
    `fileID` INT UNSIGNED AUTO_INCREMENT NOT NULL,
	`size` INT UNSIGNED NOT NULL,
    `name` VARCHAR(128) NOT NULL,
    `path` VARCHAR(512) NOT NULL,
    `description` VARCHAR(512) NOT NULL,
    UNIQUE (`userID`, `name`),
    PRIMARY KEY (`fileID`),
    FOREIGN KEY (`userID`) REFERENCES `UserAccount`(`userID`) ON DELETE CASCADE
)ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS `UserDict` (
	`userID` INT UNSIGNED NOT NULL,
    `dictID` INT UNSIGNED AUTO_INCREMENT NOT NULL,
    `name` VARCHAR(128) NOT NULL,
    `content` MEDIUMBLOB NOT NULL,
    `description` VARCHAR(512) NOT NULL,
    UNIQUE (`userID`, `name`),
    PRIMARY KEY (`dictID`),
    FOREIGN KEY (`userID`) REFERENCES `UserAccount`(`userID`) ON DELETE CASCADE
)ENGINE=INNODB;

CREATE TABLE IF NOT EXISTS `UserWorkflow` (
    -- uncomment below to give workflows the concept of ownership
    -- `userID` INT UNSIGNED NOT NULL,
    `name` VARCHAR(128) NOT NULL,
    -- texera workflows use UUID which is 32 chars long
    `workflowID` VARCHAR (32) NOT NULL,
    `workflowBODY` TEXT NOT NULL,
    -- uncomment below to give workflows the concept of ownership
    -- UNIQUE (`userID`, `name`),
    PRIMARY KEY (`workflowID`)
    -- uncomment below to give workflows the concept of ownership
    -- FOREIGN KEY (`userID`) REFERENCES `UserAccount`(`userID`) ON DELETE CASCADE
)ENGINE=INNODB;

-- the below code is a fixed workflow
INSERT INTO `UserWorkflow` VALUES (
    'tobacco-analysis-workflow',
    'tobacco-analysis-workflow',
    '{"operators":[{"limit":100,"offset":0,"operatorID":"operator-bb71eaad-615f-4298-843b-bbcca9d5b694","operatorType":"ViewResults"},{"batchSize":10,"arrowBatchSize":10,"resultAttribute":"prediction","inputAttributeModel":"NltkSentiment.pickle","attribute":"text","operatorID":"operator-71536cb7-1925-422b-abd8-8de984e6bc12","operatorType":"ArrowNltkSentiment"},{"host":"54.215.247.135","port":3306,"database":"tweets_readonly","table":"tobacco_superset","username":"team_member","password":"Summer2020@UCI","column":"text","boolean expression":"(+\"hemp wrap\")","operatorID":"operator-8d351d58-8926-4a0e-bf54-46462b2c8443","operatorType":"MysqlSource"}],"links":[{"origin":"operator-71536cb7-1925-422b-abd8-8de984e6bc12","destination":"operator-bb71eaad-615f-4298-843b-bbcca9d5b694"},{"origin":"operator-8d351d58-8926-4a0e-bf54-46462b2c8443","destination":"operator-71536cb7-1925-422b-abd8-8de984e6bc12"}]}'
);