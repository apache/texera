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

-- at current design for the tobacco project, since the insertWorkflow handler is not yet implemented
-- UserWorkflow cannot be inserted with new record, but existing records can be updated
-- thus a tobacco-analysis-workflow need to be manually inserted here within this file

INSERT INTO `UserWorkflow` VALUES (
     'tobacco-analysis-workflow',
     'tobacco-analysis-workflow',
     '{"operators":[{"operatorID":"operator-13f95767-3b3d-4381-a832-0249d50a452c","operatorType":"MysqlSource","operatorProperties":{"host":"54.215.247.135","port":3306,"database":"tweets_readonly","table":"tobacco_superset","username":"team_member","password":"Summer2020@UCI","column":"text","boolean expression":"(+\"hemp wrap\")","limit":10},"inputPorts":[],"outputPorts":["output-0"],"showAdvanced":false},{"operatorID":"operator-98ab8c8e-71ef-4d62-af65-667c6d17fef5","operatorType":"ViewResults","operatorProperties":{"limit":10,"offset":0},"inputPorts":["input-0"],"outputPorts":[],"showAdvanced":false}],"operatorPositions":{"operator-13f95767-3b3d-4381-a832-0249d50a452c":{"x":246,"y":262},"operator-98ab8c8e-71ef-4d62-af65-667c6d17fef5":{"x":704,"y":258}},"links":[{"linkID":"669b6ec2-5d01-4ab1-9ed0-3038df0fc39c","source":{"operatorID":"operator-13f95767-3b3d-4381-a832-0249d50a452c","portID":"output-0"},"target":{"operatorID":"operator-98ab8c8e-71ef-4d62-af65-667c6d17fef5","portID":"input-0"}}]}'
 );
