CREATE SCHEMA IF NOT EXISTS `texera`;
USE `texera`;

-- DROP TABLE IF EXISTS `UserAccount`;
-- DROP TABLE IF EXISTS `UserFile`;

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

