CREATE SCHEMA IF NOT EXISTS `texera`;
USE `texera`;

-- DROP TABLE IF EXISTS `UserAccount`;

SET GLOBAL time_zone = '-8:00'; # this line is mandatory

CREATE TABLE IF NOT EXISTS `UserAccount` (
    `userName` VARCHAR(32) NOT NULL,
    `userID` INT UNSIGNED AUTO_INCREMENT NOT NULL,
    UNIQUE (`userName`),
    PRIMARY KEY (`userID`)
)ENGINE=INNODB,
-- start auto increment userID from 1 because userID 0 means user not exists
AUTO_INCREMENT=1;

