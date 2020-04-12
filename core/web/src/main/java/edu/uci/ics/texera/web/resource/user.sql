CREATE SCHEMA IF NOT EXISTS `texera`;
USE `texera`;

# DROP TABLE IF EXISTS `UserAccount`;

SET GLOBAL time_zone = '-8:00'; # this line is mandatory

CREATE USER IF NOT EXISTS 'texerauser'@'localhost' IDENTIFIED BY '112358';
GRANT ALL PRIVILEGES ON texera.* TO 'texerauser'@'localhost' WITH GRANT OPTION;
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS `UserAccount` (
    `userName` VARCHAR(32) NOT NULL,
    `userID` INT UNSIGNED AUTO_INCREMENT NOT NULL,
    UNIQUE (`userName`),
    PRIMARY KEY (`userID`)
)ENGINE=INNODB, AUTO_INCREMENT=100;

