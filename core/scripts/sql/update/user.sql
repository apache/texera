ALTER TABLE `texera_db`.`user` 
ADD COLUMN `permission` INT NOT NULL DEFAULT 0 AFTER `google_id`,
CHANGE COLUMN `name` `email` VARCHAR(256) NOT NULL;

ALTER TABLE `texera_db`.`user` 
ADD COLUMN `name` VARCHAR(256) NULL DEFAULT NULL AFTER `uid`;