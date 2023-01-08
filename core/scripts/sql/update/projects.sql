USE `texera_db`;
ALTER TABLE `user_project` MODIFY COLUMN `description` VARCHAR(10000) AFTER `name`;
