USE `texera_db`;
ALTER TABLE `user_project` ADD COLUMN `description` VARCHAR(500) AFTER `name`;
