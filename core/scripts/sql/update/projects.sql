USE `texera_db`;
ALTER TABLE `user_project` RENAME 'project'; # Rename user_project table to project, PR#1827
ALTER TABLE `project` MODIFY COLUMN `description` VARCHAR(10000) AFTER `name`;
