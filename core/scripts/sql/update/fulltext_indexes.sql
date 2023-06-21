USE `texera_db`;

CREATE FULLTEXT INDEX `idx_workflow_name_description_content` ON `texera_db`.`workflow` (name, description, content);

CREATE FULLTEXT INDEX `idx_user_name` ON `texera_db`.`user` (name);

CREATE FULLTEXT INDEX `idx_user_project_name_description` ON `texera_db`.`project` (name, description);

CREATE FULLTEXT INDEX `idx_file_name_description` ON `texera_db`.`file` (name, description);