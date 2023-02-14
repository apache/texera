USE `texera_db`;
CREATE FULLTEXT INDEX `idx_workflow_name_description_content`  ON `texera_db`.`workflow` (name, description, content);