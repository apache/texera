USE `texera_db`;

ALTER TABLE dataset
MODIFY COLUMN storage_path VARCHAR(512) NOT NULL DEFAULT '';
