USE texera_db;

ALTER TABLE dataset
MODIFY COLUMN is_public BOOLEAN NOT NULL DEFAULT true;

ALTER TABLE workflow
CHANGE COLUMN is_published is_public BOOLEAN NOT NULL DEFAULT false;