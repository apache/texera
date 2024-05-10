USE `texera_db`;

ALTER TABLE texera_db.workflow_runtime_statistics
    MODIFY COLUMN operator_id VARCHAR(512);