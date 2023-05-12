USE `texera_db`;
ALTER TABLE workflow_user_access ADD `privilege` ENUM('NONE', 'READ', 'WRITE') NOT NULL DEFAULT 'NONE';
UPDATE workflow_user_access SET privilege = IF(write_privilege = true, 'WRITE', IF(read_privilege = true, 'READ', 'NONE')) where privilege = 'NONE';
ALTER TABLE workflow_user_access DROP COLUMN read_privilege;
ALTER TABLE workflow_user_access DROP COLUMN write_privilege;
