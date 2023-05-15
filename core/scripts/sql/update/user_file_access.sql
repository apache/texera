USE `texera_db`;
ALTER TABLE user_file_access ADD `privilege` ENUM('NONE', 'READ', 'WRITE') NOT NULL DEFAULT 'NONE';
UPDATE user_file_access SET privilege = IF(write_access = true, 'WRITE', IF(read_access = true, 'READ', 'NONE')) where privilege = 'NONE';
ALTER TABLE user_file_access DROP COLUMN read_access;
ALTER TABLE user_file_access DROP COLUMN write_access;
