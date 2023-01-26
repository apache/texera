USE `texera_db`;
ALTER TABLE user MODIFY name varchar(32) AFTER uid;
ALTER TABLE user ADD email varchar(256) AFTER name;