USE `texera_db`;
ALTER TABLE user MODIFY name varchar(32) AFTER uid; # Make uid first, PR#1783
ALTER TABLE user ADD email varchar(256) AFTER name; # Add email field, PR#1804
ALTER TABLE user MODIFY email varchar(256) UNIQUE;  # Make email unique, PR#1818