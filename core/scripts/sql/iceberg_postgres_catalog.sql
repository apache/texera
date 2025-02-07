-- Important: replace "texera_iceberg_admin" and "password" with your own username
-- and password before executing the script.
\set db_user texera_iceberg_admin
\set db_password '\'password\''

CREATE DATABASE texera_iceberg_catalog;

CREATE USER :db_user WITH ENCRYPTED PASSWORD :db_password;

GRANT ALL PRIVILEGES ON DATABASE texera_iceberg_catalog TO :db_user;

ALTER DATABASE texera_iceberg_catalog OWNER TO :db_user;

\c texera_iceberg_catalog;

GRANT ALL ON SCHEMA public TO :db_user;
