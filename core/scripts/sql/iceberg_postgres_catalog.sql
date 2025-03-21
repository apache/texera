\set db_user texera

\c postgres

DROP DATABASE IF EXISTS texera_iceberg_catalog;
CREATE DATABASE texera_iceberg_catalog;

-- Important: the database must exist before you GRANT or ALTER
-- So this part should ideally go into a separate connection session
-- or follow the DO block with a short delay to allow connection refresh

-- Next, grant privileges
GRANT ALL PRIVILEGES ON DATABASE texera_iceberg_catalog TO :db_user;
ALTER DATABASE texera_iceberg_catalog OWNER TO :db_user;

\c texera_iceberg_catalog

GRANT ALL ON SCHEMA public TO :db_user;
