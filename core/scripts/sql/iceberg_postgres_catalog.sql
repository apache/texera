-- Connect to the default postgres database
\c postgres

-- Create the user `texera` with `password` if it doesn't exist
DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT FROM pg_catalog.pg_roles WHERE rolname = 'texera'
        ) THEN
            CREATE ROLE texera LOGIN PASSWORD 'password';
        END IF;
    END
$$;

-- Drop and recreate the database
DROP DATABASE IF EXISTS texera_iceberg_catalog;
CREATE DATABASE texera_iceberg_catalog;

-- Grant and change ownership
GRANT ALL PRIVILEGES ON DATABASE texera_iceberg_catalog TO texera;
ALTER DATABASE texera_iceberg_catalog OWNER TO texera;

-- Reconnect to the new database
\c texera_iceberg_catalog

-- Grant schema access
GRANT ALL ON SCHEMA public TO texera;