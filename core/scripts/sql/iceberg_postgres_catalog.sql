-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

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