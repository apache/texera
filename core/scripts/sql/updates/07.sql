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

\c texera_db

SET search_path TO texera_db;

DO $$
BEGIN
    -- 1. Add cuid field to workflow_executions if not exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'workflow_executions' AND column_name = 'cuid'
    ) THEN
ALTER TABLE workflow_executions
    ADD COLUMN cuid INT;

ALTER TABLE workflow_executions
    ADD CONSTRAINT workflow_executions_cuid_fkey
        FOREIGN KEY (cuid) REFERENCES workflow_computing_unit(cuid) ON DELETE CASCADE;
END IF;

    -- 2. Add ENUM type for workflow_computing_unit.type
IF NOT EXISTS (
        SELECT 1
        FROM pg_type
        WHERE typname = 'workflow_computing_unit_type_enum'
    ) THEN
        CREATE TYPE workflow_computing_unit_type_enum AS ENUM ('local', 'kubernetes');
END IF;

    -- 3. Add type column to workflow_computing_unit if not exist
IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'workflow_computing_unit' AND column_name = 'type'
    ) THEN
ALTER TABLE workflow_computing_unit
    ADD COLUMN type workflow_computing_unit_type_enum;
END IF;

    -- 4. Add uri column to workflow_computing_unit if not exist
IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'workflow_computing_unit' AND column_name = 'uri'
    ) THEN
ALTER TABLE workflow_computing_unit
    ADD COLUMN uri TEXT NOT NULL DEFAULT '';
END IF;

    -- 5. Add resource column to workflow_computing_unit if not exist
IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'workflow_computing_unit' AND column_name = 'resource'
    ) THEN
ALTER TABLE workflow_computing_unit
    ADD COLUMN resource TEXT DEFAULT '';
END IF;
END
$$;