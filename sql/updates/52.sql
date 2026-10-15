/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

\c texera_db

SET search_path TO texera_db;

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'output_mode_enum') THEN
        CREATE TYPE output_mode_enum AS ENUM ('SET_SNAPSHOT', 'SET_DELTA', 'SINGLE_SNAPSHOT');
    END IF;
END $$;

-- The port's declared output mode and row count, so a finished execution's results
-- can be listed without the engine's physical plan or opening each table (#8994).
-- Rows written before this change keep both columns null.
ALTER TABLE operator_port_executions
    ADD COLUMN IF NOT EXISTS output_mode output_mode_enum DEFAULT NULL,
    ADD COLUMN IF NOT EXISTS result_count BIGINT DEFAULT NULL;

COMMIT;
