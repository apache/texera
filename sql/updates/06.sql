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
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db'
          AND table_name = 'workflow_executions'
          AND column_name = 'runtime_stats_size'
    ) THEN
        ALTER TABLE workflow_executions
            ADD COLUMN runtime_stats_size INT DEFAULT 0;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db'
          AND table_name = 'operator_executions'
          AND column_name = 'console_messages_size'
    ) THEN
        ALTER TABLE operator_executions
            ADD COLUMN console_messages_size INT DEFAULT 0;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db'
          AND table_name = 'operator_port_executions'
          AND column_name = 'result_size'
    ) THEN
        ALTER TABLE operator_port_executions
            ADD COLUMN result_size INT DEFAULT 0;
    END IF;
END
$$;
