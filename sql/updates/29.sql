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

-- Execution size columns hold byte counts that can exceed 2 GiB (see #6978):
-- they were INT, so Long sizes were narrowed via Scala's silent `.toInt`,
-- wrapping sizes in [2 GiB, 4 GiB) to negative values and corrupting the
-- user-quota sums computed over these columns. Widen them to BIGINT to match
-- the Long byte counts the application writes.
ALTER TABLE workflow_executions
    ALTER COLUMN runtime_stats_size TYPE BIGINT;

ALTER TABLE operator_executions
    ALTER COLUMN console_messages_size TYPE BIGINT;

ALTER TABLE operator_port_executions
    ALTER COLUMN result_size TYPE BIGINT;

COMMIT;
