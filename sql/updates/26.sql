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

-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db

SET search_path TO texera_db;

-- ============================================
-- 2. Add the operator_port_cache table
-- ============================================
BEGIN;

-- Caches a materialized output port result across executions, keyed by the cache
-- key (a SHA-256 hash of the upstream sub-DAG). cache_key_json holds the JSON
-- the cache key is computed from.
CREATE TABLE IF NOT EXISTS operator_port_cache
(
    workflow_id         INT NOT NULL,
    global_port_id      VARCHAR(200) NOT NULL,
    cache_key           CHAR(64) NOT NULL,
    cache_key_json      TEXT NOT NULL,
    result_uri          TEXT NOT NULL,
    tuple_count         BIGINT,
    source_execution_id BIGINT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, global_port_id, cache_key),
    FOREIGN KEY (workflow_id) REFERENCES workflow(wid) ON DELETE CASCADE
);

COMMIT;
