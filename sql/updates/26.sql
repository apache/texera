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

-- Caches a materialized output port result so it can be reused across executions.
-- A row is identified by (workflow_id, global_port_id, cache_key_hash), where
-- cache_key_hash is a SHA-256 hash of the upstream sub-DAG that produces the port (its
-- operators, their parameters and exec info, schemas, and wiring). cache_key_hash is the
-- lookup key; cache_key_json is the JSON the hash was computed from, kept so a hash match
-- can be confirmed against the full content (collision safety). A different upstream
-- computation (for example an operator parameter or version change) produces a different
-- cache_key_hash and therefore a new row, so existing entries are never overwritten: each
-- row is the result of one specific computation of one port. tuple_count is the result's
-- row count, kept so the coordinator can report a reused region's output stats without a
-- second query to the Iceberg catalog.
CREATE TABLE IF NOT EXISTS operator_port_cache
(
    workflow_id         INT NOT NULL,
    global_port_id      VARCHAR(200) NOT NULL,
    cache_key_hash      CHAR(64) NOT NULL,
    cache_key_json      TEXT NOT NULL,
    storage_uri         TEXT NOT NULL,
    tuple_count         BIGINT,
    source_execution_id BIGINT,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (workflow_id, global_port_id, cache_key_hash),
    FOREIGN KEY (workflow_id) REFERENCES workflow(wid) ON DELETE CASCADE
);

COMMIT;
