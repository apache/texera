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

-- Time Machine: full-snapshot workflow history (parallel to workflow_version
-- which stores JSON diffs). Each row is a complete content snapshot tagged
-- with the discrete event that produced it.
CREATE TABLE IF NOT EXISTS workflow_snapshot
(
    sid                 SERIAL PRIMARY KEY,
    wid                 INT  NOT NULL,
    uid                 INT,
    snapshot_version    INT  NOT NULL,
    content             TEXT NOT NULL,
    change_type         VARCHAR(64)  NOT NULL,
    change_summary      TEXT NOT NULL,
    changed_operators   TEXT NOT NULL DEFAULT '[]',
    source              VARCHAR(16)  NOT NULL DEFAULT 'user',
    creation_time       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (wid) REFERENCES workflow(wid) ON DELETE CASCADE,
    UNIQUE (wid, snapshot_version)
);

CREATE INDEX IF NOT EXISTS idx_workflow_snapshot_wid_time
    ON workflow_snapshot(wid, creation_time DESC);

COMMIT;
