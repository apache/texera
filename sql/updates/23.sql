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

-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
SET search_path TO texera_db;

-- ============================================
-- 2. Create the tables to store notebook and mapping
-- ============================================

BEGIN;

CREATE TABLE IF NOT EXISTS notebook
(
    nid         SERIAL  NOT NULL PRIMARY KEY,
    wid         INT     NOT NULL UNIQUE,
    notebook    JSONB   NOT NULL,
    UNIQUE (wid, nid),
    FOREIGN KEY (wid) REFERENCES workflow(wid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS workflow_notebook_mapping
(
    wid         INT     NOT NULL,
    vid         INT     NOT NULL,
    nid         INT     NOT NULL,
    mapping     JSONB   NOT NULL,
    PRIMARY KEY (wid, vid, nid),
    FOREIGN KEY (vid) REFERENCES workflow_version(vid) ON DELETE CASCADE,
    FOREIGN KEY (wid, nid) REFERENCES notebook(wid, nid) ON DELETE CASCADE
);

COMMIT;
