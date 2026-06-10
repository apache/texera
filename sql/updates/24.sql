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

-- Adds the virtual_environments table, used to persist user-owned virtual
-- environment metadata (name + installed package versions) instead of
-- relying on the filesystem layout under /tmp/texera-pve/venvs.
CREATE TABLE IF NOT EXISTS virtual_environments
(
    veid     SERIAL PRIMARY KEY,
    uid      INT           NOT NULL,
    name     VARCHAR(128)  NOT NULL,
    packages JSONB         NOT NULL DEFAULT '{}'::jsonb,
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,
    UNIQUE (uid, name)
);

COMMIT;
