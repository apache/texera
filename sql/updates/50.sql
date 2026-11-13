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

-- Computing-unit images an administrator has registered, which any user may then start a
-- computing unit from. Rows are global rather than owned -- the point is one trusted list
-- offered to everybody -- so created_by records who added a row for auditing only.
--
-- source_ref is the reference the administrator gave; source_digest is what it resolved to
-- when last validated. A unit runs the two combined, so a tag moved upstream later cannot
-- change what already ran.

\c texera_db

SET search_path TO texera_db;

BEGIN;

CREATE TABLE IF NOT EXISTS cu_image
(
    iid            SERIAL PRIMARY KEY,
    name           VARCHAR(128) NOT NULL,
    source_ref     VARCHAR(512) NOT NULL,
    source_digest  VARCHAR(128),
    -- A computing unit may only start from a READY image.
    status         VARCHAR(16)  NOT NULL DEFAULT 'PENDING'
        CONSTRAINT ck_cu_image_status
            CHECK (status IN ('PENDING', 'VALIDATING', 'READY', 'FAILED')),
    -- Numbers the validations of this row, so a retry gets a job name of its own.
    attempt        INT          NOT NULL DEFAULT 0,
    validation_log TEXT,
    -- Nulled rather than cascaded: the image stays usable if its curator is deleted.
    created_by     INT,
    creation_time  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time    TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (created_by) REFERENCES "user" (uid) ON DELETE SET NULL,
    UNIQUE (name),
    -- Enforced here, not only in the service: the check there is a read then a write, so
    -- two simultaneous registrations would both pass it.
    UNIQUE (source_ref)
);

-- Two references can turn out to be one image once their digests are known.
CREATE INDEX IF NOT EXISTS idx_cu_image_source_digest ON cu_image (source_digest);

COMMIT;
