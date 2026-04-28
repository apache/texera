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
-- 2. Update the table schema
-- ============================================
BEGIN;

-- 1. Drop old tables (if exist)
DROP TABLE IF EXISTS dataset_upload_session CASCADE;
DROP TABLE IF EXISTS dataset_upload_session_part CASCADE;

-- 2. Create dataset upload session table
CREATE TABLE IF NOT EXISTS dataset_upload_session
(
    did                 INT          NOT NULL,
    uid                 INT          NOT NULL,
    file_path           TEXT         NOT NULL,
    upload_id           VARCHAR(256) NOT NULL UNIQUE,
    physical_address    TEXT,
    num_parts_requested INT          NOT NULL,

    PRIMARY KEY (uid, did, file_path),

    FOREIGN KEY (did) REFERENCES dataset(did) ON DELETE CASCADE,
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE
    );

-- 3. Create dataset upload session parts table
CREATE TABLE IF NOT EXISTS dataset_upload_session_part
(
    upload_id   VARCHAR(256) NOT NULL,
    part_number INT          NOT NULL,
    etag        TEXT         NOT NULL DEFAULT '',

    PRIMARY KEY (upload_id, part_number),

    CONSTRAINT chk_part_number_positive CHECK (part_number > 0),

    FOREIGN KEY (upload_id)
    REFERENCES dataset_upload_session(upload_id)
    ON DELETE CASCADE
    );

COMMIT;
