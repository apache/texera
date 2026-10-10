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

-- Session-based multipart upload for model files. Tracks in-progress multipart
-- uploads so a model version can be assembled from parts and resumed across requests.

CREATE TABLE IF NOT EXISTS model_upload_session
(
    mid                 INT          NOT NULL,
    uid                 INT          NOT NULL,
    file_path           TEXT         NOT NULL,
    upload_id           VARCHAR(256) NOT NULL UNIQUE,
    physical_address    TEXT,
    num_parts_requested INT          NOT NULL,
    file_size_bytes     BIGINT       NOT NULL,
    part_size_bytes     BIGINT       NOT NULL,
    created_at          TIMESTAMPTZ  NOT NULL DEFAULT now(),

    PRIMARY KEY (uid, mid, file_path),

    FOREIGN KEY (mid) REFERENCES model(mid) ON DELETE CASCADE,
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,

    CONSTRAINT chk_model_upload_session_num_parts_requested_positive
        CHECK (num_parts_requested >= 1),

    CONSTRAINT chk_model_upload_session_file_size_bytes_positive
        CHECK (file_size_bytes > 0),

    CONSTRAINT chk_model_upload_session_part_size_bytes_positive
        CHECK (part_size_bytes > 0),

    CONSTRAINT chk_model_upload_session_part_size_bytes_s3_upper_bound
        CHECK (part_size_bytes <= 5368709120)
);

CREATE TABLE IF NOT EXISTS model_upload_session_part
(
    upload_id   VARCHAR(256) NOT NULL,
    part_number INT          NOT NULL,
    etag        TEXT         NOT NULL DEFAULT '',

    PRIMARY KEY (upload_id, part_number),

    CONSTRAINT chk_model_part_number_positive CHECK (part_number > 0),

    FOREIGN KEY (upload_id)
        REFERENCES model_upload_session(upload_id)
        ON DELETE CASCADE
);

-- Version names are generated from an unlocked count(*), and FileResolver looks a version
-- up by (mid, name) with fetchOneInto. Enforce the uniqueness the generator assumes.
ALTER TABLE model_version
    DROP CONSTRAINT IF EXISTS uq_model_version_mid_name;

ALTER TABLE model_version
    ADD CONSTRAINT uq_model_version_mid_name UNIQUE (mid, name);

COMMIT;
