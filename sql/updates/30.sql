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

-- Introduce ML models as a first-class resource, These
-- tables mirror the dataset* tables (own primary key `mid`, own LakeFS repo
-- namespace) and add model-specific attributes (framework, format).

CREATE TABLE IF NOT EXISTS model
(
    mid             SERIAL PRIMARY KEY,
    owner_uid       INT NOT NULL,
    name            VARCHAR(128) NOT NULL,
    repository_name VARCHAR(128),
    is_public       BOOLEAN NOT NULL DEFAULT TRUE,
    is_downloadable BOOLEAN NOT NULL DEFAULT TRUE,
    description     TEXT NOT NULL,
    creation_time   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    cover_image     varchar(255),
    framework       VARCHAR(32),
    format          VARCHAR(32),
    FOREIGN KEY (owner_uid) REFERENCES "user"(uid) ON DELETE CASCADE,
    UNIQUE (owner_uid, name)
);

CREATE TABLE IF NOT EXISTS model_user_access
(
    mid       INT NOT NULL,
    uid       INT NOT NULL,
    privilege privilege_enum NOT NULL DEFAULT 'NONE',
    PRIMARY KEY (mid, uid),
    FOREIGN KEY (mid) REFERENCES model(mid) ON DELETE CASCADE,
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS model_version
(
    mvid          SERIAL PRIMARY KEY,
    mid           INT NOT NULL,
    creator_uid   INT NOT NULL,
    name          VARCHAR(128) NOT NULL,
    version_hash  VARCHAR(64) NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (mid) REFERENCES model(mid) ON DELETE CASCADE
);

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

CREATE TABLE IF NOT EXISTS model_user_likes
(
    uid INTEGER NOT NULL,
    mid INTEGER NOT NULL,
    PRIMARY KEY (uid, mid),
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,
    FOREIGN KEY (mid) REFERENCES model(mid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS model_view_count
(
    mid        INTEGER NOT NULL,
    view_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (mid),
    FOREIGN KEY (mid) REFERENCES model(mid) ON DELETE CASCADE
);

COMMIT;
