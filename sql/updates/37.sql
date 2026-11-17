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

-- Introduce ML models as a first-class resource (own primary key `mid`, own LakeFS repo namespace)
-- and add model-specific attributes (framework, format).

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

COMMIT;
