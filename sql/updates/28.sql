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

-- match the existing user_role_enum style in texera_ddl.sql
CREATE TYPE provider_type_enum AS ENUM ('LOCAL', 'GOOGLE');

CREATE TABLE IF NOT EXISTS auth_provider (
    uid               INT                 NOT NULL,
    provider_type     provider_type_enum  NOT NULL,

    provider_id       VARCHAR(256),
    password          VARCHAR(256),
    provider_avatar   VARCHAR(100),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (uid, provider_type),
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,

    CONSTRAINT uq_provider_identity UNIQUE (provider_type, provider_id),
    CONSTRAINT ck_provider_credential CHECK (
        (provider_type = 'LOCAL'  AND password    IS NOT NULL AND provider_id IS NULL) OR
        (provider_type = 'GOOGLE' AND provider_id IS NOT NULL AND password    IS NULL)
    )
 );

COMMIT;