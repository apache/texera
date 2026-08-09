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

CREATE TYPE user_warehouse_flavor_enum AS ENUM ('local', 'aws');

-- Per-user warehouse registrations (#6870): one row per warehouse a user registered.
-- Base columns only; the assume-role (BYO-S3) columns come in a later change.
CREATE TABLE IF NOT EXISTS user_warehouse
(
    whid                    SERIAL PRIMARY KEY,
    uid                     INT          NOT NULL,
    name                    VARCHAR(128) NOT NULL,
    warehouse_name          VARCHAR(255) NOT NULL UNIQUE,
    lakekeeper_warehouse_id UUID         NOT NULL,
    flavor                  user_warehouse_flavor_enum NOT NULL,
    s3_bucket               VARCHAR(255),
    s3_endpoint             VARCHAR(255),
    s3_region               VARCHAR(64),
    created_at              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (uid, name),
    FOREIGN KEY (uid) REFERENCES "user" (uid) ON DELETE CASCADE
);

COMMIT;
