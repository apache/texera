/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

\c texera_db
SET search_path TO texera_db;

BEGIN;

-- Rename the old table to migrate later
ALTER TABLE user_activity RENAME TO user_action_old;

-- Validate existing values for "activate"
DO $do$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM user_action_old
            WHERE lower(activate) NOT IN ('like','unlike','clone','view')
        ) THEN
            RAISE EXCEPTION 'Error.';
        END IF;
    END
$do$;

-- Create enum type
DO $do$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM pg_type t
                     JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typname = 'action_enum' AND n.nspname = 'texera_db'
        ) THEN
            EXECUTE 'CREATE TYPE texera_db.action_enum AS ENUM (''like'',''unlike'',''clone'',''view'')';
        END IF;
    END
$do$;

-- Create the new table
CREATE TABLE user_action (
     user_action_id BIGSERIAL PRIMARY KEY,
     uid            INTEGER,
     ip             VARCHAR(15),
     action_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
     resource_type  VARCHAR(15) NOT NULL,
     resource_id    INTEGER NOT NULL,
     action         texera_db.action_enum NOT NULL
);

-- Copy data
INSERT INTO user_action (uid, ip, action_time, resource_type, resource_id, action)
SELECT
    CASE WHEN ua.uid = 0 OR u.uid IS NULL THEN NULL ELSE ua.uid END AS uid,
    ua.ip,
    ua.activity_time AS action_time,
    ua."type"        AS resource_type,
    ua.id            AS resource_id,
    lower(ua.activate)::texera_db.action_enum AS action
FROM user_action_old ua
     LEFT JOIN "user" u ON u.uid = ua.uid;

-- Add FK
ALTER TABLE user_action
    ADD CONSTRAINT fk_user_action_uid
        FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE SET NULL;

-- Drop the old table
DROP TABLE user_action_old;

COMMIT;
