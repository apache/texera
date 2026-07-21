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

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'provider_type_enum') THEN
        CREATE TYPE provider_type_enum AS ENUM ('LOCAL', 'GOOGLE', 'FACEBOOK');
    END IF;
END
$$;

-- 2. The auth_provider table.
CREATE TABLE IF NOT EXISTS auth_provider (
    uid               INT                 NOT NULL,
    provider_type     provider_type_enum  NOT NULL,
    provider_id       VARCHAR(256),          -- external subject id (e.g. Google sub, Facebook id); NULL for LOCAL
    password          VARCHAR(256),          -- hashed credential; only for LOCAL
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (uid, provider_type),
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,

    -- one external identity maps to exactly one Texera user
    CONSTRAINT uq_provider_identity UNIQUE (provider_type, provider_id),

    -- credential shape must match the provider (replaces the old ck_nulltest)
    CONSTRAINT ck_provider_credential CHECK (
        (provider_type = 'LOCAL'  AND password    IS NOT NULL AND provider_id IS NULL) OR
        (provider_type != 'LOCAL' AND provider_id IS NOT NULL AND password    IS NULL)
    )
);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'password'
    ) THEN
        INSERT INTO auth_provider (uid, provider_type, password)
        SELECT uid, 'LOCAL'::provider_type_enum, password
        FROM "user"
        WHERE password IS NOT NULL
        ON CONFLICT (uid, provider_type) DO NOTHING;

        INSERT INTO auth_provider (uid, provider_type, provider_id)
        SELECT uid, 'GOOGLE'::provider_type_enum, google_id
        FROM "user"
        WHERE google_id IS NOT NULL
        ON CONFLICT (uid, provider_type) DO NOTHING;
    END IF;
END
$$;

-- Keep the avatar as a provider-neutral profile column on "user" (rename in place).
-- Guarded so it is a no-op on a fresh DB where "user" already has "avatar".
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'google_avatar'
    ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'avatar'
    ) THEN
        ALTER TABLE "user" RENAME COLUMN google_avatar TO avatar;
    END IF;
END
$$;

ALTER TABLE "user" DROP CONSTRAINT IF EXISTS ck_nulltest;
ALTER TABLE "user" DROP COLUMN IF EXISTS password;
ALTER TABLE "user" DROP COLUMN IF EXISTS google_id;

COMMIT;