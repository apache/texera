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

-- 2. The auth_provider table. `secret` holds the single per-provider credential:
--    the hashed password for LOCAL, or the external subject id (Google sub,
--    Facebook id, ...) for every other provider. A given (provider_type, secret)
--    pair maps to exactly one Texera user.
CREATE TABLE IF NOT EXISTS auth_provider (
    uid               INT                 NOT NULL,
    provider_type     provider_type_enum  NOT NULL,
    secret            VARCHAR(256)        NOT NULL,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (uid, provider_type),
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,

    CONSTRAINT uq_provider_identity UNIQUE (provider_type, secret)
);

-- 2b. Fold a pre-existing (provider_id, password) auth_provider table into the
--     merged `secret` column. Guarded so it is a no-op on a fresh DB where the
--     table was just created above in its final shape. Exactly one of
--     provider_id / password was non-null per row (old ck_provider_credential),
--     so COALESCE picks the live value.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'auth_provider' AND column_name = 'provider_id'
    ) THEN
        ALTER TABLE auth_provider ADD COLUMN IF NOT EXISTS secret VARCHAR(256);
        UPDATE auth_provider SET secret = COALESCE(password, provider_id) WHERE secret IS NULL;

        ALTER TABLE auth_provider DROP CONSTRAINT IF EXISTS ck_provider_credential;
        ALTER TABLE auth_provider DROP CONSTRAINT IF EXISTS uq_provider_identity;
        ALTER TABLE auth_provider ADD CONSTRAINT uq_provider_identity UNIQUE (provider_type, secret);
        ALTER TABLE auth_provider ALTER COLUMN secret SET NOT NULL;

        ALTER TABLE auth_provider DROP COLUMN IF EXISTS password;
        ALTER TABLE auth_provider DROP COLUMN IF EXISTS provider_id;
    END IF;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'password'
    ) THEN
        INSERT INTO auth_provider (uid, provider_type, secret)
        SELECT uid, 'LOCAL'::provider_type_enum, password
        FROM "user"
        WHERE password IS NOT NULL
        ON CONFLICT (uid, provider_type) DO NOTHING;

        INSERT INTO auth_provider (uid, provider_type, secret)
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