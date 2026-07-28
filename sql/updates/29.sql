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
--    provider_id is the subject id at the provider: the login username for LOCAL, the
--    external id (Google sub, Facebook id) otherwise. It is created nullable here and
--    made NOT NULL at step 6, once existing rows have been backfilled.
CREATE TABLE IF NOT EXISTS auth_provider (
                                             uid               INT                 NOT NULL,
                                             provider_type     provider_type_enum  NOT NULL,
                                             provider_id       VARCHAR(256),
    password          VARCHAR(256),          -- hashed credential; only for LOCAL
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (uid, provider_type),
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,

    -- one identity per provider; for LOCAL this is what makes the login username unique
    CONSTRAINT uq_provider_identity UNIQUE (provider_type, provider_id)
    );

-- 3. Drop the credential check before touching provider_id. On a database that already
--    ran an earlier version of this migration the old check asserts provider_id IS NULL
--    for LOCAL, which would reject the backfill below. Re-added in its new shape at step 6.
ALTER TABLE auth_provider DROP CONSTRAINT IF EXISTS ck_provider_credential;

-- 4. Pre-flight. "user".name has never been unique, but it is about to become a unique
--    authentication key, so abort naming the offenders rather than let the backfill die
--    with a bare unique_violation. Handles that are blank or whitespace-padded are
--    rejected too: they are reachable today (register stores the name un-trimmed) and
--    make for handles nobody can type.
DO $$
DECLARE
offenders TEXT;
    orphans   TEXT;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'password'
    ) THEN
        -- upgrading from the pre-auth_provider schema: handles come straight from "user"
SELECT string_agg(DISTINCT quote_literal(name), ', ')
INTO offenders
FROM "user"
WHERE password IS NOT NULL
  AND (btrim(name) = '' OR name <> btrim(name) OR name IN (
    SELECT name FROM "user" WHERE password IS NOT NULL
    GROUP BY name HAVING count(*) > 1));

SELECT string_agg(uid::TEXT, ', ')
INTO orphans
FROM "user"
WHERE password IS NULL AND google_id IS NULL;
ELSE
        -- re-running: an earlier version of this migration created LOCAL rows with no handle
SELECT string_agg(DISTINCT quote_literal(u.name), ', ')
INTO offenders
FROM "user" u
         JOIN auth_provider a ON a.uid = u.uid AND a.provider_type = 'LOCAL'
WHERE a.provider_id IS NULL
  AND (btrim(u.name) = '' OR u.name <> btrim(u.name) OR u.name IN (
    SELECT u2.name
    FROM "user" u2
             JOIN auth_provider a2 ON a2.uid = u2.uid AND a2.provider_type = 'LOCAL'
    WHERE a2.provider_id IS NULL
    GROUP BY u2.name HAVING count(*) > 1));
END IF;

    IF offenders IS NOT NULL THEN
        RAISE EXCEPTION 'migration 29: cannot promote "user".name to a login handle - '
                        'the following names are duplicated, blank, or whitespace-padded: %. '
                        'Resolve them and re-run.', offenders;
END IF;

    IF orphans IS NOT NULL THEN
        RAISE NOTICE 'migration 29: uid(s) % have neither a password nor a google_id, so they '
                     'get no auth_provider row and cannot log in.', orphans;
END IF;
END
$$;

-- 5. Backfill.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'password'
    ) THEN
        INSERT INTO auth_provider (uid, provider_type, provider_id, password)
SELECT uid, 'LOCAL'::provider_type_enum, name, password
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

-- Fill handles left NULL by an earlier version of this migration; a no-op otherwise.
UPDATE auth_provider a
SET provider_id = u.name
    FROM "user" u
WHERE u.uid = a.uid
  AND a.provider_type = 'LOCAL'
  AND a.provider_id IS NULL;

-- 6. Every row now has a handle, so make it mandatory and restore the credential check
--    in its new shape: a password exists for LOCAL and only for LOCAL.
ALTER TABLE auth_provider ALTER COLUMN provider_id SET NOT NULL;
ALTER TABLE auth_provider
    ADD CONSTRAINT ck_provider_credential CHECK ((provider_type = 'LOCAL') = (password IS NOT NULL));

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