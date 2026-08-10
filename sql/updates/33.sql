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

-- Relocate login credentials out of "user" into auth_provider.
--
-- Moves `password` / `google_id` into an auth_provider row per (user, provider), so a user can
-- hold several external identities instead of exactly one Google account, and renames
-- `google_avatar` to the provider-neutral `avatar`. The rename is in place: the column keeps
-- its width and every stored value, so this migration does not change what any user's avatar
-- resolves to.

\c texera_db

SET search_path TO texera_db;

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'provider_type_enum') THEN
        CREATE TYPE provider_type_enum AS ENUM ('LOCAL', 'GOOGLE');
    END IF;
END
$$;

-- provider_id is nullable here and tightened to NOT NULL below, once the backfill has given
-- every row a handle.
CREATE TABLE IF NOT EXISTS auth_provider
(
    uid               INT                 NOT NULL,
    provider_type     provider_type_enum  NOT NULL,
    provider_id       VARCHAR(256),
    password          VARCHAR(256), -- hashed credential; only for LOCAL
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (uid, provider_type),
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE
);

-- Both constraints are (re-)added at the end, so drop them first to keep this file re-runnable
-- on a database that already has them.
ALTER TABLE auth_provider DROP CONSTRAINT IF EXISTS ck_provider_credential;
ALTER TABLE auth_provider DROP CONSTRAINT IF EXISTS uq_provider_identity;

-- Report the accounts that end up unable to log in. This only reports: a name that is blank,
-- padded or shared is normalized into a usable handle further down rather than rejected, because
-- refusing the migration over a cosmetic name turns one untrimmed row — which `AdminUserResource`
-- can write today — into a deployment that cannot start, and liquibase marks the changeset failed.
DO $$
DECLARE
    orphans   TEXT;
    has_placeholder BOOLEAN;
BEGIN
    -- `is_placeholder` accounts (migration 31) deliberately have no credential, so they are
    -- not orphans and must not be reported as such. Checked dynamically because this migration
    -- also has to run against databases predating that column.
    SELECT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'is_placeholder'
    ) INTO has_placeholder;

    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'password'
    ) THEN
        IF has_placeholder THEN
            EXECUTE $q$
                SELECT string_agg(uid::TEXT, ', ')
                FROM "user"
                WHERE password IS NULL AND google_id IS NULL AND NOT is_placeholder
            $q$ INTO orphans;
        ELSE
            SELECT string_agg(uid::TEXT, ', ')
            INTO orphans
            FROM "user"
            WHERE password IS NULL AND google_id IS NULL;
        END IF;
    END IF;

    IF orphans IS NOT NULL THEN
        RAISE NOTICE 'migration 33: uid(s) % have neither a password nor a google_id, so they '
                     'get no auth_provider row and cannot log in.', orphans;
    END IF;
END
$$;

-- Backfill one auth_provider row per credential the user already had.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'texera_db' AND table_name = 'user' AND column_name = 'password'
    ) THEN
        -- The handle is left NULL here and minted below, where trimming, blank names and
        -- collisions are all resolved in one place; `provider_id IS NULL` is what marks a row
        -- as not yet minted.
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

-- Mint every LOCAL handle from "user".name, normalizing rather than rejecting: the name is
-- trimmed, a name with nothing left is replaced by a uid-derived handle, and handles that still
-- collide are deterministically suffixed with "-<uid>" (kept: the lowest uid), truncated to fit
-- VARCHAR(256). The loop is bounded because a suffixed handle can itself collide with a literal
-- one, the same way 28.sql deduplicates dataset names.
--
-- Every change is reported via RAISE NOTICE: the handle is what the user types to log in, so an
-- operator has to be able to see which accounts got one they would not guess and tell them.
DO $$
DECLARE
    rec RECORD;
    changed    INT := 0;
    iterations INT := 0;
BEGIN
    -- Trim, give a name that is blank or whitespace-only a deterministic stand-in, and separate
    -- names that are already shared. Separating them here rather than leaving all of it to the
    -- loop below settles the common case in one pass, so the loop only has to resolve the
    -- residue: a minted "-<uid>" handle that collides with a literal one.
    FOR rec IN
        UPDATE auth_provider a
        SET provider_id = CASE
                              WHEN src.rn = 1 THEN src.base
                              ELSE LEFT(src.base, 256 - LENGTH('-' || a.uid::TEXT))
                                       || '-' || a.uid::TEXT
                          END
        FROM (
            SELECT u.uid,
                   u.name AS old_name,
                   CASE
                       WHEN btrim(u.name) = '' THEN 'user-' || u.uid::TEXT
                       ELSE btrim(u.name)
                   END AS base,
                   ROW_NUMBER() OVER (
                       PARTITION BY CASE
                                        WHEN btrim(u.name) = '' THEN 'user-' || u.uid::TEXT
                                        ELSE btrim(u.name)
                                    END
                       ORDER BY u.uid
                   ) AS rn
            FROM "user" u
                     JOIN auth_provider ap
                          ON ap.uid = u.uid
                              AND ap.provider_type = 'LOCAL'
                              AND ap.provider_id IS NULL
        ) src
        WHERE a.uid = src.uid
          AND a.provider_type = 'LOCAL'
          AND a.provider_id IS NULL
        RETURNING a.uid, src.old_name, a.provider_id AS new_handle
    LOOP
        IF rec.old_name IS DISTINCT FROM rec.new_handle THEN
            changed := changed + 1;
            RAISE NOTICE 'migration 33: minted LOCAL handle for uid=%: "%" -> "%"',
                rec.uid, rec.old_name, rec.new_handle;
        END IF;
    END LOOP;

    -- Resolve handles shared by more than one account.
    LOOP
        FOR rec IN
            UPDATE auth_provider a
            SET provider_id = LEFT(a.provider_id, 256 - LENGTH('-' || a.uid::TEXT))
                                  || '-' || a.uid::TEXT
            FROM (
                SELECT uid, provider_id AS old_handle,
                       ROW_NUMBER() OVER (PARTITION BY provider_id ORDER BY uid) AS rn
                FROM auth_provider
                WHERE provider_type = 'LOCAL'
            ) dups
            WHERE a.uid = dups.uid AND a.provider_type = 'LOCAL' AND dups.rn > 1
            RETURNING a.uid, dups.old_handle, a.provider_id AS new_handle
        LOOP
            changed := changed + 1;
            RAISE NOTICE 'migration 33: LOCAL handle for uid=% was already taken: "%" -> "%"',
                rec.uid, rec.old_handle, rec.new_handle;
        END LOOP;

        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM auth_provider
            WHERE provider_type = 'LOCAL'
            GROUP BY provider_id HAVING COUNT(*) > 1
        );

        iterations := iterations + 1;
        IF iterations > 10 THEN
            RAISE EXCEPTION 'migration 33: could not make LOCAL login handles unique after 10 '
                            'passes; resolve the duplicates in "user".name manually and re-run.';
        END IF;
    END LOOP;

    IF changed > 0 THEN
        RAISE NOTICE 'migration 33: % LOCAL login handle(s) differ from the account name they '
                     'were minted from; those users cannot guess their handle and must be told '
                     'it or given a reset.', changed;
    END IF;
END
$$;

-- Every row now has a handle, so make it mandatory, enforce uniqueness, and restore the
-- credential check in its new shape: a password exists for LOCAL and only for LOCAL.
--
-- uq_provider_identity belongs here rather than in CREATE TABLE: Postgres checks a
-- non-deferrable UNIQUE as each index tuple is inserted, so in force it aborts the minting
-- UPDATE above the moment that derives a colliding handle, before the loop can resolve it.
-- Same ordering as 28.sql, which deduplicates before adding dataset_owner_uid_name_key.
ALTER TABLE auth_provider ALTER COLUMN provider_id SET NOT NULL;
ALTER TABLE auth_provider
    ADD CONSTRAINT uq_provider_identity UNIQUE (provider_type, provider_id);
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

-- ck_nulltest constrained password/google_id, which are about to disappear. Its "every user
-- has a credential" rule cannot be a row-level check once credentials live in a child table,
-- so it is dropped rather than reshaped: a user with no auth_provider row is now legal and
-- simply cannot log in.
ALTER TABLE "user" DROP CONSTRAINT IF EXISTS ck_nulltest;
ALTER TABLE "user" DROP COLUMN IF EXISTS password;
ALTER TABLE "user" DROP COLUMN IF EXISTS google_id;

COMMIT;
