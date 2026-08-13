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

ALTER TABLE "user"
    ADD COLUMN is_placeholder BOOLEAN NOT NULL DEFAULT FALSE,
    DROP CONSTRAINT ck_nulltest,
    ADD CONSTRAINT ck_nulltest CHECK ((password IS NOT NULL) OR (google_id IS NOT NULL) OR is_placeholder);

ALTER TABLE dataset_contributor
    ADD COLUMN uid INT REFERENCES "user" (uid) ON DELETE SET NULL;

-- Contributor emails are resolved with lower(email) lookups.
CREATE INDEX idx_user_email_lower ON "user" (lower(email));

-- Drop legacy duplicate emails per dataset (keep the oldest row) so the
-- unique index below can be built.
DELETE FROM dataset_contributor dc
USING dataset_contributor keeper
WHERE keeper.did = dc.did
  AND keeper.cid < dc.cid
  AND dc.email IS NOT NULL AND trim(dc.email) <> ''
  AND keeper.email IS NOT NULL
  AND lower(trim(keeper.email)) = lower(trim(dc.email));

-- Per-dataset contributor emails are unique (blank emails exempt).
CREATE UNIQUE INDEX idx_dataset_contributor_did_email
    ON dataset_contributor (did, lower(trim(email)))
    WHERE email IS NOT NULL AND trim(email) <> '';

-- Link existing contributors to registered users by normalized email.
UPDATE dataset_contributor dc
SET uid = u.uid
FROM "user" u
WHERE dc.uid IS NULL
  AND dc.email IS NOT NULL
  AND lower(trim(dc.email)) = lower(u.email);

COMMIT;
