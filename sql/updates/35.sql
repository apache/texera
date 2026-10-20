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

-- Store the identity provider's full avatar URL instead of a Google-specific fragment.
--
-- Migration 33 renamed "user".google_avatar to "user".avatar but kept every value as-is, so the
-- column still holds only the last path segment of Google's `picture` claim and the frontend
-- still rebuilds `https://lh3.googleusercontent.com/a/<fragment>` around it. That makes the value
-- unusable for any other provider. This promotes the stored fragments to complete URLs.

\c texera_db

SET search_path TO texera_db;

BEGIN;

ALTER TABLE "user" ALTER COLUMN avatar TYPE VARCHAR(512);

UPDATE "user" SET avatar = NULL WHERE avatar = '';

UPDATE "user"
SET avatar = 'https://lh3.googleusercontent.com/a/' || avatar
WHERE avatar IS NOT NULL
  AND avatar NOT LIKE 'http://%'
  AND avatar NOT LIKE 'https://%';

COMMIT;
