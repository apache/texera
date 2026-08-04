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

-- Store the identity provider's full avatar URL instead of a Google-specific URL fragment.
--
-- "user".avatar used to hold only the last path segment of Google's `picture` claim, and the
-- frontend rebuilt `https://lh3.googleusercontent.com/a/<fragment>` around it. That made the
-- column meaningless for any other provider. This promotes the stored fragments to complete
-- URLs so the value is self-describing and provider-agnostic.

\c texera_db

SET search_path TO texera_db;

BEGIN;

-- 1. A full URL does not fit in the old width.
ALTER TABLE "user" ALTER COLUMN avatar TYPE VARCHAR(512);

-- 2. Pictureless Google logins used to record an empty string; NULL is now the single
--    representation of "this user has no avatar", so the frontend has one case to handle.
UPDATE "user" SET avatar = NULL WHERE avatar = '';

-- 3. Promote the remaining bare fragments to absolute URLs. The `NOT LIKE` guard makes this
--    idempotent and leaves already-absolute values (from a re-run, or from a provider added
--    after this migration) untouched.
UPDATE "user"
SET avatar = 'https://lh3.googleusercontent.com/a/' || avatar
WHERE avatar IS NOT NULL
  AND avatar NOT LIKE 'http://%'
  AND avatar NOT LIKE 'https://%';

COMMIT;