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

-- The dataset upload limits gained a `dataset_` prefix so that the model limits added
-- alongside them can be named symmetrically. site_settings rows are keyed by a
-- default.conf leaf's last path segment, so the renamed leaves are new row keys.
--
-- Nothing orders this migration before the config-service seeder, so the prefixed row may
-- already exist at its default by the time this runs. The old row is then the only place
-- an admin's edit survives, and it is unreachable through the API (updateSetting and
-- resetSetting both reject a key with no default.conf entry). So carry the old value over
-- rather than skipping, then drop the old row. Renaming what is left is a no-op on a
-- second run, which keeps this idempotent.

-- 1. The prefixed row already exists: the old row holds the value worth keeping.
UPDATE site_settings AS t
SET value = s.value,
    updated_by = s.updated_by,
    updated_at = s.updated_at
FROM site_settings AS s
WHERE s.key IN (
        'single_file_upload_max_size_mib',
        'multipart_upload_chunk_size_mib',
        'max_number_of_concurrent_uploading_file',
        'max_number_of_concurrent_uploading_file_chunks'
    )
  AND t.key = 'dataset_' || s.key;

-- 2. Drop the old rows whose value has just been carried over.
DELETE FROM site_settings AS s
WHERE s.key IN (
        'single_file_upload_max_size_mib',
        'multipart_upload_chunk_size_mib',
        'max_number_of_concurrent_uploading_file',
        'max_number_of_concurrent_uploading_file_chunks'
    )
  AND EXISTS (
        SELECT 1 FROM site_settings AS t WHERE t.key = 'dataset_' || s.key
    );

-- 3. No prefixed row was seeded: a plain rename preserves the value and its audit stamp.
UPDATE site_settings AS s
SET key = 'dataset_' || s.key
WHERE s.key IN (
        'single_file_upload_max_size_mib',
        'multipart_upload_chunk_size_mib',
        'max_number_of_concurrent_uploading_file',
        'max_number_of_concurrent_uploading_file_chunks'
    );

COMMIT;
