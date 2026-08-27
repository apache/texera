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
-- Without this rename the config-service seeder (insert-if-absent) would create the
-- prefixed rows at their defaults and orphan the old ones, silently reverting any value
-- an admin had changed. Skip a row whose new key somehow already exists, so the
-- migration stays idempotent and never clobbers a value that is already correct.

UPDATE site_settings AS s
SET key = 'dataset_' || s.key
WHERE s.key IN (
        'single_file_upload_max_size_mib',
        'multipart_upload_chunk_size_mib',
        'max_number_of_concurrent_uploading_file',
        'max_number_of_concurrent_uploading_file_chunks'
    )
  AND NOT EXISTS (
        SELECT 1 FROM site_settings AS t WHERE t.key = 'dataset_' || s.key
    );

COMMIT;
