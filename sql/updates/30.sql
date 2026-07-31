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

-- The file resolver now requires an explicit resource-type prefix on dataset
-- logical paths (/datasets/ownerEmail/datasetName/versionName/...) so other
-- resource types (e.g. models) can be told apart by the prefix. Existing
-- workflows store unprefixed dataset paths inside workflow.content and
-- workflow_version.content, in two operator properties:
--   * fileName            (scan-source operators): /owner/name/version/file
--   * datasetVersionPath  (file-lister operator):  /owner/name/version
-- This migration prepends the "datasets" segment to both.
--
-- A value is treated as a dataset path only when its first two segments match an
-- existing (user.email, dataset.name) pair -- that pair is unique.
-- Local file paths and URLs match no dataset and are left untouched.
-- Already-prefixed values are skipped (idempotent). jsonb_set
-- uses create_missing = false so absent properties are never added.

DO $$
DECLARE
    wf_count INT := 0;
    wv_count INT := 0;
BEGIN
    WITH affected AS (
        SELECT w.wid
        FROM workflow w,
             jsonb_array_elements(w.content::jsonb -> 'operators') AS op,
             LATERAL (
                 SELECT op #>> '{operatorProperties,fileName}'           AS fn,
                        op #>> '{operatorProperties,datasetVersionPath}' AS dvp
             ) f
        WHERE jsonb_typeof(w.content::jsonb -> 'operators') = 'array'
          AND (
              (f.fn IS NOT NULL AND left(f.fn, 10) <> '/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                               AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2)))
              OR
              (f.dvp IS NOT NULL AND left(f.dvp, 10) <> '/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 1)
                               AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 2)))
          )
        GROUP BY w.wid
    ),
    updated AS (
        UPDATE workflow w
        SET content = jsonb_set(w.content::jsonb, '{operators}', (
            SELECT jsonb_agg(
                jsonb_set(
                    jsonb_set(
                        op,
                        '{operatorProperties,fileName}',
                        CASE
                            WHEN f.fn IS NOT NULL AND left(f.fn, 10) <> '/datasets/'
                             AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                         WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                                           AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2))
                            THEN to_jsonb('/datasets/' || ltrim(f.fn, '/'))
                            ELSE COALESCE(op #> '{operatorProperties,fileName}', 'null'::jsonb)
                        END,
                        false
                    ),
                    '{operatorProperties,datasetVersionPath}',
                    CASE
                        WHEN f.dvp IS NOT NULL AND left(f.dvp, 10) <> '/datasets/'
                         AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                     WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 1)
                                       AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 2))
                        THEN to_jsonb('/datasets/' || ltrim(f.dvp, '/'))
                        ELSE COALESCE(op #> '{operatorProperties,datasetVersionPath}', 'null'::jsonb)
                    END,
                    false
                )
                ORDER BY ord
            )
            FROM jsonb_array_elements(w.content::jsonb -> 'operators') WITH ORDINALITY AS t(op, ord),
                 LATERAL (
                     SELECT op #>> '{operatorProperties,fileName}'           AS fn,
                            op #>> '{operatorProperties,datasetVersionPath}' AS dvp
                 ) f
        ))::text
        FROM affected a
        WHERE w.wid = a.wid
        RETURNING 1
    )
    SELECT count(*) INTO wf_count FROM updated;

    WITH affected AS (
        SELECT wv.vid
        FROM workflow_version wv,
             jsonb_array_elements(wv.content::jsonb -> 'operators') AS op,
             LATERAL (
                 SELECT op #>> '{operatorProperties,fileName}'           AS fn,
                        op #>> '{operatorProperties,datasetVersionPath}' AS dvp
             ) f
        WHERE jsonb_typeof(wv.content::jsonb -> 'operators') = 'array'
          AND (
              (f.fn IS NOT NULL AND left(f.fn, 10) <> '/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                               AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2)))
              OR
              (f.dvp IS NOT NULL AND left(f.dvp, 10) <> '/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 1)
                               AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 2)))
          )
        GROUP BY wv.vid
    ),
    updated AS (
        UPDATE workflow_version wv
        SET content = jsonb_set(wv.content::jsonb, '{operators}', (
            SELECT jsonb_agg(
                jsonb_set(
                    jsonb_set(
                        op,
                        '{operatorProperties,fileName}',
                        CASE
                            WHEN f.fn IS NOT NULL AND left(f.fn, 10) <> '/datasets/'
                             AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                         WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                                           AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2))
                            THEN to_jsonb('/datasets/' || ltrim(f.fn, '/'))
                            ELSE COALESCE(op #> '{operatorProperties,fileName}', 'null'::jsonb)
                        END,
                        false
                    ),
                    '{operatorProperties,datasetVersionPath}',
                    CASE
                        WHEN f.dvp IS NOT NULL AND left(f.dvp, 10) <> '/datasets/'
                         AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                     WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 1)
                                       AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 2))
                        THEN to_jsonb('/datasets/' || ltrim(f.dvp, '/'))
                        ELSE COALESCE(op #> '{operatorProperties,datasetVersionPath}', 'null'::jsonb)
                    END,
                    false
                )
                ORDER BY ord
            )
            FROM jsonb_array_elements(wv.content::jsonb -> 'operators') WITH ORDINALITY AS t(op, ord),
                 LATERAL (
                     SELECT op #>> '{operatorProperties,fileName}'           AS fn,
                            op #>> '{operatorProperties,datasetVersionPath}' AS dvp
                 ) f
        ))::text
        FROM affected a
        WHERE wv.vid = a.vid
        RETURNING 1
    )
    SELECT count(*) INTO wv_count FROM updated;

    RAISE NOTICE 'Prefixed legacy dataset paths with "datasets/" in % workflow and % workflow_version row(s).', wf_count, wv_count;
END $$;

COMMIT;
