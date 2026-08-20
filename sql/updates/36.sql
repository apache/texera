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

-- The file resolver requires an explicit resource-type prefix on dataset logical
-- paths (/dataset/ownerEmail/datasetName/versionName/...) so other resource types
-- can be told apart by the prefix. Stored workflows carry such paths inside
-- workflow.content and workflow_version.content, in two operator properties:
--   * fileName            (scan-source operators): /owner/name/version/file
--   * datasetVersionPath  (file-lister operator):  /owner/name/version
--
-- Both are normalized to that form: a "/datasets/" prefix has its leading segment
-- rewritten, and a path with no prefix gets "dataset" prepended. The prefixed case
-- is tested first, since such a path looks unprefixed to the other branch and must
-- not be prefixed twice.
--
-- Either case applies only when the path's owner and name segments match an
-- existing (user.email, dataset.name) pair -- unique, and read at parts 1 and 2
-- unprefixed, 2 and 3 prefixed. That guard is what keeps the migration off the
-- plain filesystem paths and URLs this column also holds: "/datasets" is an
-- ordinary directory name, and FileResolver tries localResolveFunc first, so a
-- fileName of /datasets/imdb/movies.csv can be a working local mount. It matches
-- no dataset, so it is left alone.
--
-- Values already in the target form match neither case, making this idempotent.
-- jsonb_set uses create_missing = false so absent properties are never added.

DO $$
DECLARE
    wf_count INT := 0;
    wv_count INT := 0;
BEGIN
    WITH affected AS (
        SELECT w.wid
        FROM workflow w,
             jsonb_array_elements(
                 CASE
                     WHEN jsonb_typeof(w.content::jsonb -> 'operators') = 'array'
                         THEN w.content::jsonb -> 'operators'
                     ELSE '[]'::jsonb
                 END
             ) AS op,
             LATERAL (
                 SELECT op #>> '{operatorProperties,fileName}'           AS fn,
                        op #>> '{operatorProperties,datasetVersionPath}' AS dvp
             ) f
        WHERE jsonb_typeof(w.content::jsonb -> 'operators') = 'array'
          AND (
              (f.fn ~ '^/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 2)
                               AND d.name  = split_part(ltrim(f.fn, '/'), '/', 3)))
              OR
              (f.dvp ~ '^/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 2)
                               AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 3)))
              OR
              (f.fn IS NOT NULL AND left(f.fn, 9) <> '/dataset/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                               AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2)))
              OR
              (f.dvp IS NOT NULL AND left(f.dvp, 9) <> '/dataset/'
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
                            WHEN f.fn ~ '^/datasets/'
                             AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                         WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 2)
                                           AND d.name  = split_part(ltrim(f.fn, '/'), '/', 3))
                            THEN to_jsonb(regexp_replace(f.fn, '^/datasets/', '/dataset/'))
                            WHEN f.fn IS NOT NULL AND left(f.fn, 9) <> '/dataset/'
                             AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                         WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                                           AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2))
                            THEN to_jsonb('/dataset/' || ltrim(f.fn, '/'))
                            ELSE COALESCE(op #> '{operatorProperties,fileName}', 'null'::jsonb)
                        END,
                        false
                    ),
                    '{operatorProperties,datasetVersionPath}',
                    CASE
                        WHEN f.dvp ~ '^/datasets/'
                         AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                     WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 2)
                                       AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 3))
                        THEN to_jsonb(regexp_replace(f.dvp, '^/datasets/', '/dataset/'))
                        WHEN f.dvp IS NOT NULL AND left(f.dvp, 9) <> '/dataset/'
                         AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                     WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 1)
                                       AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 2))
                        THEN to_jsonb('/dataset/' || ltrim(f.dvp, '/'))
                        ELSE COALESCE(op #> '{operatorProperties,datasetVersionPath}', 'null'::jsonb)
                    END,
                    false
                )
                ORDER BY ord
            )
            FROM jsonb_array_elements(
                     CASE
                         WHEN jsonb_typeof(w.content::jsonb -> 'operators') = 'array'
                             THEN w.content::jsonb -> 'operators'
                         ELSE '[]'::jsonb
                     END
                 ) WITH ORDINALITY AS t(op, ord),
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
             jsonb_array_elements(
                 CASE
                     WHEN jsonb_typeof(wv.content::jsonb -> 'operators') = 'array'
                         THEN wv.content::jsonb -> 'operators'
                     ELSE '[]'::jsonb
                 END
             ) AS op,
             LATERAL (
                 SELECT op #>> '{operatorProperties,fileName}'           AS fn,
                        op #>> '{operatorProperties,datasetVersionPath}' AS dvp
             ) f
        WHERE jsonb_typeof(wv.content::jsonb -> 'operators') = 'array'
          AND (
              (f.fn ~ '^/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 2)
                               AND d.name  = split_part(ltrim(f.fn, '/'), '/', 3)))
              OR
              (f.dvp ~ '^/datasets/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 2)
                               AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 3)))
              OR
              (f.fn IS NOT NULL AND left(f.fn, 9) <> '/dataset/'
                 AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                             WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                               AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2)))
              OR
              (f.dvp IS NOT NULL AND left(f.dvp, 9) <> '/dataset/'
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
                            WHEN f.fn ~ '^/datasets/'
                             AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                         WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 2)
                                           AND d.name  = split_part(ltrim(f.fn, '/'), '/', 3))
                            THEN to_jsonb(regexp_replace(f.fn, '^/datasets/', '/dataset/'))
                            WHEN f.fn IS NOT NULL AND left(f.fn, 9) <> '/dataset/'
                             AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                         WHERE u.email = split_part(ltrim(f.fn, '/'), '/', 1)
                                           AND d.name  = split_part(ltrim(f.fn, '/'), '/', 2))
                            THEN to_jsonb('/dataset/' || ltrim(f.fn, '/'))
                            ELSE COALESCE(op #> '{operatorProperties,fileName}', 'null'::jsonb)
                        END,
                        false
                    ),
                    '{operatorProperties,datasetVersionPath}',
                    CASE
                        WHEN f.dvp ~ '^/datasets/'
                         AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                     WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 2)
                                       AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 3))
                        THEN to_jsonb(regexp_replace(f.dvp, '^/datasets/', '/dataset/'))
                        WHEN f.dvp IS NOT NULL AND left(f.dvp, 9) <> '/dataset/'
                         AND EXISTS (SELECT 1 FROM dataset d JOIN "user" u ON d.owner_uid = u.uid
                                     WHERE u.email = split_part(ltrim(f.dvp, '/'), '/', 1)
                                       AND d.name  = split_part(ltrim(f.dvp, '/'), '/', 2))
                        THEN to_jsonb('/dataset/' || ltrim(f.dvp, '/'))
                        ELSE COALESCE(op #> '{operatorProperties,datasetVersionPath}', 'null'::jsonb)
                    END,
                    false
                )
                ORDER BY ord
            )
            FROM jsonb_array_elements(
                     CASE
                         WHEN jsonb_typeof(wv.content::jsonb -> 'operators') = 'array'
                             THEN wv.content::jsonb -> 'operators'
                         ELSE '[]'::jsonb
                     END
                 ) WITH ORDINALITY AS t(op, ord),
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

    RAISE NOTICE 'Normalized the resource-type path prefix in % workflow and % workflow_version row(s).', wf_count, wv_count;
END $$;

COMMIT;
