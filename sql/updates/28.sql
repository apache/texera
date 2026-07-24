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

-- Datasets are looked up by (owner, name) in the file service and the file
-- resolver, so this pair must be unique. Before adding the constraint,
-- deterministically rename any pre-existing duplicates (kept: the oldest did;
-- renamed: name suffixed with "-<did>", truncated to fit VARCHAR(128)).
-- Each rename is reported via RAISE NOTICE: this is a user-visible data
-- change, and workflows that reference a renamed dataset by path will resolve
-- to the surviving dataset afterward, so operators should review the notices
-- and notify the affected dataset owners.
DO $$
DECLARE
    rec RECORD;
    renamed INT := 0;
    iterations INT := 0;
BEGIN
    LOOP
        FOR rec IN
            UPDATE dataset d
            SET name = LEFT(d.name, 128 - LENGTH('-' || d.did::text)) || '-' || d.did::text
            FROM (
                SELECT did, name AS old_name,
                       ROW_NUMBER() OVER (PARTITION BY owner_uid, name ORDER BY did) AS rn
                FROM dataset
            ) dups
            WHERE d.did = dups.did AND dups.rn > 1
            RETURNING d.did, d.owner_uid, dups.old_name, d.name AS new_name
        LOOP
            renamed := renamed + 1;
            RAISE NOTICE 'Renamed duplicate dataset did=% (owner_uid=%): "%" -> "%"',
                rec.did, rec.owner_uid, rec.old_name, rec.new_name;
        END LOOP;

        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM dataset GROUP BY owner_uid, name HAVING COUNT(*) > 1
        );

        iterations := iterations + 1;
        IF iterations > 10 THEN
            RAISE EXCEPTION 'Could not deduplicate dataset (owner_uid, name) pairs after 10 passes; resolve duplicates manually before re-running.';
        END IF;
    END LOOP;

    IF renamed > 0 THEN
        RAISE NOTICE 'Renamed % duplicate dataset name(s) in total; workflows referencing the old names now resolve to the surviving datasets.', renamed;
    END IF;
END $$;

ALTER TABLE dataset
    ADD CONSTRAINT dataset_owner_uid_name_key UNIQUE (owner_uid, name);

COMMIT;
