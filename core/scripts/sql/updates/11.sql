/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


\c texera_db

SET search_path TO texera_db;

BEGIN;
ALTER TABLE time_log
    ADD COLUMN IF NOT EXISTS acc_creation TIMESTAMPTZ;

INSERT INTO time_log (uid)
SELECT u.uid
FROM "user" u
ON CONFLICT (uid) DO NOTHING;

WITH ts AS (SELECT now() AS t)
UPDATE time_log t
SET acc_creation = ts.t
FROM ts
WHERE t.acc_creation IS NULL;

ALTER TABLE time_log
    ALTER COLUMN acc_creation SET NOT NULL,
    ALTER COLUMN acc_creation SET DEFAULT now();

COMMIT;

BEGIN;

CREATE OR REPLACE FUNCTION time_log_autocreate()
RETURNS trigger AS $$
BEGIN
    INSERT INTO time_log (uid, acc_creation)
    VALUES (NEW.uid, now())
    ON CONFLICT (uid) DO NOTHING;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_time_log_autocreate ON "user";
CREATE TRIGGER trg_time_log_autocreate
AFTER INSERT ON "user"
FOR EACH ROW
EXECUTE FUNCTION time_log_autocreate();

COMMIT;