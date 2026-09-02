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

-- Hub engagement tables for models, mirroring dataset_user_likes / dataset_view_count.

CREATE TABLE IF NOT EXISTS model_user_likes
(
    uid INTEGER NOT NULL,
    mid INTEGER NOT NULL,
    PRIMARY KEY (uid, mid),
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE,
    FOREIGN KEY (mid) REFERENCES model(mid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS model_view_count
(
    mid        INTEGER NOT NULL,
    view_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (mid),
    FOREIGN KEY (mid) REFERENCES model(mid) ON DELETE CASCADE
);

COMMIT;

-- Full-text index for models. texera_ddl.sql builds this for a fresh database; a database
-- upgraded through the migrations needs it created here. The indexed expression must match
-- the predicate ModelSearchQueryBuilder renders (name || ' ' || description) exactly, or the
-- planner falls back to a sequential scan.
CREATE EXTENSION IF NOT EXISTS pgroonga;

DO $$
DECLARE
  index_expression TEXT := '((COALESCE(name, '''') || '' '' || COALESCE(description, '''')))';
  plugin_status TEXT;
BEGIN
  WITH plugin_registration AS (
    SELECT pgroonga_command('plugin_register token_filters/stem') AS result
  )
  SELECT
    CASE
      WHEN result::jsonb @> '[true]' THEN 'Plugin registered successfully'
      ELSE 'Plugin registration failed'
    END INTO plugin_status
  FROM plugin_registration;

  DROP INDEX IF EXISTS idx_model_pgroonga;

  IF plugin_status = 'Plugin registered successfully' THEN
    EXECUTE 'CREATE INDEX idx_model_pgroonga ON model USING pgroonga ' || index_expression ||
            ' WITH (tokenizer = ''TokenMecab'', plugins=''token_filters/stem'''
            ', token_filters=''TokenFilterStem'')';
  ELSE
    EXECUTE 'CREATE INDEX idx_model_pgroonga ON model USING pgroonga ' || index_expression ||
            ' WITH (tokenizer = ''TokenMecab'')';
  END IF;
END $$;
