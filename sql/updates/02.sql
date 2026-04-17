-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

\c texera_db;

set search_path to texera_db, public;

CREATE EXTENSION IF NOT EXISTS pgroonga;

DO $$
DECLARE
  r RECORD;
  stem_filter TEXT := '';
  plugin_status TEXT;
BEGIN
  -- Drop all GIN and PGroonga indexes
  FOR r IN
    SELECT indexname FROM pg_indexes
    WHERE (indexdef ILIKE '%USING gin%' OR indexdef ILIKE '%USING pgroonga%')
    AND tablename IN ('workflow', 'user', 'project', 'dataset', 'dataset_version')
  LOOP
    EXECUTE format('DROP INDEX IF EXISTS %I;', r.indexname);
  END LOOP;

  -- Check if TokenFilterStem plugin is registered
  WITH plugin_registration AS (
    SELECT pgroonga_command('plugin_register token_filters/stem') AS result
  )
  SELECT
    CASE
      WHEN result::jsonb @> '[true]' THEN 'Plugin registered successfully'
      ELSE 'Plugin registration failed'
    END INTO plugin_status
  FROM plugin_registration;

  -- Set the stem_filter based on plugin status
  IF plugin_status = 'Plugin registered successfully' THEN
    stem_filter := ', plugins=''token_filters/stem'', token_filters=''TokenFilterStem''';
    RAISE NOTICE 'Using TokenMecab + TokenFilterStem';
  ELSE
    RAISE NOTICE 'Using TokenMecab only';
  END IF;

  -- Create PGroonga indexes dynamically with correct TokenFilterStem usage
  FOR r IN
    SELECT tablename,
           CASE
             WHEN tablename = 'workflow' THEN
               '(COALESCE(name, '''') || '' '' || COALESCE(description, '''') || '' '' || COALESCE(content, ''''))'
             WHEN tablename IN ('project', 'dataset') THEN
               '(COALESCE(name, '''') || '' '' || COALESCE(description, ''''))'
             ELSE
               'COALESCE(name, '''')'
           END AS index_column
    FROM (VALUES ('workflow'), ('user'), ('project'), ('dataset'), ('dataset_version')) AS t(tablename)
  LOOP
    -- Create PGroonga index with proper TokenFilterStem usage
    EXECUTE format(
      'CREATE INDEX idx_%s_pgroonga ON %I USING pgroonga (%s) WITH (tokenizer = ''TokenMecab''%s);',
      r.tablename, r.tablename, r.index_column, stem_filter
    );
  END LOOP;
END $$;
