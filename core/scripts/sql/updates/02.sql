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
