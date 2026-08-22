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

-- Version pinning: a public workflow follows the author's latest until they pin the version they
-- have now, after which the public keeps seeing that frozen copy. is_public stays the on/off switch;
-- published_content is the pin, NULL while following. Materialized rather than replayed from
-- workflow_version, whose rows are reverse deltas that no fulltext index can cover.
ALTER TABLE workflow
    -- The version row holding the pinned copy. Its delta is the identity patch, so replaying it
    -- returns exactly what is on public show; the revision panel marks that row, which is how the
    -- author restores the public version into their editor.
    ADD COLUMN IF NOT EXISTS published_version_id  INT,
    ADD COLUMN IF NOT EXISTS published_content     TEXT,
    ADD COLUMN IF NOT EXISTS published_name        VARCHAR(128),
    ADD COLUMN IF NOT EXISTS published_description TEXT;

-- No backfill. Every workflow that is public today has no pin, which is the following state, which is
-- exactly what it does today: deploying this migration changes nothing anyone can see. Pinning is
-- something an author opts into afterwards.

-- A pin only means something while the workflow is public, so a private workflow must not carry one.
-- Making that unrepresentable is cheaper than catching it: unpublishing clears the pin, and no other
-- path writes these columns.
DO
$$
    BEGIN
        ALTER TABLE workflow DROP CONSTRAINT IF EXISTS workflow_published_consistent;
        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'workflow_pin_requires_public') THEN
            ALTER TABLE workflow
                ADD CONSTRAINT workflow_pin_requires_public
                    CHECK (published_content IS NULL OR is_public);
        END IF;
    END
$$;

COMMIT;

-- Fulltext index over the pinned copy, mirroring the latest-content index built in texera_ddl.sql.
-- Public search matches a pinned workflow against its pinned name, description and content rather
-- than the live ones, so those three need an index of their own; unpinned rows keep using the
-- latest-content index. The expression has to match the one the query builds, or the planner cannot
-- use it -- see WorkflowSearchQueryBuilder.onVisibleCopy.
-- Runs outside the transaction above because the plugin probe issues its own commands.
DO
$$
    DECLARE
        stem_filter   TEXT := '';
        plugin_status TEXT;
    BEGIN
        DROP INDEX IF EXISTS idx_workflow_published_pgroonga;

        WITH plugin_registration AS (SELECT pgroonga_command('plugin_register token_filters/stem') AS result)
        SELECT CASE
                   WHEN result::jsonb @> '[true]' THEN 'Plugin registered successfully'
                   ELSE 'Plugin registration failed'
                   END
        INTO plugin_status
        FROM plugin_registration;

        IF plugin_status = 'Plugin registered successfully' THEN
            stem_filter := ', plugins=''token_filters/stem'', token_filters=''TokenFilterStem''';
        END IF;

        EXECUTE format(
                'CREATE INDEX idx_workflow_published_pgroonga ON workflow USING pgroonga ' ||
                '((COALESCE(published_name, '''') || '' '' || COALESCE(published_description, '''') || '' '' || COALESCE(published_content, ''''))) ' ||
                'WITH (tokenizer = ''TokenMecab''%s);',
                stem_filter
                );
    END
$$;
