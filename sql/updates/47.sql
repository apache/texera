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
    ADD COLUMN IF NOT EXISTS published_description TEXT,
    -- Which view the pinned copy opens in. The form's definition rides inside published_content, so
    -- without freezing this too the public would get the live preference over the frozen graph --
    -- a form view on a copy that has no form in it. The type comes from 44.sql, which always runs
    -- first: a fresh database applies the changelog in order, and an existing one has it already.
    ADD COLUMN IF NOT EXISTS published_default_view default_view_enum;

-- No backfill. Every workflow that is public today has no pin, which is the following state, which is
-- exactly what it does today: deploying this migration changes nothing anyone can see. Pinning is
-- something an author opts into afterwards.

-- A pin only means something while the workflow is public, so a private workflow must not carry one.
-- Making that unrepresentable is cheaper than catching it in every path that could break it.
-- Dropped and re-added rather than guarded on a name lookup: constraint names are unique per table,
-- not per database, so a same-named constraint on any other table would make the guard skip this one
-- and leave the workflow table without it. Mirrors texera_ddl.sql.
--
-- All four columns are covered, not just the content: they describe one copy, so they have to be
-- either wholly absent or on a public workflow. Guarding the content alone would let a path that
-- clears three of them leave the fourth behind on a private row.
ALTER TABLE workflow
    DROP CONSTRAINT IF EXISTS workflow_pin_requires_public;
ALTER TABLE workflow
    ADD CONSTRAINT workflow_pin_requires_public
        CHECK (
            (published_content IS NULL AND published_name IS NULL
                AND published_description IS NULL AND published_version_id IS NULL
                AND published_default_view IS NULL)
                OR (is_public AND published_content IS NOT NULL AND published_name IS NOT NULL
                AND published_default_view IS NOT NULL)
            );

COMMIT;

-- Fulltext index over the pinned copy, mirroring the latest-content index built in texera_ddl.sql.
-- Public search matches a pinned workflow against its pinned name, description and content rather
-- than the live ones, so those three need an index of their own; unpinned rows keep using the
-- latest-content index. The expression has to match the one public search will build against these
-- columns, or the planner cannot use it.
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
