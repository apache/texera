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

-- Remove the deprecated "project" feature (user projects: colour-tagged
-- collections of workflows, with sharing and public projects). See issue #5172.
--
-- IRREVERSIBLE, USER-VISIBLE DATA LOSS: every project (name, description,
-- colour), every workflow-to-project assignment, every project access grant and
-- every public-project listing is deleted. Workflows, datasets and their own
-- access grants are untouched -- but a workflow that a user could previously
-- reach ONLY through a project share becomes inaccessible to that user, because
-- workflow access is now decided solely by workflow_user_access. Operators who
-- want to preserve those grants must copy them into workflow_user_access BEFORE
-- applying this migration.
--
-- Dropped in FK-dependency order (children first). The pgroonga index
-- idx_project_pgroonga is dropped implicitly with its table.
DROP TABLE IF EXISTS public_project;
DROP TABLE IF EXISTS project_user_access;
DROP TABLE IF EXISTS workflow_of_project;
DROP TABLE IF EXISTS project;

-- The gui.tabs.projects_enabled key is gone from default.conf, so this seeded
-- row would be orphaned: it is no longer served by /config/settings/public and
-- the admin update endpoint rejects keys with no default.conf entry.
DELETE FROM site_settings WHERE key = 'projects_enabled';

COMMIT;
