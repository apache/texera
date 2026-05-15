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

-- Discriminator for workflow rows: WORKFLOW = top-level workflows surfaced in
-- the Workflows tab; MACRO = reusable subgraphs surfaced in the operator
-- palette and a separate Macros tab. Reusing the workflow table inherits
-- versioning, ACL, and hub features for free.
DO $$ BEGIN
    CREATE TYPE workflow_kind_enum AS ENUM ('WORKFLOW', 'MACRO');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

ALTER TABLE workflow
    ADD COLUMN IF NOT EXISTS kind workflow_kind_enum NOT NULL DEFAULT 'WORKFLOW';

CREATE INDEX IF NOT EXISTS idx_workflow_kind ON workflow(kind);

-- Denormalized macro descriptor used by palette/listing endpoints so they do
-- not have to parse workflow.content (a JSON-serialized LogicalPlan) per row.
-- port_spec captures the macro's declared external inputs/outputs; param_spec
-- captures promoted parameters (empty in v1, populated in Phase 2).
CREATE TABLE IF NOT EXISTS macro_metadata
(
    wid        INT PRIMARY KEY,
    port_spec  JSONB        NOT NULL,
    param_spec JSONB        NOT NULL DEFAULT '[]'::JSONB,
    category   VARCHAR(128),
    icon       VARCHAR(64),
    FOREIGN KEY (wid) REFERENCES workflow(wid) ON DELETE CASCADE
);

COMMIT;
