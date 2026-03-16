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

-- ============================================
-- 1. Connect to the texera_db database
-- ============================================
\c texera_db
SET search_path TO texera_db, public;

-- ============================================
-- 2. Update the table schema
-- ============================================
BEGIN;

-- Add the 2 columns (defaults backfill existing rows safely)
ALTER TABLE dataset_upload_session
    ADD COLUMN IF NOT EXISTS file_size_bytes BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS part_size_bytes BIGINT NOT NULL DEFAULT 5242880;

-- Drop any old/alternate constraint names from previous attempts (so we end up with exactly the new names)
ALTER TABLE dataset_upload_session
    DROP CONSTRAINT IF EXISTS dataset_upload_session_num_parts_requested_positive,
    DROP CONSTRAINT IF EXISTS chk_dataset_upload_session_num_parts_requested_positive,
    DROP CONSTRAINT IF EXISTS chk_dataset_upload_session_file_size_bytes_positive,
    DROP CONSTRAINT IF EXISTS chk_dataset_upload_session_part_size_bytes_positive,
    DROP CONSTRAINT IF EXISTS dataset_upload_session_part_size_bytes_positive,
    DROP CONSTRAINT IF EXISTS dataset_upload_session_part_size_bytes_s3_upper_bound,
    DROP CONSTRAINT IF EXISTS chk_dataset_upload_session_part_size_bytes_s3_upper_bound;

-- Add constraints exactly like the new CREATE TABLE
ALTER TABLE dataset_upload_session
    ADD CONSTRAINT chk_dataset_upload_session_num_parts_requested_positive
        CHECK (num_parts_requested >= 1),
    ADD CONSTRAINT chk_dataset_upload_session_file_size_bytes_positive
        CHECK (file_size_bytes > 0),
    ADD CONSTRAINT chk_dataset_upload_session_part_size_bytes_positive
        CHECK (part_size_bytes > 0),
    ADD CONSTRAINT chk_dataset_upload_session_part_size_bytes_s3_upper_bound
        CHECK (part_size_bytes <= 5368709120);

-- Match CREATE TABLE (no defaults)
ALTER TABLE dataset_upload_session
    ALTER COLUMN file_size_bytes DROP DEFAULT,
    ALTER COLUMN part_size_bytes DROP DEFAULT;

COMMIT;
