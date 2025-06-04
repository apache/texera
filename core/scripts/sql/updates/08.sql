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

\c texera_db

SET search_path TO texera_db;

DO $$
BEGIN
    -- 1. Create site_settings table if it does not exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'texera_db' AND table_name = 'site_settings'
    ) THEN
CREATE TABLE site_settings
(
    key         VARCHAR(255)  PRIMARY KEY,
    value TEXT  NOT NULL,
    updated_by  VARCHAR(50),
    updated_at  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);
END IF;
END
$$;