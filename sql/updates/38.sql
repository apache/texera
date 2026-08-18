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

-- The Lakekeeper catalog name is no longer derived from the user-facing name
-- (#7753), and the table already has its own `name` column -- so the old
-- `warehouse_name` was ambiguous. Rename it to sit beside its sibling
-- `lakekeeper_warehouse_id`. The table is empty in every deployment (the
-- warehouse feature flag is off everywhere), so this carries no data.
ALTER TABLE user_warehouse
    RENAME COLUMN warehouse_name TO lakekeeper_warehouse_name;

COMMIT;
