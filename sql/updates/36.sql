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

-- Allow ORCID as an identity provider in auth_provider.provider_type.
--
-- ORCID is authorization-code OAuth rather than Google's id-token flow, but the identity it
-- yields lands in the same place: one auth_provider row whose provider_id is the ORCID iD.
--
-- `ADD VALUE IF NOT EXISTS` rather than a recreate: dropping and recreating the type would
-- require dropping the column that uses it. The new label is only added here, never used, so
-- this is safe inside the transaction on PG12+.

\c texera_db

SET search_path TO texera_db;

BEGIN;

ALTER TYPE provider_type_enum ADD VALUE IF NOT EXISTS 'ORCID';

COMMIT;