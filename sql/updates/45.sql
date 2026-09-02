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

-- Allow Sign in with Apple as an identity provider in auth_provider.provider_type.
--
-- Postgres forbids *using* a new enum value in the transaction that adds it. Nothing here
-- inserts an APPLE row, so the value is only declared; the first Apple login writes it.
--
-- The type is schema-qualified because the two runners disagree about the search path: the
-- liquibase runner in sql/docker-compose.yml strips `SET search_path` out of these files before
-- applying them, while bin/local-dev.sh keeps it.

\c texera_db

SET search_path TO texera_db;

BEGIN;

ALTER TYPE texera_db.provider_type_enum ADD VALUE IF NOT EXISTS 'APPLE';

COMMIT;
