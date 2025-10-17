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

CREATE TABLE IF NOT EXISTS computing_unit_user_access
(
    cuid      INT NOT NULL,
    uid       INT NOT NULL,
    privilege privilege_enum NOT NULL DEFAULT 'NONE',
    PRIMARY KEY (cuid, uid),
    FOREIGN KEY (cuid) REFERENCES workflow_computing_unit(cuid) ON DELETE CASCADE,
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE
);