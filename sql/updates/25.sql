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

-- Adds the feedback table, used to persist free-text feedback messages
-- submitted by users from the dashboard. Each row is one feedback message
-- owned by a user; deleting the user cascades to their feedback.
CREATE TABLE IF NOT EXISTS feedback
(
    fid           SERIAL PRIMARY KEY,
    uid           INT NOT NULL,
    message       TEXT NOT NULL,
    creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (uid) REFERENCES "user"(uid) ON DELETE CASCADE
);

COMMIT;
