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

package org.apache.texera.dao

/**
  * SQLSTATE codes callers match on when turning a `DataAccessException` into an HTTP response.
  *
  * These live beside [[SqlServer]] rather than in whichever resource happens to need one first:
  * the code is a property of the database, not of any one endpoint, and the callers that catch it
  * sit in unrelated packages. A constant owned by one of those packages is unreachable from the
  * rest, so each would keep its own literal.
  */
object SqlStates {

  /** Postgres unique-violation: a unique constraint or primary key was already satisfied. */
  val UNIQUE_VIOLATION = "23505"
}
