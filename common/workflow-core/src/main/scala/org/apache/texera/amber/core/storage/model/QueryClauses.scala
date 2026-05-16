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

package org.apache.texera.amber.core.storage.model

/**
  * Row-level filter predicate for a VirtualDocument query.
  *
  * Values are sent as strings over the wire for simplicity; storage backends are
  * responsible for parsing them against the column's declared type (see
  * IcebergPredicateBuilder for the canonical implementation).
  *
  * Supported `op` values: eq, ne, lt, le, gt, ge, contains, startsWith, endsWith,
  * isNull, isNotNull, in.
  */
case class ColumnFilter(
    columnName: String,
    op: String,
    value: Option[String] = None,
    values: Option[Seq[String]] = None
)

/** Sort specification for a single column. `direction` must be "asc" or "desc". */
case class SortSpec(columnName: String, direction: String)
