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

package org.apache.texera.web.observability.gateway

/**
  * Typed query builders. Each takes a validated DTO + the caller's
  * resolved scope and returns a backend-specific query string + the
  * query parameters that should accompany it.
  *
  * Security invariant: no field of the input DTO is concatenated into
  * the output query without first passing through a typed accessor.
  * Even free-text fields are emitted only as escaped *values* in the
  * query DSL — never as DSL syntax.
  *
  * Each builder is pure (no side effects, no I/O). Exhaustive
  * injection tests live in BuildersSpec.
  */

/** Tenancy / scope envelope. Computed by [[ObservabilityScope]];
  *  every builder consumes it so the caller cannot widen scope.
  */
case class GatewayScope(
    userId: Long,
    allowedWorkflowIds: Set[Long],
    allowedProjectIds: Set[Long]
) {

  /** Allowed list joined as the typed parameter to a backend query.
    *  Empty allowed-set yields "0" (a workflow id that cannot exist),
    *  which produces a zero-result query without breaking syntax.
    */
  def workflowIdsCsv: String = {
    if (allowedWorkflowIds.isEmpty) "0"
    else allowedWorkflowIds.toSeq.sorted.mkString(",")
  }

  /** Allowed list joined as a regex-alternation body (no anchors, no
    *  parens). For use inside a LogsQL stream filter as
    *  ``field=~"^(<body>)$"``. Empty allow-set yields "0" — a numeric
    *  literal that matches nothing real and keeps regex syntax valid.
    */
  def workflowIdsRegexAlt: String = {
    if (allowedWorkflowIds.isEmpty) "0"
    else allowedWorkflowIds.toSeq.sorted.mkString("|")
  }
}
