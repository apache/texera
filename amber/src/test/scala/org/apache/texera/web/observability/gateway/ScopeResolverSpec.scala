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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ScopeResolverSpec extends AnyFlatSpec with Matchers {

  private val scope =
    GatewayScope(userId = 42L, allowedWorkflowIds = Set(7L, 8L), allowedProjectIds = Set(1L))
  private val resolver = new ScopeResolver.Stub(scope)

  // ----- assertWorkflowAllowed ----------------------------------------

  "assertWorkflowAllowed" should "permit a workflow id inside the allow-set" in {
    resolver.assertWorkflowAllowed(scope, Some(7L)) shouldBe true
    resolver.assertWorkflowAllowed(scope, Some(8L)) shouldBe true
  }

  it should "reject a workflow id outside the allow-set (no widening possible)" in {
    resolver.assertWorkflowAllowed(scope, Some(999L)) shouldBe false
    resolver.assertWorkflowAllowed(scope, Some(-1L)) shouldBe false
  }

  it should "default to permitted when no workflow id is supplied" in {
    // Caller's full scope applies — None is the "default to my allowed
    // set" path used by LogsQLBuilder via scope.workflowIdsCsv.
    resolver.assertWorkflowAllowed(scope, None) shouldBe true
  }

  // ----- GatewayScope.workflowIdsCsv ----------------------------------

  "GatewayScope.workflowIdsCsv" should "render the allowed set as a sorted CSV" in {
    scope.workflowIdsCsv shouldBe "7,8"
  }

  it should "emit '0' for an empty allow-set so backend queries syntax-check" in {
    val empty = GatewayScope(0L, Set.empty, Set.empty)
    empty.workflowIdsCsv shouldBe "0"
  }

  // ----- GatewayScope.workflowIdsRegexAlt -----------------------------

  "GatewayScope.workflowIdsRegexAlt" should "render the allowed set as sorted '|'-alternation" in {
    scope.workflowIdsRegexAlt shouldBe "7|8"
  }

  it should "emit '0' for an empty allow-set" in {
    val empty = GatewayScope(0L, Set.empty, Set.empty)
    empty.workflowIdsRegexAlt shouldBe "0"
  }
}
