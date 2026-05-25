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

package org.apache.texera.auth

import jakarta.annotation.Priority
import jakarta.ws.rs.Priorities
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JwtAuthFilterSpec extends AnyFlatSpec with Matchers {

  // Regression guard: without an explicit @Priority(AUTHENTICATION) on this
  // filter, Jersey defaults to Priorities.USER (5000), which runs AFTER
  // RolesAllowedRequestFilter (AUTHORIZATION = 2000). The authz filter would
  // then see no principal and 403 every authenticated request with
  // "User not authorized." — even for ADMIN tokens.
  "JwtAuthFilter" should "carry @Priority(Priorities.AUTHENTICATION) so it runs before authorization" in {
    val priority = classOf[JwtAuthFilter].getAnnotation(classOf[Priority])
    priority should not be null
    priority.value() shouldBe Priorities.AUTHENTICATION
  }

  it should "run before Jersey's RolesAllowedRequestFilter priority" in {
    val priority = classOf[JwtAuthFilter].getAnnotation(classOf[Priority]).value()
    priority should be < Priorities.AUTHORIZATION
  }
}
