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

package org.apache.texera.service.resource

import jakarta.annotation.security.{PermitAll, RolesAllowed}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AccessControlResourcePermissionsSpec extends AnyFlatSpec with Matchers {

  // /auth/* is the ExtAuth endpoint Envoy calls — it validates JWTs itself.
  // Requiring a JWT-bearing role here would be circular and lock the gateway out.
  "AccessControlResource" should "be @PermitAll because it IS the auth check" in {
    val permit = classOf[AccessControlResource].getAnnotation(classOf[PermitAll])
    val roles = classOf[AccessControlResource].getAnnotation(classOf[RolesAllowed])
    permit should not be null
    roles shouldBe null
  }

  // /chat/* proxies copilot requests to LiteLLM; only logged-in users should burn LLM credits.
  "LiteLLMProxyResource" should "require REGULAR or ADMIN role" in {
    val roles = classOf[LiteLLMProxyResource].getAnnotation(classOf[RolesAllowed])
    roles should not be null
    roles.value() should contain theSameElementsAs Array("REGULAR", "ADMIN")
  }

  // /models is used by the copilot UI; same reasoning as the proxy.
  "LiteLLMModelsResource" should "require REGULAR or ADMIN role" in {
    val roles = classOf[LiteLLMModelsResource].getAnnotation(classOf[RolesAllowed])
    roles should not be null
    roles.value() should contain theSameElementsAs Array("REGULAR", "ADMIN")
  }
}
