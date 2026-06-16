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

import jakarta.annotation.security.{DenyAll, PermitAll, RolesAllowed}
import jakarta.ws.rs.{DELETE, GET, POST}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RoleAnnotationEnforcerSpec extends AnyFlatSpec with Matchers {

  "findUnannotatedEndpoints" should "return nothing when every HTTP method is annotated" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.FullyAnnotatedResource])
    ) shouldBe empty
  }

  it should "flag an HTTP method with no security annotation" in {
    val violations =
      RoleAnnotationEnforcer.findUnannotatedEndpoints(
        Seq(classOf[RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource])
      )
    violations should have size 1
    violations.head should endWith("#openEndpoint")
  }

  it should "treat a class-level annotation as covering every method" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.ClassLevelResource])
    ) shouldBe empty
  }

  it should "accept @PermitAll and @DenyAll, not only @RolesAllowed" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.PermitAndDenyResource])
    ) shouldBe empty
  }

  it should "ignore methods that are not HTTP-mapped" in {
    // helper has no @RolesAllowed but is not a JAX-RS endpoint, so it is not a hole
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.NonEndpointMethodResource])
    ) shouldBe empty
  }

  "enforce" should "throw when an endpoint is unannotated" in {
    val ex = intercept[IllegalStateException] {
      RoleAnnotationEnforcer.enforce(
        Seq(classOf[RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource]),
        "TestService"
      )
    }
    ex.getMessage should include("TestService")
    ex.getMessage should include("openEndpoint")
  }

  "enforce" should "not throw when every endpoint is annotated" in {
    noException should be thrownBy RoleAnnotationEnforcer.enforce(
      Seq(classOf[RoleAnnotationEnforcerSpec.FullyAnnotatedResource]),
      "TestService"
    )
  }
}

object RoleAnnotationEnforcerSpec {

  class FullyAnnotatedResource {
    @GET @RolesAllowed(Array("REGULAR")) def read: String = ""
    @POST @PermitAll def create: String = ""
  }

  class PartiallyAnnotatedResource {
    @GET @RolesAllowed(Array("ADMIN")) def securedEndpoint: String = ""
    @POST def openEndpoint: String = ""
  }

  @RolesAllowed(Array("ADMIN"))
  class ClassLevelResource {
    @GET def read: String = ""
    @DELETE def remove: String = ""
  }

  class PermitAndDenyResource {
    @PermitAll @GET def open: String = ""
    @DenyAll @POST def closed: String = ""
  }

  class NonEndpointMethodResource {
    @GET @RolesAllowed(Array("REGULAR")) def read: String = ""
    def helper: String = ""
  }
}
