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

import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import jakarta.annotation.security.{DenyAll, PermitAll, RolesAllowed}
import jakarta.ws.rs.{DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT}
import org.glassfish.jersey.server.ResourceConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

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

  it should "return nothing when given no resources" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(Seq.empty) shouldBe empty
  }

  it should "report every hole across multiple resources as fully-qualified Class#method" in {
    val violations = RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(
        classOf[RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource],
        classOf[RoleAnnotationEnforcerSpec.MultiHoleResource]
      )
    )
    violations should contain allOf (
      s"${classOf[RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource].getName}#openEndpoint",
      s"${classOf[RoleAnnotationEnforcerSpec.MultiHoleResource].getName}#put",
      s"${classOf[RoleAnnotationEnforcerSpec.MultiHoleResource].getName}#patch"
    )
  }

  it should "detect verbs beyond GET/POST/DELETE via the @HttpMethod meta-annotation" in {
    val violations = RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.AllVerbsUnannotatedResource])
    )
    violations.map(_.split("#").last) should contain theSameElementsAs
      Seq("put", "patch", "head", "options")
  }

  it should "treat a security annotation inherited from a superclass method as covering it" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.InheritsAnnotatedEndpoint])
    ) shouldBe empty
  }

  it should "let a subclass class-level annotation cover an inherited unannotated endpoint" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.SecuredSubclass])
    ) shouldBe empty
  }

  it should "flag an inherited unannotated endpoint against the scanned subclass" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq(classOf[RoleAnnotationEnforcerSpec.UnsecuredSubclass])
    ) should contain(
      s"${classOf[RoleAnnotationEnforcerSpec.UnsecuredSubclass].getName}#inheritedWrite"
    )
  }

  it should "deduplicate when the same resource is scanned more than once" in {
    RoleAnnotationEnforcer.findUnannotatedEndpoints(
      Seq.fill(3)(classOf[RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource])
    ) should have size 1
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

  it should "list every offending endpoint in the thrown message" in {
    val ex = intercept[IllegalStateException] {
      RoleAnnotationEnforcer.enforce(
        Seq(classOf[RoleAnnotationEnforcerSpec.MultiHoleResource]),
        "TestService"
      )
    }
    ex.getMessage should include("#put")
    ex.getMessage should include("#patch")
  }

  it should "not throw when given no resources" in {
    noException should be thrownBy RoleAnnotationEnforcer.enforce(Seq.empty, "TestService")
  }

  "enforce(ResourceConfig)" should "pass when every registered resource is annotated" in {
    val resourceConfig = new ResourceConfig()
    resourceConfig.register(classOf[RoleAnnotationEnforcerSpec.FullyAnnotatedResource])
    noException should be thrownBy RoleAnnotationEnforcer.enforce(resourceConfig, "TestService")
  }

  it should "throw when a registered resource class has an unannotated endpoint" in {
    val resourceConfig = new ResourceConfig()
    resourceConfig.register(classOf[RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource])
    val ex = intercept[IllegalStateException] {
      RoleAnnotationEnforcer.enforce(resourceConfig, "TestService")
    }
    ex.getMessage should include("TestService")
    ex.getMessage should include("openEndpoint")
  }

  it should "throw when a resource registered as an instance has an unannotated endpoint" in {
    val resourceConfig = new ResourceConfig()
    resourceConfig.register(new RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource)
    an[IllegalStateException] should be thrownBy
      RoleAnnotationEnforcer.enforce(resourceConfig, "TestService")
  }

  it should "still fail closed when error logging is disabled" in {
    // enforce logs the violation at error level before throwing. Even with error
    // logging suppressed, enforcement must still throw rather than silently pass.
    val backendLogger = LoggerFactory
      .getLogger(RoleAnnotationEnforcer.getClass.getName)
      .asInstanceOf[LogbackLogger]
    val previousLevel = backendLogger.getLevel
    backendLogger.setLevel(Level.OFF)
    try {
      an[IllegalStateException] should be thrownBy
        RoleAnnotationEnforcer.enforce(
          Seq(classOf[RoleAnnotationEnforcerSpec.PartiallyAnnotatedResource]),
          "TestService"
        )
    } finally {
      backendLogger.setLevel(previousLevel)
    }
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

  // One secured endpoint plus two holes on distinct verbs.
  class MultiHoleResource {
    @GET @RolesAllowed(Array("ADMIN")) def get: String = ""
    @PUT def put: String = ""
    @PATCH def patch: String = ""
  }

  // Every method maps to a verb that is not GET/POST/DELETE; all are holes.
  class AllVerbsUnannotatedResource {
    @PUT def put: String = ""
    @PATCH def patch: String = ""
    @HEAD def head: String = ""
    @OPTIONS def options: String = ""
  }

  class AnnotatedBaseResource {
    @GET @PermitAll def inheritedOpen: String = ""
  }
  // Inherits an endpoint whose annotation lives on the superclass method.
  class InheritsAnnotatedEndpoint extends AnnotatedBaseResource

  class UnannotatedBaseResource {
    @PUT def inheritedWrite: String = ""
  }
  // Class-level annotation on the subclass covers the inherited unannotated endpoint.
  @RolesAllowed(Array("ADMIN"))
  class SecuredSubclass extends UnannotatedBaseResource
  // No annotation anywhere: the inherited endpoint is a hole.
  class UnsecuredSubclass extends UnannotatedBaseResource
}
