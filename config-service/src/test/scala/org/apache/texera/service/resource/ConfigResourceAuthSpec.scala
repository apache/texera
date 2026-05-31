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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.dropwizard.jackson.Jackson
import io.dropwizard.testing.junit5.ResourceExtension
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{GET, Path, Produces}
import org.apache.texera.auth.{JwtAuth, JwtAuthFilter}
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Wires ConfigResource through the same Jersey auth pipeline production uses
// (JwtAuthFilter + RolesAllowedDynamicFeature) and fires HTTP requests with no
// Authorization header. Regression guard for the bootstrap break that caused
// PR #5049 to be reverted in #5173: /config/gui and /config/user-system are
// loaded by the frontend's APP_INITIALIZER before any login, so they must
// return 200 to unauthenticated callers even with role enforcement enabled.
class ConfigResourceAuthSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Mirror production's mapper: ConfigService bootstraps Dropwizard's default mapper
  // (Jackson.newObjectMapper) and registers DefaultScalaModule on top. Same call here.
  private val testMapper: ObjectMapper =
    Jackson.newObjectMapper().registerModule(DefaultScalaModule)

  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .setMapper(testMapper)
    .addProvider(classOf[JwtAuthFilter])
    .addProvider(classOf[RolesAllowedDynamicFeature])
    .addResource(new ConfigResource)
    .addResource(new ConfigResourceAuthSpec.ProtectedProbe)
    .build()

  override protected def beforeAll(): Unit = resources.before()
  override protected def afterAll(): Unit = resources.after()

  "GET /config/gui" should "return 200 without an Authorization header" in {
    val response = resources.target("/config/gui").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 200
  }

  "GET /config/user-system" should "return 200 without an Authorization header" in {
    val response =
      resources.target("/config/user-system").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 200
  }

  "GET an @RolesAllowed endpoint" should "return 403 without an Authorization header" in {
    // Sanity: with no SecurityContext set by JwtAuthFilter, RolesAllowedDynamicFeature
    // must reject. Catches the case where the feature is registered but somehow
    // disabled (e.g. swallowed exception during setup).
    val response =
      resources.target("/auth-probe").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 403
  }

  it should "return 200 with a valid Bearer token whose role matches @RolesAllowed" in {
    // Positive-direction sibling to the previous test. Without this, a filter-
    // priority bug that lets RolesAllowedRequestFilter run *before* JwtAuthFilter
    // is invisible to the spec: the no-auth case still 403s, the @PermitAll cases
    // still 200, and the only path that actually exercises auth → authz ordering
    // is "valid JWT → 200". Manual integration testing of PR #5199 found this:
    // a real admin JWT was getting 403 on every @RolesAllowed endpoint until
    // JwtAuthFilter was pinned to Priorities.AUTHENTICATION.
    val u = new User()
    u.setUid(1)
    u.setName("test-admin")
    u.setEmail("test-admin@example.com")
    u.setGoogleId(null)
    u.setRole(UserRoleEnum.ADMIN)
    val token = JwtAuth.jwtToken(JwtAuth.jwtClaims(u, expireInDays = 1))
    val response = resources
      .target("/auth-probe")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer $token")
      .get()
    response.getStatus shouldBe 200
  }
}

object ConfigResourceAuthSpec {
  // A deliberately @RolesAllowed companion to ConfigResource, so the same setup also
  // proves the feature actually rejects when it should — a 200 on the @PermitAll
  // endpoints would otherwise be consistent with the feature being silently no-op'd.
  @Path("/auth-probe")
  @Produces(Array(MediaType.APPLICATION_JSON))
  class ProtectedProbe {
    @GET
    @RolesAllowed(Array("REGULAR", "ADMIN"))
    def probe: String = "should never reach this"
  }
}
