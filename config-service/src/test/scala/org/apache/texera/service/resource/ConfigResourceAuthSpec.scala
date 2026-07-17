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
import io.dropwizard.auth.AuthValueFactoryProvider
import io.dropwizard.jackson.Jackson
import io.dropwizard.testing.junit5.ResourceExtension
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.MediaType
import jakarta.ws.rs.{GET, Path, Produces}
import org.apache.texera.auth.{JwtAuth, JwtAuthFilter, SessionUser, UnauthorizedExceptionMapper}
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Wires ConfigResource through the same Jersey auth pipeline production uses
// (JwtAuthFilter + RolesAllowedDynamicFeature) and fires HTTP requests with and
// without an Authorization header. /config/pre-login and /config/settings/public
// are the @PermitAll endpoints and must answer unauthenticated callers
// (bootstrap regression guard, same shape as the break that caused PR #5049 to
// be reverted in #5173). /config/gui and /config/user-system are @RolesAllowed;
// they must reject anonymous traffic with a 401 (now from JwtAuthFilter's eager
// check, not from a downstream RolesAllowedRequestFilter 403) and accept
// callers with a valid Bearer token. The embedded database backs the @PermitAll
// settings read, which executes its query even for anonymous callers.
class ConfigResourceAuthSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  // Mirror production's mapper: ConfigService bootstraps Dropwizard's default mapper
  // (Jackson.newObjectMapper) and registers DefaultScalaModule on top. Same call here.
  private val testMapper: ObjectMapper =
    Jackson.newObjectMapper().registerModule(DefaultScalaModule)

  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .setMapper(testMapper)
    .addProvider(classOf[JwtAuthFilter])
    .addProvider(classOf[UnauthorizedExceptionMapper])
    .addProvider(classOf[RolesAllowedDynamicFeature])
    // Production (AuthFeatures.register) binds this so @Auth SessionUser
    // parameters resolve; without it the /config/settings write endpoints
    // fail resource-model validation at startup.
    .addProvider(new AuthValueFactoryProvider.Binder(classOf[SessionUser]))
    .addResource(new ConfigResource)
    .addResource(new ConfigResourceAuthSpec.ProtectedProbe)
    .build()

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    resources.before()
  }

  override protected def afterAll(): Unit = {
    resources.after()
    shutdownDB()
  }

  private def regularToken(): String = {
    val u = new User()
    u.setUid(2)
    u.setName("test-regular")
    u.setEmail("test-regular@example.com")
    u.setGoogleId(null)
    u.setRole(UserRoleEnum.REGULAR)
    JwtAuth.jwtToken(JwtAuth.jwtClaims(u, expireInDays = 1))
  }

  private def adminToken(): String = {
    val u = new User()
    u.setUid(1)
    u.setName("test-admin")
    u.setEmail("test-admin@example.com")
    u.setGoogleId(null)
    u.setRole(UserRoleEnum.ADMIN)
    JwtAuth.jwtToken(JwtAuth.jwtClaims(u, expireInDays = 1))
  }

  "GET /config/pre-login" should "return 200 without an Authorization header" in {
    val response = resources.target("/config/pre-login").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 200
  }

  it should "expose exactly the fields the login UI needs and nothing else" in {
    // Locking down the payload keeps anonymous callers from reading workspace flags,
    // feature toggles, or session timers. If a new field is needed before login, it
    // must be added here explicitly; the assertion forces that decision into review.
    val payload = resources
      .target("/config/pre-login")
      .request(MediaType.APPLICATION_JSON)
      .get(classOf[Map[String, Any]])
    payload.keySet shouldBe Set(
      "localLogin",
      "googleLogin",
      "defaultLocalUser",
      "attributionEnabled",
      "deploymentVersionCheckEnabled",
      "inviteOnly"
    )
  }

  "GET /config/gui" should "return 401 with a Bearer challenge without an Authorization header" in {
    val response = resources.target("/config/gui").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 200 with a valid Bearer token whose role matches @RolesAllowed" in {
    val response = resources
      .target("/config/gui")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get()
    response.getStatus shouldBe 200
  }

  it should "not leak any pre-login field through the authenticated payload" in {
    // The split is only meaningful if /gui drops the fields that /pre-login owns.
    // Without this, a future refactor could re-add them under the @RolesAllowed
    // endpoint, doubling the surface and creating two sources of truth.
    val payload = resources
      .target("/config/gui")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get(classOf[Map[String, Any]])
    payload.keySet should contain noneOf (
      "localLogin",
      "googleLogin",
      "defaultLocalUser",
      "attributionEnabled"
    )
  }

  "GET /config/user-system" should "return 401 with a Bearer challenge without an Authorization header" in {
    val response =
      resources.target("/config/user-system").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 200 with a valid Bearer token whose role matches @RolesAllowed" in {
    val response = resources
      .target("/config/user-system")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get()
    response.getStatus shouldBe 200
  }

  "GET /config/amber" should "return 401 with a Bearer challenge without an Authorization header" in {
    val response =
      resources.target("/config/amber").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 200 with a valid Bearer token whose role matches @RolesAllowed" in {
    val response = resources
      .target("/config/amber")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get()
    response.getStatus shouldBe 200
  }

  it should "expose the engine config separated from the gui payload" in {
    // The endpoint exists to keep engine configs out of /config/gui (see PR #5545).
    // Pin that defaultDataTransferBatchSize is served here and not folded back into gui.
    val amberPayload = resources
      .target("/config/amber")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get(classOf[Map[String, Any]])
    amberPayload.keySet should contain("defaultDataTransferBatchSize")

    val guiPayload = resources
      .target("/config/gui")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get(classOf[Map[String, Any]])
    guiPayload.keySet should not contain "defaultDataTransferBatchSize"
  }

  // /config/settings is the site_settings API: /settings/public serves the
  // user-visible keys (gui/dataset sections of default.conf) to anonymous
  // callers — the values render on the logged-out shell (custom logo, Hub/About
  // sidebar entries) — while the bulk read, single-key read, and all mutation
  // are ADMIN-only. This spec pins the auth gates; the positive read/write
  // bodies live in ConfigResourceSpec.
  "GET /config/settings/public" should "return 200 without an Authorization header (anonymous branding read)" in {
    val response =
      resources.target("/config/settings/public").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 200
  }

  "GET /config/settings" should "return 401 with a Bearer challenge without an Authorization header" in {
    val response =
      resources.target("/config/settings").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 403 for a REGULAR user (bulk management read is ADMIN-only)" in {
    val response = resources
      .target("/config/settings")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get()
    response.getStatus shouldBe 403
  }

  "GET /config/settings/{key}" should "return 401 with a Bearer challenge without an Authorization header" in {
    val response =
      resources.target("/config/settings/logo").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 403 for a REGULAR user (management read is ADMIN-only)" in {
    val response = resources
      .target("/config/settings/logo")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .get()
    response.getStatus shouldBe 403
  }

  "PUT /config/settings/{key}" should "return 401 without an Authorization header" in {
    val response = resources
      .target("/config/settings/logo")
      .request(MediaType.APPLICATION_JSON)
      .put(Entity.json("""{"key":"logo","value":"x"}"""))
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 403 for a REGULAR user" in {
    val response = resources
      .target("/config/settings/logo")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .put(Entity.json("""{"key":"logo","value":"x"}"""))
    response.getStatus shouldBe 403
  }

  "POST /config/settings/reset/{key}" should "return 403 for a REGULAR user" in {
    val response = resources
      .target("/config/settings/reset/logo")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${regularToken()}")
      .post(Entity.json("{}"))
    response.getStatus shouldBe 403
  }

  it should "pass the role gate for an ADMIN (404 for a key with no default)" in {
    val response = resources
      .target("/config/settings/reset/no-such-key")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${adminToken()}")
      .post(Entity.json("{}"))
    response.getStatus shouldBe 404
  }

  "GET an @RolesAllowed probe endpoint" should "return 401 without an Authorization header" in {
    // Sanity: JwtAuthFilter is now eager — missing Authorization is rejected
    // by the filter itself with a 401 + Bearer challenge, before
    // RolesAllowedDynamicFeature ever sees the request. Pre-eager behavior
    // here was a 403 from the role filter; the test pins the new contract.
    val response =
      resources.target("/auth-probe").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 200 with a valid Bearer token whose role matches @RolesAllowed" in {
    // Positive-direction sibling to the previous test. Without this, a filter-
    // priority bug that lets RolesAllowedRequestFilter run *before* JwtAuthFilter
    // is invisible to the spec: the no-auth case still 403s, and the only path
    // that actually exercises auth → authz ordering is "valid JWT → 200". Manual
    // integration testing of PR #5199 found this: a real admin JWT was getting
    // 403 on every @RolesAllowed endpoint until JwtAuthFilter was pinned to
    // Priorities.AUTHENTICATION.
    val response = resources
      .target("/auth-probe")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${adminToken()}")
      .get()
    response.getStatus shouldBe 200
  }
}

object ConfigResourceAuthSpec {
  // A deliberately @RolesAllowed companion to ConfigResource, so the same setup also
  // proves the feature actually rejects when it should — a 200 on the @PermitAll
  // endpoint would otherwise be consistent with the feature being silently no-op'd.
  @Path("/auth-probe")
  @Produces(Array(MediaType.APPLICATION_JSON))
  class ProtectedProbe {
    @GET
    @RolesAllowed(Array("REGULAR", "ADMIN"))
    def probe: String = "should never reach this"
  }
}
