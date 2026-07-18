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
import org.apache.texera.common.config.DefaultsConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.SITE_SETTINGS
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Covers ConfigResource from two directions against one embedded database.
//
// HTTP auth gates: wires the resource through the same Jersey auth pipeline
// production uses (JwtAuthFilter + RolesAllowedDynamicFeature) and fires
// requests with and without an Authorization header. /config/pre-login and
// /config/settings/public are the @PermitAll endpoints and must answer
// unauthenticated callers (bootstrap regression guard, same shape as the break
// that caused PR #5049 to be reverted in #5173); the remaining endpoints are
// @RolesAllowed and must reject anonymous traffic with a 401 from
// JwtAuthFilter's eager check.
//
// Endpoint bodies: calls the resource methods directly for the positive
// read/write paths — read-miss, insert, upsert-on-conflict, null-value 400,
// public-whitelist filtering, reset-to-default.
class ConfigResourceSpec
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
    .addResource(new ConfigResourceSpec.ProtectedProbe)
    .build()

  // Direct-call handle for the endpoint-body tests below.
  private val resource = new ConfigResource

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    resources.before()
  }

  override protected def afterAll(): Unit = {
    resources.after()
    shutdownDB()
  }

  private def adminSession(name: String = "test-admin"): SessionUser = {
    val u = new User()
    u.setUid(1)
    u.setName(name)
    new SessionUser(u)
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
  // are ADMIN-only. These tests pin the auth gates; the endpoint bodies are
  // covered by the direct-call tests further down.
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

  // ----- endpoint bodies, called directly against the embedded database -----

  "getSetting" should "return null for a key that has no row" in {
    resource.getSetting("no-such-key") shouldBe null
  }

  "updateSetting" should "insert a new row and record who wrote it" in {
    val response =
      resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "custom.png"))
    response.getStatus shouldBe 200

    val stored = resource.getSetting("logo")
    stored.settingKey shouldBe "logo"
    stored.settingValue shouldBe "custom.png"

    getDSLContext
      .select(SITE_SETTINGS.UPDATED_BY)
      .from(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq("logo"))
      .fetchOne(SITE_SETTINGS.UPDATED_BY) shouldBe "test-admin"
  }

  it should "update the existing row on a repeated PUT (upsert conflict path)" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "v1.png"))
    resource.updateSetting(
      adminSession("second-admin"),
      "logo",
      ConfigSettingPojo("logo", "v2.png")
    )

    resource.getSetting("logo").settingValue shouldBe "v2.png"
    getDSLContext.fetchCount(SITE_SETTINGS, SITE_SETTINGS.KEY.eq("logo")) shouldBe 1
    getDSLContext
      .select(SITE_SETTINGS.UPDATED_BY)
      .from(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq("logo"))
      .fetchOne(SITE_SETTINGS.UPDATED_BY) shouldBe "second-admin"
  }

  it should "reject a null value with 400 and leave the stored row untouched" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "kept.png"))
    val response = resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", null))
    response.getStatus shouldBe 400
    resource.getSetting("logo").settingValue shouldBe "kept.png"
  }

  it should "reject a null body with 400 rather than a 500 (empty/malformed request)" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "kept.png"))
    val response = resource.updateSetting(adminSession(), "logo", null)
    response.getStatus shouldBe 400
    resource.getSetting("logo").settingValue shouldBe "kept.png"
  }

  it should "reject a key with no default.conf entry with 400 and write nothing" in {
    val response =
      resource.updateSetting(adminSession(), "no-such-key", ConfigSettingPojo("no-such-key", "x"))
    response.getStatus shouldBe 400
    resource.getSetting("no-such-key") shouldBe null
  }

  "getPublicSettings" should "serve whitelisted keys and hide management-only ones" in {
    resource.updateSetting(adminSession(), "favicon", ConfigSettingPojo("favicon", "fav.ico"))
    resource.updateSetting(
      adminSession(),
      "csv_parser_max_columns",
      ConfigSettingPojo("csv_parser_max_columns", "4096")
    )

    val publicSettings = resource.getPublicSettings
    publicSettings("favicon") shouldBe "fav.ico"
    publicSettings should not contain key("csv_parser_max_columns")
  }

  // The public whitelist is derived from the gui/dataset sections of
  // default.conf. This pins the derived set, so moving a key between sections
  // (or adding one) forces the visibility decision into review here.
  it should "expose exactly the gui and dataset section keys of default.conf" in {
    DefaultsConfig.keysUnderSections(Set("gui", "dataset")) shouldBe Set(
      "logo",
      "mini_logo",
      "favicon",
      "hub_enabled",
      "home_enabled",
      "workflow_enabled",
      "dataset_enabled",
      "your_work_enabled",
      "projects_enabled",
      "workflows_enabled",
      "datasets_enabled",
      "compute_enabled",
      "quota_enabled",
      "forum_enabled",
      "about_enabled",
      "single_file_upload_max_size_mib",
      "multipart_upload_chunk_size_mib",
      "max_number_of_concurrent_uploading_file",
      "max_number_of_concurrent_uploading_file_chunks"
    )
  }

  "getAllSettings" should "serve every stored row, including management-only keys" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "all.png"))
    resource.updateSetting(
      adminSession(),
      "csv_parser_max_columns",
      ConfigSettingPojo("csv_parser_max_columns", "2048")
    )

    val allSettings = resource.getAllSettings
    allSettings("logo") shouldBe "all.png"
    allSettings("csv_parser_max_columns") shouldBe "2048"
  }

  "resetSetting" should "restore the default.conf value for a known key" in {
    resource.updateSetting(adminSession(), "logo", ConfigSettingPojo("logo", "overridden.png"))

    val response = resource.resetSetting(adminSession(), "logo")
    response.getStatus shouldBe 200
    resource.getSetting("logo").settingValue shouldBe DefaultsConfig.allDefaults("logo")
  }

  it should "return 404 for a key that has no default" in {
    resource.resetSetting(adminSession(), "no-such-key").getStatus shouldBe 404
  }

  it should "restore a management-only key (csv_parser_max_columns) that is seeded from default.conf" in {
    resource.updateSetting(
      adminSession(),
      "csv_parser_max_columns",
      ConfigSettingPojo("csv_parser_max_columns", "4096")
    )
    resource.resetSetting(adminSession(), "csv_parser_max_columns").getStatus shouldBe 200
    resource.getSetting("csv_parser_max_columns").settingValue shouldBe
      DefaultsConfig.allDefaults("csv_parser_max_columns")
  }
}

object ConfigResourceSpec {
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
