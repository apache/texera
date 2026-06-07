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
import jakarta.ws.rs.client.Entity
import jakarta.ws.rs.core.MediaType
import org.apache.texera.auth.{JwtAuth, JwtAuthFilter, UnauthorizedExceptionMapper}
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.glassfish.jersey.server.filter.RolesAllowedDynamicFeature
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Wires the LiteLLM proxy resources through the same Jersey auth pipeline
// production uses and fires HTTP requests with no / wrong-role / right-role
// Bearer tokens. The @RolesAllowed annotations are only enforced when
// AccessControlService registers RolesAllowedDynamicFeature; this spec is
// the regression guard that the annotation, the registration, and the
// JwtAuthFilter priority continue to compose into the expected status codes.
class LiteLLMProxyAuthSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val testMapper: ObjectMapper =
    Jackson.newObjectMapper().registerModule(DefaultScalaModule)

  // Inject copilotEnabled = true and a guaranteed-unreachable upstream so
  // requests that pass the auth + role gates fall straight into the
  // resource's "proxy attempt failed" branch — that 502 confirms the auth
  // pipeline didn't short-circuit before the resource body ran.
  private val unreachableLiteLLM = "http://127.0.0.1:1"

  private val resources: ResourceExtension = ResourceExtension
    .builder()
    .setMapper(testMapper)
    .addProvider(classOf[JwtAuthFilter])
    .addProvider(classOf[UnauthorizedExceptionMapper])
    .addProvider(classOf[RolesAllowedDynamicFeature])
    .addResource(
      new LiteLLMProxyResource(
        copilotEnabled = true,
        litellmBaseUrl = unreachableLiteLLM,
        litellmApiKey = "test"
      )
    )
    .addResource(
      new LiteLLMModelsResource(
        copilotEnabled = true,
        litellmBaseUrl = unreachableLiteLLM,
        litellmApiKey = "test"
      )
    )
    .build()

  override protected def beforeAll(): Unit = resources.before()
  override protected def afterAll(): Unit = resources.after()

  private def token(role: UserRoleEnum): String = {
    val u = new User()
    u.setUid(1)
    u.setName("test")
    u.setEmail("test@example.com")
    u.setGoogleId(null)
    u.setRole(role)
    JwtAuth.jwtToken(JwtAuth.jwtClaims(u, expireInDays = 1))
  }

  private val chatBody = """{"model":"gpt-4o-mini","messages":[]}"""

  "POST /chat/completions without an Authorization header" should "return 401 with a Bearer challenge" in {
    val response = resources
      .target("/chat/completions")
      .request(MediaType.APPLICATION_JSON)
      .post(Entity.json(chatBody))
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 403 with an INACTIVE-role token" in {
    val response = resources
      .target("/chat/completions")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${token(UserRoleEnum.INACTIVE)}")
      .post(Entity.json(chatBody))
    response.getStatus shouldBe 403
  }

  it should "reach the LiteLLM proxy path with a REGULAR-role token" in {
    val response = resources
      .target("/chat/completions")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${token(UserRoleEnum.REGULAR)}")
      .post(Entity.json(chatBody))
    response.getStatus shouldBe 502
    response.readEntity(classOf[String]) should include("Failed to proxy request to LiteLLM")
  }

  "GET /models without an Authorization header" should "return 401 with a Bearer challenge" in {
    val response = resources.target("/models").request(MediaType.APPLICATION_JSON).get()
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 403 with an INACTIVE-role token" in {
    val response = resources
      .target("/models")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${token(UserRoleEnum.INACTIVE)}")
      .get()
    response.getStatus shouldBe 403
  }

  it should "reach the LiteLLM proxy path with an ADMIN-role token" in {
    val response = resources
      .target("/models")
      .request(MediaType.APPLICATION_JSON)
      .header("Authorization", s"Bearer ${token(UserRoleEnum.ADMIN)}")
      .get()
    response.getStatus shouldBe 502
    response.readEntity(classOf[String]) should include("Failed to fetch models from LiteLLM")
  }

  // Regression guard for the no-arg auxiliary constructor that Jersey
  // reflection picks at production startup. Jersey resolves constructors in
  // descending parameter count and skips any whose parameters are not
  // @Context / HK2 injectable; LiteLLMProxyResource's 3-arg ctor takes plain
  // Boolean / String values, so the no-arg form must exist and be picked.
  // addResource(classOf[...]) (vs. addResource(new ...)) exercises that path.
  "Jersey reflection" should "instantiate both LiteLLM resources via their no-arg constructors" in {
    val reflective = ResourceExtension
      .builder()
      .setMapper(testMapper)
      .addProvider(classOf[JwtAuthFilter])
      .addProvider(classOf[UnauthorizedExceptionMapper])
      .addProvider(classOf[RolesAllowedDynamicFeature])
      .addResource(classOf[LiteLLMProxyResource])
      .addResource(classOf[LiteLLMModelsResource])
      .build()
    reflective.before()
    try {
      val chat = reflective.target("/chat/completions").request(MediaType.APPLICATION_JSON).get()
      // Unauthenticated GET on the POST-only chat path: we just need any
      // response that proves Jersey wired the resource (4xx is fine; an
      // instantiation failure surfaces as 500 or a test setup error).
      chat.getStatus should (be >= 400 and be < 500)

      val models = reflective.target("/models").request(MediaType.APPLICATION_JSON).get()
      models.getStatus shouldBe 401
    } finally {
      reflective.after()
    }
  }
}
