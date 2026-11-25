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
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
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

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets

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

  // The proxy and models resources behind the same auth providers as production.
  private def proxyExtension(copilotEnabled: Boolean, baseUrl: String): ResourceExtension =
    ResourceExtension
      .builder()
      .setMapper(testMapper)
      .addProvider(classOf[JwtAuthFilter])
      .addProvider(classOf[UnauthorizedExceptionMapper])
      .addProvider(classOf[RolesAllowedDynamicFeature])
      .addResource(new LiteLLMProxyResource(copilotEnabled, baseUrl, litellmApiKey = "test"))
      .addResource(new LiteLLMModelsResource(copilotEnabled, baseUrl, litellmApiKey = "test"))
      .build()

  private val resources: ResourceExtension =
    proxyExtension(copilotEnabled = true, unreachableLiteLLM)

  // Second resource extension with copilotEnabled = false, used to exercise
  // the resource's early-exit branch that returns "Copilot feature is disabled".
  private val resourcesCopilotDisabled: ResourceExtension =
    proxyExtension(copilotEnabled = false, unreachableLiteLLM)

  // An in-process HTTP server stands in for LiteLLM so the resources' success
  // path (header / body / status forwarding) is exercised end-to-end without
  // a network dependency. Bound to port 0 to pick any free port.
  private val mockChatBody = """{"id":"mock-chat","choices":[{"message":{"content":"hi"}}]}"""
  private val mockModelsBody = """{"data":[{"id":"mock-gpt"}]}"""

  private val mockLiteLLM: HttpServer = HttpServer.create(new InetSocketAddress(0), 0)
  mockLiteLLM.createContext("/chat/completions", respondWith(200, mockChatBody))
  mockLiteLLM.createContext("/models", respondWith(200, mockModelsBody))

  private def respondWith(status: Int, body: String): HttpHandler =
    (exchange: HttpExchange) => {
      val bytes = body.getBytes(StandardCharsets.UTF_8)
      exchange.getResponseHeaders.add("Content-Type", MediaType.APPLICATION_JSON)
      exchange.sendResponseHeaders(status, bytes.length.toLong)
      val os = exchange.getResponseBody
      try os.write(bytes)
      finally os.close()
    }

  private def mockBaseUrl: String = s"http://127.0.0.1:${mockLiteLLM.getAddress.getPort}"

  // Third extension: copilot on, upstream reachable. Resource is built lazily
  // because litellmBaseUrl depends on the mock server's bound port.
  private lazy val resourcesMockLiteLLM: ResourceExtension =
    proxyExtension(copilotEnabled = true, mockBaseUrl)

  override protected def beforeAll(): Unit = {
    mockLiteLLM.start()
    resources.before()
    resourcesCopilotDisabled.before()
    resourcesMockLiteLLM.before()
  }
  override protected def afterAll(): Unit = {
    resourcesMockLiteLLM.after()
    resourcesCopilotDisabled.after()
    resources.after()
    mockLiteLLM.stop(0)
  }

  private def token(role: UserRoleEnum): String = {
    val u = new User()
    u.setUid(1)
    u.setName("test")
    u.setEmail("test@example.com")
    u.setRole(role)
    JwtAuth.jwtToken(JwtAuth.jwtClaims(u))
  }

  private val chatBody = """{"model":"gpt-4o-mini","messages":[]}"""

  // A JSON request to `path`, with a Bearer token for `role` when one is given.
  private def jsonRequest(ext: ResourceExtension, path: String, role: Option[UserRoleEnum]) = {
    val builder = ext.target(path).request(MediaType.APPLICATION_JSON)
    role.fold(builder)(r => builder.header("Authorization", s"Bearer ${token(r)}"))
  }

  // Posts the chat body to /chat/completions.
  private def postChat(ext: ResourceExtension, role: Option[UserRoleEnum] = None) =
    jsonRequest(ext, "/chat/completions", role).post(Entity.json(chatBody))

  // Lists the models at /models.
  private def getModels(ext: ResourceExtension, role: Option[UserRoleEnum] = None) =
    jsonRequest(ext, "/models", role).get()

  "POST /chat/completions without an Authorization header" should "return 401 with a Bearer challenge" in {
    val response = postChat(resources)
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 403 with an INACTIVE-role token" in {
    postChat(resources, Some(UserRoleEnum.INACTIVE)).getStatus shouldBe 403
  }

  it should "return 502 when the upstream LiteLLM call fails" in {
    // Exercises the resource's catch branch via a connect-refused upstream.
    postChat(resources, Some(UserRoleEnum.REGULAR)).getStatus shouldBe 502
  }

  it should "return the resource's Copilot-disabled response when copilot is off" in {
    // 403 alone is ambiguous (could be from RolesAllowedDynamicFeature);
    // matching the entity to the same constant the resource emits proves the
    // role check passed and the resource's own early-exit branch fired.
    val response = postChat(resourcesCopilotDisabled, Some(UserRoleEnum.REGULAR))
    response.getStatus shouldBe 403
    response.readEntity(classOf[String]) shouldBe LiteLLMProxyResource.CopilotDisabledBody
  }

  "GET /models without an Authorization header" should "return 401 with a Bearer challenge" in {
    val response = getModels(resources)
    response.getStatus shouldBe 401
    response.getHeaderString("WWW-Authenticate") shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "return 403 with an INACTIVE-role token" in {
    getModels(resources, Some(UserRoleEnum.INACTIVE)).getStatus shouldBe 403
  }

  it should "return 502 when the upstream LiteLLM call fails" in {
    getModels(resources, Some(UserRoleEnum.ADMIN)).getStatus shouldBe 502
  }

  it should "return the resource's Copilot-disabled response when copilot is off" in {
    val response = getModels(resourcesCopilotDisabled, Some(UserRoleEnum.ADMIN))
    response.getStatus shouldBe 403
    response.readEntity(classOf[String]) shouldBe LiteLLMProxyResource.CopilotDisabledBody
  }

  "POST /chat/completions" should "forward the upstream response when copilot is on and upstream is reachable" in {
    val response = postChat(resourcesMockLiteLLM, Some(UserRoleEnum.REGULAR))
    response.getStatus shouldBe 200
    response.readEntity(classOf[String]) shouldBe mockChatBody
  }

  "GET /models" should "forward the upstream response when copilot is on and upstream is reachable" in {
    val response = getModels(resourcesMockLiteLLM, Some(UserRoleEnum.ADMIN))
    response.getStatus shouldBe 200
    response.readEntity(classOf[String]) shouldBe mockModelsBody
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
