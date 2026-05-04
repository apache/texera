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

import jakarta.annotation.security.PermitAll
import jakarta.ws.rs.WebApplicationException
import jakarta.ws.rs.container.{ContainerRequestContext, ResourceInfo}
import jakarta.ws.rs.core.{HttpHeaders, Response, SecurityContext}
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.jose4j.jwt.JwtClaims
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.{Field, Method}
import java.util.concurrent.atomic.AtomicReference

class JwtAuthFilterSpec extends AnyFlatSpec with Matchers {

  // Minimal stand-in for a request context. Only the methods the filter
  // actually touches are wired up; the rest are unimplemented.
  private class StubRequestContext(authHeader: String) extends ContainerRequestContext {
    val securityContext = new AtomicReference[SecurityContext](null)

    override def getHeaderString(name: String): String =
      if (name == HttpHeaders.AUTHORIZATION) authHeader else null
    override def setSecurityContext(context: SecurityContext): Unit = securityContext.set(context)
    override def getSecurityContext: SecurityContext = securityContext.get()

    // unused
    override def abortWith(response: Response): Unit = ()
    override def getProperty(x$1: String): Object = null
    override def getPropertyNames: java.util.Collection[String] =
      java.util.Collections.emptyList()
    override def setProperty(x$1: String, x$2: Object): Unit = ()
    override def removeProperty(x$1: String): Unit = ()
    override def getRequest: jakarta.ws.rs.core.Request = null
    override def getMethod: String = null
    override def setMethod(x$1: String): Unit = ()
    override def getUriInfo: jakarta.ws.rs.core.UriInfo = null
    override def setRequestUri(x$1: java.net.URI): Unit = ()
    override def setRequestUri(x$1: java.net.URI, x$2: java.net.URI): Unit = ()
    override def getHeaders: jakarta.ws.rs.core.MultivaluedMap[String, String] = null
    override def getCookies: java.util.Map[String, jakarta.ws.rs.core.Cookie] = null
    override def getDate: java.util.Date = null
    override def getLanguage: java.util.Locale = null
    override def getLength: Int = 0
    override def getMediaType: jakarta.ws.rs.core.MediaType = null
    override def getAcceptableMediaTypes: java.util.List[jakarta.ws.rs.core.MediaType] = null
    override def getAcceptableLanguages: java.util.List[java.util.Locale] = null
    override def hasEntity: Boolean = false
    override def getEntityStream: java.io.InputStream = null
    override def setEntityStream(x$1: java.io.InputStream): Unit = ()
  }

  private def buildClaims(): JwtClaims = {
    val c = new JwtClaims
    c.setSubject("alice")
    c.setClaim("userId", 42)
    c.setClaim("googleId", "g-123")
    c.setClaim("email", "alice@example.com")
    c.setClaim("role", UserRoleEnum.ADMIN.name)
    c.setClaim("googleAvatar", "avatar")
    c.setExpirationTimeMinutesInTheFuture(10f)
    c
  }

  private def challenge(thrown: WebApplicationException): String = {
    thrown.getResponse.getStatus shouldBe 401
    thrown.getResponse.getHeaderString(HttpHeaders.WWW_AUTHENTICATE)
  }

  // -------------------- tests --------------------

  "JwtAuthFilter" should "challenge with bare Bearer realm when no Authorization header is present" in {
    val filter = new JwtAuthFilter
    val ctx = new StubRequestContext(null)
    val thrown = the[WebApplicationException] thrownBy filter.filter(ctx)
    challenge(thrown) shouldBe "Bearer realm=\"texera\""
    ctx.getSecurityContext shouldBe null
  }

  it should "challenge with bare Bearer realm when the header is not a Bearer token" in {
    val filter = new JwtAuthFilter
    val ctx = new StubRequestContext("Basic abc")
    val thrown = the[WebApplicationException] thrownBy filter.filter(ctx)
    challenge(thrown) shouldBe "Bearer realm=\"texera\""
  }

  it should "challenge with error=invalid_token when the Bearer token cannot be verified" in {
    val filter = new JwtAuthFilter
    val ctx = new StubRequestContext("Bearer not-a-real-jwt")
    val thrown = the[WebApplicationException] thrownBy filter.filter(ctx)
    challenge(thrown) shouldBe "Bearer realm=\"texera\", error=\"invalid_token\""
  }

  it should "install a SecurityContext with the parsed SessionUser when the token is valid" in {
    val filter = new JwtAuthFilter
    val ctx = new StubRequestContext(s"Bearer ${JwtAuth.jwtToken(buildClaims())}")

    filter.filter(ctx)

    val sc = ctx.getSecurityContext
    sc should not be null
    sc.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
    sc.getAuthenticationScheme shouldBe "Bearer"
    sc.isUserInRole(UserRoleEnum.ADMIN.name) shouldBe true
    sc.isUserInRole(UserRoleEnum.REGULAR.name) shouldBe false
  }

  // -------------------- @PermitAll opt-out --------------------

  private class RequiredAuthResource { def secured(): Unit = () }
  private class OptionalAuthResource { @PermitAll def cover(): Unit = () }
  @PermitAll private class OpenResource { def anything(): Unit = () }

  private def methodOf(cls: Class[_], name: String): Method =
    cls.getDeclaredMethods.find(_.getName == name).get

  private def withResourceInfo(filter: JwtAuthFilter, info: ResourceInfo): Unit = {
    val f: Field = classOf[JwtAuthFilter].getDeclaredField("resourceInfo")
    f.setAccessible(true)
    f.set(filter, info)
  }

  private class StubResourceInfo(method: Method, cls: Class[_]) extends ResourceInfo {
    override def getResourceMethod: Method = method
    override def getResourceClass: Class[_] = cls
  }

  "JwtAuthFilter on a @PermitAll method" should "let an unauthenticated request pass through with no SecurityContext" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[OptionalAuthResource], "cover"),
        classOf[OptionalAuthResource]
      )
    )
    val ctx = new StubRequestContext(null)

    filter.filter(ctx) // must NOT throw
    ctx.getSecurityContext shouldBe null
  }

  it should "still 401 when a token is supplied but invalid (tampered or stale)" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[OptionalAuthResource], "cover"),
        classOf[OptionalAuthResource]
      )
    )
    val ctx = new StubRequestContext("Bearer not-a-real-jwt")

    val thrown = the[WebApplicationException] thrownBy filter.filter(ctx)
    challenge(thrown) shouldBe "Bearer realm=\"texera\", error=\"invalid_token\""
  }

  it should "install a SecurityContext when a valid token is supplied" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[OptionalAuthResource], "cover"),
        classOf[OptionalAuthResource]
      )
    )
    val ctx = new StubRequestContext(s"Bearer ${JwtAuth.jwtToken(buildClaims())}")

    filter.filter(ctx)
    ctx.getSecurityContext.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
  }

  "JwtAuthFilter on a class-level @PermitAll" should "honor the class annotation when the method has none" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(methodOf(classOf[OpenResource], "anything"), classOf[OpenResource])
    )
    val ctx = new StubRequestContext(null)

    filter.filter(ctx) // must NOT throw
    ctx.getSecurityContext shouldBe null
  }

  "JwtAuthFilter without resource info" should "default to required-auth (eager 401)" in {
    val filter = new JwtAuthFilter
    // resourceInfo left as null — pre-matching path or test scenario
    val ctx = new StubRequestContext(null)
    val thrown = the[WebApplicationException] thrownBy filter.filter(ctx)
    challenge(thrown) shouldBe "Bearer realm=\"texera\""
  }
}
