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
import jakarta.ws.rs.container.{ContainerRequestContext, ResourceInfo}
import jakarta.ws.rs.core.{HttpHeaders, Response, SecurityContext}
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.jose4j.jwt.JwtClaims
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.{Field, Method}
import java.util.concurrent.atomic.AtomicReference

class JwtAuthFilterSpec extends AnyFlatSpec with Matchers {

  // Minimal stand-in for ContainerRequestContext. The filter only reads the
  // Authorization header and writes a SecurityContext; everything else is
  // unimplemented.
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

  // Inject @Context ResourceInfo via reflection so tests can flip annotation
  // states per-case without spinning up Jersey.
  private def withResourceInfo(filter: JwtAuthFilter, info: ResourceInfo): Unit = {
    val f: Field = classOf[JwtAuthFilter].getDeclaredField("resourceInfo")
    f.setAccessible(true)
    f.set(filter, info)
  }

  private class StubResourceInfo(method: Method, cls: Class[_]) extends ResourceInfo {
    override def getResourceMethod: Method = method
    override def getResourceClass: Class[_] = cls
  }

  private def methodOf(cls: Class[_], name: String): Method =
    cls.getDeclaredMethods.find(_.getName == name).get

  private class RequiredAuthResource { def secured(): Unit = () }
  private class OptionalAuthResource { @PermitAll def cover(): Unit = () }
  @PermitAll private class OpenResource { def anything(): Unit = () }

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

  // -------------------- challenge constants --------------------

  "JwtAuthFilter constants" should "match RFC 6750 §3 challenge syntax" in {
    JwtAuthFilter.BearerChallenge shouldBe "Bearer realm=\"texera\""
    JwtAuthFilter.InvalidTokenChallenge shouldBe "Bearer realm=\"texera\", error=\"invalid_token\""
  }

  // -------------------- required-auth method --------------------

  "JwtAuthFilter on a required-auth method" should "throw UnauthorizedException(BearerChallenge) when no Authorization header is present" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[RequiredAuthResource], "secured"),
        classOf[RequiredAuthResource]
      )
    )
    val ctx = new StubRequestContext(null)
    val thrown = the[UnauthorizedException] thrownBy filter.filter(ctx)
    thrown.challenge shouldBe JwtAuthFilter.BearerChallenge
    ctx.getSecurityContext shouldBe null
  }

  it should "throw UnauthorizedException(BearerChallenge) when the header is not a Bearer token" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[RequiredAuthResource], "secured"),
        classOf[RequiredAuthResource]
      )
    )
    val ctx = new StubRequestContext("Basic abc")
    val thrown = the[UnauthorizedException] thrownBy filter.filter(ctx)
    thrown.challenge shouldBe JwtAuthFilter.BearerChallenge
  }

  it should "throw UnauthorizedException(InvalidTokenChallenge) when the Bearer token cannot be verified" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[RequiredAuthResource], "secured"),
        classOf[RequiredAuthResource]
      )
    )
    val ctx = new StubRequestContext("Bearer not-a-real-jwt")
    val thrown = the[UnauthorizedException] thrownBy filter.filter(ctx)
    thrown.challenge shouldBe JwtAuthFilter.InvalidTokenChallenge
  }

  it should "install a SecurityContext with the parsed SessionUser when the token is valid" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[RequiredAuthResource], "secured"),
        classOf[RequiredAuthResource]
      )
    )
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

  it should "still throw UnauthorizedException(InvalidTokenChallenge) when a token is supplied but invalid" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[OptionalAuthResource], "cover"),
        classOf[OptionalAuthResource]
      )
    )
    val ctx = new StubRequestContext("Bearer not-a-real-jwt")
    val thrown = the[UnauthorizedException] thrownBy filter.filter(ctx)
    thrown.challenge shouldBe JwtAuthFilter.InvalidTokenChallenge
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
    val thrown = the[UnauthorizedException] thrownBy filter.filter(ctx)
    thrown.challenge shouldBe JwtAuthFilter.BearerChallenge
  }

  // -------------------- case-insensitive Bearer scheme --------------------

  // RFC 7235 §2.1: auth-scheme is case-insensitive. The header parser must
  // accept any capitalization of "Bearer" and tolerate surrounding /
  // intra-header whitespace.
  private def filterFor(authHeader: String): StubRequestContext = {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[RequiredAuthResource], "secured"),
        classOf[RequiredAuthResource]
      )
    )
    val ctx = new StubRequestContext(authHeader)
    filter.filter(ctx)
    ctx
  }

  "JwtAuthFilter Bearer scheme parsing" should "accept lowercase 'bearer'" in {
    val ctx = filterFor(s"bearer ${JwtAuth.jwtToken(buildClaims())}")
    ctx.getSecurityContext.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
  }

  it should "accept uppercase 'BEARER'" in {
    val ctx = filterFor(s"BEARER ${JwtAuth.jwtToken(buildClaims())}")
    ctx.getSecurityContext.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
  }

  it should "accept mixed-case 'BeArEr'" in {
    val ctx = filterFor(s"BeArEr ${JwtAuth.jwtToken(buildClaims())}")
    ctx.getSecurityContext.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
  }

  it should "tolerate leading whitespace before the scheme" in {
    val ctx = filterFor(s"   Bearer ${JwtAuth.jwtToken(buildClaims())}")
    ctx.getSecurityContext.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
  }

  it should "tolerate multiple spaces between scheme and token" in {
    val ctx = filterFor(s"Bearer   ${JwtAuth.jwtToken(buildClaims())}")
    ctx.getSecurityContext.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
  }

  it should "tolerate trailing whitespace after the token" in {
    val ctx = filterFor(s"Bearer ${JwtAuth.jwtToken(buildClaims())}   ")
    ctx.getSecurityContext.getUserPrincipal.asInstanceOf[SessionUser].getUid shouldBe 42
  }

  it should "reject a Bearer header with no token" in {
    val filter = new JwtAuthFilter
    withResourceInfo(
      filter,
      new StubResourceInfo(
        methodOf(classOf[RequiredAuthResource], "secured"),
        classOf[RequiredAuthResource]
      )
    )
    val ctx = new StubRequestContext("Bearer   ")
    val thrown = the[UnauthorizedException] thrownBy filter.filter(ctx)
    thrown.challenge shouldBe JwtAuthFilter.BearerChallenge
  }

  // -------------------- exception is stack-trace-less --------------------

  // UnauthorizedException is thrown on every unauthenticated request and the
  // stack is never inspected. Ensure fillInStackTrace was suppressed so the
  // auth hot path does not pay for stack capture.
  "UnauthorizedException" should "carry no stack trace" in {
    val e = new UnauthorizedException(JwtAuthFilter.BearerChallenge)
    e.getStackTrace.length shouldBe 0
    e.getMessage shouldBe JwtAuthFilter.BearerChallenge
  }
}
