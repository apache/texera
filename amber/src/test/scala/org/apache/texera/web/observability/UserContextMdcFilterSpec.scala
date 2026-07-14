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

package org.apache.texera.web.observability

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.observability.LogSanitizer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.MDC

import java.security.Principal
import javax.ws.rs.core.SecurityContext

class UserContextMdcFilterSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  override def beforeEach(): Unit = MDC.clear()
  override def afterEach(): Unit = MDC.clear()

  private def userPrincipal(uid: Int): SessionUser = {
    val u = new User()
    u.setUid(uid)
    u.setName(s"user-$uid")
    u.setEmail(s"user-$uid@example.com")
    new SessionUser(u)
  }

  private def secCtxWith(principal: Principal): SecurityContext =
    new SecurityContext {
      override def getUserPrincipal: Principal = principal
      override def isUserInRole(role: String): Boolean = false
      override def isSecure: Boolean = false
      override def getAuthenticationScheme: String = "STUB"
    }

  /** Minimal ContainerRequestContext that only implements the
    *  surface the filter actually touches. Avoids pulling Mockito for
    *  what is effectively a one-method stub.
    */
  private class StubRequest(secCtx: SecurityContext) extends StubRequestContextBase {
    override def getSecurityContext: SecurityContext = secCtx
  }

  // ---- request-filter behaviour --------------------------------------

  "UserContextMdcFilter" should "push texera.user.id into MDC when SecurityContext has a SessionUser" in {
    val filter = new UserContextMdcFilter()
    filter.filter(new StubRequest(secCtxWith(userPrincipal(42))))
    MDC.get("texera.user.id") shouldBe "42"
  }

  it should "do nothing when the request is anonymous" in {
    val filter = new UserContextMdcFilter()
    filter.filter(new StubRequest(secCtxWith(null)))
    MDC.get("texera.user.id") shouldBe null
  }

  it should "do nothing when SecurityContext itself is null" in {
    val filter = new UserContextMdcFilter()
    filter.filter(new StubRequest(null))
    MDC.get("texera.user.id") shouldBe null
  }

  it should "ignore principals that aren't SessionUser" in {
    val filter = new UserContextMdcFilter()
    val other = new Principal { override def getName: String = "robot" }
    filter.filter(new StubRequest(secCtxWith(other)))
    MDC.get("texera.user.id") shouldBe null
  }

  // ---- response-filter clears MDC ------------------------------------

  it should "clear texera.user.id after the response filter runs" in {
    val filter = new UserContextMdcFilter()
    filter.filter(new StubRequest(secCtxWith(userPrincipal(7))))
    MDC.get("texera.user.id") shouldBe "7"
    filter.filter(new StubRequest(secCtxWith(userPrincipal(7))), null)
    MDC.get("texera.user.id") shouldBe null
  }

  // ---- contract with the OTel appender allowlist --------------------

  it should "use an MDC key that the OTel log appender will actually forward" in {
    // If the key here isn't in LogSanitizer.AllowedMdcKeys, the OTel
    // bridge silently drops it and the dashboard never sees the user
    // id on emitted records.
    LogSanitizer.AllowedMdcKeys should contain(UserContextMdcFilter.UserIdKey)
  }
}

/** Base stub that throws on every ContainerRequestContext method;
  *  subclasses override only what they need. Keeps the spec narrow on
  *  the actual filter dependencies (just `getSecurityContext`).
  */
private abstract class StubRequestContextBase
    extends javax.ws.rs.container.ContainerRequestContext {
  private def stub(): Nothing =
    throw new UnsupportedOperationException("not stubbed; override if a test needs it")
  override def getProperty(name: String): AnyRef = null
  override def getPropertyNames: java.util.Collection[String] = stub()
  override def setProperty(name: String, `object`: Any): Unit = ()
  override def removeProperty(name: String): Unit = ()
  override def getUriInfo: javax.ws.rs.core.UriInfo = stub()
  override def setRequestUri(requestUri: java.net.URI): Unit = stub()
  override def setRequestUri(baseUri: java.net.URI, requestUri: java.net.URI): Unit = stub()
  override def getRequest: javax.ws.rs.core.Request = stub()
  override def getMethod: String = "GET"
  override def setMethod(method: String): Unit = stub()
  override def getHeaders: javax.ws.rs.core.MultivaluedMap[String, String] = stub()
  override def getHeaderString(name: String): String = null
  override def getDate: java.util.Date = null
  override def getLanguage: java.util.Locale = null
  override def getLength: Int = -1
  override def getMediaType: javax.ws.rs.core.MediaType = null
  override def getAcceptableMediaTypes: java.util.List[javax.ws.rs.core.MediaType] = stub()
  override def getAcceptableLanguages: java.util.List[java.util.Locale] = stub()
  override def getCookies: java.util.Map[String, javax.ws.rs.core.Cookie] = stub()
  override def hasEntity: Boolean = false
  override def getEntityStream: java.io.InputStream = null
  override def setEntityStream(input: java.io.InputStream): Unit = ()
  override def getSecurityContext: javax.ws.rs.core.SecurityContext = null
  override def setSecurityContext(context: javax.ws.rs.core.SecurityContext): Unit = stub()
  override def abortWith(response: javax.ws.rs.core.Response): Unit = stub()
}
