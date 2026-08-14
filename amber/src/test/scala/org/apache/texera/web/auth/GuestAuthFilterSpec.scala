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

package org.apache.texera.web.auth

import io.dropwizard.auth.Authorizer
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import javax.ws.rs.container.ContainerRequestContext
import javax.ws.rs.core.SecurityContext

/**
  * Covers the guest identity [[GuestAuthFilter]] installs on an unauthenticated request.
  * The filter is pure — it only rewrites the request's SecurityContext — so no database or
  * Jersey runtime is involved here.
  */
class GuestAuthFilterSpec extends AnyFlatSpec with Matchers with MockFactory {

  /** Authorizes exactly the "REGULAR" role, so both arms of isUserInRole are observable. */
  private val roleAuthorizer: Authorizer[SessionUser] =
    (_: SessionUser, role: String) => role == UserRoleEnum.REGULAR.getLiteral

  /**
    * Runs the filter over a request whose incoming SecurityContext is `incoming` and returns the
    * SecurityContext the filter installed in its place.
    */
  private def installedContext(incoming: SecurityContext): SecurityContext = {
    val filter = new GuestAuthFilter.Builder().setAuthorizer(roleAuthorizer).buildAuthFilter()
    var installed: SecurityContext = null

    val requestContext = stub[ContainerRequestContext]
    (() => requestContext.getSecurityContext).when().returns(incoming)
    (requestContext.setSecurityContext _)
      .when(*)
      .onCall((ctx: SecurityContext) => installed = ctx)

    filter.filter(requestContext)
    installed should not be null
    installed
  }

  private def secureContext(secure: Boolean): SecurityContext = {
    val ctx = stub[SecurityContext]
    (() => ctx.isSecure).when().returns(secure)
    ctx
  }

  "GuestAuthFilter.GUEST" should "be a REGULAR user named guest" in {
    GuestAuthFilter.GUEST.getName shouldBe "guest"
    GuestAuthFilter.GUEST.getRole shouldBe UserRoleEnum.REGULAR
  }

  "GuestAuthFilter.Builder" should "build a fresh filter each time" in {
    val builder = new GuestAuthFilter.Builder().setAuthorizer(roleAuthorizer)
    val first = builder.buildAuthFilter()
    val second = builder.buildAuthFilter()

    first should not be theSameInstanceAs(second)
  }

  "the installed SecurityContext" should "carry the guest user as its principal" in {
    val principal = installedContext(secureContext(secure = false)).getUserPrincipal

    principal shouldBe a[SessionUser]
    principal.asInstanceOf[SessionUser].getUser shouldBe GuestAuthFilter.GUEST
  }

  it should "delegate isUserInRole to the authorizer" in {
    val context = installedContext(secureContext(secure = false))

    context.isUserInRole(UserRoleEnum.REGULAR.getLiteral) shouldBe true
    context.isUserInRole(UserRoleEnum.ADMIN.getLiteral) shouldBe false
  }

  it should "inherit isSecure from a secure incoming context" in {
    installedContext(secureContext(secure = true)).isSecure shouldBe true
  }

  it should "inherit isSecure from an insecure incoming context" in {
    installedContext(secureContext(secure = false)).isSecure shouldBe false
  }

  it should "report an insecure request when there is no incoming context" in {
    // filter() guards with `securityContext != null` before reading isSecure
    installedContext(null).isSecure shouldBe false
  }

  it should "expose the scheme the filter authenticated with" in {
    // GuestAuthFilter.filter passes an empty scheme to authenticate()
    installedContext(secureContext(secure = false)).getAuthenticationScheme shouldBe ""
  }
}
