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

package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.auth.SessionUser
import edu.uci.ics.texera.web.auth.GuestAuthFilter.GUEST
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import io.dropwizard.auth.AuthFilter

import java.io.IOException
import java.util.Optional
import javax.annotation.{Nullable, Priority}
import javax.ws.rs.Priorities
import javax.ws.rs.container.{ContainerRequestContext, PreMatching}
import javax.ws.rs.core.SecurityContext

@PreMatching
@Priority(Priorities.AUTHENTICATION) object GuestAuthFilter {
  class Builder extends AuthFilter.AuthFilterBuilder[String, SessionUser, GuestAuthFilter] {
    override protected def newInstance = new GuestAuthFilter
  }

  val GUEST: User = new User(null, "guest", null, null, null, null, UserRoleEnum.REGULAR, null)
}

@PreMatching
@Priority(Priorities.AUTHENTICATION) class GuestAuthFilter extends AuthFilter[String, SessionUser] {
  @throws[IOException]
  override def filter(requestContext: ContainerRequestContext): Unit =
    authenticate(requestContext, "", "")

  override protected def authenticate(
      requestContext: ContainerRequestContext,
      @Nullable credentials: String,
      scheme: String
  ): Boolean = {

    val principal = Optional.of(new SessionUser(GUEST))
    val securityContext = requestContext.getSecurityContext
    val secure = securityContext != null && securityContext.isSecure
    requestContext.setSecurityContext(new SecurityContext() {
      override def getUserPrincipal: SessionUser = principal.get

      override def isUserInRole(role: String): Boolean = authorizer.authorize(principal.get, role)

      override def isSecure: Boolean = secure

      override def getAuthenticationScheme: String = scheme
    })
    true
  }
}
