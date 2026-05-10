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

import com.typesafe.scalalogging.LazyLogging
import jakarta.annotation.security.PermitAll
import jakarta.ws.rs.container.{ContainerRequestContext, ContainerRequestFilter, ResourceInfo}
import jakarta.ws.rs.core.{Context, HttpHeaders, SecurityContext}
import jakarta.ws.rs.ext.Provider
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum

import java.security.Principal

/** JAX-RS request filter that authenticates a Bearer JWT and installs a
  * [[SessionUser]] security context.
  *
  * Failure semantics (RFC 6750 §3):
  *   - No `Authorization: Bearer …` header: throw [[UnauthorizedException]]
  *     carrying a bare `Bearer realm="texera"` challenge — unless the
  *     resource method or class is annotated with `@PermitAll`, in which
  *     case the request continues with no security context. This supports
  *     the `@Auth Optional[SessionUser]` pattern for endpoints that need
  *     to serve anonymous users.
  *   - Header present but token verification / claim extraction fails:
  *     throw [[UnauthorizedException]] with `error="invalid_token"`
  *     always, even on `@PermitAll` endpoints — a tampered or stale token
  *     is never silently treated as anonymous.
  *   - Header present and valid: install a `SecurityContext` whose
  *     principal is the parsed [[SessionUser]].
  *
  * HTTP translation (status 401, `WWW-Authenticate` header) is done by
  * [[UnauthorizedExceptionMapper]], registered alongside this filter in
  * each service.
  */
@Provider
class JwtAuthFilter extends ContainerRequestFilter with LazyLogging {

  @Context
  private var resourceInfo: ResourceInfo = _

  override def filter(requestContext: ContainerRequestContext): Unit = {
    val tokenOpt = extractBearerToken(requestContext.getHeaderString(HttpHeaders.AUTHORIZATION))

    if (tokenOpt.isEmpty) {
      if (isPermitAll) return
      throw new UnauthorizedException(JwtAuthFilter.BearerChallenge)
    }

    val userOpt = JwtParser.parseToken(tokenOpt.get)
    if (!userOpt.isPresent) {
      logger.warn("Invalid JWT: Unable to parse token")
      throw new UnauthorizedException(JwtAuthFilter.InvalidTokenChallenge)
    }

    val user = userOpt.get()
    requestContext.setSecurityContext(new SecurityContext {
      override def getUserPrincipal: Principal = user
      override def isUserInRole(role: String): Boolean =
        user.isRoleOf(UserRoleEnum.valueOf(role))
      override def isSecure: Boolean = false
      override def getAuthenticationScheme: String = "Bearer"
    })
  }

  private def isPermitAll: Boolean = {
    if (resourceInfo == null) return false
    val m = resourceInfo.getResourceMethod
    val c = resourceInfo.getResourceClass
    (m != null && m.isAnnotationPresent(classOf[PermitAll])) ||
    (c != null && c.isAnnotationPresent(classOf[PermitAll]))
  }

  // RFC 7235 §2.1: auth-scheme is case-insensitive and the credentials
  // follow after 1*SP. Tolerate surrounding whitespace and any
  // capitalization of "Bearer" so that e.g. `authorization: bearer <jwt>`
  // is accepted instead of being rejected as a malformed header.
  private def extractBearerToken(authHeader: String): Option[String] = {
    if (authHeader == null) return None
    val parts = authHeader.trim.split("\\s+", 2)
    if (parts.length != 2 || !parts(0).equalsIgnoreCase("Bearer")) return None
    val token = parts(1).trim
    if (token.isEmpty) None else Some(token)
  }
}

object JwtAuthFilter {
  // RFC 6750 §3: bare challenge = "please authenticate". The
  // `error="invalid_token"` parameter signals "the token you sent is
  // malformed / expired / signature failed" so a well-behaved client can
  // discard it instead of retrying.
  val BearerChallenge: String = "Bearer realm=\"texera\""
  val InvalidTokenChallenge: String = "Bearer realm=\"texera\", error=\"invalid_token\""
}
