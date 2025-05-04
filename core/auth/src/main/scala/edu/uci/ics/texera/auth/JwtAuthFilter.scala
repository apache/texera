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

package edu.uci.ics.texera.auth

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import jakarta.ws.rs.container.{ContainerRequestContext, ContainerRequestFilter, ResourceInfo}
import jakarta.ws.rs.core.{Context, HttpHeaders, SecurityContext}
import jakarta.ws.rs.ext.Provider

import java.security.Principal

@Provider
class JwtAuthFilter extends ContainerRequestFilter with LazyLogging {

  @Context
  private var resourceInfo: ResourceInfo = _

  override def filter(requestContext: ContainerRequestContext): Unit = {
    val authHeader = requestContext.getHeaderString(HttpHeaders.AUTHORIZATION)

    if (authHeader != null && authHeader.startsWith("Bearer ")) {
      val token = authHeader.substring(7) // Remove "Bearer " prefix
      val userOpt = JwtParser.parseToken(token)

      if (userOpt.isPresent) {
        val user = userOpt.get()
        requestContext.setSecurityContext(new SecurityContext {
          override def getUserPrincipal: Principal = user
          override def isUserInRole(role: String): Boolean =
            user.isRoleOf(UserRoleEnum.valueOf(role))
          override def isSecure: Boolean = false
          override def getAuthenticationScheme: String = "Bearer"
        })
      } else {
        logger.warn("Invalid JWT: Unable to parse token")
      }
    }
  }
}
