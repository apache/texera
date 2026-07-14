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
import org.slf4j.MDC

import javax.annotation.Priority
import javax.ws.rs.container.{
  ContainerRequestContext,
  ContainerRequestFilter,
  ContainerResponseContext,
  ContainerResponseFilter
}
import javax.ws.rs.ext.Provider
import javax.ws.rs.Priorities

/**
  * Jersey filter that pushes the authenticated user's id into the
  * SLF4J MDC so every log record emitted while handling the request
  * carries `texera.user.id`.
  *
  * Why Jersey and not a Servlet filter:
  *  - Authentication is wired as a Jersey `ContainerRequestFilter`
  *    (see [[org.apache.texera.auth.JwtAuthFilter]]) — by the time
  *    Servlet filters run, `SecurityContext.getUserPrincipal` is
  *    still null. A Jersey filter ordered after auth picks it up.
  *  - Priorities.AUTHENTICATION + 100 places us in the
  *    AUTHORIZATION bucket which is guaranteed to run AFTER auth.
  *
  * Symmetric request/response interfaces let us pair set + clear
  * without leaking MDC across threads in the worker pool.
  */
@Provider
@Priority(Priorities.AUTHORIZATION)
class UserContextMdcFilter extends ContainerRequestFilter with ContainerResponseFilter {

  override def filter(requestContext: ContainerRequestContext): Unit = {
    val secCtx = requestContext.getSecurityContext
    if (secCtx != null) {
      secCtx.getUserPrincipal match {
        case user: SessionUser =>
          val uid = user.getUid
          if (uid != null) MDC.put(UserContextMdcFilter.UserIdKey, uid.toString)
        case _ => // anonymous request, or auth chose a different Principal type
      }
    }
  }

  override def filter(
      requestContext: ContainerRequestContext,
      responseContext: ContainerResponseContext
  ): Unit = {
    // Defence in depth: clear only the key we own. The Servlet-layer
    // filter clears its own keys via the same pattern.
    MDC.remove(UserContextMdcFilter.UserIdKey)
  }
}

object UserContextMdcFilter {

  /** MDC key — must stay in sync with
    *  [[org.apache.texera.observability.LogSanitizer.AllowedMdcKeys]]
    *  or the OTel log appender drops it before emit.
    */
  val UserIdKey: String = "texera.user.id"
}
