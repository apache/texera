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

import jakarta.ws.rs.core.{HttpHeaders, Response}
import jakarta.ws.rs.ext.{ExceptionMapper, Provider}

/** Carries an RFC 6750 §3 `WWW-Authenticate: Bearer …` challenge to be
  * returned alongside a `401 Unauthorized` response.
  *
  * Extends `RuntimeException` (not `WebApplicationException`) so it can be
  * constructed without a JAX-RS `RuntimeDelegate` on the classpath, which
  * keeps unit tests for [[JwtAuthFilter]] independent of any Jersey
  * implementation. The companion [[UnauthorizedExceptionMapper]] converts
  * the exception to the actual HTTP response at the JAX-RS edge.
  *
  * Constructed with `writableStackTrace = false` because this exception is
  * thrown on every unauthenticated request and the stack trace is never
  * inspected — skipping `fillInStackTrace` avoids a per-request CPU hit on
  * the auth hot path.
  */
class UnauthorizedException(val challenge: String)
    extends RuntimeException(
      challenge,
      /* cause = */ null,
      /* enableSuppression = */ false,
      /* writableStackTrace = */ false
    )

/** Maps [[UnauthorizedException]] to a `401` response with the carried
  * `WWW-Authenticate` challenge header.
  */
@Provider
class UnauthorizedExceptionMapper extends ExceptionMapper[UnauthorizedException] {
  override def toResponse(e: UnauthorizedException): Response =
    Response
      .status(Response.Status.UNAUTHORIZED)
      .header(HttpHeaders.WWW_AUTHENTICATE, e.challenge)
      .build()
}
