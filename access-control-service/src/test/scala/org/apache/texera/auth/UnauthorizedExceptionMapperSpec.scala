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

import jakarta.ws.rs.core.HttpHeaders
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UnauthorizedExceptionMapperSpec extends AnyFlatSpec with Matchers {

  // The mapper sits behind every microservice's `environment.jersey.register(
  // classOf[UnauthorizedExceptionMapper])` wiring. JwtAuthFilter throws
  // `UnauthorizedException(challenge)` (covered by JwtAuthFilterSpec); this
  // spec pins what the mapper turns that exception into when JAX-RS calls
  // `toResponse` at the edge.

  private val mapper = new UnauthorizedExceptionMapper

  "UnauthorizedExceptionMapper" should "map any UnauthorizedException to HTTP 401" in {
    val response = mapper.toResponse(new UnauthorizedException("Bearer realm=\"texera\""))
    response.getStatus shouldBe 401
  }

  it should "carry the exception's challenge string verbatim in the WWW-Authenticate header" in {
    // The challenge is RFC 6750 §3 syntax. The mapper must not rewrite it —
    // JwtAuthFilter is the single source of truth for which challenge fires
    // (Bearer vs. Bearer + invalid_token), and any rewrite here would mask
    // a regression in the filter.
    val challenge =
      """Bearer realm="texera", error="invalid_token", error_description="JWT verification failed""""
    val response = mapper.toResponse(new UnauthorizedException(challenge))
    response.getHeaderString(HttpHeaders.WWW_AUTHENTICATE) shouldBe challenge
  }

  it should "produce no entity body — only status + challenge header" in {
    // Browsers and curl expect `WWW-Authenticate` on a body-less 401; an
    // accidental JSON entity (e.g. via Dropwizard's default error mapper)
    // would suppress the auth challenge prompt in some clients.
    val response = mapper.toResponse(new UnauthorizedException("Bearer realm=\"texera\""))
    response.hasEntity shouldBe false
  }
}
