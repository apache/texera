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

package org.apache.texera.service.resource

import jakarta.ws.rs.core.{HttpHeaders, MultivaluedHashMap, Response, UriInfo}
import org.apache.texera.auth.JwtAuth
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.mockito.Mockito.{mock, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.util.Collections

/**
  * Exercises the `/api/agents` branch of [[AccessControlResource.authorize]] —
  * the Phase 1 ext_authz gate that authenticates the JWT and requires a
  * REGULAR/ADMIN role. Tokens are minted with the same [[JwtAuth]] the rest of
  * Texera uses, so this is a real signature round-trip against auth.conf.
  */
class AgentAccessAuthSpec extends AnyFlatSpec with Matchers {

  private def token(role: UserRoleEnum): String = {
    val u = new User()
    u.setUid(7)
    u.setName("agent-user")
    u.setEmail("agent-user@example.com")
    u.setGoogleId(null)
    u.setRole(role)
    JwtAuth.jwtToken(JwtAuth.jwtClaims(u, expireInDays = 1))
  }

  private def authorize(path: String, authHeader: Option[String]): Response = {
    val uriInfo = mock(classOf[UriInfo])
    when(uriInfo.getPath).thenReturn(path)
    when(uriInfo.getRequestUri).thenReturn(URI.create(s"http://localhost/$path"))
    when(uriInfo.getQueryParameters()).thenReturn(new MultivaluedHashMap[String, String]())

    val headers = mock(classOf[HttpHeaders])
    when(headers.getRequestHeaders).thenReturn(new MultivaluedHashMap[String, String]())
    when(headers.getRequestHeader("Authorization")).thenReturn(
      authHeader.map((h: String) => Collections.singletonList(h)).orNull
    )

    AccessControlResource.authorize(uriInfo, headers)
  }

  private val agentPath = "auth/api/agents/agent-1"

  "authorize on /api/agents" should "return 200 for a REGULAR user" in {
    authorize(agentPath, Some(s"Bearer ${token(UserRoleEnum.REGULAR)}")).getStatus shouldBe 200
  }

  it should "return 200 for an ADMIN user" in {
    authorize(agentPath, Some(s"Bearer ${token(UserRoleEnum.ADMIN)}")).getStatus shouldBe 200
  }

  it should "forward the user identity headers on success" in {
    val resp = authorize(agentPath, Some(s"Bearer ${token(UserRoleEnum.REGULAR)}"))
    resp.getHeaderString("x-user-id") shouldBe "7"
    resp.getHeaderString("x-user-name") shouldBe "agent-user"
    resp.getHeaderString("x-user-email") shouldBe "agent-user@example.com"
  }

  it should "return 401 when no token is present" in {
    authorize(agentPath, None).getStatus shouldBe 401
  }

  it should "return 401 for a malformed / mis-signed token" in {
    authorize(agentPath, Some("Bearer not-a-real-jwt")).getStatus shouldBe 401
  }

  it should "return 403 for an INACTIVE-role user" in {
    authorize(agentPath, Some(s"Bearer ${token(UserRoleEnum.INACTIVE)}")).getStatus shouldBe 403
  }
}
