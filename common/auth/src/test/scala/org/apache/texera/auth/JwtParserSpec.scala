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

import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jwt.JwtClaims
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JwtParserSpec extends AnyFlatSpec with Matchers {

  private def buildClaims(): JwtClaims = {
    // Mirror exactly what JwtAuth.jwtClaims would write at issue time, so
    // this spec doubles as a contract test between the issuer and parser.
    val claims = new JwtClaims
    claims.setSubject("alice")
    claims.setClaim("userId", 42)
    claims.setClaim("googleId", "g-123")
    claims.setClaim("email", "alice@example.com")
    claims.setClaim("role", UserRoleEnum.ADMIN.name)
    claims.setClaim("googleAvatar", "avatar-blob")
    claims.setExpirationTimeMinutesInTheFuture(10f)
    claims
  }

  "JwtParser.claimsToSessionUser" should "populate every issued claim including googleAvatar" in {
    val user: User = JwtParser.claimsToSessionUser(buildClaims()).getUser
    user.getUid shouldBe 42
    user.getName shouldBe "alice"
    user.getEmail shouldBe "alice@example.com"
    user.getGoogleId shouldBe "g-123"
    user.getGoogleAvatar shouldBe "avatar-blob"
    user.getRole shouldBe UserRoleEnum.ADMIN
  }

  it should "leave non-issued slots null (password, comment, accountCreation, affiliation, joiningReason)" in {
    val user: User = JwtParser.claimsToSessionUser(buildClaims()).getUser
    user.getPassword shouldBe null
    user.getComment shouldBe null
    user.getAccountCreationTime shouldBe null
    user.getAffiliation shouldBe null
    user.getJoiningReason shouldBe null
  }

  it should "round-trip a token issued by JwtAuth.jwtToken" in {
    val token = JwtAuth.jwtToken(buildClaims())
    val parsed = JwtParser.parseToken(token)
    parsed.isPresent shouldBe true
    val u = parsed.get().getUser
    u.getUid shouldBe 42
    u.getGoogleAvatar shouldBe "avatar-blob"
  }

  "JwtParser.parseToken" should "return empty on a structurally invalid token" in {
    JwtParser.parseToken("not-a-real-jwt").isPresent shouldBe false
  }
}
