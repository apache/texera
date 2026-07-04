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

import org.apache.texera.common.config.AuthConfig
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jwt.NumericDate
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JwtAuthSpec extends AnyFlatSpec with Matchers {

  private def buildUser(): User = {
    val user = new User()
    user.setUid(42)
    user.setName("alice")
    user.setEmail("alice@example.com")
    user.setGoogleId("g-123")
    user.setGoogleAvatar("avatar-blob")
    user.setRole(UserRoleEnum.ADMIN)
    user
  }

  "JwtAuth.jwtClaims" should "map every User field onto the matching claim" in {
    val claims = JwtAuth.jwtClaims(buildUser(), 7)
    claims.getSubject shouldBe "alice"
    claims.getClaimValueAsString("userId") shouldBe "42"
    claims.getClaimValueAsString("googleId") shouldBe "g-123"
    claims.getClaimValueAsString("email") shouldBe "alice@example.com"
    claims.getClaimValueAsString("googleAvatar") shouldBe "avatar-blob"
    claims.getClaimValueAsString("role") shouldBe UserRoleEnum.ADMIN.name
  }

  it should "derive the expiration from config, ignoring the expireInDays argument" in {
    // two very different expireInDays values must yield the same config-derived expiry window
    def expiryWindowMinutes(expireInDays: Int): Double = {
      val claims = JwtAuth.jwtClaims(buildUser(), expireInDays)
      claims.getExpirationTime should not be null
      claims.getExpirationTime.getValue / 60.0 - NumericDate.now().getValue / 60.0
    }
    expiryWindowMinutes(1) shouldBe (AuthConfig.jwtExpirationMinutes.toDouble +- 2.0)
    expiryWindowMinutes(100000) shouldBe (AuthConfig.jwtExpirationMinutes.toDouble +- 2.0)
  }

  it should "produce a token that round-trips back to the same user via JwtParser" in {
    val token = JwtAuth.jwtToken(JwtAuth.jwtClaims(buildUser(), 1))
    val parsed = JwtParser.parseToken(token)
    parsed.isPresent shouldBe true
    val user = parsed.get().getUser
    user.getUid shouldBe 42
    user.getName shouldBe "alice"
    user.getEmail shouldBe "alice@example.com"
    user.getGoogleId shouldBe "g-123"
    user.getGoogleAvatar shouldBe "avatar-blob"
    user.getRole shouldBe UserRoleEnum.ADMIN
  }

  it should "carry through null optional fields without error" in {
    val user = new User()
    user.setUid(7)
    user.setName("bob")
    user.setRole(UserRoleEnum.ADMIN)
    val claims = JwtAuth.jwtClaims(user, 1)
    claims.getSubject shouldBe "bob"
    claims.getClaimValueAsString("email") shouldBe null
  }
}
