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

import org.apache.texera.auth.JwtAuth
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.JwtContext
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserAuthenticatorSpec extends AnyFlatSpec with Matchers {

  // Mirror exactly what JwtAuth.jwtClaims would write at issue time, so
  // the spec doubles as a contract check between the issuer and the
  // amber-side authenticator.
  private def buildClaims(): JwtClaims = {
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

  // Run a token through the production consumer to get a real JwtContext —
  // matches what the toastshaman filter hands the authenticator at runtime.
  private def contextFor(claims: JwtClaims): JwtContext =
    JwtAuth.jwtConsumer.process(JwtAuth.jwtToken(claims))

  "UserAuthenticator.authenticate" should "delegate to JwtParser and return a populated SessionUser" in {
    val result = UserAuthenticator.authenticate(contextFor(buildClaims()))
    result.isPresent shouldBe true
    val u = result.get().getUser
    u.getUid shouldBe 42
    u.getName shouldBe "alice"
    u.getEmail shouldBe "alice@example.com"
    u.getGoogleId shouldBe "g-123"
    u.getGoogleAvatar shouldBe "avatar-blob"
    u.getRole shouldBe UserRoleEnum.ADMIN
  }

  it should "return empty when a required custom claim (userId) is missing" in {
    val claims = new JwtClaims
    claims.setSubject("alice")
    claims.setClaim("role", UserRoleEnum.ADMIN.name)
    claims.setExpirationTimeMinutesInTheFuture(10f)
    UserAuthenticator.authenticate(contextFor(claims)).isPresent shouldBe false
  }

  it should "return empty when a required custom claim (role) is missing" in {
    val claims = new JwtClaims
    claims.setSubject("alice")
    claims.setClaim("userId", 42)
    claims.setExpirationTimeMinutesInTheFuture(10f)
    UserAuthenticator.authenticate(contextFor(claims)).isPresent shouldBe false
  }
}
