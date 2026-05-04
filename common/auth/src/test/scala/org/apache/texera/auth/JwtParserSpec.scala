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
import org.jose4j.jws.AlgorithmIdentifiers.HMAC_SHA256
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.JwtClaims
import org.jose4j.keys.HmacKey
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets

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

  it should "return empty when the token is signed with the wrong secret" in {
    val token = signWith(buildClaims(), "definitely-not-the-real-secret-for-testing-only")
    JwtParser.parseToken(token).isPresent shouldBe false
  }

  it should "return empty when the token is expired" in {
    val claims = new JwtClaims
    claims.setSubject("alice")
    claims.setClaim("userId", 42)
    claims.setClaim("role", UserRoleEnum.ADMIN.name)
    claims.setExpirationTimeMinutesInTheFuture(-10f) // expired 10 minutes ago
    val token = JwtAuth.jwtToken(claims)
    JwtParser.parseToken(token).isPresent shouldBe false
  }

  it should "return empty when the token has no subject claim" in {
    val claims = new JwtClaims
    claims.setClaim("userId", 42)
    claims.setClaim("role", UserRoleEnum.ADMIN.name)
    claims.setExpirationTimeMinutesInTheFuture(10f)
    val token = JwtAuth.jwtToken(claims)
    JwtParser.parseToken(token).isPresent shouldBe false
  }

  it should "return empty when the token has no exp claim" in {
    // The shared consumer is built with setRequireExpirationTime(); a token
    // missing exp must be rejected even if the signature is valid.
    val claims = new JwtClaims
    claims.setSubject("alice")
    claims.setClaim("userId", 42)
    claims.setClaim("role", UserRoleEnum.ADMIN.name)
    val token = JwtAuth.jwtToken(claims)
    JwtParser.parseToken(token).isPresent shouldBe false
  }

  it should "still accept a token expired within the 30s clock-skew window" in {
    val claims = buildClaims()
    // 5 seconds ago — well inside setAllowedClockSkewInSeconds(30).
    claims.setExpirationTimeMinutesInTheFuture(-0.083f)
    val token = JwtAuth.jwtToken(claims)
    JwtParser.parseToken(token).isPresent shouldBe true
  }

  it should "reject a token expired beyond the 30s clock-skew window" in {
    val claims = buildClaims()
    // 90 seconds ago — past the 30s allowance.
    claims.setExpirationTimeMinutesInTheFuture(-1.5f)
    val token = JwtAuth.jwtToken(claims)
    JwtParser.parseToken(token).isPresent shouldBe false
  }

  it should "return empty when the signature segment is tampered" in {
    val token = JwtAuth.jwtToken(buildClaims())
    val parts = token.split('.')
    parts.length shouldBe 3
    val tampered = s"${parts(0)}.${parts(1)}.${parts(2).reverse}"
    JwtParser.parseToken(tampered).isPresent shouldBe false
  }

  it should "return empty when the payload segment is tampered" in {
    // Re-base64 a claim with a different userId; the signature in parts(2)
    // covers parts(0).parts(1), so the rebuilt token won't verify.
    val token = JwtAuth.jwtToken(buildClaims())
    val parts = token.split('.')
    val swappedClaims = new JwtClaims
    swappedClaims.setSubject("mallory")
    swappedClaims.setClaim("userId", 99999)
    swappedClaims.setClaim("role", UserRoleEnum.ADMIN.name)
    swappedClaims.setExpirationTimeMinutesInTheFuture(10f)
    val rebuiltPayload = java.util.Base64.getUrlEncoder.withoutPadding.encodeToString(
      swappedClaims.toJson.getBytes(StandardCharsets.UTF_8)
    )
    val tampered = s"${parts(0)}.$rebuiltPayload.${parts(2)}"
    JwtParser.parseToken(tampered).isPresent shouldBe false
  }

  "JwtParser life cycle" should "round-trip distinct users without state leakage" in {
    val alice = buildClaims()
    val bob = new JwtClaims
    bob.setSubject("bob")
    bob.setClaim("userId", 7)
    bob.setClaim("googleId", "g-bob")
    bob.setClaim("email", "bob@example.com")
    bob.setClaim("role", UserRoleEnum.REGULAR.name)
    bob.setClaim("googleAvatar", "bob-avatar")
    bob.setExpirationTimeMinutesInTheFuture(10f)

    val aliceUser = JwtParser.parseToken(JwtAuth.jwtToken(alice)).get().getUser
    val bobUser = JwtParser.parseToken(JwtAuth.jwtToken(bob)).get().getUser

    aliceUser.getUid shouldBe 42
    aliceUser.getName shouldBe "alice"
    aliceUser.getRole shouldBe UserRoleEnum.ADMIN
    bobUser.getUid shouldBe 7
    bobUser.getName shouldBe "bob"
    bobUser.getRole shouldBe UserRoleEnum.REGULAR
  }

  it should "produce equivalent SessionUser objects when re-parsed multiple times" in {
    val token = JwtAuth.jwtToken(buildClaims())
    val first = JwtParser.parseToken(token).get().getUser
    val second = JwtParser.parseToken(token).get().getUser
    first.getUid shouldBe second.getUid
    first.getName shouldBe second.getName
    first.getEmail shouldBe second.getEmail
    first.getGoogleAvatar shouldBe second.getGoogleAvatar
    first.getRole shouldBe second.getRole
  }

  /** Sign a JwtClaims payload with an arbitrary secret. Used to produce a
    * token whose signature won't verify against the real consumer's key.
    */
  private def signWith(claims: JwtClaims, secret: String): String = {
    val jws = new JsonWebSignature
    jws.setPayload(claims.toJson)
    jws.setAlgorithmHeaderValue(HMAC_SHA256)
    jws.setKey(new HmacKey(secret.getBytes(StandardCharsets.UTF_8)))
    jws.getCompactSerialization
  }
}
