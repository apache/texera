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
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jwt.JwtClaims
import org.jose4j.lang.UnresolvableKeyException

import java.util.Optional

/** Single source of truth for converting a verified JWT into a [[SessionUser]].
  *
  * Verification reuses [[JwtAuth.jwtConsumer]] (same secret, same clock-skew
  * config). The claim set extracted here mirrors what [[JwtAuth.jwtClaims]]
  * writes when issuing a token.
  */
object JwtParser extends LazyLogging {

  /** Verify and parse a Bearer token string. */
  def parseToken(token: String): Optional[SessionUser] = {
    try {
      Optional.of(claimsToSessionUser(JwtAuth.jwtConsumer.processToClaims(token)))
    } catch {
      case _: UnresolvableKeyException =>
        logger.error("Invalid JWT Signature")
        Optional.empty()
      case e: Exception =>
        logger.error(s"Failed to parse JWT: ${e.getMessage}")
        Optional.empty()
    }
  }

  /** Build a [[SessionUser]] from already-verified claims. Used by both
    * [[parseToken]] (which verifies then calls this) and amber's
    * `UserAuthenticator` (which the toastshaman filter calls after its own
    * signature verification).
    */
  def claimsToSessionUser(claims: JwtClaims): SessionUser = {
    val userName = claims.getSubject
    val email = claims.getClaimValue("email", classOf[String])
    // jose4j returns Long after JSON round-trip but the original setClaim
    // call writes Integer; widen via Number to handle both cases.
    val userId = claims.getClaimValue("userId", classOf[Number]).intValue()
    val role = UserRoleEnum.valueOf(claims.getClaimValue("role").asInstanceOf[String])
    val googleId = claims.getClaimValue("googleId", classOf[String])
    val googleAvatar = claims.getClaimValue("googleAvatar", classOf[String])
    val user = new User(
      userId,
      userName,
      email,
      null,
      googleId,
      googleAvatar,
      role,
      null,
      null,
      null,
      null
    )
    new SessionUser(user)
  }
}
