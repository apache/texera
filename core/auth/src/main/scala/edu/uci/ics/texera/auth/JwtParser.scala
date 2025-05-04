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
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey
import org.jose4j.lang.UnresolvableKeyException

import java.nio.charset.StandardCharsets
import java.util.Optional

object JwtParser extends LazyLogging {

  private val TOKEN_SECRET = AuthConfig.jwtSecretKey

  private val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    .setRelaxVerificationKeyValidation()
    .build()

  def parseToken(token: String): Optional[SessionUser] = {
    try {
      val jwtClaims: JwtClaims = jwtConsumer.processToClaims(token)
      val userName = jwtClaims.getSubject
      val email = jwtClaims.getClaimValue("email", classOf[String])
      val userId = jwtClaims.getClaimValue("userId").asInstanceOf[Long].toInt
      val role = UserRoleEnum.valueOf(jwtClaims.getClaimValue("role").asInstanceOf[String])
      val googleId = jwtClaims.getClaimValue("googleId", classOf[String])

      val user = new User(userId, userName, email, null, googleId, null, role, null)
      Optional.of(new SessionUser(user))
    } catch {
      case _: UnresolvableKeyException =>
        logger.error("Invalid JWT Signature")
        Optional.empty()
      case e: Exception =>
        logger.error(s"Failed to parse JWT: ${e.getMessage}")
        Optional.empty()
    }
  }
}
