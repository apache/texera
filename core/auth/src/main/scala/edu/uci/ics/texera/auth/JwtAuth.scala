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

import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jws.AlgorithmIdentifiers.HMAC_SHA256
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey

import java.nio.charset.StandardCharsets

// TODO: move this logic to Auth
object JwtAuth {

  final val TOKEN_EXPIRE_TIME_IN_DAYS = AuthConfig.jwtExpirationDays
  final val TOKEN_SECRET: String = AuthConfig.jwtSecretKey

  val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    .setRelaxVerificationKeyValidation()
    .build

  def jwtToken(claims: JwtClaims): String = {
    val jws = new JsonWebSignature()
    jws.setPayload(claims.toJson)
    jws.setAlgorithmHeaderValue(HMAC_SHA256)
    jws.setKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    jws.getCompactSerialization
  }

  def jwtClaims(user: User, expireInDays: Int): JwtClaims = {
    val claims = new JwtClaims
    claims.setSubject(user.getName)
    claims.setClaim("userId", user.getUid)
    claims.setClaim("googleId", user.getGoogleId)
    claims.setClaim("email", user.getEmail)
    claims.setClaim("role", user.getRole)
    claims.setClaim("googleAvatar", user.getGoogleAvatar)
    claims.setExpirationTimeMinutesInTheFuture(dayToMin(expireInDays).toFloat)
    claims
  }

  def dayToMin(days: Int): Int = {
    days * 24 * 60
  }
}
