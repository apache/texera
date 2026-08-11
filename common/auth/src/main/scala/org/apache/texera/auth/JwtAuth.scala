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
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jws.AlgorithmIdentifiers.HMAC_SHA256
import org.jose4j.jws.JsonWebSignature
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey

import java.nio.charset.StandardCharsets

// TODO: move this logic to Auth
object JwtAuth {

  final val TOKEN_SECRET: String = AuthConfig.jwtSecretKey
  final val TOKEN_EXPIRE_TIME_IN_MINUTES: Int = AuthConfig.jwtExpirationMinutes

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

  /**
    * Build the claim set for `user`. The claim names are a contract with the hand-written
    * TypeScript reader in `frontend/src/app/common/service/user/auth.service.ts`, which is not
    * compiled against this file — so renaming one here silently breaks the frontend. `avatar`
    * now lives on `"user"` rather than a Google-specific column, but the claim keeps its
    * `googleAvatar` name until the frontend is migrated in lockstep.
    *
    * `googleId` is passed in rather than read off `user`, because the GOOGLE provider id lives
    * in `auth_provider` and this module must stay DB-free: the specs in
    * `access-control-service` / `config-service` and the token re-issue paths in
    * `ResultExportService` / `ComputingUnitManagingResource` all call this with no
    * `auth_provider` context, as does `AuthResource.register`. Omitting the claim is harmless
    * on all of them: a service-to-service token never reaches the browser, and a freshly
    * registered LOCAL account has no Google identity to name.
    *
    * Note this changes the token's shape, not just where the value comes from: `googleId` was
    * previously written unconditionally, so a local-only user's token carried `"googleId": null`
    * and now omits the claim. The frontend declares it optional (`common/type/user.ts`), so the
    * only reader that can tell is `flarum.service.ts`, which passes it as a Flarum account
    * password — a path that was already broken for exactly the users who lack the claim.
    */
  def jwtClaims(user: User, googleId: Option[String] = None): JwtClaims = {
    val claims = new JwtClaims
    claims.setSubject(user.getName)
    claims.setClaim("userId", user.getUid)
    claims.setClaim("email", user.getEmail)
    claims.setClaim("role", user.getRole)
    claims.setClaim("googleAvatar", user.getAvatar)
    googleId.foreach(claims.setClaim("googleId", _))
    claims.setExpirationTimeMinutesInTheFuture(TOKEN_EXPIRE_TIME_IN_MINUTES.toFloat)
    claims
  }
}
