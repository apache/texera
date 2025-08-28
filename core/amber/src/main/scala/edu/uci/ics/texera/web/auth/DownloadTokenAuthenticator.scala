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

package edu.uci.ics.texera.web.auth

import edu.uci.ics.texera.config.AuthConfig
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.HmacKey
import org.jose4j.lang.UnresolvableKeyException

import java.nio.charset.StandardCharsets
import javax.ws.rs.NotAuthorizedException

object DownloadTokenAuthenticator {

  private val TOKEN_SECRET = AuthConfig.jwtSecretKey

  private val jwtConsumer: JwtConsumer = new JwtConsumerBuilder()
    .setAllowedClockSkewInSeconds(30)
    .setRequireExpirationTime()
    .setRequireSubject()
    .setVerificationKey(new HmacKey(TOKEN_SECRET.getBytes(StandardCharsets.UTF_8)))
    .setRelaxVerificationKeyValidation()
    .build()

  def parseToken(token: String): DownloadTokenClaims = {
    try {
      val jwtClaims: JwtClaims = jwtConsumer.processToClaims(token)
      val exportType = jwtClaims.getClaimValue("exportType", classOf[String])
      val workflowId = jwtClaims.getClaimValue("workflowId").asInstanceOf[Long].toInt
      val workflowName = jwtClaims.getClaimValue("workflowName", classOf[String])
      val rowIndex = jwtClaims.getClaimValue("rowIndex").asInstanceOf[Long].toInt
      val columnIndex = jwtClaims.getClaimValue("columnIndex").asInstanceOf[Long].toInt
      val filename = jwtClaims.getClaimValue("filename", classOf[String])
      val computingUnitId = jwtClaims.getClaimValue("computingUnitId").asInstanceOf[Long].toInt
      val destination = jwtClaims.getClaimValue("destination", classOf[String])
      val role = jwtClaims.getClaimValue("role", classOf[String])

      DownloadTokenClaims(
        exportType,
        workflowId,
        workflowName,
        rowIndex,
        columnIndex,
        filename,
        computingUnitId,
        destination,
        role
      )

    } catch {
      case _: UnresolvableKeyException =>
        throw new NotAuthorizedException("Invalid JWT Signature")
      case e: Exception =>
        throw new NotAuthorizedException(s"Failed to parse JWT: ${e.getMessage}")
    }
  }

}
