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

package org.apache.texera.web.resource.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import org.apache.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.jooq.generated.enums.ProviderTypeEnum
import org.apache.texera.web.model.http.response.TokenIssueResponse

import java.util.Collections
import javax.ws.rs._
import javax.ws.rs.core.MediaType

@Path("/auth/google")
class GoogleAuthResource {
  final private lazy val clientId = UserSystemConfig.googleClientId

  @GET
  @Path("/clientid")
  def getClientId: String = clientId

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(credential: String): TokenIssueResponse = {
    val idToken =
      new GoogleIdTokenVerifier.Builder(new NetHttpTransport, GsonFactory.getDefaultInstance)
        .setAudience(
          Collections.singletonList(clientId)
        )
        .build()
        .verify(credential)
    if (idToken != null) {
      val payload = idToken.getPayload
      val googleId = payload.getSubject
      val googleEmail = payload.getEmail
      val googleName =
        Option(payload.get("name").asInstanceOf[String]).filter(_.nonEmpty).getOrElse(googleEmail)
      val googleAvatar = Option(payload.get("picture").asInstanceOf[String])
        .flatMap(_.split("/").lastOption)
        .getOrElse("")

      val user = ExternalAuthProvisioner.loginOrProvision(
        ExternalProfile(
          ProviderTypeEnum.GOOGLE,
          googleId,
          googleName,
          googleEmail,
          Some(googleAvatar)
        )
      )
      TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
    } else throw new NotAuthorizedException("Login credentials are incorrect.")
  }
}
