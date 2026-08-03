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

import com.google.api.client.googleapis.auth.oauth2.{GoogleIdToken, GoogleIdTokenVerifier}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import org.apache.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.jooq.generated.enums.ProviderTypeEnum
import org.apache.texera.web.model.http.response.TokenIssueResponse

import java.util.Collections
import javax.ws.rs._
import javax.ws.rs.core.MediaType

object GoogleAuthResource {

  /**
    * Reduce a verified Google id-token payload to the fields we persist. Google omits `name`
    * for accounts with no profile name, and the provisioner writes `name` straight to a NOT
    * NULL column, so the address stands in for it. Only the last path segment of `picture` is
    * kept. The frontend rebuilds the full `lh3.googleusercontent.com` URL around it.
    */
  private[auth] def profileOf(payload: GoogleIdToken.Payload): ExternalProfile = {
    val googleEmail = payload.getEmail
    ExternalProfile(
      ProviderTypeEnum.GOOGLE,
      payload.getSubject,
      Option(payload.get("name").asInstanceOf[String]).filter(_.nonEmpty).getOrElse(googleEmail),
      googleEmail,
      // getEmailVerified boxes to null when the claim is absent; absent means unverified.
      emailVerified = Option(payload.getEmailVerified).exists(_.booleanValue()),
      avatar = Option(payload.get("picture").asInstanceOf[String])
        .filter(_.nonEmpty)
        .map(_.split("/").last)
    )
  }
}

@Path("/auth/google")
class GoogleAuthResource {
  final private lazy val clientId = UserSystemConfig.googleClientId

  @GET
  @Path("/clientid")
  def getClientId: String = clientId

  /**
    * Verify `credential` against Google, yielding its payload, or None if it is not a valid
    * token for this client. The only seam that reaches the network, so tests override it
    * instead of signing a token; kept a method rather than a constructor parameter because
    * Jersey instantiates this resource from `classOf[GoogleAuthResource]`.
    */
  protected def verifiedPayload(credential: String): Option[GoogleIdToken.Payload] =
    Option(
      new GoogleIdTokenVerifier.Builder(new NetHttpTransport, GsonFactory.getDefaultInstance)
        .setAudience(
          Collections.singletonList(clientId)
        )
        .build()
        .verify(credential)
    ).map(_.getPayload)

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(credential: String): TokenIssueResponse =
    verifiedPayload(credential) match {
      case Some(payload) =>
        val profile = GoogleAuthResource.profileOf(payload)
        val user = ExternalAuthProvisioner.loginOrProvision(profile)
        // The frontend reads `googleId` off the raw token; the provider id is already in hand
        // here, so no lookup is needed.
        TokenIssueResponse(
          jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES, Some(profile.providerId)))
        )
      case None => throw new NotAuthorizedException("Login credentials are incorrect.")
    }
}
