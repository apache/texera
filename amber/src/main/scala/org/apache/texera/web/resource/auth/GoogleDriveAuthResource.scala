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

import io.dropwizard.auth.Auth
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.auth.{JwtParser, SessionUser}
import org.apache.texera.web.model.http.response.DriveTokenIssueResponse
import org.apache.texera.web.resource.auth.GoogleDriveAuthResource._
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.SqlServer
import org.apache.texera.config.UserSystemConfig
import org.apache.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims}
import org.apache.texera.auth.JwtAuth
import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeRequestUrl,
  GoogleAuthorizationCodeTokenRequest,
  GoogleRefreshTokenRequest,
  GoogleTokenResponse
}
import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory

import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

object GoogleDriveAuthResource {
  // Status codes for token
  private val STATUS_OK = "ok"
  private val STATUS_NO_REFRESH_TOKEN = "no_refresh_token"
  private val STATUS_INVALID_GRANT = "invalid_grant"

  private def userDao =
    new UserDao(
      SqlServer
        .getInstance()
        .createDSLContext()
        .configuration
    )
}

@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class GoogleDriveAuthResource extends LazyLogging {
  final private lazy val clientId = UserSystemConfig.googleClientId
  final private lazy val clientSecret = UserSystemConfig.googleClientSecret
  final private lazy val redirectUri = UserSystemConfig.appDomain
    .map(domain => s"https://$domain/api/auth/google/drive/callback")
    .getOrElse("http://localhost:4200/api/auth/google/drive/callback")

  @GET
  @Path("/token")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getDriveAccessToken(@Auth sessionUser: SessionUser): Response = {
    val user = userDao.fetchOneByUid(sessionUser.getUid)
    val refreshToken = user.getGoogleDriveRefreshToken
    if (refreshToken == null) {
      return Response.ok(DriveTokenIssueResponse(STATUS_NO_REFRESH_TOKEN, None)).build()
    }
    try {
      val tokenResponse = new GoogleRefreshTokenRequest(
        new NetHttpTransport(),
        GsonFactory.getDefaultInstance,
        refreshToken,
        clientId,
        clientSecret
      ).execute()
      val accessToken = tokenResponse.getAccessToken
      Response.ok(DriveTokenIssueResponse(STATUS_OK, Some(accessToken))).build()
    } catch {
      case e: TokenResponseException =>
        if (e.getDetails != null && e.getDetails.getError == STATUS_INVALID_GRANT) {
          Response.ok(DriveTokenIssueResponse(STATUS_INVALID_GRANT, None)).build()
        } else {
          logger.error("Failed to refresh access token", e)
          Response.status(Response.Status.INTERNAL_SERVER_ERROR).build()
        }
      case e: Exception =>
        logger.error("Unexpected error refreshing access token", e)
        Response.status(Response.Status.INTERNAL_SERVER_ERROR).build()
    }
  }

  @GET
  @Path("/callback")
  @Produces(Array(MediaType.TEXT_HTML, MediaType.APPLICATION_JSON))
  def getCallback(
      @QueryParam("code") @DefaultValue("") code: String,
      @QueryParam("state") @DefaultValue("") state: String
  ): Response = {
    if (code.isEmpty || state.isEmpty) {
      return Response.status(Response.Status.BAD_REQUEST).build()
    }
    try {
      val sessionUserOpt = JwtParser.parseToken(state)
      if (!sessionUserOpt.isPresent) {
        return Response
          .status(Response.Status.UNAUTHORIZED)
          .entity("User is not authenticated")
          .build()
      }

      val userId = sessionUserOpt.get().getUid
      val user = userDao.fetchOneByUid(userId)

      val response: GoogleTokenResponse = new GoogleAuthorizationCodeTokenRequest(
        new NetHttpTransport(),
        GsonFactory.getDefaultInstance,
        clientId,
        clientSecret,
        code,
        redirectUri
      ).execute()

      user.setGoogleDriveRefreshToken(response.getRefreshToken)
      userDao.update(user)

      val html =
        """<html><body><script>
          |window.opener.postMessage('gdrive-connected', window.location.origin);
          |window.close();
          |</script></body></html>""".stripMargin
      Response.ok(html).build()
    } catch {
      case e: TokenResponseException =>
        logger.error("Google token exchange failed in callback", e)
        Response.status(Response.Status.BAD_GATEWAY).build()
      case e: Exception =>
        logger.error("Unexpected error in OAuth callback", e)
        Response.status(Response.Status.INTERNAL_SERVER_ERROR).build()
    }
  }

  @GET
  @Path("/connect")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  def getOAuth(
      @Auth sessionUser: SessionUser,
      @QueryParam("reauth") @DefaultValue("false") reauth: Boolean
  ): Response = {
    val user = sessionUser.getUser
    val state = JwtAuth.jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES))

    val url = new GoogleAuthorizationCodeRequestUrl(
      clientId,
      redirectUri,
      java.util.Arrays.asList("https://www.googleapis.com/auth/drive")
    )
      .setState(state)
      .setAccessType("offline")
      .set("prompt", if (reauth) "consent" else null)
      .set("include_granted_scopes", true)
      .build()

    Response.ok(url).build()
  }
}
