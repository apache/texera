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
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.auth.{SessionUser, TokenEncryptionService}
import org.apache.texera.web.model.http.response.DriveTokenIssueResponse
import org.apache.texera.web.resource.auth.GoogleDriveAuthResource._
import org.apache.texera.dao.jooq.generated.tables.daos.UserOauthTokenDao
import org.apache.texera.dao.jooq.generated.tables.pojos.UserOauthToken
import org.apache.texera.dao.SqlServer
import org.apache.texera.config.UserSystemConfig
import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeRequestUrl,
  GoogleAuthorizationCodeTokenRequest,
  GoogleRefreshTokenRequest,
  GoogleTokenResponse
}
import com.google.api.client.auth.oauth2.TokenResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory

import java.util.concurrent.ConcurrentHashMap
import javax.annotation.security.RolesAllowed
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response

object GoogleDriveAuthResource {
  private val STATUS_OK = "ok"
  private val STATUS_NO_REFRESH_TOKEN = "no_refresh_token"
  private val STATUS_INVALID_GRANT = "invalid_grant"
  private val PROVIDER_GOOGLE_DRIVE = "google_drive"

  private val STATE_TTL_MS = 10 * 60 * 1000L

  private val mapper = new ObjectMapper()

  // state token → (uid, expiresAtMs)
  private val pendingStates = new ConcurrentHashMap[String, (Int, Long)]()

  private def oauthTokenDao =
    new UserOauthTokenDao(
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
    val uid = sessionUser.getUid
    val record = oauthTokenDao.fetchByUid(uid).stream()
      .filter(r => r.getProvider == PROVIDER_GOOGLE_DRIVE)
      .findFirst()
      .orElse(null)

    if (record == null) {
      return Response.ok(DriveTokenIssueResponse(STATUS_NO_REFRESH_TOKEN, None)).build()
    }

    try {
      val blob = mapper.readTree(TokenEncryptionService.decrypt(record.getAuthBlob))
      val refreshToken = blob.get("refreshToken").asText()

      val tokenResponse = new GoogleRefreshTokenRequest(
        new NetHttpTransport(),
        GsonFactory.getDefaultInstance,
        refreshToken,
        clientId,
        clientSecret
      ).execute()

      Response.ok(DriveTokenIssueResponse(STATUS_OK, Some(tokenResponse.getAccessToken))).build()
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
      val entry = pendingStates.remove(state)
      if (entry == null || System.currentTimeMillis() > entry._2) {
        return Response
          .status(Response.Status.UNAUTHORIZED)
          .entity("OAuth state token is invalid or expired")
          .build()
      }

      val uid = entry._1

      val tokenResponse: GoogleTokenResponse = new GoogleAuthorizationCodeTokenRequest(
        new NetHttpTransport(),
        GsonFactory.getDefaultInstance,
        clientId,
        clientSecret,
        code,
        redirectUri
      ).execute()

      val blobMap = new java.util.HashMap[String, String]()
      blobMap.put("refreshToken", tokenResponse.getRefreshToken)
      blobMap.put("scopes", tokenResponse.getScope)
      val blobJson = mapper.writeValueAsString(blobMap)
      val encryptedBlob = TokenEncryptionService.encrypt(blobJson)

      val existing = oauthTokenDao.fetchByUid(uid).stream()
        .filter(r => r.getProvider == PROVIDER_GOOGLE_DRIVE)
        .findFirst()

      if (existing.isPresent) {
        existing.get().setAuthBlob(encryptedBlob)
        oauthTokenDao.update(existing.get())
      } else {
        val record = new UserOauthToken()
        record.setUid(uid)
        record.setProvider(PROVIDER_GOOGLE_DRIVE)
        record.setAuthBlob(encryptedBlob)
        oauthTokenDao.insert(record)
      }

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
    val stateToken = java.util.UUID.randomUUID().toString
    pendingStates.put(stateToken, (sessionUser.getUid, System.currentTimeMillis() + STATE_TTL_MS))

    val url = new GoogleAuthorizationCodeRequestUrl(
      clientId,
      redirectUri,
      java.util.Arrays.asList("https://www.googleapis.com/auth/drive")
    )
      .setState(stateToken)
      .setAccessType("offline")
      .set("prompt", if (reauth) "consent" else null)
      .set("include_granted_scopes", true)
      .build()

    Response.ok(url).build()
  }
}
