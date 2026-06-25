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

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.web.resource.auth.GoogleDriveAuthResource._
import org.apache.texera.common.config.UserSystemConfig
import com.google.api.client.googleapis.auth.oauth2.{
  GoogleAuthorizationCodeRequestUrl,
  GoogleAuthorizationCodeTokenRequest
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
  private val STATE_TTL_MS = 10 * 60 * 1000L

  // Maps state token → expiresAtMs.
  // Expired entries are swept out on each getOAuth call to prevent unbounded growth from abandoned flows.
  private val pendingStates = new ConcurrentHashMap[String, Long]()
}

@Path("/auth/google/drive")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class GoogleDriveAuthResource extends LazyLogging {

  private def errorHtml(message: String): String =
    s"""<html><body>
       |<p style="font-family:sans-serif;padding:20px">$message</p>
       |<script>
       |window.opener?.postMessage('gdrive-error', window.location.origin);
       |setTimeout(function(){ window.close(); }, 10000);
       |</script>
       |</body></html>""".stripMargin

  final private lazy val clientId = UserSystemConfig.googleClientId
  final private lazy val clientSecret = UserSystemConfig.googleClientSecret
  final private lazy val redirectUri = UserSystemConfig.appDomain
    .map(domain => s"https://$domain/api/auth/google/drive/callback")
    .getOrElse("http://localhost:4200/api/auth/google/drive/callback")

  @GET
  @Path("/connect")
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.TEXT_PLAIN))
  def getOAuth(): Response = {
    val now = System.currentTimeMillis()
    pendingStates.entrySet().removeIf(e => now > e.getValue)

    val stateToken = java.util.UUID.randomUUID().toString
    pendingStates.put(stateToken, now + STATE_TTL_MS)

    val url = new GoogleAuthorizationCodeRequestUrl(
      clientId,
      redirectUri,
      java.util.Arrays.asList("https://www.googleapis.com/auth/drive.file")
    )
      .setState(stateToken)
      .build()

    Response.ok(url).build()
  }

  @GET
  @Path("/callback")
  @Produces(Array(MediaType.TEXT_HTML, MediaType.APPLICATION_JSON))
  def getCallback(
      @QueryParam("code") @DefaultValue("") code: String,
      @QueryParam("state") @DefaultValue("") state: String
  ): Response = {
    if (code.isEmpty || state.isEmpty) {
      return Response.ok(errorHtml("Connection failed: invalid request. Please try again.")).build()
    }
    try {
      val expiresAt = pendingStates.remove(state)
      if (expiresAt == null || System.currentTimeMillis() > expiresAt) {
        return Response
          .ok(errorHtml("Connection failed: the authorisation request expired. Please try again."))
          .build()
      }

      new GoogleAuthorizationCodeTokenRequest(
        new NetHttpTransport(),
        GsonFactory.getDefaultInstance,
        clientId,
        clientSecret,
        code,
        redirectUri
      ).execute()

      val html =
        """<html><body><script>
          |window.opener.postMessage('gdrive-connected', window.location.origin);
          |window.close();
          |</script></body></html>""".stripMargin
      Response.ok(html).build()
    } catch {
      case e: TokenResponseException =>
        logger.error("Google token exchange failed in callback", e)
        Response
          .ok(
            errorHtml(
              "Connection failed: could not complete sign-in with Google. Please try again."
            )
          )
          .build()
      case e: Exception =>
        logger.error("Unexpected error in OAuth callback", e)
        Response.ok(errorHtml("An unexpected error occurred. Please try again.")).build()
    }
  }
}
