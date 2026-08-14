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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.scalalogging.Logger
import kong.unirest.Unirest
import org.apache.texera.auth.JwtAuth.{jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.common.config.UserSystemConfig.orcidBaseUrl
import org.apache.texera.dao.jooq.generated.enums.ProviderTypeEnum
import org.apache.texera.web.model.http.response.TokenIssueResponse
import org.apache.texera.web.resource.auth.OrcidAuthResource._

import javax.ws.rs.core.MediaType
import javax.ws.rs.{
  Consumes,
  GET,
  NotAuthorizedException,
  POST,
  Path,
  Produces,
  ServiceUnavailableException
}

object OrcidAuthResource {
  private val logger: Logger = Logger(classOf[OrcidAuthResource])

  final private lazy val clientId = UserSystemConfig.orcidClientId
  final private lazy val clientSecret = UserSystemConfig.orcidClientSecret
  final private lazy val redirectUri = UserSystemConfig.orcidRedirectUri

  private val CONNECT_TIMEOUT_MS = 5000
  private val SOCKET_TIMEOUT_MS = 10000

  private val mapper = new ObjectMapper()

  private[auth] final case class OrcidIdentity(orcidId: String, name: Option[String])

  private def textOf(node: JsonNode, field: String): Option[String] =
    Option(node.path(field).asText(null)).map(_.trim).filter(_.nonEmpty)

  /**
    * The names of the settings the ORCID flow cannot run without, among those given. Taken as
    * parameters rather than read from [[UserSystemConfig]] because those are object vals resolved
    * once per JVM, which leaves both the configured and unconfigured cases at the mercy of the
    * environment a test happens to run in — the same reason `AuthResource.createAdminUser` takes
    * its credentials as parameters.
    */
  private[auth] def missingSettings(
      clientId: String,
      clientSecret: String,
      redirectUri: String,
      baseUrl: String
  ): Seq[String] =
    Seq(
      "clientId" -> clientId,
      "clientSecret" -> clientSecret,
      "redirectUri" -> redirectUri,
      "baseUrl" -> baseUrl
    ).collect { case (name, value) if value == null || value.isBlank => name }

  /**
    * Read the identity out of a token-endpoint response body.
    *
    * A body with no `orcid` is refused rather than defaulted: it means the exchange authenticated
    * nobody, and provisioning against a synthesized id would hand out an account.
    */
  private[auth] def identityOf(body: String): OrcidIdentity = {
    val tree = mapper.readTree(body)
    OrcidIdentity(
      textOf(tree, "orcid").getOrElse(
        throw new NotAuthorizedException("Login credentials are incorrect.")
      ),
      textOf(tree, "name")
    )
  }

}

/**
  * ORCID sign-in. Unlike Google — whose SDK runs the whole handshake in the browser and hands the
  * frontend a signed id-token to post here — ORCID is plain authorization-code OAuth, so its
  * second leg happens on this side: the frontend forwards the one-time `code` it was redirected to
  * `/callback/orcid` with, and this trades it for the identity behind it. That code is useless
  * without `clientSecret`, which is the only reason it may travel through a browser at all.
  *
  * ORCID asserts no email under the `/authenticate` scope the login page requests, so the account
  * provisioned here has a NULL email and is deliberately not matched against any existing account.
  * See [[ExternalProfile]] for why that is the safe reading, and `AuthResource.setEmail` for how an
  * address is collected once the user is in.
  */
@Path("/auth/orcid")
class OrcidAuthResource {

  /**
    * What the login page needs to build its authorize redirect.
    *
    * A deployment missing any of the three settings the flow needs is reported unavailable rather
    * than answered with blanks. The login page enables its ORCID button the moment this resolves,
    * and each blank fails later and worse: an empty `client_id` lands the user on an ORCID error
    * page, and an empty `redirect_uri` gets the exchange rejected after they have already
    * consented. Failing here instead leaves the button disabled behind "ORCID sign-in is
    * unavailable", which is what the page already does with a failed fetch
    * (`texera-login.component.ts`).
    *
    * `redirectUri` and `baseUrl` are checked here even though only [[exchangeCode]] sends them,
    * because both are easily left empty: the deployment templates ship them for an operator to
    * fill in, and HOCON treats an env var set to "" as set, so it overrides the config default. An
    * empty `baseUrl` would otherwise answer with the relative `authorizeUrl` "/oauth/authorize",
    * which navigates the SPA to itself instead of ORCID.
    */
  @GET
  @Path("/config")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getConfig: Map[String, String] = {
    val missing = missingSettings(clientId, clientSecret, redirectUri, orcidBaseUrl)
    if (missing.nonEmpty) {
      logger.warn(
        s"ORCID sign-in is enabled but ${missing.map("user-sys.orcid." + _).mkString(", ")} " +
          "is not configured; reporting it unavailable."
      )
      throw new ServiceUnavailableException("ORCID sign-in is not configured.")
    }
    Map(
      "clientId" -> clientId,
      "authorizeUrl" -> s"$orcidBaseUrl/oauth/authorize"
    )
  }

  /**
    * Trade `code` for ORCID's token response, returning the raw body.
    *
    * `redirect_uri` is read from configuration rather than the request: ORCID requires it to match
    * the authorize call byte-for-byte, and honouring a caller-supplied one would let the browser
    * choose which registered redirect an exchange is attributed to.
    *
    * The one seam that reaches the network. Kept as a method rather than a constructor parameter
    * for the same reason [[GoogleAuthResource.verifiedPayload]] is: Jersey instantiates this
    * resource from `classOf[OrcidAuthResource]`, so tests override instead of injecting.
    */
  protected def exchangeCode(code: String): String = {
    val response = Unirest
      .post(s"$orcidBaseUrl/oauth/token")
      .header("Accept", MediaType.APPLICATION_JSON)
      .field("client_id", clientId)
      .field("client_secret", clientSecret)
      .field("grant_type", "authorization_code")
      .field("code", code)
      .field("redirect_uri", redirectUri)
      .connectTimeout(CONNECT_TIMEOUT_MS)
      .socketTimeout(SOCKET_TIMEOUT_MS)
      .asString()

    if (response.getStatus != 200) {
      logger.warn(s"ORCID token exchange returned ${response.getStatus}")
      throw new NotAuthorizedException("Login credentials are incorrect.")
    }
    response.getBody
  }

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(code: String): TokenIssueResponse = {
    val trimmedCode = Option(code).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new NotAuthorizedException("Login credentials are incorrect.")
    }

    val identity = identityOf(exchangeCode(trimmedCode))

    val user = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.ORCID,
        identity.orcidId,
        identity.name.getOrElse(identity.orcidId),
        email = None,
        avatar = None
      )
    )

    // No provider id in the claims. `jwtClaims`' second parameter is specifically the GOOGLE one —
    // it writes a claim named `googleId` — and the frontend spends that claim as a Flarum account
    // password (`flarum.service.ts`). An ORCID iD is public, so putting it there would set a
    // guessable password on that account; the iD is in `auth_provider` for anything that needs it.
    TokenIssueResponse(jwtToken(jwtClaims(user)))
  }
}
