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
import org.apache.texera.common.util.EmailUtil
import org.apache.texera.dao.jooq.generated.enums.ProviderTypeEnum
import org.apache.texera.web.model.http.response.OrcidLoginResponse
import org.apache.texera.web.resource.auth.OrcidAuthResource._

import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, GET, NotAuthorizedException, POST, Path, Produces}
import scala.jdk.CollectionConverters.IteratorHasAsScala

object OrcidAuthResource {
  private val logger: Logger = Logger(classOf[OrcidAuthResource])

  final private lazy val clientId = UserSystemConfig.orcidClientId
  final private lazy val clientSecret = UserSystemConfig.orcidClientSecret
  final private lazy val redirectUri = UserSystemConfig.orcidRedirectUri

  // A user is waiting on the callback page while these run, so both sit far below the browser's
  // patience: ORCID either answers promptly or this login has failed.
  private val CONNECT_TIMEOUT_MS = 5000
  private val SOCKET_TIMEOUT_MS = 10000

  private val mapper = new ObjectMapper()

  /**
    * The identity behind a redeemed authorization code. `orcidId` is the ORCID iD
    * (`0000-0002-1825-0097`); `name` is absent when the record's owner keeps it private.
    *
    * Both arrived over the back channel, on a connection our client secret opened, which is what
    * separates them from anything in the redirect URL: the browser cannot have chosen them.
    */
  private[auth] final case class OrcidIdentity(orcidId: String, name: Option[String])

  private def textOf(node: JsonNode, field: String): Option[String] =
    Option(node.path(field).asText(null)).map(_.trim).filter(_.nonEmpty)

  /** `a=1&b=2` with both halves percent-encoded — the client secret in particular may need it. */
  private def formEncode(fields: Seq[(String, String)]): String =
    fields
      .map {
        case (name, value) =>
          s"${URLEncoder.encode(name, StandardCharsets.UTF_8)}=${URLEncoder.encode(value, StandardCharsets.UTF_8)}"
      }
      .mkString("&")

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

  /**
    * The address to offer as a prefill from ORCID's email response, preferring the one the record
    * marks primary. Anything unparseable yields None, which costs a filled-in form field.
    */
  private[auth] def prefillFrom(body: String): Option[String] = {
    val entries = mapper.readTree(body).path("email")
    if (!entries.isArray) None
    else {
      val all = entries.elements().asScala.toSeq
      all
        .find(_.path("primary").asBoolean(false))
        .orElse(all.headOption)
        .flatMap(textOf(_, "email"))
        .filter(EmailUtil.isValid)
    }
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
  @GET
  @Path("/config")
  @Produces(Array(MediaType.APPLICATION_JSON))
  def getConfig: Map[String, String] =
    Map(
      "clientId" -> clientId,
      "authorizeUrl" -> s"$orcidBaseUrl/oauth/authorize"
    )

  /**
    * Trade `code` for ORCID's token response, returning the raw body.
    *
    * `redirect_uri` is read from configuration rather than the request: ORCID requires it to match
    * the authorize call byte-for-byte, and honouring a caller-supplied one would let the browser
    * choose which registered redirect an exchange is attributed to.
    *
    * One of the two seams that reach the network. Kept as a method rather than a constructor
    * parameter for the same reason [[GoogleAuthResource.verifiedPayload]] is: Jersey instantiates
    * this resource from `classOf[OrcidAuthResource]`, so tests override instead of injecting.
    */
  protected def exchangeCode(code: String): String = {
    // Encoded by hand rather than with Unirest's `.field()`, which switches the request to
    // multipart/form-data under conditions that are not obvious from the call site. ORCID accepts
    // only application/x-www-form-urlencoded here, so the encoding is stated outright.
    val form = formEncode(
      Seq(
        "client_id" -> clientId,
        "client_secret" -> clientSecret,
        "grant_type" -> "authorization_code",
        "code" -> code,
        "redirect_uri" -> redirectUri
      )
    )

    val response = Unirest
      .post(s"$orcidBaseUrl/oauth/token")
      .header("Content-Type", MediaType.APPLICATION_FORM_URLENCODED)
      .header("Accept", MediaType.APPLICATION_JSON)
      .body(form)
      .connectTimeout(CONNECT_TIMEOUT_MS)
      .socketTimeout(SOCKET_TIMEOUT_MS)
      .asString()

    if (response.getStatus != 200) {
      // Status only. The body of a failed exchange quotes the request back, and the body of a
      // successful one carries a bearer token; neither belongs in a log.
      logger.warn(s"ORCID token exchange returned ${response.getStatus}")
      throw new NotAuthorizedException("Login credentials are incorrect.")
    }
    response.getBody
  }

  /**
    * The email this ORCID record publishes, if any — a prefill for the address prompt, never a key
    * anything is matched on. ORCID returns only addresses the owner chose to make public, and an
    * address being public is no evidence the owner controls it, so linking on this would be
    * exactly the takeover [[ExternalProfile]] warns about.
    *
    * Best-effort: a failure here costs a prefilled form field, so it is logged and swallowed
    * rather than failing a login that has already succeeded.
    */
  protected def publishedEmail(orcidId: String, accessToken: String): Option[String] =
    try {
      val response = Unirest
        .get(s"$orcidBaseUrl/v3.0/$orcidId/email")
        .header("Accept", MediaType.APPLICATION_JSON)
        .header("Authorization", s"Bearer $accessToken")
        .connectTimeout(CONNECT_TIMEOUT_MS)
        .socketTimeout(SOCKET_TIMEOUT_MS)
        .asString()

      if (response.getStatus != 200) {
        logger.info(s"ORCID published-email lookup returned ${response.getStatus}")
        None
      } else prefillFrom(response.getBody)
    } catch {
      case e: Exception =>
        logger.info(s"ORCID published-email lookup failed: ${e.getClass.getSimpleName}")
        None
    }

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(code: String): OrcidLoginResponse = {
    val trimmedCode = Option(code).map(_.trim).filter(_.nonEmpty).getOrElse {
      throw new NotAuthorizedException("Login credentials are incorrect.")
    }

    val body = exchangeCode(trimmedCode)
    val identity = identityOf(body)

    val user = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.ORCID,
        identity.orcidId,
        // The iD stands in for a private name: `"user".name` is NOT NULL, and an ORCID iD is at
        // least a real handle rather than an invented placeholder.
        identity.name.getOrElse(identity.orcidId),
        email = None,
        avatar = None
      )
    )

    // Only worth a second round trip on a login that will actually prompt for an address — a
    // returning user who already supplied one is not asked again.
    val suggestedEmail = Option(user.getEmail) match {
      case Some(_) => None
      case None =>
        textOf(mapper.readTree(body), "access_token")
          .flatMap(token => publishedEmail(identity.orcidId, token))
    }

    OrcidLoginResponse(jwtToken(jwtClaims(user, Some(identity.orcidId))), suggestedEmail)
  }
}
