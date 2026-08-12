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
import org.apache.texera.auth.JwtAuth.{jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.jooq.generated.enums.ProviderTypeEnum
import org.apache.texera.web.model.http.response.TokenIssueResponse
import org.jose4j.jwa.AlgorithmConstraints
import org.jose4j.jwk.HttpsJwks
import org.jose4j.jws.AlgorithmIdentifiers
import org.jose4j.jwt.JwtClaims
import org.jose4j.jwt.consumer.{InvalidJwtException, JwtConsumer, JwtConsumerBuilder}
import org.jose4j.keys.resolvers.HttpsJwksVerificationKeyResolver

import javax.ws.rs.core.MediaType
import javax.ws.rs.{Consumes, GET, NotAuthorizedException, POST, Path, Produces}

object AppleAuthResource extends LazyLogging {

  /** Where Apple publishes the public keys its identity tokens are signed with. */
  private[auth] val APPLE_JWKS_URL = "https://appleid.apple.com/auth/keys"
  private[auth] val APPLE_ISSUER = "https://appleid.apple.com"

  // HttpsJwks caches the key set and refetches when a token arrives bearing an unknown `kid`,
  // so Apple's key rotation needs no redeploy. Shared, because the cache is the whole point.
  private lazy val appleJwks = new HttpsJwks(APPLE_JWKS_URL)

  /**
    * Read a claim Apple types inconsistently. `email_verified` and `is_private_email` arrive as
    * either a JSON boolean or a quoted string, and Apple documents both shapes. Reading only one
    * silently yields `false`, which for `email_verified` means [[ExternalAuthProvisioner]]
    * refuses a legitimate login.
    */
  private[auth] def booleanClaim(claims: JwtClaims, name: String): Boolean =
    claims.getClaimValue(name) match {
      case b: java.lang.Boolean => b.booleanValue()
      case s: String            => s.trim.equalsIgnoreCase("true")
      case _                    => false
    }

  /**
    * Reduce a verified Apple identity token to the fields we persist.
    *
    * `sub` is the stable per-user identifier and becomes the provider id. Note it is scoped to
    * the Apple developer *team*: transferring the app to another team changes it for every
    * user, and Apple exposes `transfer_sub` for only 60 days after such a move.
    *
    * Apple sends the display name solely on a user's first ever authorization, and outside the
    * identity token — it rides in the JS response body, so it never reaches this endpoint, and
    * being unsigned it would be untrusted anyway. The address stands in for it, exactly as it
    * does for a Google account with no name. Apple supplies no avatar at all, hence the empty
    * string, which the provisioner treats as "leave whatever is stored alone".
    *
    * A token with no address, or one Apple has not verified, is refused rather than mapped — see
    * [[ExternalProfile]] for why an unverified address is a takeover. Apple omits `email` for
    * Sign in with Apple at Work & School accounts, and there would be nothing to link or
    * provision against.
    */
  private[auth] def profileOf(claims: JwtClaims): ExternalProfile = {
    val email = Option(claims.getClaimValueAsString("email")).map(_.trim).getOrElse("")
    if (email.isEmpty) {
      logger.warn(
        s"Refusing Apple identity ${claims.getSubject}: the token carries no email address."
      )
      throw new NotAuthorizedException("Login credentials are incorrect.")
    }
    if (!booleanClaim(claims, "email_verified")) {
      logger.warn(
        s"Refusing Apple identity ${claims.getSubject}: Apple did not verify its email address."
      )
      throw new NotAuthorizedException("Login credentials are incorrect.")
    }
    ExternalProfile(
      ProviderTypeEnum.APPLE,
      claims.getSubject,
      name = email,
      email = email,
      avatar = ""
    )
  }
}

@Path("/auth/apple")
class AppleAuthResource extends LazyLogging {
  final private lazy val clientId = UserSystemConfig.appleClientId

  @GET
  @Path("/clientid")
  def getClientId: String = clientId

  /**
    * Rejects anything not signed by a current Apple key, not issued by Apple, not addressed to
    * this Services ID, or expired. The algorithm is pinned to RS256 so a token cannot talk the
    * verifier into a weaker one.
    */
  private lazy val jwtConsumer: JwtConsumer =
    new JwtConsumerBuilder()
      .setRequireExpirationTime()
      .setRequireSubject()
      .setExpectedIssuer(AppleAuthResource.APPLE_ISSUER)
      .setExpectedAudience(clientId)
      .setVerificationKeyResolver(
        new HttpsJwksVerificationKeyResolver(AppleAuthResource.appleJwks)
      )
      .setJwsAlgorithmConstraints(
        new AlgorithmConstraints(
          AlgorithmConstraints.ConstraintType.PERMIT,
          AlgorithmIdentifiers.RSA_USING_SHA256
        )
      )
      .build()

  /**
    * Verify `credential` against Apple's published keys, yielding its claims, or None if it is
    * not a valid token for this client. The only seam that reaches the network, so tests
    * override it rather than sign a token; kept a method rather than a constructor parameter
    * because Jersey instantiates this resource from `classOf[AppleAuthResource]`.
    *
    * A malformed or unverifiable credential becomes None, and so a 401, instead of escaping as
    * a 500.
    */
  protected def verifiedClaims(credential: String): Option[JwtClaims] =
    try Option(jwtConsumer.processToClaims(credential))
    catch {
      case e: InvalidJwtException =>
        logger.warn(s"Rejecting Apple credential: ${e.getMessage}")
        None
    }

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(credential: String): TokenIssueResponse =
    verifiedClaims(credential) match {
      case Some(claims) =>
        val user = ExternalAuthProvisioner.loginOrProvision(AppleAuthResource.profileOf(claims))
        // No `googleId` claim: that one names a Google identity, and this login has none.
        TokenIssueResponse(jwtToken(jwtClaims(user)))
      case None => throw new NotAuthorizedException("Login credentials are incorrect.")
    }
}
