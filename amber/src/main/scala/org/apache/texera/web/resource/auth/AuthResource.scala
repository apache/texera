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

import com.typesafe.scalalogging.Logger
import org.apache.texera.auth.JwtAuth.{jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.common.util.EmailUtil
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.model.http.request.auth.{UserLoginRequest, UserRegistrationRequest}
import org.apache.texera.web.model.http.response.TokenIssueResponse
import org.apache.texera.web.resource.auth.AuthResource._
import org.jooq.impl.DSL

import java.time.Instant
import java.time.temporal.ChronoUnit
import javax.ws.rs._
import javax.ws.rs.core.MediaType

object AuthResource {
  private val logger: Logger = Logger(classOf[AuthResource])

  private def context = SqlServer.getInstance().context

  private[auth] val UNIQUE_VIOLATION = "23505"

  /**
    * Retrieve exactly one User from databases with the given username and password.
    * The password is used to validate against the hashed password stored in the db.
    *
    * @param username the LOCAL login handle to authenticate
    * @param password String, plain text password
    * @return
    */
  def retrieveUserByUsernameAndPassword(username: String, password: String): Option[User] = {
    if (password == null || username == null) return None

    val record = context
      .select()
      .from(AUTH_PROVIDER)
      .join(USER)
      .on(USER.UID.eq(AUTH_PROVIDER.UID))
      .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
      .and(AUTH_PROVIDER.PROVIDER_ID.eq(username))
      .fetchOne()

    Option(record).flatMap(r => {
      val encryptedPassword = r.get(AUTH_PROVIDER.PASSWORD)
      if (LocalAuthProvisioner.checkPassword(password, encryptedPassword)) {
        Some(r.into(USER).into(classOf[User]))
      } else {
        None
      }
    })
  }

  /**
    * Email identity is matched case-insensitively (backed by idx_user_email_lower),
    * while stored emails keep their original casing.
    */
  def fetchUserByEmailIgnoreCase(email: String): User =
    SqlServer
      .getInstance()
      .createDSLContext()
      .selectFrom(USER)
      .where(DSL.lower(USER.EMAIL).eq(EmailUtil.normalize(email)))
      .fetchOneInto(classOf[User])

  /**
    * Marks a placeholder account (auto-created for a dataset contributor) as
    * claimed, leaving persistence to the caller.
    */
  def claimPlaceholder(user: User): Unit = {
    user.setIsPlaceholder(false)
    val claimedAt = Instant.now().truncatedTo(ChronoUnit.SECONDS)
    user.setComment(
      Option(user.getComment).map(_ + "; ").getOrElse("") +
        s"Claimed contributor placeholder at $claimedAt"
    )
  }

  def createAdminUser(): Unit =
    createAdminUser(UserSystemConfig.adminUsername.trim, UserSystemConfig.adminPassword.trim)

  /**
    * Bootstrap the configured admin account, doing nothing if it already exists. The credentials
    * are parameters rather than reads of [[UserSystemConfig]] because those are object vals
    * resolved once per JVM, which leaves the unconfigured case unreachable from a test.
    */
  private[auth] def createAdminUser(adminUsername: String, adminPassword: String): Unit = {
    if (adminUsername.isEmpty || adminPassword.isEmpty) return

    if (LocalAuthProvisioner.handleExists(adminUsername)) return

    if (fetchUserByEmailIgnoreCase(adminUsername) != null) {
      logger.warn(
        s"Not creating the admin account: '$adminUsername' is already used as an email address " +
          "by an account with no local credential. Grant that account the ADMIN role instead."
      )
      return
    }

    val user = new User
    user.setName(adminUsername)
    user.setEmail(adminUsername)
    user.setRole(UserRoleEnum.ADMIN)

    LocalAuthProvisioner.createLocalAccount(user, adminUsername, adminPassword)
  }
}

@Path("/auth/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class AuthResource {

  @POST
  @Path("/login")
  def login(request: UserLoginRequest): TokenIssueResponse = {
    retrieveUserByUsernameAndPassword(request.username, request.password) match {
      case Some(user) =>
        // An account can hold both a LOCAL and a GOOGLE credential, and the frontend expects
        // `googleId` in the token regardless of which one was used to sign in.
        val googleId =
          ExternalAuthProvisioner.providerIdOf(user.getUid, ProviderTypeEnum.GOOGLE)
        TokenIssueResponse(jwtToken(jwtClaims(user, googleId)))
      case None => throw new NotAuthorizedException("Login credentials are incorrect.")
    }
  }

  @POST
  @Path("/register")
  def register(request: UserRegistrationRequest): TokenIssueResponse = {
    val username = Option(request.username).getOrElse("").trim
    val useremail = Option(request.email).getOrElse("").trim
    val userpassword = request.password
    if (username.isEmpty)
      throw new NotAcceptableException("Username cannot be empty")
    if (useremail.isEmpty)
      throw new NotAcceptableException("Email cannot be empty")
    if (!EmailUtil.isValid(useremail))
      throw new NotAcceptableException("Email format is invalid.")
    if (userpassword == null || userpassword.isEmpty)
      throw new NotAcceptableException("Password cannot be empty")

    // The username being registered becomes a LOCAL login handle, so the handle is what has to
    // be free, not the display name. Asking `"user".name` instead both missed genuinely taken
    // handles (letting the insert die on uq_provider_identity as a 500) and rejected free ones,
    // because an external login rewrites the display name but never the handle.
    val usernameExists = LocalAuthProvisioner.handleExists(username)
    val existingByEmail = fetchUserByEmailIgnoreCase(useremail)
    val emailExists = existingByEmail != null

    // A placeholder account (created for a dataset contributor, never had any
    // credential) is claimed by the first registration with its email. The
    // account keeps its uid, so existing contributor links stay valid, and it
    // stays INACTIVE until an admin approves it.
    //
    // The credential is written to auth_provider rather than onto the user row, in the same
    // transaction as the claim, so the account cannot end up marked claimed with nothing to
    // log in with.
    if (!usernameExists && emailExists && existingByEmail.getIsPlaceholder) {
      existingByEmail.setName(username)
      claimPlaceholder(existingByEmail)
      LocalAuthProvisioner.claimWithLocalCredential(existingByEmail, username, userpassword)
      return TokenIssueResponse(jwtToken(jwtClaims(existingByEmail)))
    }

    (usernameExists, emailExists) match {
      case (true, _) =>
        throw new NotAcceptableException("Username exists already.")
      case (_, true) =>
        throw new NotAcceptableException("Email exists already.")
      case (false, false) =>
        val user = new User
        user.setName(username)
        user.setEmail(useremail)
        user.setRole(UserRoleEnum.INACTIVE)
        // Reports losing the race to a concurrent registration of the same handle as a 409.
        LocalAuthProvisioner.createLocalAccount(user, username, userpassword)
        TokenIssueResponse(jwtToken(jwtClaims(user)))
    }
  }

}
