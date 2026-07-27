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
import org.apache.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.apache.texera.web.model.http.request.auth.{UserLoginRequest, UserRegistrationRequest}
import org.apache.texera.web.model.http.response.TokenIssueResponse
import org.apache.texera.web.resource.auth.AuthResource._
import org.jasypt.util.password.StrongPasswordEncryptor
import org.jooq.exception.DataAccessException
import org.jooq.impl.DSL.{field, name, table}

import javax.ws.rs._
import javax.ws.rs.core.MediaType

object AuthResource {

  // Explicitly typed rather than mixing in LazyLogging: the class below imports this
  // object's members, and inferring the object's signature through a mixin deadlocks
  // that import.
  private val logger: Logger = Logger(classOf[AuthResource])

  /** Postgres SQLSTATE for unique_violation. */
  private val UNIQUE_VIOLATION = "23505"

  private def context = SqlServer.getInstance().createDSLContext()

  private def userDao = new UserDao(context.configuration)

  /**
    * The login handle for a local account lives in `auth_provider.provider_id`, not in
    * `"user".name` — the latter is a display name that an admin edit or an external login
    * may rewrite at any time. Everything below therefore resolves accounts by handle.
    */
  private def localHandleExists(handle: String): Boolean =
    context.fetchExists(
      context
        .selectFrom(AUTH_PROVIDER)
        .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
        .and(AUTH_PROVIDER.PROVIDER_ID.eq(handle))
    )

  /**
    * Fail loudly at startup if the database still predates the migration that moved the
    * login handle into `auth_provider.provider_id`. Without this check the mismatch is
    * silent: every local login simply finds no row and reports bad credentials, which is
    * indistinguishable from a wrong password. There is no automated migration step in the
    * deployment path, so this is the only thing standing between a skipped migration and a
    * total login outage.
    */
  private def assertLoginHandleMigrationApplied(): Unit = {
    // `equal` rather than `eq`, which Scala resolves to reference equality on AnyRef.
    val isNullable = context
      .select(field(name("is_nullable"), classOf[String]))
      .from(table(name("information_schema", "columns")))
      .where(field(name("table_schema"), classOf[String]).equal(AUTH_PROVIDER.getSchema.getName))
      .and(field(name("table_name"), classOf[String]).equal(AUTH_PROVIDER.getName))
      .and(field(name("column_name"), classOf[String]).equal(AUTH_PROVIDER.PROVIDER_ID.getName))
      .fetchOneInto(classOf[String])

    if (isNullable == null || isNullable.equalsIgnoreCase("YES")) {
      throw new IllegalStateException(
        s"${AUTH_PROVIDER.getName}.${AUTH_PROVIDER.PROVIDER_ID.getName} is missing or still " +
          "nullable, so local login handles have not been migrated. Apply sql/updates/29.sql " +
          "before starting this version, otherwise every local login will be rejected."
      )
    }
  }

  /**
    * Retrieve exactly one User given a local login handle and a plain-text password. The
    * password is validated against the hash stored on the account's LOCAL auth_provider row.
    *
    * @param handle   String, the local login handle (what the UI calls the username)
    * @param password String, plain text password
    * @return
    */
  def retrieveUserByUsernameAndPassword(handle: String, password: String): Option[User] = {
    if (password == null || handle == null) return None

    // (provider_type, provider_id) is unique, so at most one row can match.
    val record = context
      .select()
      .from(AUTH_PROVIDER)
      .join(USER)
      .on(USER.UID.eq(AUTH_PROVIDER.UID))
      .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
      .and(AUTH_PROVIDER.PROVIDER_ID.eq(handle.trim))
      .fetchOne()

    Option(record).flatMap { r =>
      val encryptedPassword = r.get(AUTH_PROVIDER.PASSWORD)
      if (new StrongPasswordEncryptor().checkPassword(password, encryptedPassword)) {
        Some(r.into(USER).into(classOf[User]))
      } else {
        None
      }
    }
  }

  /**
    * Create a user together with the LOCAL credential it logs in with. The handle is passed
    * explicitly rather than read off `user.getName`, so that identity is never re-derived
    * from the mutable display name.
    */
  private def insertLocalUser(user: User, handle: String, hashedPassword: String): Unit = {
    SqlServer.withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
      val txUserDao = new UserDao(ctx.configuration())
      val txAuthDao = new AuthProviderDao(ctx.configuration())

      txUserDao.insert(user)

      val auth = new AuthProvider
      auth.setUid(user.getUid)
      auth.setProviderType(ProviderTypeEnum.LOCAL)
      auth.setProviderId(handle)
      auth.setPassword(hashedPassword)
      txAuthDao.insert(auth)
    }
  }

  private def isUniqueViolation(e: DataAccessException): Boolean = e.sqlState() == UNIQUE_VIOLATION

  def createAdminUser(): Unit = {
    // Checked before anything else: a deployment with no admin configured still needs to
    // find out at boot that its schema is stale, rather than by rejecting every login.
    assertLoginHandleMigrationApplied()

    val adminUsername = UserSystemConfig.adminUsername
    val adminPassword = UserSystemConfig.adminPassword

    if (adminUsername.trim.isEmpty || adminPassword.trim.isEmpty) return

    val handle = adminUsername.trim
    if (localHandleExists(handle)) return

    // The admin address may already belong to an account with no local credential (it signed
    // in with Google, say). "user".email is UNIQUE and this runs during startup with no
    // error handling above it, so inserting would abort the boot; leave that account be.
    if (userDao.fetchOneByEmail(handle) != null) {
      logger.warn(
        s"Not creating the admin account: '$handle' is already used as an email address by an " +
          "account that has no local credential. Grant that account the ADMIN role instead."
      )
      return
    }

    val user = new User
    user.setName(handle)
    user.setEmail(handle)
    user.setRole(UserRoleEnum.ADMIN)

    val hashedPassword = new StrongPasswordEncryptor().encryptPassword(adminPassword)
    insertLocalUser(user, handle, hashedPassword)
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
        TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
      case None => throw new NotAuthorizedException("Login credentials are incorrect.")
    }
  }

  @POST
  @Path("/register")
  def register(request: UserRegistrationRequest): TokenIssueResponse = {
    if (request.username == null) throw new NotAcceptableException("Username cannot be null.")
    // Store the handle trimmed: it is an authentication key, and " alice" and "alice" being
    // two different accounts is a trap rather than a feature.
    val username = request.username.trim
    if (username.isEmpty) throw new NotAcceptableException("Username cannot be empty.")
    if (localHandleExists(username)) throw new NotAcceptableException("Username exists already.")

    val user = new User
    user.setName(username)
    user.setEmail(username)
    user.setRole(UserRoleEnum.RESTRICTED)

    // hash the plain text password
    val hashedPassword = new StrongPasswordEncryptor().encryptPassword(request.password)
    try {
      insertLocalUser(user, username, hashedPassword)
    } catch {
      // lost a race with a concurrent registration of the same handle or email; the
      // constraint is the real arbiter, the check above is just the fast path
      case e: DataAccessException if isUniqueViolation(e) =>
        throw new NotAcceptableException("Username exists already.")
    }
    TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
  }

}
