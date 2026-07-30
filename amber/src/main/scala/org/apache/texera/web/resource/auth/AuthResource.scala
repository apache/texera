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

import javax.ws.rs._
import javax.ws.rs.core.MediaType

object AuthResource {
  private val logger: Logger = Logger(classOf[AuthResource])

  /** Postgres SQLSTATE for unique_violation. */
  private val UNIQUE_VIOLATION = "23505"

  private def context = SqlServer.getInstance().context
  private def userDao = new UserDao(context.configuration)

  private val passwordEncryptor = new StrongPasswordEncryptor

  private def localHandleExists(handle: String): Boolean = {
    context.fetchExists(
      context
        .selectFrom(AUTH_PROVIDER)
        .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
        .and(AUTH_PROVIDER.PROVIDER_ID.eq(handle))
    )
  }

  //TODO ASSERT THAT ALL USERS WERE MIGRATED CORRECTLY AND CHECK

  /**
    * Retrieve exactly one User from databases with the given username and password.
    * The password is used to validate against the hashed password stored in the db.
    *
    * @param username String
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
      if (passwordEncryptor.checkPassword(password, encryptedPassword)) {
        Some(r.into(USER).into(classOf[User]))
      } else {
        None
      }
    })
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

  def createAdminUser(): Unit = {
    val adminUsername = UserSystemConfig.adminUsername.trim
    val adminPassword = UserSystemConfig.adminPassword.trim

    if (adminUsername.isEmpty || adminPassword.isEmpty) return

    if (localHandleExists(adminUsername)) return

    if (userDao.fetchOneByEmail(adminUsername) != null) {
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

    insertLocalUser(user, adminUsername, passwordEncryptor.encryptPassword(adminPassword))
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
    val username = Option(request.username).getOrElse("").trim
    val useremail = Option(request.email).getOrElse("").trim
    val userpassword = request.password
    if (username.isEmpty)
      throw new NotAcceptableException("Username cannot be empty")
    if (useremail.isEmpty)
      throw new NotAcceptableException("Email cannot be empty")
    if (!useremail.matches("""^[^\s@]+@[^\s@]+\.[^\s@]+$"""))
      throw new NotAcceptableException("Email format is invalid.")
    if (userpassword == null || userpassword.isEmpty)
      throw new NotAcceptableException("Password cannot be empty")

    // Check if email already exists
    val usernameExists = !userDao.fetchByName(username).isEmpty
    val emailExists = userDao.fetchOneByEmail(useremail) != null

    (usernameExists, emailExists) match {
      case (true, _) =>
        throw new NotAcceptableException("Username exists already.")
      case (_, true) =>
        throw new NotAcceptableException("Email exists already.")
      case (false, false) =>
        val user = new User
        user.setName(username)
        user.setEmail(useremail)
        user.setRole(UserRoleEnum.RESTRICTED)
        insertLocalUser(
          user,
          username,
          AuthResource.passwordEncryptor.encryptPassword(userpassword)
        )
        TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
    }
  }

}
