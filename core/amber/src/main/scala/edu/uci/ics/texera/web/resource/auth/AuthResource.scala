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

package edu.uci.ics.texera.web.resource.auth

import edu.uci.ics.texera.auth.JwtAuth.{
  TOKEN_EXPIRE_TIME_IN_MINUTES,
  jwtClaims,
  jwtConsumer,
  jwtToken
}
import edu.uci.ics.texera.config.UserSystemConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.web.model.http.request.auth.{
  RefreshTokenRequest,
  UserLoginRequest,
  UserRegistrationRequest
}
import edu.uci.ics.texera.web.model.http.response.TokenIssueResponse
import edu.uci.ics.texera.dao.jooq.generated.Tables.USER
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.auth.AuthResource._
import org.jasypt.util.password.StrongPasswordEncryptor

import javax.ws.rs._
import javax.ws.rs.core.MediaType

object AuthResource {

  final private lazy val userDao = new UserDao(
    SqlServer
      .getInstance()
      .createDSLContext()
      .configuration
  )

  /**
    * Retrieve exactly one User from databases with the given username and password.
    * The password is used to validate against the hashed password stored in the db.
    *
    * @param name     String
    * @param password String, plain text password
    * @return
    */
  def retrieveUserByUsernameAndPassword(name: String, password: String): Option[User] = {
    Option(
      SqlServer
        .getInstance()
        .createDSLContext()
        .select()
        .from(USER)
        .where(USER.NAME.eq(name))
        .fetchOneInto(classOf[User])
    ).filter(user => new StrongPasswordEncryptor().checkPassword(password, user.getPassword))
  }

  def createAdminUser(): Unit = {
    val adminUsername = UserSystemConfig.adminUsername
    val adminPassword = UserSystemConfig.adminPassword

    if (adminUsername.trim.nonEmpty && adminPassword.trim.nonEmpty) {
      val existingUser = userDao.fetchByName(adminUsername)
      if (existingUser.isEmpty) {
        val user = new User
        user.setName(adminUsername)
        user.setEmail(adminUsername)
        user.setRole(UserRoleEnum.ADMIN)
        user.setPassword(new StrongPasswordEncryptor().encryptPassword(adminPassword))
        userDao.insert(user)
      }
    }
  }
}

@Path("/auth/")
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
class AuthResource {

  @POST
  @Path("/login")
  def login(request: UserLoginRequest): TokenIssueResponse = {
    if (!UserSystemConfig.isUserSystemEnabled)
      throw new NotAcceptableException("User System is disabled on the backend!")
    retrieveUserByUsernameAndPassword(request.username, request.password) match {
      case Some(user) =>
        TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
      case None => throw new NotAuthorizedException("Login credentials are incorrect.")
    }
  }

  @POST
  @Path("/refresh")
  def refresh(request: RefreshTokenRequest): TokenIssueResponse = {
    val claims = jwtConsumer.process(request.accessToken).getJwtClaims
    claims.setExpirationTimeMinutesInTheFuture(TOKEN_EXPIRE_TIME_IN_MINUTES.toFloat)
    TokenIssueResponse(jwtToken(claims))
  }

  @POST
  @Path("/register")
  def register(request: UserRegistrationRequest): TokenIssueResponse = {
    if (!UserSystemConfig.isUserSystemEnabled)
      throw new NotAcceptableException("User System is disabled on the backend!")
    val username = request.username
    if (username == null) throw new NotAcceptableException("Username cannot be null.")
    if (username.trim.isEmpty) throw new NotAcceptableException("Username cannot be empty.")
    userDao.fetchByName(username).size() match {
      case 0 =>
        val user = new User
        user.setName(username)
        user.setEmail(username)
        user.setRole(UserRoleEnum.RESTRICTED)
        // hash the plain text password
        user.setPassword(new StrongPasswordEncryptor().encryptPassword(request.password))
        userDao.insert(user)
        TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
      case _ =>
        // the username exists already
        throw new NotAcceptableException("Username exists already.")
    }
  }

}
