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

import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import edu.uci.ics.texera.config.UserSystemConfig
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims, jwtToken}
import edu.uci.ics.texera.web.model.http.response.TokenIssueResponse
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRoleEnum
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.UserDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.User
import edu.uci.ics.texera.web.resource.auth.GoogleAuthResource.userDao

import java.util.Collections
import javax.ws.rs._
import javax.ws.rs.core.MediaType

object GoogleAuthResource {
  final private lazy val userDao = new UserDao(
    SqlServer
      .getInstance()
      .createDSLContext()
      .configuration
  )
}

@Path("/auth/google")
class GoogleAuthResource {
  final private lazy val clientId = UserSystemConfig.googleClientId

  @GET
  @Path("/clientid")
  def getClientId: String = clientId

  @POST
  @Consumes(Array(MediaType.TEXT_PLAIN))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/login")
  def login(credential: String): TokenIssueResponse = {
    if (!UserSystemConfig.isUserSystemEnabled)
      throw new NotAcceptableException("User System is disabled on the backend!")
    val idToken =
      new GoogleIdTokenVerifier.Builder(new NetHttpTransport, GsonFactory.getDefaultInstance)
        .setAudience(
          Collections.singletonList(clientId)
        )
        .build()
        .verify(credential)
    if (idToken != null) {
      val payload = idToken.getPayload
      val googleId = payload.getSubject
      val googleName = payload.get("name").asInstanceOf[String]
      val googleEmail = payload.getEmail
      val googleAvatar = Option(payload.get("picture").asInstanceOf[String])
        .flatMap(_.split("/").lastOption)
        .getOrElse("")
      val user = Option(userDao.fetchOneByGoogleId(googleId)) match {
        case Some(user) =>
          if (user.getName != googleName) {
            user.setName(googleName)
            userDao.update(user)
          }
          if (user.getEmail != googleEmail) {
            user.setEmail(googleEmail)
            userDao.update(user)
          }
          if (user.getGoogleAvatar != googleAvatar) {
            user.setGoogleAvatar(googleAvatar)
            userDao.update(user)
          }
          user
        case None =>
          Option(userDao.fetchOneByEmail(googleEmail)) match {
            case Some(user) =>
              if (user.getName != googleName) {
                user.setName(googleName)
              }
              user.setGoogleId(googleId)
              user.setGoogleAvatar(googleAvatar)
              userDao.update(user)
              user
            case None =>
              // create a new user with googleId
              val user = new User
              user.setName(googleName)
              user.setEmail(googleEmail)
              user.setGoogleId(googleId)
              user.setRole(UserRoleEnum.INACTIVE)
              user.setGoogleAvatar(googleAvatar)
              userDao.insert(user)
              user
          }
      }
      TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
    } else throw new NotAuthorizedException("Login credentials are incorrect.")
  }
}
