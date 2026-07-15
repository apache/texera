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

import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.gson.GsonFactory
import org.apache.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims, jwtToken}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.apache.texera.web.model.http.response.TokenIssueResponse

import java.time.OffsetDateTime
import java.util.Collections
import javax.ws.rs._
import javax.ws.rs.core.MediaType
import scala.util.chaining.scalaUtilChainingOps

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
      val googleEmail = payload.getEmail
      // "name" is not guaranteed on the payload; fall back to the email so we
      // never write null into the NOT NULL user.name column
      val googleName =
        Option(payload.get("name").asInstanceOf[String]).filter(_.nonEmpty).getOrElse(googleEmail)
      val googleAvatar = Option(payload.get("picture").asInstanceOf[String])
        .flatMap(_.split("/").lastOption)
        .getOrElse("")

      val user = SqlServer.withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
        val txUserDao = new UserDao(ctx.configuration())
        val txAuthDao = new AuthProviderDao(ctx.configuration())

        Option(
          ctx
            .select()
            .from(USER)
            .join(AUTH_PROVIDER)
            .on(USER.UID.eq(AUTH_PROVIDER.UID))
            .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.GOOGLE))
            .and(AUTH_PROVIDER.PROVIDER_ID.eq(googleId))
            .fetchOne()
        ) match {
          case Some(record) =>
            val uid = record.get(USER.UID)
            txUserDao.fetchOneByUid(uid).tap { user =>
              // name/email/avatar are profile fields on "user"; refresh from Google if changed
              if (user.getName != googleName || user.getEmail != googleEmail || user.getAvatar != googleAvatar)
              {
                user.setName(googleName)
                user.setEmail(googleEmail)
                user.setAvatar(googleAvatar)
                txUserDao.update(user)
              }
            }
          case None =>
            val user = Option(txUserDao.fetchOneByEmail(googleEmail)) match {
              case Some(user) =>
                user.tap{ user =>
                  if (user.getName != googleName || user.getAvatar != googleAvatar) {
                    user.setName(googleName)
                    user.setAvatar(googleAvatar)
                    txUserDao.update(user)
                  }
                }
              case None =>
                (new User).tap { user =>
                  user.setName(googleName)
                  user.setEmail(googleEmail)
                  user.setAvatar(googleAvatar)
                  user.setRole(UserRoleEnum.INACTIVE)
                  txUserDao.insert(user)
                }
            }

            // an email-matched user may already have a GOOGLE provider row, so
            // upsert rather than blindly insert (avoids a (uid, provider_type) PK collision)
            val hasGoogleProvider = ctx.fetchExists(
              ctx
                .selectFrom(AUTH_PROVIDER)
                .where(AUTH_PROVIDER.UID.eq(user.getUid))
                .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.GOOGLE))
            )
            if (hasGoogleProvider) {
              ctx
                .update(AUTH_PROVIDER)
                .set(AUTH_PROVIDER.PROVIDER_ID, googleId)
                .where(AUTH_PROVIDER.UID.eq(user.getUid))
                .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.GOOGLE))
                .execute()
            } else {
              val auth = new AuthProvider
              auth.setUid(user.getUid)
              auth.setProviderType(ProviderTypeEnum.GOOGLE)
              auth.setProviderId(googleId)
              auth.setCreatedAt(OffsetDateTime.now())
              txAuthDao.insert(auth)
            }
            user
        }
      }
      TokenIssueResponse(jwtToken(jwtClaims(user, TOKEN_EXPIRE_TIME_IN_MINUTES)))
    } else throw new NotAuthorizedException("Login credentials are incorrect.")
  }
}
