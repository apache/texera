package org.apache.texera.web.resource.auth

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

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.jooq.DSLContext

import java.time.OffsetDateTime
import scala.util.chaining.scalaUtilChainingOps

/**
 * A verified external identity (Google, Facebook, ...) reduced to the fields we
 * persist. `avatar` is optional: `None` means the provider supplies no avatar, so
 * the user's existing avatar column is left untouched rather than overwritten.
 */
final case class ExternalProfile(
                                  providerType: ProviderTypeEnum,
                                  providerId: String,
                                  name: String,
                                  email: String,
                                  avatar: Option[String] = None
                                )

object ExternalAuthProvisioner {

  /**
   * Resolve the user behind an external identity, creating one if necessary, and
   * ensure its auth-provider row is present and up to date. Runs in a single
   * transaction and returns the (possibly newly created) user.
   */
  def loginOrProvision(profile: ExternalProfile): User =
    SqlServer.withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
      val txUserDao = new UserDao(ctx.configuration())
      val txAuthDao = new AuthProviderDao(ctx.configuration())

      Option(
        ctx
          .select()
          .from(USER)
          .join(AUTH_PROVIDER)
          .on(USER.UID.eq(AUTH_PROVIDER.UID))
          .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(profile.providerType))
          .and(AUTH_PROVIDER.PROVIDER_ID.eq(profile.providerId))
          .fetchOne()
      ) match {
        case Some(record) =>
          // known identity: refresh the profile fields if they drifted
          txUserDao.fetchOneByUid(record.get(USER.UID)).tap { user =>
            if (refresh(user, profile)) txUserDao.update(user)
          }

        case None =>
          val user = Option(txUserDao.fetchOneByEmail(profile.email)) match {
            case Some(existing) =>
              existing.tap { user =>
                if (refresh(user, profile)) txUserDao.update(user)
              }
            case None =>
              new User().tap { user =>
                user.setName(profile.name)
                user.setEmail(profile.email)
                profile.avatar.foreach(user.setAvatar)
                user.setRole(UserRoleEnum.INACTIVE)
                txUserDao.insert(user)
              }
          }

          upsertProvider(ctx, txAuthDao, user, profile)
          user
      }
    }

  /**
   * Mutate `user` in place to match `profile`, returning true iff anything changed
   * (so the caller only issues an UPDATE when needed).
   */
  private def refresh(user: User, profile: ExternalProfile): Boolean = {
    var changed = false
    if (user.getName != profile.name) {
      user.setName(profile.name)
      changed = true
    }
    if (user.getEmail != profile.email) {
      user.setEmail(profile.email)
      changed = true
    }
    profile.avatar.foreach { avatar =>
      if (user.getAvatar != avatar) {
        user.setAvatar(avatar)
        changed = true
      }
    }
    changed
  }

  private def upsertProvider(
                              ctx: DSLContext,
                              authDao: AuthProviderDao,
                              user: User,
                              profile: ExternalProfile
                            ): Unit = {
    val hasProvider = ctx.fetchExists(
      ctx
        .selectFrom(AUTH_PROVIDER)
        .where(AUTH_PROVIDER.UID.eq(user.getUid))
        .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(profile.providerType))
    )
    if (hasProvider) {
      ctx
        .update(AUTH_PROVIDER)
        .set(AUTH_PROVIDER.PROVIDER_ID, profile.providerId)
        .where(AUTH_PROVIDER.UID.eq(user.getUid))
        .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(profile.providerType))
        .execute()
    } else {
      authDao.insert(
        new AuthProvider().tap { auth =>
          auth.setUid(user.getUid)
          auth.setProviderType(profile.providerType)
          auth.setProviderId(profile.providerId)
          auth.setCreatedAt(OffsetDateTime.now())
        }
      )
    }
  }
}