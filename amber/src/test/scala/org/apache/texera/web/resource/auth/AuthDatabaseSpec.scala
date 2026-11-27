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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.texera.web.resource.auth

import org.apache.texera.auth.InviteOnly
import org.apache.texera.dao.{DatabaseFlatSpec, SiteSettings}
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, SITE_SETTINGS, USER}
import org.apache.texera.dao.jooq.generated.enums.ProviderTypeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jooq.impl.DSL

/** Shared embedded-Postgres base for authentication and provisioning integration specs. */
abstract class AuthDatabaseSpec(protected val emailDomain: String = "")
    extends DatabaseFlatSpec(cleanupAfterEach = true) {

  protected var userDao: UserDao = _
  protected var authDao: AuthProviderDao = _

  override protected def onBeforeAll(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    authDao = new AuthProviderDao(getDSLContext.configuration())
  }

  protected def userByEmail(localPart: String): User =
    userDao.fetchOneByEmail(localPart + emailDomain)

  protected def providerIdOf(uid: Integer, providerType: ProviderTypeEnum): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(providerType))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)

  protected def deleteUsersByEmailDomain(caseInsensitive: Boolean = false): Unit = {
    val cond =
      if (caseInsensitive) DSL.lower(USER.EMAIL).like("%" + emailDomain)
      else USER.EMAIL.like("%" + emailDomain)
    getDSLContext.deleteFrom(USER).where(cond).execute()
  }

  protected def deleteUsersByProviderType(providerType: ProviderTypeEnum): Unit =
    getDSLContext
      .deleteFrom(USER)
      .where(
        USER.UID.in(
          getDSLContext
            .select(AUTH_PROVIDER.UID)
            .from(AUTH_PROVIDER)
            .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(providerType))
        )
      )
      .execute()

  protected def cleanupInviteOnlySetting(): Unit =
    getDSLContext
      .deleteFrom(SITE_SETTINGS)
      .where(SITE_SETTINGS.KEY.eq(InviteOnly.SettingKey))
      .execute()

  protected def setInviteOnly(on: Boolean, updatedBy: String): Unit =
    SiteSettings.upsert(getDSLContext, InviteOnly.SettingKey, on.toString, updatedBy)
}
