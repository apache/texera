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

import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.AUTH_PROVIDER
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.jasypt.util.password.StrongPasswordEncryptor
import org.jooq.exception.DataAccessException

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

/**
  * The LOCAL half of authentication: password hashing and the "insert a user together with the
  * credential it logs in with" transaction. The counterpart to [[ExternalAuthProvisioner]].
  *
  * This exists because self-registration ([[AuthResource]]), admin-created accounts
  * (`AdminUserResource`) and the admin bootstrap all need the same two-row insert, and each
  * previously carried its own copy plus its own `StrongPasswordEncryptor`. A change to how a
  * local credential is stored now lands in one place.
  */
object LocalAuthProvisioner {

  /** Postgres unique-violation SQLSTATE. */
  private val UNIQUE_VIOLATION = "23505"

  private val passwordEncryptor = new StrongPasswordEncryptor

  private def context = SqlServer.getInstance().context

  def hashPassword(rawPassword: String): String =
    passwordEncryptor.encryptPassword(rawPassword)

  def checkPassword(rawPassword: String, hashedPassword: String): Boolean =
    passwordEncryptor.checkPassword(rawPassword, hashedPassword)

  /**
    * Whether `handle` is already taken as a LOCAL login handle. Note this asks
    * `auth_provider.provider_id`, not `"user".name` — the display name is mutable and is not
    * identity, so it cannot answer this question.
    */
  def handleExists(handle: String): Boolean =
    context.fetchExists(
      context
        .selectFrom(AUTH_PROVIDER)
        .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
        .and(AUTH_PROVIDER.PROVIDER_ID.eq(handle))
    )

  /**
    * Insert `user` and its LOCAL credential in one transaction, so a user row can never be left
    * behind without the credential that makes it usable. `user` is mutated in place with the
    * generated uid.
    *
    * The handle is passed explicitly rather than read off `user.getName`, so that identity is
    * never re-derived from the mutable display name. Callers should pre-check with
    * [[handleExists]] to report a friendly error; the unique-violation mapping here is the
    * race fallback for two registrations of the same handle interleaving.
    */
  def createLocalAccount(user: User, handle: String, rawPassword: String): Unit = {
    val hashedPassword = hashPassword(rawPassword)

    try {
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
    } catch {
      case e: DataAccessException if e.sqlState() == UNIQUE_VIOLATION =>
        throw new WebApplicationException(
          s"Login handle $handle is already taken",
          e,
          Response.Status.CONFLICT
        )
    }
  }

  /**
    * Persist `user` and give it a LOCAL credential in one transaction, for an account row that
    * already exists — claiming a dataset-contributor placeholder. The counterpart to
    * [[createLocalAccount]], which inserts the user instead of updating it; both write the
    * credential in the same transaction as the user row so an account can never be left in a
    * state where it looks claimed but has nothing to log in with.
    */
  def claimWithLocalCredential(user: User, handle: String, rawPassword: String): Unit = {
    val hashedPassword = hashPassword(rawPassword)

    try {
      SqlServer.withTransaction(SqlServer.getInstance().createDSLContext()) { ctx =>
        new UserDao(ctx.configuration()).update(user)

        val auth = new AuthProvider
        auth.setUid(user.getUid)
        auth.setProviderType(ProviderTypeEnum.LOCAL)
        auth.setProviderId(handle)
        auth.setPassword(hashedPassword)
        new AuthProviderDao(ctx.configuration()).insert(auth)
      }
    } catch {
      case e: DataAccessException if e.sqlState() == UNIQUE_VIOLATION =>
        throw new WebApplicationException(
          s"Login handle $handle is already taken",
          e,
          Response.Status.CONFLICT
        )
    }
  }

  /** Create an INACTIVE account whose display name is its login handle. */
  def createLocalAccount(handle: String, rawPassword: String): Unit = {
    val user = new User
    user.setName(handle)
    user.setRole(UserRoleEnum.INACTIVE)
    createLocalAccount(user, handle, rawPassword)
  }
}
