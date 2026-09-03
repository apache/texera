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
import org.apache.texera.dao.{SqlServer, SqlStates}
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.jooq.DSLContext
import org.jooq.exception.DataAccessException

import java.time.OffsetDateTime
import scala.util.chaining.scalaUtilChainingOps

/**
  * A verified external identity (Google, Facebook, ...) reduced to the fields we persist.
  *
  * `email` must be non-blank and provider-verified: `loginOrProvision` links the identity to the
  * account owning that address and claims its placeholder, so an unverified address is a
  * takeover. Each provider checks this in its own mapping function (Google: `email_verified`).
  *
  * `avatar` is the complete URL the provider supplied, already sanitized by `AvatarUtil`.
  * `None` means the provider offered no avatar we would store, in which case the account keeps
  * whatever is on file rather than having it blanked.
  */
final case class ExternalProfile(
    providerType: ProviderTypeEnum,
    providerId: String,
    name: String,
    email: String,
    avatar: Option[String]
)

/**
  * An identity a provider authenticates without asserting any address — ORCID, whose
  * `/authenticate` scope yields an iD and a name and nothing else.
  *
  * A separate type rather than an optional `email` on [[ExternalProfile]]: the difference is what
  * the provider vouches for, not how much of it is filled in, and an email-asserting provider's
  * contract should stay a plain `String`. Provisioned through
  * [[ExternalAuthProvisioner.loginOrProvisionIdentityOnly]].
  */
final case class ExternalIdentity(
    providerType: ProviderTypeEnum,
    providerId: String,
    name: String
)

object ExternalAuthProvisioner extends LazyLogging {

  /**
    * What provisioning actually works with: the fields a provider may or may not have asserted.
    * Private, so the optionality never reaches a caller — each public entry point below states
    * plainly which kind of provider it serves.
    */
  private final case class Asserted(
      providerType: ProviderTypeEnum,
      providerId: String,
      name: String,
      email: Option[String],
      avatar: Option[String]
  )

  /**
    * The account owning `email`, matched case-insensitively and within the caller's transaction
    * so it reads that transaction's own writes. See
    * [[AuthResource.fetchUserByEmailIgnoreCase]] for why the match cannot be exact.
    */
  private def userByEmailIgnoreCase(ctx: DSLContext, email: String): Option[User] =
    Option(AuthResource.fetchUserByEmailIgnoreCase(ctx, email))

  /**
    * Resolve the user behind an external identity, creating one if necessary, and
    * ensure its auth-provider row is present and up to date. Each attempt runs in one
    * transaction. A unique violation is taken to mean a concurrent login won the race, so the
    * attempt is re-run once; if the retry violates a constraint too, that exception propagates.
    */
  def loginOrProvision(profile: ExternalProfile): User =
    attempt(
      Asserted(
        profile.providerType,
        profile.providerId,
        profile.name,
        Some(profile.email),
        profile.avatar
      )
    )

  /**
    * As [[loginOrProvision]], for a provider that asserts no address.
    *
    * The account is created with a NULL email and is deliberately never matched to an existing
    * one: the only address available for matching would be one the user typed, and linking on
    * that is the takeover [[ExternalProfile]] describes. Such an account signs in but is inert
    * for the email-keyed parts of the product (dataset paths, access grants) until the address is
    * collected — see `AuthResource.setEmail`.
    */
  def loginOrProvisionIdentityOnly(identity: ExternalIdentity): User =
    attempt(
      Asserted(identity.providerType, identity.providerId, identity.name, None, None)
    )

  private def attempt(profile: Asserted): User = {
    try {
      provision(profile)
    } catch {
      case e: DataAccessException if e.sqlState() == SqlStates.UNIQUE_VIOLATION =>
        provision(profile)
    }
  }

  private def provision(profile: Asserted): User = {
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
          // The join above already selected every USER column, so map in place rather than
          // re-reading the same row by uid.
          record.into(USER).into(classOf[User]).tap { user =>
            if (refresh(user, profile)) txUserDao.update(user)
          }

        case None =>
          // An identity-only provider skips the lookup entirely rather than matching on nothing,
          // so it always lands in the insert branch below.
          val user = profile.email.flatMap(userByEmailIgnoreCase(ctx, _)) match {
            case Some(existing) =>
              existing.tap { user =>
                val wasPlaceholder = user.getIsPlaceholder
                if (wasPlaceholder) AuthResource.claimPlaceholder(user)
                val drifted = refresh(user, profile)
                if (drifted || wasPlaceholder) txUserDao.update(user)
              }
            case None =>
              val created = new User()
              created.setName(profile.name)
              // Left NULL for an identity-only provider. The column is nullable and its UNIQUE
              // index tolerates repeated NULLs, so several such accounts can coexist.
              profile.email.foreach(created.setEmail)
              profile.avatar.foreach(created.setAvatar)
              created.setRole(UserRoleEnum.INACTIVE)
              txUserDao.insert(created)
              created
          }
          upsertProvider(ctx, txAuthDao, user, profile)
          user
      }
    }
  }

  /** The external id `uid` authenticates with at `providerType`, if it has one. */
  def providerIdOf(uid: Integer, providerType: ProviderTypeEnum): Option[String] =
    Option(
      SqlServer
        .getInstance()
        .context
        .select(AUTH_PROVIDER.PROVIDER_ID)
        .from(AUTH_PROVIDER)
        .where(AUTH_PROVIDER.UID.eq(uid))
        .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(providerType))
        .fetchOne(AUTH_PROVIDER.PROVIDER_ID)
    )

  /**
    * Mutate `user` in place to match `profile`, returning true iff anything changed
    * (so the caller only issues an UPDATE when needed).
    *
    * A field the provider did not assert is left as it is rather than blanked: an identity-only
    * provider carries no address, and on a returning login the account may well have one by then
    * — collected through `AuthResource.setEmail` — which this must not undo.
    */
  private def refresh(user: User, profile: Asserted): Boolean = {
    var changed = false
    if (user.getName != profile.name) {
      user.setName(profile.name)
      changed = true
    }
    profile.email.filter(_ != user.getEmail).foreach { email =>
      user.setEmail(email)
      changed = true
    }
    profile.avatar.filter(_ != user.getAvatar).foreach { url =>
      user.setAvatar(url)
      changed = true
    }
    changed
  }

  private def upsertProvider(
      ctx: DSLContext,
      authDao: AuthProviderDao,
      user: User,
      profile: Asserted
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
