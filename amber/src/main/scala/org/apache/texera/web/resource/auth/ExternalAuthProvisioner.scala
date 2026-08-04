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
import org.apache.texera.dao.SqlServer
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.jooq.DSLContext

import java.net.URI
import java.time.OffsetDateTime
import javax.ws.rs.NotAuthorizedException
import scala.util.chaining.scalaUtilChainingOps
import scala.util.Try

/**
  * A verified external identity (Google, Facebook, ...) reduced to the fields we
  * persist. `avatar` is the complete URL the provider supplied, and is optional:
  * `None` means the provider supplies no avatar, so the user's existing avatar column
  * is left untouched rather than overwritten.
  *
  * `emailVerified` reports whether the provider itself vouches for `email`. It has no
  * default on purpose: an email address is what links an external identity to an
  * existing account, so treating an unverified one as trusted is an account-takeover
  * path, and a defaulted flag is how that mistake comes back.
  */
final case class ExternalProfile(
    providerType: ProviderTypeEnum,
    providerId: String,
    name: String,
    email: String,
    emailVerified: Boolean,
    avatar: Option[String] = None
)

object ExternalAuthProvisioner extends LazyLogging {
  // ── avatar host allowlist ──
  private val ALLOWED_AVATAR_HOST_SUFFIXES: Set[String] = Set(
    "googleusercontent.com"
  )

  /** Allow an exact host or any subdomain of an allowlisted suffix. */
  private[auth] def isAllowedAvatarHost(host: String): Boolean = {
    if (host == null || host.isEmpty) return false
    val lower = host.toLowerCase
    ALLOWED_AVATAR_HOST_SUFFIXES.exists(suffix => lower == suffix || lower.endsWith("." + suffix))
  }

  /**
    * The avatar URL to persist, or `None` to leave the stored value alone. Anything that is not
    * an http(s) URL on an allowlisted host is dropped rather than rejected: a surprising avatar
    * is not a reason to deny someone a login, and treating it as "provider supplied no avatar"
    * falls back to the initials avatar.
    */
  private[auth] def sanitizedAvatar(profile: ExternalProfile): Option[String] =
    profile.avatar.filter { url =>
      val host = Try(URI.create(url)).toOption.filter { uri =>
        val scheme = Option(uri.getScheme).map(_.toLowerCase)
        scheme.contains("http") || scheme.contains("https")
      }.flatMap(uri => Option(uri.getHost))

      val allowed = host.exists(isAllowedAvatarHost)
      if (!allowed) {
        logger.warn(
          s"Ignoring avatar from ${profile.providerType} identity ${profile.providerId}: " +
            s"'$url' is not an http(s) URL on an allowlisted host."
        )
      }
      allowed
    }

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
          // First time we have seen this identity, so the email address is the only thing
          // tying it to an account. It is either an existing one to link onto, or a new row that
          // claims the address. Trusting an unverified address for that lets anyone who can
          // mint an `email` claim take over, or squat on, someone else's account. The error
          // is deliberately the same one a bad credential yields, so this does not become an
          // oracle for which addresses are registered.
          if (!profile.emailVerified) {
            logger.warn(
              s"Refusing to provision ${profile.providerType} identity ${profile.providerId}: " +
                "the provider did not verify its email address."
            )
            throw new NotAuthorizedException("Login credentials are incorrect.")
          }

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
    * `name` is deliberately not refreshed. It is the display name the user owns and may
    * have edited in Texera, and it is not identity — the login handle lives in
    * `auth_provider.provider_id`. Re-deriving it from the provider on every login silently
    * reverted such edits.
    */
  private def refresh(user: User, profile: ExternalProfile): Boolean = {
    var changed = false
    // Same reasoning as the link path: only a provider-verified address may move the
    // column that identifies the account.
    if (profile.emailVerified && user.getEmail != profile.email) {
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
