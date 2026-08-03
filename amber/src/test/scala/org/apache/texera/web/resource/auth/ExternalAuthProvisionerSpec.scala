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

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import javax.ws.rs.NotAuthorizedException

/**
  * Integration spec for [[ExternalAuthProvisioner]] against embedded Postgres
  * ([[MockTexeraDB]] loads the real `texera_ddl.sql`, so the `auth_provider` table and
  * its `ck_provider_credential` / `uq_provider_identity` constraints are exercised).
  * `loginOrProvision` runs the same transaction the Google resources call.
  */
class ExternalAuthProvisionerSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // All test users share this email suffix so cleanup can target them precisely;
  // the auth_provider FK is ON DELETE CASCADE, so deleting the user clears its rows.
  private val emailDomain = "@provisioner-test.com"

  private var userDao: UserDao = _
  private var authDao: AuthProviderDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    authDao = new AuthProviderDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = cleanup()
  override protected def afterEach(): Unit = cleanup()

  private def cleanup(): Unit =
    getDSLContext.deleteFrom(USER).where(USER.EMAIL.like("%" + emailDomain)).execute()

  // ---- helpers -------------------------------------------------------------

  /** Seed a user row directly; uid is DB-assigned and read back into the pojo. */
  private def seedUser(name: String, localPart: String, avatar: String = null): User = {
    val user = new User
    user.setName(name)
    user.setEmail(localPart + emailDomain)
    user.setRole(UserRoleEnum.REGULAR)
    if (avatar != null) user.setAvatar(avatar)
    userDao.insert(user)
    user
  }

  /** Seed an external (non-LOCAL) provider row for an existing user. */
  private def seedExternalProvider(uid: Integer, pt: ProviderTypeEnum, providerId: String): Unit = {
    val auth = new AuthProvider
    auth.setUid(uid)
    auth.setProviderType(pt)
    auth.setProviderId(providerId)
    authDao.insert(auth)
  }

  private def providerRowCount(uid: Integer): Int =
    getDSLContext.fetchCount(AUTH_PROVIDER, AUTH_PROVIDER.UID.eq(uid))

  private def providerIdOf(uid: Integer, pt: ProviderTypeEnum): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(pt))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)

  private def userCountByEmail(localPart: String): Int =
    getDSLContext.fetchCount(USER, USER.EMAIL.eq(localPart + emailDomain))

  // ---- new-identity provisioning -------------------------------------------

  "ExternalAuthProvisioner.loginOrProvision" should "create an INACTIVE user and provider row for a brand-new Google identity" in {
    val user = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "google-sub-1",
        "New User",
        "new" + emailDomain,
        emailVerified = true,
        avatar = Some("avatar1")
      )
    )

    user.getUid should not be null
    user.getName shouldBe "New User"
    user.getEmail shouldBe "new" + emailDomain
    user.getAvatar shouldBe "avatar1"
    user.getRole shouldBe UserRoleEnum.INACTIVE

    providerRowCount(user.getUid) shouldBe 1
    providerIdOf(user.getUid, ProviderTypeEnum.GOOGLE) shouldBe "google-sub-1"
  }

  // ---- returning known identity --------------------------------------------

  it should "be idempotent for a returning identity (same uid, no duplicate provider row or user)" in {
    val profile =
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "google-sub-return",
        "Ret",
        "ret" + emailDomain,
        emailVerified = true,
        avatar = Some("a")
      )

    val first = ExternalAuthProvisioner.loginOrProvision(profile)
    val second = ExternalAuthProvisioner.loginOrProvision(profile)

    second.getUid shouldBe first.getUid
    providerRowCount(first.getUid) shouldBe 1
    userCountByEmail("ret") shouldBe 1
  }

  // The display name is the user's to edit and is not identity — the login handle lives in
  // auth_provider.provider_id. Re-deriving it from the provider on every login silently reverted
  // any rename made in Texera, so refresh leaves it alone and only the avatar follows the drift.
  it should "refresh the avatar but keep the local display name for a known identity" in {
    ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-drift",
        "Old Name",
        "drift" + emailDomain,
        emailVerified = true,
        avatar = Some("oldpic")
      )
    )
    val updated = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-drift",
        "New Name",
        "drift" + emailDomain,
        emailVerified = true,
        avatar = Some("newpic")
      )
    )

    updated.getName shouldBe "Old Name"
    updated.getAvatar shouldBe "newpic"
    // confirm it persisted, not just mutated in memory
    userDao.fetchOneByUid(updated.getUid).getName shouldBe "Old Name"
    userDao.fetchOneByUid(updated.getUid).getAvatar shouldBe "newpic"
  }

  // ---- unverified email ------------------------------------------------------

  // The email address is the only thing tying a first-time external identity to an account, so
  // an unverified one must not be able to link onto — or squat on — someone else's row. The
  // rejection reuses the generic credential error so it cannot be used to enumerate addresses.
  it should "refuse to link a first-time identity onto an existing account on an unverified email" in {
    val existing = seedUser("Victim", "victim")

    assertThrows[NotAuthorizedException] {
      ExternalAuthProvisioner.loginOrProvision(
        ExternalProfile(
          ProviderTypeEnum.GOOGLE,
          "sub-attacker",
          "Attacker",
          "victim" + emailDomain,
          emailVerified = false
        )
      )
    }

    providerRowCount(existing.getUid) shouldBe 0
    userDao.fetchOneByUid(existing.getUid).getName shouldBe "Victim"
  }

  it should "refuse to provision a brand-new account on an unverified email" in {
    assertThrows[NotAuthorizedException] {
      ExternalAuthProvisioner.loginOrProvision(
        ExternalProfile(
          ProviderTypeEnum.GOOGLE,
          "sub-unverified",
          "Unverified",
          "unverified" + emailDomain,
          emailVerified = false
        )
      )
    }

    userCountByEmail("unverified") shouldBe 0
  }

  // A provider that stops vouching for the address must not be able to move it either.
  it should "not adopt an unverified email for a known identity" in {
    val created = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-unverified-drift",
        "Drifter",
        "verified" + emailDomain,
        emailVerified = true
      )
    )

    ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-unverified-drift",
        "Drifter",
        "hijack" + emailDomain,
        emailVerified = false
      )
    )

    userDao.fetchOneByUid(created.getUid).getEmail shouldBe "verified" + emailDomain
    userCountByEmail("hijack") shouldBe 0
  }

  it should "adopt the provider's new email address for a known identity" in {
    val created = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-rename",
        "Renamer",
        "before" + emailDomain,
        emailVerified = true,
        avatar = Some("pic")
      )
    )

    val updated = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-rename",
        "Renamer",
        "after" + emailDomain,
        emailVerified = true,
        avatar = Some("pic")
      )
    )

    updated.getUid shouldBe created.getUid
    userDao.fetchOneByUid(created.getUid).getEmail shouldBe "after" + emailDomain
    userCountByEmail("before") shouldBe 0
  }

  // `avatar = None` is the documented contract for providers that supply no picture: the
  // column keeps whatever it held rather than being blanked on every login.
  it should "leave the stored avatar untouched when the provider supplies none" in {
    val existing = seedUser("Keeper", "keeper", avatar = "keep-me")

    val result = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-keeper",
        "Keeper",
        "keeper" + emailDomain,
        emailVerified = true,
        avatar = None
      )
    )

    result.getUid shouldBe existing.getUid
    result.getAvatar shouldBe "keep-me"
    userDao.fetchOneByUid(existing.getUid).getAvatar shouldBe "keep-me"
  }

  // ---- email match, no provider yet ----------------------------------------

  it should "link a new provider to an existing email-matched user instead of creating a duplicate" in {
    val existing = seedUser("Local User", "linkme")

    val result = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "sub-link",
        "Local User",
        "linkme" + emailDomain,
        emailVerified = true,
        avatar = Some("pic")
      )
    )

    result.getUid shouldBe existing.getUid
    userCountByEmail("linkme") shouldBe 1
    providerIdOf(existing.getUid, ProviderTypeEnum.GOOGLE) shouldBe "sub-link"
  }

  // ---- email match, provider row exists with a different id (upsert) --------

  it should "update the existing provider id in place rather than inserting a colliding row" in {
    val existing = seedUser("Rotating", "rotate")
    seedExternalProvider(existing.getUid, ProviderTypeEnum.GOOGLE, "old-sub")

    val result = ExternalAuthProvisioner.loginOrProvision(
      ExternalProfile(
        ProviderTypeEnum.GOOGLE,
        "new-sub",
        "Rotating",
        "rotate" + emailDomain,
        emailVerified = true,
        avatar = Some("p")
      )
    )

    result.getUid shouldBe existing.getUid
    providerRowCount(existing.getUid) shouldBe 1 // upserted, not a second row
    providerIdOf(existing.getUid, ProviderTypeEnum.GOOGLE) shouldBe "new-sub"
  }
}
