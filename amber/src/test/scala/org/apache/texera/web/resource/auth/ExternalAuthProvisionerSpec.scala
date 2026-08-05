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
import org.jooq.impl.DSL
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

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

  // Case-insensitive so it also collects rows seeded with a differing casing.
  private def cleanup(): Unit =
    getDSLContext.deleteFrom(USER).where(DSL.lower(USER.EMAIL).like("%" + emailDomain)).execute()

  // ---- helpers -------------------------------------------------------------

  private def profile(
      providerId: String,
      name: String,
      email: String,
      avatar: String = "pic"
  ): ExternalProfile =
    ExternalProfile(ProviderTypeEnum.GOOGLE, providerId, name, email, avatar)

  /** Seed a user row directly; uid is DB-assigned and read back into the pojo. */
  private def seedUser(name: String, localPart: String, avatar: String = null): User =
    seedUserWithEmail(name, localPart + emailDomain, avatar)

  /** Seed a user row at a verbatim address, for the casing tests. */
  private def seedUserWithEmail(name: String, email: String, avatar: String = null): User = {
    val user = new User
    user.setName(name)
    user.setEmail(email)
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

  /** Counts case-insensitively, so a duplicate differing only in case is still counted. */
  private def userCountByEmail(localPart: String): Int =
    getDSLContext.fetchCount(USER, DSL.lower(USER.EMAIL).eq(localPart + emailDomain))

  // ---- new-identity provisioning -------------------------------------------

  "ExternalAuthProvisioner.loginOrProvision" should "create an INACTIVE user and provider row for a brand-new Google identity" in {
    val user = ExternalAuthProvisioner.loginOrProvision(
      profile("google-sub-1", "New User", "new" + emailDomain, avatar = "avatar1")
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
    val p = profile("google-sub-return", "Ret", "ret" + emailDomain, avatar = "a")

    val first = ExternalAuthProvisioner.loginOrProvision(p)
    val second = ExternalAuthProvisioner.loginOrProvision(p)

    second.getUid shouldBe first.getUid
    providerRowCount(first.getUid) shouldBe 1
    userCountByEmail("ret") shouldBe 1
  }

  it should "refresh drifted profile fields for a known identity" in {
    ExternalAuthProvisioner.loginOrProvision(
      profile("sub-drift", "Old Name", "drift" + emailDomain, avatar = "oldpic")
    )
    val updated = ExternalAuthProvisioner.loginOrProvision(
      profile("sub-drift", "New Name", "drift" + emailDomain, avatar = "newpic")
    )

    updated.getName shouldBe "New Name"
    updated.getAvatar shouldBe "newpic"
    // confirm it persisted, not just mutated in memory
    userDao.fetchOneByUid(updated.getUid).getName shouldBe "New Name"
    userDao.fetchOneByUid(updated.getUid).getAvatar shouldBe "newpic"
  }

  it should "adopt the provider's new email address for a known identity" in {
    val created = ExternalAuthProvisioner.loginOrProvision(
      profile("sub-rename", "Renamer", "before" + emailDomain)
    )

    val updated = ExternalAuthProvisioner.loginOrProvision(
      profile("sub-rename", "Renamer", "after" + emailDomain)
    )

    updated.getUid shouldBe created.getUid
    userDao.fetchOneByUid(created.getUid).getEmail shouldBe "after" + emailDomain
    userCountByEmail("before") shouldBe 0
  }

  // ---- email match, no provider yet ----------------------------------------

  it should "link a new provider to an existing email-matched user instead of creating a duplicate" in {
    val existing = seedUser("Local User", "linkme")

    val result = ExternalAuthProvisioner.loginOrProvision(
      profile("sub-link", "Local User", "linkme" + emailDomain)
    )

    result.getUid shouldBe existing.getUid
    userCountByEmail("linkme") shouldBe 1
    providerIdOf(existing.getUid, ProviderTypeEnum.GOOGLE) shouldBe "sub-link"
  }

  // `"user".email` is a plain case-sensitive UNIQUE and `idx_user_email_lower` is not unique, so
  // an exact-match lookup would not merely miss — the follow-up insert would succeed and fork the
  // account silently. Registration stores the address as typed while contributor placeholders are
  // stored lower-cased, so the casings really do differ in practice. Mirrors the register-path
  // guard asserted in AuthResourceSpec.
  it should "link to an existing account whose stored email differs only in case" in {
    val existing = seedUserWithEmail("Mixed Case", "MixedCase" + emailDomain)

    val result = ExternalAuthProvisioner.loginOrProvision(
      profile("sub-casing", "Mixed Case", "mixedcase" + emailDomain)
    )

    result.getUid shouldBe existing.getUid
    userCountByEmail("mixedcase") shouldBe 1
    providerIdOf(existing.getUid, ProviderTypeEnum.GOOGLE) shouldBe "sub-casing"
  }

  // A contributor placeholder is stored lower-cased by DatasetResource, so a provider reporting
  // the address with different casing must still claim it rather than orphan the contributor link.
  it should "claim a placeholder account whose stored email differs only in case" in {
    val placeholder = seedUserWithEmail("Ghost", "ghost" + emailDomain)
    placeholder.setIsPlaceholder(true)
    userDao.update(placeholder)

    val result = ExternalAuthProvisioner.loginOrProvision(
      profile("sub-ghost", "Ghost", "GHOST" + emailDomain)
    )

    result.getUid shouldBe placeholder.getUid
    userDao.fetchOneByUid(placeholder.getUid).getIsPlaceholder shouldBe false
    userCountByEmail("ghost") shouldBe 1
  }

  it should "claim a placeholder account when an external identity presents its email" in {
    val placeholder = seedUser("Placeholder", "claimme")
    placeholder.setIsPlaceholder(true)
    userDao.update(placeholder)

    val result = ExternalAuthProvisioner.loginOrProvision(
      profile("sub-claim", "Claimer", "claimme" + emailDomain)
    )

    result.getUid shouldBe placeholder.getUid
    val claimed = userDao.fetchOneByUid(placeholder.getUid)
    claimed.getIsPlaceholder shouldBe false
    claimed.getComment should include("Claimed contributor placeholder at ")
  }

  // ---- provider id rotation -------------------------------------------------

  it should "update the stored provider id when the same user returns with a new one" in {
    val existing = seedUser("Rotating", "rotate")
    seedExternalProvider(existing.getUid, ProviderTypeEnum.GOOGLE, "old-sub")

    ExternalAuthProvisioner.loginOrProvision(
      profile("new-sub", "Rotating", "rotate" + emailDomain)
    )

    providerRowCount(existing.getUid) shouldBe 1
    providerIdOf(existing.getUid, ProviderTypeEnum.GOOGLE) shouldBe "new-sub"
  }
}
