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

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

/**
  * Integration spec for [[LocalAuthProvisioner]]'s unique-violation handling against embedded
  * Postgres ([[MockTexeraDB]] loads the real `texera_ddl.sql`, so `uq_provider_identity`,
  * `PRIMARY KEY (uid, provider_type)` and `user_email_key` are the constraints that actually
  * fire here).
  *
  * These paths are only reachable by losing a race, so callers pre-check and the handlers are
  * the fallback — which is exactly why they need a test rather than a caller. Each case drives
  * the constraint directly and asserts the message names the cause that actually fired: naming
  * the wrong one tells a user their free handle is taken, and the two write methods reach
  * different constraint sets, so a fix applied to one does not cover the other.
  */
class LocalAuthProvisionerSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Shared suffix so cleanup can target this suite's rows precisely; the auth_provider FK is
  // ON DELETE CASCADE, so deleting the user clears its credential rows.
  private val emailDomain = "@local-provisioner-test.com"

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
    getDSLContext.deleteFrom(USER).where(DSL.lower(USER.EMAIL).like("%" + emailDomain)).execute()

  // ---- helpers -------------------------------------------------------------

  private def newUser(name: String, localPart: String, placeholder: Boolean = false): User = {
    val user = new User
    user.setName(name)
    user.setEmail(localPart + emailDomain)
    user.setRole(UserRoleEnum.INACTIVE)
    user.setIsPlaceholder(placeholder)
    user
  }

  /** Seed a user that already holds a LOCAL credential under `handle`. */
  private def seedLocalAccount(name: String, localPart: String, handle: String): User = {
    val user = newUser(name, localPart)
    user.setRole(UserRoleEnum.REGULAR)
    userDao.insert(user)
    val auth = new AuthProvider
    auth.setUid(user.getUid)
    auth.setProviderType(ProviderTypeEnum.LOCAL)
    auth.setProviderId(handle)
    auth.setPassword(LocalAuthProvisioner.hashPassword("seeded-pw"))
    authDao.insert(auth)
    user
  }

  /** Seed a credential-less placeholder, the row a dataset contributor gets. */
  private def seedPlaceholder(localPart: String): User = {
    val user = newUser(localPart, localPart, placeholder = true)
    userDao.insert(user)
    user
  }

  private def userCountByEmail(localPart: String): Int =
    getDSLContext.fetchCount(USER, DSL.lower(USER.EMAIL).eq(localPart + emailDomain))

  private def conflictFrom(body: => Unit): WebApplicationException = {
    val thrown = intercept[WebApplicationException](body)
    thrown.getResponse.getStatus shouldBe Response.Status.CONFLICT.getStatusCode
    thrown
  }

  // ---- createLocalAccount --------------------------------------------------

  "LocalAuthProvisioner.createLocalAccount" should
    "report a taken handle when uq_provider_identity fires" in {
    seedLocalAccount("Existing", "existing", handle = "shared-handle")

    val thrown = conflictFrom(
      LocalAuthProvisioner.createLocalAccount(newUser("New", "fresh"), "shared-handle", "pw")
    )

    thrown.getMessage should include("Login handle shared-handle is already taken")
    // The user insert and the credential insert share one transaction, so losing the race must
    // not leave a credential-less account behind under the email that was being registered.
    userCountByEmail("fresh") shouldBe 0
  }

  it should "report a registered email when user_email_key fires" in {
    seedLocalAccount("Existing", "taken-email", handle = "handle-a")

    val thrown = conflictFrom(
      LocalAuthProvisioner
        .createLocalAccount(newUser("New", "taken-email"), "handle-b", "pw")
    )

    thrown.getMessage should include(s"Email taken-email$emailDomain is already registered")
    // The seeded row is the only one; the losing insert rolled back rather than adding a second.
    userCountByEmail("taken-email") shouldBe 1
    getDSLContext.fetchCount(AUTH_PROVIDER, AUTH_PROVIDER.PROVIDER_ID.eq("handle-b")) shouldBe 0
  }

  // ---- claimWithLocalCredential --------------------------------------------

  "LocalAuthProvisioner.claimWithLocalCredential" should
    "report an already-claimed account when the (uid, provider_type) primary key fires" in {
    // A placeholder someone else already claimed: it holds a LOCAL row under *their* handle,
    // so the handle this caller asks for is free and only the primary key can fire.
    val claimed = seedPlaceholder("contributor")
    val auth = new AuthProvider
    auth.setUid(claimed.getUid)
    auth.setProviderType(ProviderTypeEnum.LOCAL)
    auth.setProviderId("first-claimer")
    auth.setPassword(LocalAuthProvisioner.hashPassword("pw"))
    authDao.insert(auth)

    claimed.setIsPlaceholder(false)
    val thrown = conflictFrom(
      LocalAuthProvisioner.claimWithLocalCredential(claimed, "second-claimer", "pw")
    )

    thrown.getMessage should include(
      s"Account for contributor$emailDomain has already been claimed"
    )
    // The handle was free, so reporting it as taken would be the wrong cause.
    thrown.getMessage should not include "already taken"
    // The claim and the credential share one transaction, so the rollback must leave the
    // placeholder flag as it was rather than marking an account claimed with no new credential.
    userDao.fetchOneByUid(claimed.getUid).getIsPlaceholder shouldBe true
    providerIdOf(claimed.getUid) shouldBe "first-claimer"
  }

  it should "report a taken handle when uq_provider_identity fires" in {
    seedLocalAccount("Other", "other", handle = "wanted-handle")
    val placeholder = seedPlaceholder("claimant")

    placeholder.setIsPlaceholder(false)
    val thrown = conflictFrom(
      LocalAuthProvisioner.claimWithLocalCredential(placeholder, "wanted-handle", "pw")
    )

    thrown.getMessage should include("Login handle wanted-handle is already taken")
    thrown.getMessage should not include "has already been claimed"
    userDao.fetchOneByUid(placeholder.getUid).getIsPlaceholder shouldBe true
  }

  private def providerIdOf(uid: Integer): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)
}
