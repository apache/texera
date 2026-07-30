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

import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.apache.texera.web.model.http.request.auth.{UserLoginRequest, UserRegistrationRequest}
import org.jooq.exception.DataAccessException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import javax.ws.rs.{NotAcceptableException, NotAuthorizedException}

/**
  * Integration spec for [[AuthResource]] against embedded Postgres ([[MockTexeraDB]] loads
  * the real `texera_ddl.sql`, so `uq_provider_identity` / `ck_provider_credential` and the
  * NOT NULL on `provider_id` are all exercised).
  *
  * The point of these tests is that the local login handle lives in
  * `auth_provider.provider_id`, not in `"user".name` — so rewriting the display name (which
  * an admin edit or a social login both do) must not disturb login.
  */
class AuthResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Every handle this spec registers starts with this, so cleanup can target the rows it
  // made. Registration takes the email separately from the handle, so `register` derives one
  // (see `emailFor`); cleanup keys off email because tests here deliberately rewrite `name`.
  private val handlePrefix = "authspec_"

  private var userDao: UserDao = _
  private var authDao: AuthProviderDao = _
  private var resource: AuthResource = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    authDao = new AuthProviderDao(getDSLContext.configuration())
    resource = new AuthResource()
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = cleanup()
  override protected def afterEach(): Unit = cleanup()

  // The auth_provider FK is ON DELETE CASCADE, so deleting the user clears its rows.
  // The bootstrap admin is not prefixed, so it needs deleting by its configured handle.
  private def cleanup(): Unit =
    getDSLContext
      .deleteFrom(USER)
      .where(
        USER.EMAIL
          .like(handlePrefix + "%")
          .or(USER.EMAIL.eq(UserSystemConfig.adminUsername))
      )
      .execute()

  // ---- helpers -------------------------------------------------------------

  /** Registration validates the email's shape, so a bare handle cannot double as one. */
  private def emailFor(handle: String): String = handle + "@example.com"

  private def register(handle: String, password: String): Unit =
    resource.register(UserRegistrationRequest(handle, emailFor(handle), password))

  /** The LOCAL login handle recorded for a user, or null if they have no LOCAL row. */
  private def localHandleOf(uid: Integer): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)

  private def userCountByEmail(email: String): Int =
    getDSLContext.fetchCount(USER, USER.EMAIL.eq(email))

  private def userByEmail(email: String): User = userDao.fetchOneByEmail(email)

  /** The account `register(handle, _)` created, looked up the way cleanup keys off it. */
  private def userByHandle(handle: String): User = userByEmail(emailFor(handle))

  /** Rename the display name, the way an admin edit or a social-login refresh would. */
  private def renameDisplayName(uid: Integer, newName: String): Unit = {
    val user = userDao.fetchOneByUid(uid)
    user.setName(newName)
    userDao.update(user)
  }

  /** Seed a user backed by an external provider only — no LOCAL row, no password. */
  private def seedExternalUser(name: String, email: String, providerId: String): User = {
    val user = new User
    user.setName(name)
    user.setEmail(email)
    user.setRole(UserRoleEnum.REGULAR)
    userDao.insert(user)

    val auth = new AuthProvider
    auth.setUid(user.getUid)
    auth.setProviderType(ProviderTypeEnum.GOOGLE)
    auth.setProviderId(providerId)
    authDao.insert(auth)
    user
  }

  // ---- registration and login round-trip -----------------------------------

  behavior of "local registration and login"

  it should "log in a freshly registered user and record the handle in auth_provider" in {
    val handle = handlePrefix + "alice"
    register(handle, "pw-alice")

    val user = userByHandle(handle)
    user should not be null
    localHandleOf(user.getUid) shouldBe handle

    val loggedIn = AuthResource.retrieveUserByUsernameAndPassword(handle, "pw-alice")
    loggedIn.map(_.getUid) shouldBe Some(user.getUid)
  }

  it should "reject a wrong password" in {
    val handle = handlePrefix + "bob"
    register(handle, "pw-bob")

    AuthResource.retrieveUserByUsernameAndPassword(handle, "not-pw-bob") shouldBe None
  }

  it should "reject an unknown handle" in {
    AuthResource.retrieveUserByUsernameAndPassword(handlePrefix + "nobody", "pw") shouldBe None
  }

  // provider_id is only unique per provider_type, so an external subject id is not a login
  // handle. Without the LOCAL predicate this matched a row whose password is null.
  it should "refuse to treat an external provider's subject id as a login handle" in {
    seedExternalUser("Külli", handlePrefix + "kulli@example.com", "google-sub-kulli")

    AuthResource.retrieveUserByUsernameAndPassword("google-sub-kulli", "pw") shouldBe None
  }

  it should "log in the local account when an external identity shares its provider id" in {
    val handle = handlePrefix + "liam"
    register(handle, "pw-liam")
    val uid = userByHandle(handle).getUid

    // Same string as Liam's local handle, but registered under GOOGLE on another account —
    // permitted by uq_provider_identity, and previously enough to make fetchOne() throw.
    seedExternalUser("Liam Social", handlePrefix + "liam-social@example.com", handle)

    AuthResource.retrieveUserByUsernameAndPassword(handle, "pw-liam").map(_.getUid) shouldBe Some(
      uid
    )
  }

  it should "reject null credentials without touching the database" in {
    AuthResource.retrieveUserByUsernameAndPassword(null, "pw") shouldBe None
    AuthResource.retrieveUserByUsernameAndPassword(handlePrefix + "alice", null) shouldBe None
  }

  it should "surface a bad login through the endpoint as 401" in {
    val handle = handlePrefix + "carol"
    register(handle, "pw-carol")

    a[NotAuthorizedException] should be thrownBy
      resource.login(UserLoginRequest(handle, "wrong"))
  }

  it should "refuse an empty or whitespace-only handle" in {
    a[NotAcceptableException] should be thrownBy register("", "pw")
    a[NotAcceptableException] should be thrownBy register("   ", "pw")
  }

  it should "refuse a null handle" in {
    a[NotAcceptableException] should be thrownBy register(null, "pw")
  }

  it should "refuse a handle that is already taken" in {
    val handle = handlePrefix + "dave"
    register(handle, "pw-dave")

    a[NotAcceptableException] should be thrownBy register(handle, "another-pw")
  }

  // ---- the regressions this design exists to prevent ------------------------

  behavior of "the login handle's independence from the display name"

  it should "keep login working after the display name is rewritten" in {
    val handle = handlePrefix + "frank"
    register(handle, "pw-frank")
    val uid = userByHandle(handle).getUid

    // exactly what AdminUserResource.updateUser and ExternalAuthProvisioner.refresh do
    renameDisplayName(uid, "Frank The Renamed")

    AuthResource.retrieveUserByUsernameAndPassword(handle, "pw-frank").map(_.getUid) shouldBe Some(
      uid
    )
    localHandleOf(uid) shouldBe handle
  }

  it should "log in the right user when another account's display name collides with the handle" in {
    val handle = handlePrefix + "grace"
    register(handle, "pw-grace")
    val uid = userByHandle(handle).getUid

    // a social signup whose provider display name happens to equal Grace's login handle
    seedExternalUser(handle, handlePrefix + "grace-social@example.com", "google-sub-grace")

    AuthResource.retrieveUserByUsernameAndPassword(handle, "pw-grace").map(_.getUid) shouldBe Some(
      uid
    )
  }

  // ---- admin bootstrap -----------------------------------------------------

  behavior of "createAdminUser"

  it should "create exactly one admin and let it log in" in {
    AuthResource.createAdminUser()

    userCountByEmail(UserSystemConfig.adminUsername) shouldBe 1
    val admin = userByEmail(UserSystemConfig.adminUsername)
    admin.getRole shouldBe UserRoleEnum.ADMIN
    localHandleOf(admin.getUid) shouldBe UserSystemConfig.adminUsername
    AuthResource.retrieveUserByUsernameAndPassword(
      UserSystemConfig.adminUsername,
      UserSystemConfig.adminPassword
    ) should not be None
  }

  it should "be idempotent across restarts" in {
    AuthResource.createAdminUser()
    AuthResource.createAdminUser()

    userCountByEmail(UserSystemConfig.adminUsername) shouldBe 1
  }

  it should "not create a second admin after the first one is renamed" in {
    AuthResource.createAdminUser()
    val uid = userByEmail(UserSystemConfig.adminUsername).getUid
    renameDisplayName(uid, "Renamed Admin")

    AuthResource.createAdminUser()

    userCountByEmail(UserSystemConfig.adminUsername) shouldBe 1
    localHandleOf(uid) shouldBe UserSystemConfig.adminUsername
  }

  // The case that actually pins idempotency to the *handle*: once both the display name and the
  // email have been edited, neither a fetchByName nor an email lookup recognises the admin, so a
  // name-based check tries to insert and dies on uq_provider_identity — taking the boot with it,
  // since nothing above createAdminUser catches.
  it should "stay idempotent after the admin's name and email are both changed" in {
    AuthResource.createAdminUser()
    val uid = userByEmail(UserSystemConfig.adminUsername).getUid

    val renamed = userDao.fetchOneByUid(uid)
    renamed.setName("Renamed Admin")
    renamed.setEmail(handlePrefix + "renamed-admin@example.com")
    userDao.update(renamed)

    noException should be thrownBy AuthResource.createAdminUser()

    // still exactly one account holding the admin handle, and it is the original one
    getDSLContext.fetchCount(
      AUTH_PROVIDER,
      AUTH_PROVIDER.PROVIDER_TYPE
        .eq(ProviderTypeEnum.LOCAL)
        .and(AUTH_PROVIDER.PROVIDER_ID.eq(UserSystemConfig.adminUsername))
    ) shouldBe 1
    localHandleOf(uid) shouldBe UserSystemConfig.adminUsername
  }

  it should "not fail when the admin address is already held by an account with no local row" in {
    // e.g. somebody signed in with Google using the configured admin address
    seedExternalUser("Google Admin", UserSystemConfig.adminUsername, "google-sub-admin")

    noException should be thrownBy AuthResource.createAdminUser()
    userCountByEmail(UserSystemConfig.adminUsername) shouldBe 1
  }

  // ---- schema-level guarantees ---------------------------------------------

  behavior of "the auth_provider constraints"

  it should "refuse a LOCAL row with no handle" in {
    val user = seedExternalUser(
      "Handleless",
      handlePrefix + "handleless@example.com",
      "google-sub-handleless"
    )

    val auth = new AuthProvider
    auth.setUid(user.getUid)
    auth.setProviderType(ProviderTypeEnum.LOCAL)
    auth.setPassword("hashed")
    a[DataAccessException] should be thrownBy authDao.insert(auth)
  }

  it should "refuse two accounts sharing a LOCAL handle" in {
    val handle = handlePrefix + "heidi"
    register(handle, "pw-heidi")

    val other =
      seedExternalUser("Heidi Two", handlePrefix + "heidi2@example.com", "google-sub-heidi")
    val auth = new AuthProvider
    auth.setUid(other.getUid)
    auth.setProviderType(ProviderTypeEnum.LOCAL)
    auth.setProviderId(handle)
    auth.setPassword("hashed")
    a[DataAccessException] should be thrownBy authDao.insert(auth)
  }

  it should "refuse a LOCAL row with no password" in {
    val user =
      seedExternalUser("Ivan", handlePrefix + "ivan@example.com", "google-sub-ivan")

    val passwordless = new AuthProvider
    passwordless.setUid(user.getUid)
    passwordless.setProviderType(ProviderTypeEnum.LOCAL)
    passwordless.setProviderId(handlePrefix + "ivan-local")
    a[DataAccessException] should be thrownBy authDao.insert(passwordless)
  }

  it should "refuse a non-LOCAL row that carries a password" in {
    // A separate user: (uid, provider_type) is the primary key, so the GOOGLE slot on Ivan is
    // already taken and the collision would be a PK violation rather than the check we want.
    val user = new User
    user.setName("Judy")
    user.setEmail(handlePrefix + "judy@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    userDao.insert(user)

    val externalWithPassword = new AuthProvider
    externalWithPassword.setUid(user.getUid)
    externalWithPassword.setProviderType(ProviderTypeEnum.GOOGLE)
    externalWithPassword.setProviderId("google-sub-judy")
    externalWithPassword.setPassword("hashed")
    a[DataAccessException] should be thrownBy authDao.insert(externalWithPassword)
  }
}
