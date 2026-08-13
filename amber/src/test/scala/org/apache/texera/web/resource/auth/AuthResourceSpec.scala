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

import org.apache.texera.auth.{JwtAuth, SessionUser}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.apache.texera.web.model.http.request.auth.{
  SetEmailRequest,
  UserLoginRequest,
  UserRegistrationRequest
}
import org.jasypt.util.password.StrongPasswordEncryptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util.UUID
import javax.ws.rs.{NotAcceptableException, NotAuthorizedException, WebApplicationException}

class AuthResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Random suffix keeps this suite's rows from colliding with data other suites may
  // have left in the shared (singleton) embedded DB; suites run sequentially.
  private val runId = UUID.randomUUID().toString.substring(0, 8)
  // jasypt verifies a plain password against a stored hash; a fresh instance works fine.
  private val encryptor = new StrongPasswordEncryptor()

  private var userDao: UserDao = _
  private var authDao: AuthProviderDao = _
  private var resource: AuthResource = _

  private def uname(tag: String): String = s"authspec_${tag}_$runId"

  private def uemail(tag: String): String = s"authspec_${tag}_$runId@example.com"

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    authDao = new AuthProviderDao(getDSLContext.configuration())
    resource = new AuthResource()
    cleanup()
  }

  override protected def afterEach(): Unit = cleanup()

  // The auth_provider FK is ON DELETE CASCADE, so deleting the user clears its credential rows.
  private def cleanup(): Unit = {
    // startsWith escapes SQL LIKE wildcards, so the literal "authspec_" prefix is matched exactly.
    getDSLContext.deleteFrom(USER).where(USER.NAME.startsWith("authspec_")).execute()
    // createAdminUser() seeds the configured admin — remove it too so the test starts clean.
    getDSLContext.deleteFrom(USER).where(USER.NAME.eq(UserSystemConfig.adminUsername)).execute()
  }

  /**
    * Seed a user plus the LOCAL auth_provider row it logs in with, mirroring
    * `LocalAuthProvisioner.createLocalAccount`.
    */
  private def seedUser(
      name: String,
      password: String,
      role: UserRoleEnum = UserRoleEnum.REGULAR
  ): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"$name@example.com")
    user.setRole(role)
    userDao.insert(user)

    val auth = new AuthProvider
    auth.setUid(user.getUid)
    auth.setProviderType(ProviderTypeEnum.LOCAL)
    // The login handle is the provider id, not the (mutable) display name.
    auth.setProviderId(name)
    auth.setPassword(encryptor.encryptPassword(password))
    authDao.insert(auth)
    user
  }

  /** The external id `uid` authenticates with at `providerType`, if it has one. */
  private def providerIdOf(uid: Integer, providerType: ProviderTypeEnum): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(providerType))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)

  /** The stored LOCAL password hash for a login handle. */
  private def storedPasswordOf(handle: String): String =
    getDSLContext
      .select(AUTH_PROVIDER.PASSWORD)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
      .and(AUTH_PROVIDER.PROVIDER_ID.eq(handle))
      .fetchOne(AUTH_PROVIDER.PASSWORD)

  private def subjectOf(token: String): String =
    JwtAuth.jwtConsumer.processToClaims(token).getSubject

  // ─── retrieveUserByUsernameAndPassword ──────────────────────────────────────

  "retrieveUserByUsernameAndPassword" should "return the user for correct credentials" in {
    seedUser(uname("ok"), "secret")
    AuthResource
      .retrieveUserByUsernameAndPassword(uname("ok"), "secret")
      .map(_.getName) shouldBe Some(uname("ok"))
  }

  it should "return None for a wrong password" in {
    seedUser(uname("wp"), "secret")
    AuthResource.retrieveUserByUsernameAndPassword(uname("wp"), "wrong") shouldBe None
  }

  it should "return None for an unknown user" in {
    AuthResource.retrieveUserByUsernameAndPassword(uname("nobody"), "secret") shouldBe None
  }

  it should "return None when the username or password is null" in {
    seedUser(uname("nul"), "secret")
    AuthResource.retrieveUserByUsernameAndPassword(null, "secret") shouldBe None
    AuthResource.retrieveUserByUsernameAndPassword(uname("nul"), null) shouldBe None
  }

  // ─── login ──────────────────────────────────────────────────────────────────

  "login" should "issue a JWT whose subject is the username for valid credentials" in {
    seedUser(uname("login"), "pw")
    val response = resource.login(UserLoginRequest(uname("login"), "pw"))
    response.accessToken should not be empty
    subjectOf(response.accessToken) shouldBe uname("login")
  }

  it should "reject invalid credentials with NotAuthorizedException" in {
    seedUser(uname("bad"), "pw")
    assertThrows[NotAuthorizedException](resource.login(UserLoginRequest(uname("bad"), "nope")))
  }

  // ─── register ─────────────────────────────────────────────────────────────

  "register" should "persist an INACTIVE user with a hashed password and issue a token" in {
    val response = resource.register(UserRegistrationRequest(uname("reg"), uemail("reg"), "pw"))

    subjectOf(response.accessToken) shouldBe uname("reg")
    val persisted = userDao.fetchByName(uname("reg"))
    persisted.size() shouldBe 1
    val stored = persisted.get(0)
    stored.getRole shouldBe UserRoleEnum.INACTIVE
    stored.getEmail shouldBe uemail("reg")
    stored.getIsPlaceholder shouldBe false
    // stored hashed, not in plain text, but verifies against the plain password
    val storedPassword = storedPasswordOf(uname("reg"))
    storedPassword should not be "pw"
    encryptor.checkPassword("pw", storedPassword) shouldBe true
  }

  it should "reject an empty username" in {
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest("   ", uemail("eu"), "pw"))
    )
    ex.getMessage should include("Username")
  }

  it should "reject an empty email" in {
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("ee"), "  ", "pw"))
    )
    ex.getMessage should include("Email cannot be empty")
  }

  it should "reject a malformed email" in {
    val ex =
      intercept[NotAcceptableException](
        resource.register(UserRegistrationRequest(uname("mf"), "not-an-email", "pw"))
      )
    ex.getMessage should include("Email format is invalid")
  }

  it should "reject an empty password" in {
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("ep"), uemail("ep"), ""))
    )
    ex.getMessage should include("Password")
  }

  it should "reject a duplicate username" in {
    seedUser(uname("dupu"), "pw")
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("dupu"), uemail("dupu_other"), "pw2"))
    )
    ex.getMessage should include("Username exists")
  }

  it should "reject a duplicate email" in {
    val existing = seedUser(uname("dupeowner"), "pw")
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("dupenew"), existing.getEmail, "pw2"))
    )
    ex.getMessage should include("Email exists")
  }

  // ─── register: placeholder claiming ─────────────────────────────────────────

  private def seedPlaceholder(name: String, email: String): User = {
    val user = new User
    user.setName(name)
    user.setEmail(email)
    user.setRole(UserRoleEnum.INACTIVE)
    user.setIsPlaceholder(true)
    user.setComment("Auto-created as contributor of dataset 1")
    userDao.insert(user)
    user
  }

  /**
    * Seed an external (non-LOCAL) credential for an existing user. ck_provider_credential
    * requires a password for LOCAL and only for LOCAL, so this leaves it null.
    */
  private def seedExternalProvider(
      uid: Integer,
      providerType: ProviderTypeEnum,
      providerId: String
  ): Unit = {
    val auth = new AuthProvider
    auth.setUid(uid)
    auth.setProviderType(providerType)
    auth.setProviderId(providerId)
    authDao.insert(auth)
  }

  /** Whether `uid` holds a credential of the given kind. */
  private def hasProvider(uid: Integer, providerType: ProviderTypeEnum): Boolean =
    getDSLContext.fetchExists(
      getDSLContext
        .selectFrom(AUTH_PROVIDER)
        .where(AUTH_PROVIDER.UID.eq(uid))
        .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(providerType))
    )

  it should "claim a placeholder account with the matching email" in {
    val placeholder = seedPlaceholder(uname("ghost"), uemail("claim"))

    val response =
      resource.register(UserRegistrationRequest(uname("claimer"), uemail("claim"), "secret-pw"))

    response.accessToken should not be empty
    val claimed = userDao.fetchOneByEmail(uemail("claim"))
    claimed.getUid shouldEqual placeholder.getUid
    claimed.getIsPlaceholder shouldBe false
    // the credential lives in auth_provider, keyed by the login handle just registered
    encryptor.checkPassword("secret-pw", storedPasswordOf(uname("claimer"))) shouldBe true
    claimed.getRole shouldEqual UserRoleEnum.INACTIVE
    claimed.getComment should include("Claimed contributor placeholder at ")
  }

  it should "allow logging in with the claimed credentials" in {
    seedPlaceholder(uname("ghost2"), uemail("claimlogin"))
    resource.register(UserRegistrationRequest(uname("claimer2"), uemail("claimlogin"), "secret-pw"))

    AuthResource.retrieveUserByUsernameAndPassword(
      uname("claimer2"),
      "secret-pw"
    ) should not be None
  }

  it should "not claim an INACTIVE account that has credentials" in {
    val real = new User
    real.setName(uname("real"))
    real.setEmail(uemail("real"))
    real.setRole(UserRoleEnum.INACTIVE)
    userDao.insert(real)
    seedExternalProvider(real.getUid, ProviderTypeEnum.GOOGLE, s"google-$runId")

    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("attacker"), uemail("real"), "attacker-pw"))
    )
    ex.getMessage should include("Email exists")

    val untouched = userDao.fetchOneByEmail(uemail("real"))
    untouched.getIsPlaceholder shouldBe false
    // no LOCAL credential was grafted on, and the Google identity is intact
    hasProvider(untouched.getUid, ProviderTypeEnum.LOCAL) shouldBe false
    providerIdOf(untouched.getUid, ProviderTypeEnum.GOOGLE) shouldEqual s"google-$runId"
  }

  it should "reject claiming with an already-taken username" in {
    seedUser(uname("taken"), "pw")
    seedPlaceholder(uname("ghost3"), uemail("clash"))

    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("taken"), uemail("clash"), "pw2"))
    )
    ex.getMessage should include("Username exists")

    userDao.fetchOneByEmail(uemail("clash")).getIsPlaceholder shouldBe true
  }

  it should "reject a duplicate email that differs only in case" in {
    val existing = seedUser(uname("case"), "pw")
    existing.setEmail(s"MixedCase_$runId@Example.com")
    userDao.update(existing)

    val ex = intercept[NotAcceptableException](
      resource.register(
        UserRegistrationRequest(uname("casenew"), s"mixedcase_$runId@example.com", "pw2")
      )
    )
    ex.getMessage should include("Email exists")
  }

  // The taken-handle check has to ask auth_provider, not "user".name: an external login rewrites
  // the display name but never the handle, so a name-based guard let the handle through and the
  // insert then died on uq_provider_identity as a 500 instead of this 406.
  it should "reject a taken handle even when the owner's display name has since drifted" in {
    val owner = seedUser(uname("drift"), "pw")
    // keep the cleanup prefix so the renamed row is still collected
    owner.setName(uname("drift_renamed_by_google"))
    userDao.update(owner)

    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("drift"), uemail("drift_other"), "pw2"))
    )
    ex.getMessage should include("Username exists")
  }

  // The mirror case: a display name that is not a handle must not block registration.
  it should "allow a handle that only collides with some other account's display name" in {
    val squatter = seedUser(uname("squatter"), "pw")
    squatter.setName(uname("wanted"))
    userDao.update(squatter)

    val response =
      resource.register(UserRegistrationRequest(uname("wanted"), uemail("wanted"), "pw2"))
    subjectOf(response.accessToken) shouldBe uname("wanted")
  }

  // ─── createAdminUser ────────────────────────────────────────────────────────

  "createAdminUser" should "insert the configured admin with the ADMIN role and a hashed password" in {
    AuthResource.createAdminUser()

    val admins = userDao.fetchByName(UserSystemConfig.adminUsername)
    admins.size() shouldBe 1
    admins.get(0).getRole shouldBe UserRoleEnum.ADMIN
    encryptor.checkPassword(
      UserSystemConfig.adminPassword,
      storedPasswordOf(UserSystemConfig.adminUsername)
    ) shouldBe true
  }

  it should "not create a second admin when one already exists" in {
    AuthResource.createAdminUser()
    AuthResource.createAdminUser()
    userDao.fetchByName(UserSystemConfig.adminUsername).size() shouldBe 1
  }

  // ─── setEmail ───────────────────────────────────────────────────────────────

  /** An account as an identity-only login leaves it: authenticated, INACTIVE, no address. */
  private def seedEmaillessUser(tag: String, role: UserRoleEnum = UserRoleEnum.INACTIVE): User = {
    val user = new User
    user.setName(uname(tag))
    user.setRole(role)
    userDao.insert(user)
    seedExternalProvider(user.getUid, ProviderTypeEnum.ORCID, s"0000-0002-0000-$tag")
    user
  }

  private def emailClaimOf(token: String): AnyRef =
    JwtAuth.jwtConsumer.processToClaims(token).getClaimValue("email")

  private def statusOf(thrown: WebApplicationException): Int = thrown.getResponse.getStatus

  "setEmail" should "store the address and reissue a token carrying it" in {
    val user = seedEmaillessUser("fill")

    val response = resource.setEmail(SetEmailRequest(uemail("fill")), new SessionUser(user))

    userDao.fetchOneByUid(user.getUid).getEmail shouldBe uemail("fill")
    emailClaimOf(response.accessToken) shouldBe uemail("fill")
  }

  it should "reject a malformed address" in {
    val user = seedEmaillessUser("bad")

    assertThrows[NotAcceptableException] {
      resource.setEmail(SetEmailRequest("not-an-address"), new SessionUser(user))
    }
    userDao.fetchOneByUid(user.getUid).getEmail shouldBe null
  }

  it should "reject a blank address" in {
    val user = seedEmaillessUser("blank")

    assertThrows[NotAcceptableException] {
      resource.setEmail(SetEmailRequest("   "), new SessionUser(user))
    }
  }

  // Filling a blank only — replacing an address that is already set is a different operation with
  // a different threat model.
  it should "refuse to replace an address that is already set" in {
    val user = seedUser(uname("has"), "pw")

    val thrown = intercept[WebApplicationException] {
      resource.setEmail(SetEmailRequest(uemail("other")), new SessionUser(user))
    }

    statusOf(thrown) shouldBe 409
    userDao.fetchOneByUid(user.getUid).getEmail shouldBe s"${uname("has")}@example.com"
  }

  // Anyone can type someone else's address, so attaching to an account that already holds a
  // credential would be a takeover of it.
  it should "refuse an address owned by an account that holds a credential" in {
    val owner = seedUser(uname("owner"), "pw")
    val caller = seedEmaillessUser("intruder")

    val thrown = intercept[WebApplicationException] {
      resource.setEmail(SetEmailRequest(s"${uname("owner")}@example.com"), new SessionUser(caller))
    }

    statusOf(thrown) shouldBe 409
    userDao.fetchOneByUid(caller.getUid).getEmail shouldBe null
    hasProvider(owner.getUid, ProviderTypeEnum.ORCID) shouldBe false
    hasProvider(owner.getUid, ProviderTypeEnum.LOCAL) shouldBe true
  }

  // The placeholder keeps its uid because dataset contributor rows already reference it; the
  // caller's own row is discarded, which is only safe while it is INACTIVE and emailless.
  it should "move the identity onto a contributor placeholder owning the address" in {
    val placeholder = seedPlaceholder(uname("ghost"), uemail("ghost"))
    val caller = seedEmaillessUser("claimer")

    val response = resource.setEmail(SetEmailRequest(uemail("ghost")), new SessionUser(caller))

    val claimed = userDao.fetchOneByUid(placeholder.getUid)
    claimed.getIsPlaceholder shouldBe false
    claimed.getComment should include("Claimed contributor placeholder at ")
    claimed.getName shouldBe uname("claimer")
    hasProvider(placeholder.getUid, ProviderTypeEnum.ORCID) shouldBe true
    providerIdOf(placeholder.getUid, ProviderTypeEnum.ORCID) shouldBe "0000-0002-0000-claimer"

    // The account the provider created moments ago is gone, and the session continues as the
    // claimed one.
    userDao.fetchOneByUid(caller.getUid) shouldBe null
    subjectOf(response.accessToken) shouldBe uname("claimer")
    emailClaimOf(response.accessToken) shouldBe uemail("ghost")
  }

  // Past INACTIVE the caller may own content, so its row cannot be discarded and the placeholder
  // has to be left for someone who can prove the address.
  it should "not discard a caller that is no longer INACTIVE to claim a placeholder" in {
    val placeholder = seedPlaceholder(uname("kept"), uemail("kept"))
    val caller = seedEmaillessUser("regular", role = UserRoleEnum.REGULAR)

    val thrown = intercept[WebApplicationException] {
      resource.setEmail(SetEmailRequest(uemail("kept")), new SessionUser(caller))
    }

    statusOf(thrown) shouldBe 409
    userDao.fetchOneByUid(caller.getUid) should not be null
    userDao.fetchOneByUid(placeholder.getUid).getIsPlaceholder shouldBe true
  }
}
