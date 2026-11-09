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

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.ObjectMapper
import io.dropwizard.jersey.errors.LoggingExceptionMapper
import org.apache.texera.auth.{JwtAuth, SessionUser}
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER, USER_LAST_ACTIVE_TIME}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.apache.texera.web.resource.EmailMessage
import org.apache.texera.web.model.http.request.auth.{UserLoginRequest, UserRegistrationRequest}
import org.jasypt.util.password.StrongPasswordEncryptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.slf4j.LoggerFactory

import java.time.{Instant, OffsetDateTime}
import java.util.UUID
import javax.ws.rs.{NotAcceptableException, NotAuthorizedException, WebApplicationException}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

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

  /** Every message the verifying resource below handed its sender, newest last. */
  private var mailed: ArrayBuffer[(EmailMessage, String)] = _

  /**
    * A resource that behaves as a deployment with `user-sys.email-verification = true` does. The
    * flag and the verifier are overridable defs rather than constructor arguments because Jersey
    * instantiates `AuthResource` from its class and needs the no-arg constructor.
    */
  private var verifying: AuthResource = _

  private def uname(tag: String): String = s"authspec_${tag}_$runId"

  private def uemail(tag: String): String = s"authspec_${tag}_$runId@example.com"

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    authDao = new AuthProviderDao(getDSLContext.configuration())
    // Pinned rather than inherited: `emailVerificationRequired` reads `UserSystemConfig`, an
    // object val resolved once per JVM, so a developer with USER_SYS_EMAIL_VERIFICATION=true
    // exported would otherwise send every test below down the code path and fail a dozen of them
    // for reasons nothing here states. `verifying` pins the opposite the same way.
    resource = new AuthResource {
      override protected def emailVerificationRequired: Boolean = false
    }

    mailed = ArrayBuffer.empty
    val testVerifier = new EmailCodeVerifier(
      secret = "0123456789abcdef0123456789abcdef",
      clock = () => Instant.parse("2026-08-18T12:00:00Z"),
      send = (message, recipient) => { mailed += ((message, recipient)); Right(()) },
      smtpConfigured = () => true
    )
    verifying = new AuthResource {
      override protected def emailVerificationRequired: Boolean = true
      override protected def verifier: EmailCodeVerifier = testVerifier
    }

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

  private def googleIdClaimOf(token: String): AnyRef =
    JwtAuth.jwtConsumer.processToClaims(token).getClaimValue("googleId")

  /**
    * Runs `body` with a capturing appender on the logger `AuthResource`'s companion writes to, and
    * returns the WARN messages it emitted. The level is forced because the root logger governs
    * this module (there is no logback-test.xml) and CI lowers it.
    */
  private def warningsDuring(body: => Unit): Seq[String] = {
    val logger = LoggerFactory.getLogger(classOf[AuthResource]).asInstanceOf[LogbackLogger]
    val appender = new ListAppender[ILoggingEvent]
    appender.start()
    val previousLevel = logger.getLevel
    logger.setLevel(Level.WARN)
    logger.addAppender(appender)
    try body
    finally {
      logger.detachAppender(appender)
      appender.stop()
      logger.setLevel(previousLevel)
    }
    appender.list.asScala.toSeq.filter(_.getLevel == Level.WARN).map(_.getFormattedMessage)
  }

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

  // An account can hold a LOCAL and a GOOGLE credential at once, and the frontend reads `googleId`
  // off the token whichever one was used to sign in. It has to be the GOOGLE provider id and not
  // the login handle: `flarum.service.ts` passes the claim on as an account password, so a token
  // that carried the handle under that name would be handing the handle out.
  it should "carry the account's Google identity in the token of a local sign-in" in {
    val user = seedUser(uname("dual"), "pw")
    seedExternalProvider(user.getUid, ProviderTypeEnum.GOOGLE, s"google-dual-$runId")

    val response = resource.login(UserLoginRequest(uname("dual"), "pw"))

    googleIdClaimOf(response.accessToken) shouldBe s"google-dual-$runId"
  }

  // The claim is omitted rather than sent as null for an account that has no Google identity —
  // `common/type/user.ts` declares it optional, and a null would reach Flarum as a password.
  it should "omit googleId from the token of an account with no Google identity" in {
    seedUser(uname("localonly"), "pw")

    val response = resource.login(UserLoginRequest(uname("localonly"), "pw"))

    JwtAuth.jwtConsumer.processToClaims(response.accessToken).hasClaim("googleId") shouldBe false
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

  // All three fields of the request are plain Strings, so a body that omits any of them arrives
  // null, and each has to be read null-safely before anything asks it a question. Otherwise a
  // half-filled body gets a 500 out of a NullPointerException instead of the 406 naming the field.
  // The intercept is the whole assertion: every guard runs before every write in `register`, so
  // "nothing was persisted" cannot fail once the exception escaped.
  it should "reject a missing username" in {
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(null, uemail("mu"), "pw"))
    )
    ex.getMessage should include("Username cannot be empty")
  }

  it should "reject a missing email" in {
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("me"), null, "pw"))
    )
    ex.getMessage should include("Email cannot be empty")
  }

  it should "reject a missing password" in {
    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("mp"), uemail("mp"), null))
    )
    ex.getMessage should include("Password cannot be empty")
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

  /**
    * Seed a LOCAL credential for an existing user. Unlike [[seedExternalProvider]] this must carry
    * a password: ck_provider_credential requires one for LOCAL and only for LOCAL.
    */
  private def seedLocalProvider(uid: Integer, handle: String): Unit = {
    val auth = new AuthProvider
    auth.setUid(uid)
    auth.setProviderType(ProviderTypeEnum.LOCAL)
    auth.setProviderId(handle)
    auth.setPassword(encryptor.encryptPassword("pw"))
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

  // What makes an account claimable is the `is_placeholder` flag, and that flag is not a synonym
  // for "holds no credential": `AdminUserResource.addUser` creates a row that is deliberately not
  // a placeholder and has no credential either, and an admin fills its address in afterwards. So
  // a takeover gated on "holds no credential" would hand every pre-provisioned account to the
  // first stranger who registers its address.
  it should "reject an address owned by a credential-less account that is not a placeholder" in {
    val provisioned = new User
    provisioned.setName(uname("provisioned"))
    provisioned.setEmail(uemail("provisioned"))
    provisioned.setRole(UserRoleEnum.INACTIVE)
    userDao.insert(provisioned)

    val ex = intercept[NotAcceptableException](
      resource.register(UserRegistrationRequest(uname("takeover"), uemail("provisioned"), "pw"))
    )
    ex.getMessage should include("Email exists")

    // Nothing was grafted onto it: still no way to sign in, and still under its own name.
    hasProvider(provisioned.getUid, ProviderTypeEnum.LOCAL) shouldBe false
    userDao.fetchOneByUid(provisioned.getUid).getName shouldBe uname("provisioned")
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
    // The handle doubles as the address, which is what the clash guard below has to find on every
    // later boot — and what keeps the first sign-in from prompting the admin for one.
    admins.get(0).getEmail shouldBe UserSystemConfig.adminUsername
    encryptor.checkPassword(
      UserSystemConfig.adminPassword,
      storedPasswordOf(UserSystemConfig.adminUsername)
    ) shouldBe true
  }

  it should "not create a second admin when one already exists" in {
    AuthResource.createAdminUser()
    // Silently, too. The admin's own address is the handle it was created from, so a bootstrap that
    // asked about the address before asking about the handle would match the admin's own row on
    // every restart and warn about a clash with itself — telling the operator to hand-grant a role
    // that is already granted.
    val warnings = warningsDuring(AuthResource.createAdminUser())

    userDao.fetchByName(UserSystemConfig.adminUsername).size() shouldBe 1
    warnings shouldBe empty
  }

  // The taken-handle check has to ask auth_provider, exactly as `register`'s does. Both the display
  // name and the address on that row are rewritten by `ExternalAuthProvisioner.refresh` when the
  // admin signs in externally and their provider profile has moved on, so neither can answer
  // "is this handle already mine?". A guard that read either would stop recognising the account it
  // created and the next restart would die on uq_provider_identity, inside application bootstrap.
  it should "not create a second admin when the existing one's name and address have drifted" in {
    val handle = uname("boothandle")
    AuthResource.createAdminUser(handle, "bootstrap-pw")
    val admin = userDao.fetchByName(handle).get(0)
    admin.setName(uname("boothandle_renamed_by_google"))
    admin.setEmail(uemail("boothandle_real"))
    userDao.update(admin)

    AuthResource.createAdminUser(handle, "bootstrap-pw")

    // Still exactly the one credential, and no second account inserted under the handle.
    getDSLContext.fetchCount(
      AUTH_PROVIDER,
      AUTH_PROVIDER.PROVIDER_TYPE
        .eq(ProviderTypeEnum.LOCAL)
        .and(AUTH_PROVIDER.PROVIDER_ID.eq(handle))
    ) shouldBe 1
    userDao.fetchByName(handle).size() shouldBe 0
  }

  // A deployment that configures neither credential must come up with no admin at all rather than
  // one anybody can guess. `createAdminUser()` reads two object vals resolved once per JVM, so the
  // unconfigured case is only reachable through the parameterised overload.
  //
  // That overload takes the values already normalised, which leaves half of the guard unpinned:
  // whitespace is turned into "unconfigured" by the no-arg overload's `.trim`, and no test can
  // reach it — the values come from `user-system.conf` / the environment, resolved once per JVM
  // before any test runs, and the checked-in config has no whitespace to trim. Dropping those two
  // `.trim` calls therefore leaves this suite green while `USER_SYS_ADMIN_PASSWORD=" "` starts
  // bootstrapping an admin whose password is a space. Closing that needs the normalisation moved
  // into the overload, which is a production change.
  it should "create nothing when the configured admin username is blank" in {
    AuthResource.createAdminUser("", "bootstrap-pw")

    // The username is what the account is named and what it logs in with, so a bootstrap that ran
    // anyway would leave a blank-handled account holding the configured password.
    getDSLContext.fetchCount(USER, USER.NAME.eq("")) shouldBe 0
    storedPasswordOf("") shouldBe null
  }

  it should "create nothing when the configured admin password is blank" in {
    AuthResource.createAdminUser(uname("blankpw"), "")

    userDao.fetchByName(uname("blankpw")).size() shouldBe 0
    storedPasswordOf(uname("blankpw")) shouldBe null
  }

  // The admin username doubles as the account's email address. `"user".email` is a case-sensitive
  // UNIQUE, so inserting a second account holding the same address in different casing violates
  // nothing — the original account would simply stop being the one that address resolves to, and
  // its data would be stranded. Hence the bootstrap looks the address up case-insensitively and
  // stands down.
  it should "not create an admin whose username is already someone's email address" in {
    val adminHandle = s"authspec_adminhandle_$runId@example.com"
    val owner = new User
    owner.setName(uname("addressowner"))
    // Registration stores an address as the user typed it, so the casings provably differ.
    owner.setEmail(s"AuthSpec_AdminHandle_$runId@Example.com")
    owner.setRole(UserRoleEnum.REGULAR)
    userDao.insert(owner)

    val warnings = warningsDuring(AuthResource.createAdminUser(adminHandle, "bootstrap-pw"))

    userDao.fetchByName(adminHandle).size() shouldBe 0
    storedPasswordOf(adminHandle) shouldBe null
    // Standing down is the whole of the behaviour, so the warning is the whole of the payload: the
    // deployment comes up with no admin account, and this line is the only place an operator learns
    // why and what to do instead. (Promoting the account it found is deliberately not done —
    // holding the address is not consent to be an admin — but nothing in `createAdminUser` writes
    // a role onto an existing row, so there is nothing there to assert.)
    warnings should have size 1
    warnings.head should include(adminHandle)
    warnings.head should include("Grant that account the ADMIN role instead")
  }

  // ─── setEmail ───────────────────────────────────────────────────────────────

  /**
    * An account holding a credential but no address. Nothing creates one any more —
    * `AdminUserResource.addUser` was the last path that did, and it no longer writes a credential —
    * so this stands in for the rows older deployments still carry from it, and for a sign-in
    * method that authenticates someone without asserting an address.
    */
  private def seedEmaillessUser(tag: String, role: UserRoleEnum = UserRoleEnum.INACTIVE): User = {
    val user = new User
    user.setName(uname(tag))
    user.setRole(role)
    userDao.insert(user)

    val auth = new AuthProvider
    auth.setUid(user.getUid)
    auth.setProviderType(ProviderTypeEnum.LOCAL)
    auth.setProviderId(uname(tag))
    auth.setPassword(encryptor.encryptPassword("pw"))
    authDao.insert(auth)
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

    val ex = intercept[NotAcceptableException] {
      resource.setEmail(SetEmailRequest("   "), new SessionUser(user))
    }
    // As the empty-address refusal, not the malformed-address one: EmailUtil's pattern rejects
    // whitespace too, so both arms throw the same type here and only the message says which
    // ran — i.e. whether the address was normalised before being validated.
    ex.getMessage should include("Email cannot be empty")
  }

  // Addresses arrive padded from paste and autofill. Normalising before storing is not cosmetic:
  // `"user".email` is UNIQUE and the lookup is on the trimmed form, so a padded address stored as
  // typed would be a second row for an address someone already holds.
  it should "store a padded address without its surrounding whitespace" in {
    val user = seedEmaillessUser("pad")

    val response =
      resource.setEmail(SetEmailRequest(s"  ${uemail("pad")}  "), new SessionUser(user))

    userDao.fetchOneByUid(user.getUid).getEmail shouldBe uemail("pad")
    emailClaimOf(response.accessToken) shouldBe uemail("pad")
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
    // Neither account's credential moved.
    providerIdOf(owner.getUid, ProviderTypeEnum.LOCAL) shouldBe uname("owner")
    providerIdOf(caller.getUid, ProviderTypeEnum.LOCAL) shouldBe uname("intruder")
  }

  // What the browser shows comes out of Dropwizard's default exception mapper rather than the throw
  // site: `AuthService.promptForEmail` reads `error.message` off the body and falls back to a
  // generic line when it is absent. A refusal carrying only a status would leave the user with no
  // idea which address to try instead, so the text has to survive as far as the wire.
  it should "hand that refusal to the client as a JSON message, not a bare 409" in {
    seedUser(uname("owner"), "pw")
    val caller = seedEmaillessUser("reader")

    val thrown = intercept[WebApplicationException] {
      resource.setEmail(SetEmailRequest(s"${uname("owner")}@example.com"), new SessionUser(caller))
    }

    // The same mapper Dropwizard registers for every throwable a resource lets out.
    val mapped = new LoggingExceptionMapper[Throwable]() {}.toResponse(thrown)
    mapped.getStatus shouldBe 409
    val body = new ObjectMapper().writeValueAsString(mapped.getEntity)
    body should include(""""message":"That email address already belongs to an account.""")
  }

  // The placeholder keeps its uid because dataset contributor rows already reference it; the
  // caller's own row is discarded, which is only safe while it is INACTIVE and emailless.
  it should "move the credential onto a contributor placeholder owning the address" in {
    val placeholder = seedPlaceholder(uname("ghost"), uemail("ghost"))
    val caller = seedEmaillessUser("claimer")

    val response = resource.setEmail(SetEmailRequest(uemail("ghost")), new SessionUser(caller))

    val claimed = userDao.fetchOneByUid(placeholder.getUid)
    claimed.getIsPlaceholder shouldBe false
    claimed.getComment should include("Claimed contributor placeholder at ")
    claimed.getName shouldBe uname("claimer")
    hasProvider(placeholder.getUid, ProviderTypeEnum.LOCAL) shouldBe true
    providerIdOf(placeholder.getUid, ProviderTypeEnum.LOCAL) shouldBe uname("claimer")

    // The account the caller was signed in as is gone, and the session continues as the claimed one.
    userDao.fetchOneByUid(caller.getUid) shouldBe null
    subjectOf(response.accessToken) shouldBe uname("claimer")
    emailClaimOf(response.accessToken) shouldBe uemail("ghost")
  }

  // `user_last_active_time.uid` references "user"(uid) with no ON DELETE CASCADE — the only FK to
  // "user" that does not cascade — so discarding the caller's row fails unless that row goes first.
  // Any authenticated request can have created it, so the adoption must not depend on its absence.
  it should "adopt a placeholder even when the caller has an activity row" in {
    val placeholder = seedPlaceholder(uname("tracked"), uemail("tracked"))
    val caller = seedEmaillessUser("active")
    getDSLContext
      .insertInto(USER_LAST_ACTIVE_TIME)
      .set(USER_LAST_ACTIVE_TIME.UID, caller.getUid)
      .set(USER_LAST_ACTIVE_TIME.LAST_ACTIVE_TIME, OffsetDateTime.now())
      .execute()

    resource.setEmail(SetEmailRequest(uemail("tracked")), new SessionUser(caller))

    userDao.fetchOneByUid(caller.getUid) shouldBe null
    userDao.fetchOneByUid(placeholder.getUid).getIsPlaceholder shouldBe false
    providerIdOf(placeholder.getUid, ProviderTypeEnum.LOCAL) shouldBe uname("active")
  }

  // What makes an account safe to hand over is that nobody can sign into it, and that is decided
  // by whether it holds a credential — not by the `is_placeholder` flag, which is only a note about
  // where the row came from. The two are kept in step by every path that writes a credential
  // (`claimPlaceholder` runs in the same transaction), so this pairing is not one the application
  // produces; nothing in the schema forbids it either, and the check that would catch it if it ever
  // showed up is the difference between a 409 and handing someone else's sign-in away.
  it should "not adopt a placeholder that already holds a credential" in {
    val placeholder = seedPlaceholder(uname("credited"), uemail("credited"))
    // A LOCAL credential specifically, so that the refusal has to come *before* the move rather
    // than merely instead of it: the caller holds a LOCAL credential too and auth_provider is keyed
    // (uid, provider_type), so re-pointing the caller's row at this uid first would break the
    // primary key and surface as a 500 in place of this 409.
    seedLocalProvider(placeholder.getUid, uname("credited_signin"))
    val caller = seedEmaillessUser("wouldadopt")

    val thrown = intercept[WebApplicationException] {
      resource.setEmail(SetEmailRequest(uemail("credited")), new SessionUser(caller))
    }

    statusOf(thrown) shouldBe 409
    // Both accounts hold a credential here, so this fixture cannot say *whose* the check reads —
    // that is pinned by the two adoption tests above, which fail if it reads the caller's. What it
    // pins is that holding one at all is what stops the adoption. The post-state below is likewise
    // documentation rather than a second pin: `setEmail` runs in one transaction, so a caught
    // exception already implies an unchanged database.
    providerIdOf(placeholder.getUid, ProviderTypeEnum.LOCAL) shouldBe uname("credited_signin")
    providerIdOf(caller.getUid, ProviderTypeEnum.LOCAL) shouldBe uname("wouldadopt")
    val untouched = userDao.fetchOneByUid(placeholder.getUid)
    untouched.getIsPlaceholder shouldBe true
    untouched.getName shouldBe uname("credited")
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

  // ─── with email verification switched on ────────────────────────────────────
  //
  // The flag is off in the test config (as it is by default), so these build a resource with the
  // seam overridden and a verifier whose sender is captured — that is how the mailed code is read
  // back without a mail server.

  private def mailedCode: String = {
    val body = mailed.lastOption.getOrElse(fail("no code was mailed"))._1.content
    "\\d{6}".r.findFirstIn(body).getOrElse(fail(s"no six-digit code in: $body"))
  }

  "register with verification on" should "mail a code and create nothing" in {
    val response =
      verifying.register(UserRegistrationRequest(uname("pend"), uemail("pend"), "secret"))

    // A null token is the whole signal: there is no separate flag that could disagree with it.
    response.accessToken shouldBe null
    mailed.map(_._2) shouldBe Seq(uemail("pend"))
    // The whole point of the stateless design: nothing exists yet, anywhere.
    LocalAuthProvisioner.handleExists(uname("pend")) shouldBe false
    AuthResource.fetchUserByEmailIgnoreCase(uemail("pend")) shouldBe null
  }

  "register/verify" should "create the account when the mailed code comes back" in {
    verifying.register(UserRegistrationRequest(uname("done"), uemail("done"), "secret"))

    val response = verifying.registerVerify(
      UserRegistrationRequest(uname("done"), uemail("done"), "secret", mailedCode)
    )

    subjectOf(response.accessToken) shouldBe uname("done")
    val created = AuthResource.fetchUserByEmailIgnoreCase(uemail("done"))
    created should not be null
    created.getRole shouldBe UserRoleEnum.INACTIVE
    // The password was re-submitted rather than stashed, so the credential is usable.
    AuthResource
      .retrieveUserByUsernameAndPassword(uname("done"), "secret")
      .map(_.getUid) shouldBe Some(created.getUid)
  }

  it should "refuse a wrong code and create nothing" in {
    verifying.register(UserRegistrationRequest(uname("wrong"), uemail("wrong"), "secret"))
    val wrong = if (mailedCode == "000000") "111111" else "000000"

    intercept[NotAcceptableException] {
      verifying.registerVerify(
        UserRegistrationRequest(uname("wrong"), uemail("wrong"), "secret", wrong)
      )
    }

    AuthResource.fetchUserByEmailIgnoreCase(uemail("wrong")) shouldBe null
    LocalAuthProvisioner.handleExists(uname("wrong")) shouldBe false
  }

  it should "refuse a missing code" in {
    verifying.register(UserRegistrationRequest(uname("nocode"), uemail("nocode"), "secret"))

    intercept[NotAcceptableException] {
      verifying.registerVerify(UserRegistrationRequest(uname("nocode"), uemail("nocode"), "secret"))
    }

    AuthResource.fetchUserByEmailIgnoreCase(uemail("nocode")) shouldBe null
  }

  it should "refuse a code minted for a different address" in {
    verifying.register(UserRegistrationRequest(uname("swap"), uemail("swap"), "secret"))
    val code = mailedCode

    intercept[NotAcceptableException] {
      verifying.registerVerify(
        UserRegistrationRequest(uname("swap"), uemail("elsewhere"), "secret", code)
      )
    }

    AuthResource.fetchUserByEmailIgnoreCase(uemail("elsewhere")) shouldBe null
  }

  it should "still claim a placeholder that owns the verified address" in {
    val placeholder = seedPlaceholder(uname("ph"), uemail("ph"))
    verifying.register(UserRegistrationRequest(uname("phclaim"), uemail("ph"), "secret"))

    val response = verifying.registerVerify(
      UserRegistrationRequest(uname("phclaim"), uemail("ph"), "secret", mailedCode)
    )

    subjectOf(response.accessToken) shouldBe uname("phclaim")
    // The placeholder keeps its uid, so contributor rows pointing at it stay valid.
    val claimed = userDao.fetchOneByUid(placeholder.getUid)
    claimed.getIsPlaceholder shouldBe false
    claimed.getName shouldBe uname("phclaim")
  }

  "setEmail with verification on" should "refuse without a code and leave the row alone" in {
    val user = seedEmaillessUser("needcode")

    intercept[NotAcceptableException] {
      verifying.setEmail(SetEmailRequest(uemail("needcode")), new SessionUser(user))
    }

    userDao.fetchOneByUid(user.getUid).getEmail shouldBe null
  }

  it should "store the address when the mailed code comes back" in {
    val user = seedEmaillessUser("withcode")
    verifying.sendEmailCode(SetEmailRequest(uemail("withcode")), new SessionUser(user))

    val response = verifying.setEmail(
      SetEmailRequest(uemail("withcode"), mailedCode),
      new SessionUser(user)
    )

    userDao.fetchOneByUid(user.getUid).getEmail shouldBe uemail("withcode")
    emailClaimOf(response.accessToken) shouldBe uemail("withcode")
  }

  it should "refuse a code issued to another account" in {
    val mine = seedEmaillessUser("mine")
    val theirs = seedEmaillessUser("theirs")
    verifying.sendEmailCode(SetEmailRequest(uemail("shared")), new SessionUser(theirs))

    intercept[NotAcceptableException] {
      verifying.setEmail(SetEmailRequest(uemail("shared"), mailedCode), new SessionUser(mine))
    }

    userDao.fetchOneByUid(mine.getUid).getEmail shouldBe null
  }

  "sendEmailCode" should "mail a code to the address being claimed" in {
    val user = seedEmaillessUser("sender")

    verifying.sendEmailCode(SetEmailRequest(uemail("sender")), new SessionUser(user))

    mailed.map(_._2) shouldBe Seq(uemail("sender"))
  }

  it should "refuse an account that already has an address, so it cannot relay mail" in {
    val user = seedUser(uname("hasone"), "secret")

    val thrown = intercept[WebApplicationException] {
      verifying.sendEmailCode(SetEmailRequest(uemail("target")), new SessionUser(user))
    }

    statusOf(thrown) shouldBe 409
    mailed shouldBe empty
  }

  it should "refuse a malformed address without mailing" in {
    val user = seedEmaillessUser("badaddr")

    intercept[NotAcceptableException] {
      verifying.sendEmailCode(SetEmailRequest("not-an-address"), new SessionUser(user))
    }

    mailed shouldBe empty
  }
}
