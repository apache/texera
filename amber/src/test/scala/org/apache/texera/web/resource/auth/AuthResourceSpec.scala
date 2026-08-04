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

import org.apache.texera.auth.JwtAuth
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.USER
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.model.http.request.auth.{UserLoginRequest, UserRegistrationRequest}
import org.jasypt.util.password.StrongPasswordEncryptor
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.util.UUID
import javax.ws.rs.{NotAcceptableException, NotAuthorizedException}

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
  private var resource: AuthResource = _

  private def uname(tag: String): String = s"authspec_${tag}_$runId"

  private def uemail(tag: String): String = s"authspec_${tag}_$runId@example.com"

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    resource = new AuthResource()
    cleanup()
  }

  override protected def afterEach(): Unit = cleanup()

  private def cleanup(): Unit = {
    // startsWith escapes SQL LIKE wildcards, so the literal "authspec_" prefix is matched exactly.
    getDSLContext.deleteFrom(USER).where(USER.NAME.startsWith("authspec_")).execute()
    // createAdminUser() seeds the configured admin — remove it too so the test starts clean.
    getDSLContext.deleteFrom(USER).where(USER.NAME.eq(UserSystemConfig.adminUsername)).execute()
  }

  private def seedUser(
      name: String,
      password: String,
      role: UserRoleEnum = UserRoleEnum.REGULAR
  ): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"$name@example.com")
    user.setPassword(encryptor.encryptPassword(password))
    user.setRole(role)
    userDao.insert(user)
    user
  }

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
    // stored hashed, not in plain text, but verifies against the plain password
    stored.getPassword should not be "pw"
    encryptor.checkPassword("pw", stored.getPassword) shouldBe true
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

  // ─── createAdminUser ────────────────────────────────────────────────────────

  "createAdminUser" should "insert the configured admin with the ADMIN role and a hashed password" in {
    AuthResource.createAdminUser()

    val admins = userDao.fetchByName(UserSystemConfig.adminUsername)
    admins.size() shouldBe 1
    admins.get(0).getRole shouldBe UserRoleEnum.ADMIN
    encryptor.checkPassword(UserSystemConfig.adminPassword, admins.get(0).getPassword) shouldBe true
  }

  it should "not create a second admin when one already exists" in {
    AuthResource.createAdminUser()
    AuthResource.createAdminUser()
    userDao.fetchByName(UserSystemConfig.adminUsername).size() shouldBe 1
  }
}
