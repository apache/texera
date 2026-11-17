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
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.jose4j.jwt.JwtClaims
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import javax.ws.rs.NotAuthorizedException

/**
  * Integration spec for [[AppleAuthResource]] against embedded Postgres.
  *
  * Signature verification is the one part that cannot run here — it needs an Apple-signed JWT and a
  * network round trip to Apple's JWKS endpoint — so the suite overrides `verifiedClaims` and drives
  * the resource with claim sets built by hand. What it pins down is the mapping downstream, and the
  * two cases Apple's own documentation warns about: a string-typed `email_verified`, and a token
  * with no `email` at all.
  */
class AppleAuthResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val emailDomain = "@apple-auth-test.com"

  private var userDao: UserDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = cleanup()
  override protected def afterEach(): Unit = cleanup()

  // Identity-only accounts have a NULL email, so they are matched by the name they are given
  // instead — the Apple `sub`. AUTH_PROVIDER cascades on delete.
  private def cleanup(): Unit =
    getDSLContext
      .deleteFrom(USER)
      .where(USER.EMAIL.like("%" + emailDomain).or(USER.NAME.like("apple-sub-%")))
      .execute()

  // ---- helpers -------------------------------------------------------------

  /** A resource whose verification step always yields `claims`, standing in for Apple. */
  private class StubbedAppleAuthResource(claims: Option[JwtClaims]) extends AppleAuthResource {
    override protected def verifiedClaims(credential: String): Option[JwtClaims] = claims
  }

  /**
    * A claim set shaped like Apple's. `emailVerified` is typed `Any` on purpose: Apple sends it as
    * a JSON boolean or as a quoted string, and both have to work.
    */
  private def claims(subject: String, email: String, emailVerified: Any = true): JwtClaims = {
    val c = new JwtClaims
    c.setSubject(subject)
    if (email != null) c.setClaim("email", email)
    if (emailVerified != null) c.setClaim("email_verified", emailVerified)
    c
  }

  private def loginWith(c: JwtClaims): Unit =
    new StubbedAppleAuthResource(Some(c))
      .login("stubbed-credential")
      .accessToken should not be empty

  private def userByEmail(localPart: String): User =
    userDao.fetchOneByEmail(localPart + emailDomain)

  private def userByName(name: String): User =
    userDao.fetchByName(name).stream().findFirst().orElse(null)

  private def appleIdOf(uid: Integer): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.APPLE))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)

  // ---- login ---------------------------------------------------------------

  behavior of "login"

  it should "provision an INACTIVE user and an APPLE provider row on a first login" in {
    loginWith(claims("apple-sub-new", "newcomer" + emailDomain))

    val user = userByEmail("newcomer")
    user should not be null
    user.getRole shouldBe UserRoleEnum.INACTIVE
    appleIdOf(user.getUid) shouldBe "apple-sub-new"
  }

  it should "return the same account on a second login rather than provisioning again" in {
    loginWith(claims("apple-sub-repeat", "repeat" + emailDomain))
    val first = userByEmail("repeat").getUid

    loginWith(claims("apple-sub-repeat", "repeat" + emailDomain))

    getDSLContext.fetchCount(USER, USER.EMAIL.eq("repeat" + emailDomain)) shouldBe 1
    userByEmail("repeat").getUid shouldBe first
  }

  // Apple only ever sends a display name on the very first authorization, and outside the token,
  // so there is nothing else to fall back to.
  it should "use the email address as the display name" in {
    loginWith(claims("apple-sub-name", "named" + emailDomain))

    userByEmail("named").getName shouldBe "named" + emailDomain
  }

  // ---- a token with no email -----------------------------------------------

  // Apple omits `email` for Sign in with Apple at Work & School accounts. Refusing would lock
  // those users out of a provider the deployment has enabled, so they are provisioned
  // identity-only: NULL email, `sub` as the name, and an address collected later.
  it should "provision an account with no email when Apple asserts none" in {
    loginWith(claims("apple-sub-noemail", null))

    val user = userByName("apple-sub-noemail")
    user should not be null
    user.getEmail shouldBe null
    appleIdOf(user.getUid) shouldBe "apple-sub-noemail"
  }

  // Repeated NULL emails do not collide on the UNIQUE index, but the identity still has to match
  // the existing row rather than pile up new ones.
  it should "return the same email-less account on a second login" in {
    loginWith(claims("apple-sub-noemail", null))
    val first = userByName("apple-sub-noemail").getUid

    loginWith(claims("apple-sub-noemail", null))

    getDSLContext.fetchCount(USER, USER.NAME.eq("apple-sub-noemail")) shouldBe 1
    userByName("apple-sub-noemail").getUid shouldBe first
  }

  // ---- email_verified -------------------------------------------------------

  // Apple documents this claim as "either a string ("true" or "false") or a Boolean". Reading only
  // the boolean shape yields false for the string case, which would refuse a legitimate login.
  // `booleanClaim` below covers the shapes exhaustively; this pins the wiring through the resource.
  it should "accept email_verified sent as the string \"true\"" in {
    loginWith(claims("apple-sub-strtrue", "strtrue" + emailDomain, emailVerified = "true"))

    userByEmail("strtrue") should not be null
  }

  it should "refuse an address Apple has not verified" in {
    val resource = new StubbedAppleAuthResource(
      Some(claims("apple-sub-unverified", "unverified" + emailDomain, emailVerified = false))
    )

    assertThrows[NotAuthorizedException](resource.login("stubbed-credential"))
    userByEmail("unverified") shouldBe null
  }

  // ---- verification failure -------------------------------------------------

  it should "reject a credential Apple does not verify with a 401" in {
    a[NotAuthorizedException] should be thrownBy new StubbedAppleAuthResource(None)
      .login("not-a-real-credential")
  }

  // The one case that runs the real `verifiedClaims` rather than the stub. A string that is not
  // three dot-separated parts fails the local parse, so no request to Apple is made.
  it should "reject a malformed credential with a 401 before reaching Apple" in {
    a[NotAuthorizedException] should be thrownBy new AppleAuthResource().login("not-a-jwt")
  }

  // ---- booleanClaim ---------------------------------------------------------

  behavior of "booleanClaim"

  it should "read both the boolean and string shapes Apple uses" in {
    def read(value: Any): Boolean = {
      val c = new JwtClaims
      if (value != null) c.setClaim("flag", value)
      AppleAuthResource.booleanClaim(c, "flag")
    }

    read(true) shouldBe true
    read("true") shouldBe true
    read("TRUE") shouldBe true
    read(" true ") shouldBe true
    read(false) shouldBe false
    read("false") shouldBe false
    read(null) shouldBe false
    read(42) shouldBe false
  }

  // ---- client id ------------------------------------------------------------

  behavior of "getClientId"

  it should "expose the configured Apple client id" in {
    new AppleAuthResource().getClientId shouldBe UserSystemConfig.appleClientId
  }
}
