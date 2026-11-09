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

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import org.apache.texera.common.config.UserSystemConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import javax.ws.rs.NotAuthorizedException

/**
  * Integration spec for [[GoogleAuthResource]] against embedded Postgres.
  *
  * Token verification is the one part that cannot run here — it needs a Google-signed JWT and a
  * network round trip — so the suite overrides `verifiedPayload` and drives the resource with
  * payloads built by hand. What it pins down is everything downstream of verification: how a
  * Google payload becomes an [[ExternalProfile]] (the name fallback, and the avatar kept as the
  * complete URL the provider supplied) and that a credential Google does not verify is a 401.
  */
class GoogleAuthResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val emailDomain = "@google-auth-test.com"
  private val avatarUrl = "https://lh3.googleusercontent.com/a/AVATAR-ID"

  private var userDao: UserDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = cleanup()
  override protected def afterEach(): Unit = cleanup()

  private def cleanup(): Unit =
    getDSLContext.deleteFrom(USER).where(USER.EMAIL.like("%" + emailDomain)).execute()

  // ---- helpers -------------------------------------------------------------

  /** A resource whose verification step always yields `payload`, standing in for Google. */
  private class StubbedGoogleAuthResource(payload: Option[GoogleIdToken.Payload])
      extends GoogleAuthResource {
    override protected def verifiedPayload(credential: String): Option[GoogleIdToken.Payload] =
      payload
  }

  /**
    * A payload shaped like Google's. `name` and `picture` are ordinary JSON members rather than
    * typed fields, and passing null omits them — which is exactly the case the name fallback
    * exists for. `emailVerified` defaults to true, the only kind of payload the resource accepts;
    * null omits the claim.
    */
  private def payload(
      subject: String,
      email: String,
      name: String = "Given Name",
      picture: String = avatarUrl,
      emailVerified: java.lang.Boolean = true
  ): GoogleIdToken.Payload = {
    val p = new GoogleIdToken.Payload()
    p.setSubject(subject)
    p.setEmail(email)
    if (name != null) p.set("name", name)
    if (picture != null) p.set("picture", picture)
    if (emailVerified != null) p.setEmailVerified(emailVerified)
    p
  }

  private def loginWith(p: GoogleIdToken.Payload): Unit =
    new StubbedGoogleAuthResource(Some(p))
      .login("stubbed-credential")
      .accessToken should not be empty

  private def userByEmail(localPart: String): User =
    userDao.fetchOneByEmail(localPart + emailDomain)

  private def googleIdOf(uid: Integer): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.GOOGLE))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)

  // ---- first login ---------------------------------------------------------

  behavior of "login"

  it should "provision an INACTIVE user and a GOOGLE provider row on a first login" in {
    loginWith(payload("google-sub-new", "newcomer" + emailDomain, name = "New Comer"))

    val user = userByEmail("newcomer")
    user should not be null
    user.getName shouldBe "New Comer"
    user.getRole shouldBe UserRoleEnum.INACTIVE
    googleIdOf(user.getUid) shouldBe "google-sub-new"
  }

  it should "return the same account on a second login rather than provisioning again" in {
    loginWith(payload("google-sub-repeat", "repeat" + emailDomain))
    val first = userByEmail("repeat").getUid

    loginWith(payload("google-sub-repeat", "repeat" + emailDomain))

    getDSLContext.fetchCount(USER, USER.EMAIL.eq("repeat" + emailDomain)) shouldBe 1
    userByEmail("repeat").getUid shouldBe first
  }

  // ---- payload mapping -----------------------------------------------------

  it should "fall back to the email address when the payload carries no name" in {
    loginWith(payload("google-sub-nameless", "nameless" + emailDomain, name = null))

    userByEmail("nameless").getName shouldBe "nameless" + emailDomain
  }

  it should "fall back to the email address when the name is present but blank" in {
    loginWith(payload("google-sub-blank", "blank" + emailDomain, name = ""))

    userByEmail("blank").getName shouldBe "blank" + emailDomain
  }

  it should "store the picture URL in full rather than its last path segment" in {
    loginWith(payload("google-sub-avatar", "avatar" + emailDomain))

    userByEmail("avatar").getAvatar shouldBe avatarUrl
  }

  it should "refresh the stored avatar when the provider's URL changes" in {
    loginWith(payload("google-sub-newpic", "newpic" + emailDomain))
    val rotated = "https://lh3.googleusercontent.com/a/ROTATED-ID"

    loginWith(payload("google-sub-newpic", "newpic" + emailDomain, picture = rotated))

    userByEmail("newpic").getAvatar shouldBe rotated
  }

  it should "leave the avatar unset when the payload carries no picture" in {
    loginWith(payload("google-sub-nopic", "nopic" + emailDomain, picture = null))

    userByEmail("nopic").getAvatar shouldBe null
  }

  // "No avatar" is one case rather than two: an absent picture leaves what is on file instead of
  // blanking it, so a payload that omits the claim does not wipe a working avatar.
  it should "keep an already-stored avatar when a later login carries no picture" in {
    loginWith(payload("google-sub-keeppic", "keeppic" + emailDomain))
    userByEmail("keeppic").getAvatar shouldBe avatarUrl

    loginWith(payload("google-sub-keeppic", "keeppic" + emailDomain, picture = null))

    userByEmail("keeppic").getAvatar shouldBe avatarUrl
  }

  // Storing the provider's URL verbatim means the browser fetches whatever it names, so the host
  // is allowlisted on write; a rejected avatar is dropped, not a failed login.
  it should "not store an avatar served from a host outside the allowlist" in {
    loginWith(
      payload("google-sub-badhost", "badhost" + emailDomain, picture = "https://evil.test/a/ID")
    )

    userByEmail("badhost").getAvatar shouldBe null
  }

  // ---- verification failure ------------------------------------------------

  it should "reject a credential Google does not verify with a 401" in {
    val resource = new StubbedGoogleAuthResource(None)

    a[NotAuthorizedException] should be thrownBy resource.login("not-a-real-credential")
  }

  // Not merely untidy input: matching on an unverified address is an account takeover.
  it should "reject a token whose email_verified is false, leaving the matching account alone" in {
    loginWith(payload("google-sub-owner", "victim" + emailDomain, name = "Real Owner"))
    val owner = userByEmail("victim").getUid

    val resource = new StubbedGoogleAuthResource(
      Some(payload("google-sub-attacker", "victim" + emailDomain, emailVerified = false))
    )
    a[NotAuthorizedException] should be thrownBy resource.login("stubbed-credential")

    // the victim's account still points at the original identity, and no second one was added
    googleIdOf(owner) shouldBe "google-sub-owner"
    getDSLContext.fetchCount(USER, USER.EMAIL.eq("victim" + emailDomain)) shouldBe 1
  }

  it should "reject a token that omits email_verified rather than assuming it" in {
    val resource = new StubbedGoogleAuthResource(
      Some(payload("google-sub-noflag", "noflag" + emailDomain, emailVerified = null))
    )

    a[NotAuthorizedException] should be thrownBy resource.login("stubbed-credential")
    userByEmail("noflag") shouldBe null
  }

  // A null address NPEs in `EmailUtil.normalize`, and as the name fallback violates NOT NULL.
  it should "reject a token with no email address" in {
    val resource = new StubbedGoogleAuthResource(Some(payload("google-sub-noemail", null)))

    a[NotAuthorizedException] should be thrownBy resource.login("stubbed-credential")
  }

  it should "reject a token whose email is blank" in {
    val resource = new StubbedGoogleAuthResource(Some(payload("google-sub-blankemail", "   ")))

    a[NotAuthorizedException] should be thrownBy resource.login("stubbed-credential")
  }

  // ---- client id -----------------------------------------------------------

  behavior of "getClientId"

  it should "expose the configured Google client id" in {
    new GoogleAuthResource().getClientId shouldBe UserSystemConfig.googleClientId
  }
}
