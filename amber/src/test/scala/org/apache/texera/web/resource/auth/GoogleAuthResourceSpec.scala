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
  * Google payload becomes an [[ExternalProfile]] (the name fallback and the avatar reduced to
  * its last path segment) and that an unverifiable credential is a 401 rather than a crash.
  */
class GoogleAuthResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val emailDomain = "@google-auth-test.com"

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
    * exists for. `emailVerified` defaults to true because that is the ordinary case; the
    * provisioner refuses an unverified address outright.
    */
  private def payload(
      subject: String,
      email: String,
      name: String = "Given Name",
      picture: String = "https://lh3.googleusercontent.com/a/AVATAR-ID",
      emailVerified: java.lang.Boolean = true
  ): GoogleIdToken.Payload = {
    val p = new GoogleIdToken.Payload()
    p.setSubject(subject)
    p.setEmail(email)
    p.setEmailVerified(emailVerified)
    if (name != null) p.set("name", name)
    if (picture != null) p.set("picture", picture)
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

  it should "store only the last path segment of the picture URL" in {
    loginWith(payload("google-sub-avatar", "avatar" + emailDomain))

    userByEmail("avatar").getAvatar shouldBe "AVATAR-ID"
  }

  // A missing `picture` maps to None, not Some("") — so the provisioner's documented
  // "leave the stored avatar alone" path is reachable instead of blanking the column.
  it should "leave the avatar unset when the payload carries no picture" in {
    loginWith(payload("google-sub-nopic", "nopic" + emailDomain, picture = null))

    userByEmail("nopic").getAvatar shouldBe null
  }

  it should "keep an already-stored avatar when a later login carries no picture" in {
    loginWith(payload("google-sub-keeppic", "keeppic" + emailDomain))
    userByEmail("keeppic").getAvatar shouldBe "AVATAR-ID"

    loginWith(payload("google-sub-keeppic", "keeppic" + emailDomain, picture = null))

    userByEmail("keeppic").getAvatar shouldBe "AVATAR-ID"
  }

  // ---- unverified email ----------------------------------------------------

  it should "reject a payload whose email Google has not verified" in {
    val resource = new StubbedGoogleAuthResource(
      Some(payload("google-sub-unverified", "unverified" + emailDomain, emailVerified = false))
    )

    assertThrows[NotAuthorizedException](resource.login("stubbed-credential"))
    userByEmail("unverified") shouldBe null
  }

  // ---- verification failure ------------------------------------------------

  it should "reject a credential Google does not verify with a 401" in {
    val resource = new StubbedGoogleAuthResource(None)

    a[NotAuthorizedException] should be thrownBy resource.login("not-a-real-credential")
  }

  // The one case that runs the real `verifiedPayload` rather than the stub. A string that is not
  // three dot-separated parts fails in the local JWT parse, so no request to Google is made and
  // this stays a unit test. Characterises today's behaviour: the parse error propagates, which
  // Jersey renders as a 500 rather than the 401 a bad credential gets.
  it should "fail on a malformed credential before reaching Google" in {
    an[IllegalArgumentException] should be thrownBy new GoogleAuthResource().login("not-a-jwt")
  }

  // ---- client id -----------------------------------------------------------

  behavior of "getClientId"

  it should "expose the configured Google client id" in {
    new GoogleAuthResource().getClientId shouldBe UserSystemConfig.googleClientId
  }
}
