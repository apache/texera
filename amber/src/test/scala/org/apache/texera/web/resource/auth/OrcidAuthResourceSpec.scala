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
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import javax.ws.rs.NotAuthorizedException

/**
  * Integration spec for [[OrcidAuthResource]] against embedded Postgres.
  *
  * The token exchange is what cannot run here, so the suite overrides that one seam and drives the
  * resource with bodies shaped like ORCID's. What that leaves under test is everything the exchange
  * feeds: that an authenticated iD becomes an emailless INACTIVE account with an ORCID provider row,
  * and that a response authenticating nobody is a 401 rather than an account.
  */
class OrcidAuthResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val orcidId = "0000-0002-1825-0097"

  private var userDao: UserDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = cleanup()
  override protected def afterEach(): Unit = cleanup()

  // Accounts provisioned here have no email, so they are identified by the provider row that
  // cascades from them rather than by an address pattern.
  private def cleanup(): Unit =
    getDSLContext
      .deleteFrom(USER)
      .where(
        USER.UID.in(
          getDSLContext
            .select(AUTH_PROVIDER.UID)
            .from(AUTH_PROVIDER)
            .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.ORCID))
        )
      )
      .execute()

  // ---- helpers -------------------------------------------------------------

  /** A token response shaped like ORCID's. Passing null for `name` omits the member entirely. */
  private def tokenBody(id: String = orcidId, name: String = "Sofia Garcia"): String = {
    val nameMember = if (name == null) "" else s""""name":"$name","""
    s"""{"access_token":"tok-abc","token_type":"bearer","refresh_token":"ref",
       |"expires_in":631138518,"scope":"/authenticate",$nameMember"orcid":"$id"}""".stripMargin
  }

  /** A resource whose one network leg is canned: `body` stands in for the token exchange. */
  private class StubbedOrcidAuthResource(body: String) extends OrcidAuthResource {
    var exchangedCode: Option[String] = None

    override protected def exchangeCode(code: String): String = {
      exchangedCode = Some(code)
      body
    }
  }

  private def userBehind(orcidId: String): User =
    getDSLContext
      .select(USER.fields(): _*)
      .from(USER)
      .join(AUTH_PROVIDER)
      .on(USER.UID.eq(AUTH_PROVIDER.UID))
      .where(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.ORCID))
      .and(AUTH_PROVIDER.PROVIDER_ID.eq(orcidId))
      .fetchOneInto(classOf[User])

  // ---- login ---------------------------------------------------------------

  behavior of "login"

  it should "provision an emailless INACTIVE account and an ORCID provider row on a first login" in {
    val response = new StubbedOrcidAuthResource(tokenBody()).login("auth-code")

    response.accessToken should not be empty
    val user = userBehind(orcidId)
    user should not be null
    user.getName shouldBe "Sofia Garcia"
    user.getEmail shouldBe null
    user.getRole shouldBe UserRoleEnum.INACTIVE
  }

  it should "return the same account on a second login rather than provisioning again" in {
    val first = new StubbedOrcidAuthResource(tokenBody())
    first.login("code-1")
    val uid = userBehind(orcidId).getUid

    new StubbedOrcidAuthResource(tokenBody()).login("code-2")

    userBehind(orcidId).getUid shouldBe uid
  }

  // `"user".name` is NOT NULL and ORCID omits the member for a record whose owner made it private,
  // so the iD has to stand in rather than the insert failing.
  it should "fall back to the ORCID iD when the record publishes no name" in {
    new StubbedOrcidAuthResource(tokenBody(name = null)).login("auth-code")

    userBehind(orcidId).getName shouldBe orcidId
  }

  it should "pass the code through to the exchange with surrounding whitespace trimmed" in {
    val resource = new StubbedOrcidAuthResource(tokenBody())
    resource.login("  auth-code\n")

    resource.exchangedCode shouldBe Some("auth-code")
  }

  // An address the user supplied later has to survive: every subsequent ORCID login still asserts
  // none, and refreshing must not blank what `AuthResource.setEmail` collected.
  it should "leave a later-collected address alone when the identity returns" in {
    new StubbedOrcidAuthResource(tokenBody()).login("c")
    val user = userBehind(orcidId)
    user.setEmail("collected@example.com")
    userDao.update(user)

    new StubbedOrcidAuthResource(tokenBody()).login("c")

    userBehind(orcidId).getEmail shouldBe "collected@example.com"
  }

  // ---- refusals ------------------------------------------------------------

  // A response with no `orcid` authenticated nobody. Provisioning against a synthesized id would
  // hand out an account, so this must fail rather than default.
  it should "reject a token response that names no ORCID iD" in {
    assertThrows[NotAuthorizedException] {
      new StubbedOrcidAuthResource("""{"access_token":"tok","scope":"/authenticate"}""").login("c")
    }
  }

  it should "reject a blank authorization code without reaching the exchange" in {
    val resource = new StubbedOrcidAuthResource(tokenBody())

    assertThrows[NotAuthorizedException](resource.login("   "))
    resource.exchangedCode shouldBe None
  }

  // ---- configuration gating ------------------------------------------------

  // What `getConfig` refuses on. Driven through the pure helper rather than the endpoint, because
  // the endpoint reads `UserSystemConfig` object vals: a developer with USER_SYS_ORCID_* exported
  // would see the opposite outcome from CI.
  //
  // Each blank matters at a different moment, and both are worse than failing here: an empty
  // client id lands the user on an ORCID error page, and an empty redirect uri gets the exchange
  // rejected after they have already consented.
  behavior of "missingSettings"

  it should "accept a fully configured deployment" in {
    OrcidAuthResource.missingSettings(
      "APP-1",
      "secret",
      "http://127.0.0.1:4200/callback/orcid",
      "https://sandbox.orcid.org"
    ) shouldBe empty
  }

  it should "name each setting that is empty, blank, or absent" in {
    OrcidAuthResource.missingSettings("", "secret", "uri", "base") shouldBe Seq("clientId")
    OrcidAuthResource.missingSettings("APP-1", "   ", "uri", "base") shouldBe Seq("clientSecret")
    OrcidAuthResource.missingSettings("APP-1", "secret", null, "base") shouldBe Seq("redirectUri")
    // A blank baseUrl would make authorizeUrl the relative "/oauth/authorize", so the button would
    // navigate the app to itself rather than to ORCID.
    OrcidAuthResource.missingSettings("APP-1", "secret", "uri", "") shouldBe Seq("baseUrl")
    OrcidAuthResource.missingSettings("", "", "", "") shouldBe Seq(
      "clientId",
      "clientSecret",
      "redirectUri",
      "baseUrl"
    )
  }
}
