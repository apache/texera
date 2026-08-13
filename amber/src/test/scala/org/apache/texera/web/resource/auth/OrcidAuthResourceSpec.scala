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
  * The two network legs are what cannot run here, so the suite overrides them and drives the
  * resource with bodies shaped like ORCID's. What that leaves under test is everything the
  * exchange feeds: that an authenticated iD becomes an emailless INACTIVE account with an ORCID
  * provider row, that the published address is offered as a suggestion without being written
  * anywhere, and that a response authenticating nobody is a 401 rather than an account.
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

  /**
    * A resource whose network legs are canned: `body` stands in for the token exchange and
    * `published` for the public-API email lookup.
    */
  private class StubbedOrcidAuthResource(body: String, published: Option[String] = None)
      extends OrcidAuthResource {
    var exchangedCode: Option[String] = None

    override protected def exchangeCode(code: String): String = {
      exchangedCode = Some(code)
      body
    }

    override protected def publishedEmail(orcidId: String, accessToken: String): Option[String] =
      published
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

  // ---- the suggested address -----------------------------------------------

  // The suggestion is for the prompt to prefill and nothing else: writing it would be linking on
  // an address ORCID merely publishes, which is the takeover ExternalProfile warns about.
  it should "offer the published address as a suggestion without storing it" in {
    val response =
      new StubbedOrcidAuthResource(tokenBody(), published = Some("sofia@example.com")).login("c")

    response.suggestedEmail shouldBe Some("sofia@example.com")
    userBehind(orcidId).getEmail shouldBe null
  }

  it should "carry no suggestion when the record publishes no address" in {
    new StubbedOrcidAuthResource(tokenBody(), published = None)
      .login("c")
      .suggestedEmail shouldBe None
  }

  it should "stop suggesting an address once the account has one" in {
    new StubbedOrcidAuthResource(tokenBody()).login("c")
    val user = userBehind(orcidId)
    user.setEmail("collected@example.com")
    userDao.update(user)

    val response =
      new StubbedOrcidAuthResource(tokenBody(), published = Some("published@example.com"))
        .login("c")

    response.suggestedEmail shouldBe None
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

  // ---- prefill parsing -----------------------------------------------------

  behavior of "prefillFrom"

  it should "prefer the address the record marks primary" in {
    val body =
      """{"email":[{"email":"secondary@example.com","primary":false},
        |{"email":"primary@example.com","primary":true}]}""".stripMargin

    OrcidAuthResource.prefillFrom(body) shouldBe Some("primary@example.com")
  }

  it should "fall back to the first address when none is marked primary" in {
    val body = """{"email":[{"email":"only@example.com"}]}"""

    OrcidAuthResource.prefillFrom(body) shouldBe Some("only@example.com")
  }

  it should "yield nothing for an empty or absent email array" in {
    OrcidAuthResource.prefillFrom("""{"email":[]}""") shouldBe None
    OrcidAuthResource.prefillFrom("""{"last-modified-date":null}""") shouldBe None
  }

  it should "discard an address that is not a valid email" in {
    OrcidAuthResource.prefillFrom("""{"email":[{"email":"not-an-address"}]}""") shouldBe None
  }
}
