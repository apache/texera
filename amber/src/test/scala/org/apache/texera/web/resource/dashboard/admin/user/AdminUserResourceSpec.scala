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

package org.apache.texera.web.resource.dashboard.admin.user

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{AUTH_PROVIDER, USER}
import org.apache.texera.dao.jooq.generated.enums.{ProviderTypeEnum, UserRoleEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{AuthProviderDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{AuthProvider, User}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, LoneElement}

import scala.jdk.CollectionConverters._

/**
  * Integration spec for [[AdminUserResource]] against embedded Postgres.
  *
  * Two things are worth pinning down here: the local login handle an admin-created account
  * gets (it is no longer readable off `"user".name`, which an admin may rename), and the fact
  * that `list()` joins `auth_provider` twice — once per provider — so the two handles must
  * not bleed into each other's column.
  */
class AdminUserResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with LoneElement
    with MockTexeraDB {

  private var userDao: UserDao = _
  private var authDao: AuthProviderDao = _
  private var resource: AdminUserResource = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    authDao = new AuthProviderDao(getDSLContext.configuration())
    resource = new AdminUserResource()
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = cleanup()
  override protected def afterEach(): Unit = cleanup()

  // This suite owns its embedded database, so clearing "user" is enough; auth_provider
  // rows go with it via ON DELETE CASCADE.
  private def cleanup(): Unit = getDSLContext.deleteFrom(USER).execute()

  // ---- helpers -------------------------------------------------------------

  private def seedUser(name: String, email: String): User = {
    val user = new User
    user.setName(name)
    user.setEmail(email)
    user.setRole(UserRoleEnum.REGULAR)
    userDao.insert(user)
    user
  }

  private def seedProvider(
      uid: Integer,
      providerType: ProviderTypeEnum,
      providerId: String,
      password: String = null
  ): Unit = {
    val auth = new AuthProvider
    auth.setUid(uid)
    auth.setProviderType(providerType)
    auth.setProviderId(providerId)
    if (password != null) auth.setPassword(password)
    authDao.insert(auth)
  }

  private def infoFor(uid: Integer): UserInfo =
    resource.list().asScala.find(_.uid == uid).getOrElse(fail(s"uid $uid missing from list()"))

  private def localHandleOf(uid: Integer): String =
    getDSLContext
      .select(AUTH_PROVIDER.PROVIDER_ID)
      .from(AUTH_PROVIDER)
      .where(AUTH_PROVIDER.UID.eq(uid))
      .and(AUTH_PROVIDER.PROVIDER_TYPE.eq(ProviderTypeEnum.LOCAL))
      .fetchOne(AUTH_PROVIDER.PROVIDER_ID)

  private def allUsers: Seq[User] = userDao.findAll().asScala.toSeq

  // ---- addUser -------------------------------------------------------------

  behavior of "addUser"

  it should "give the new account a local handle matching its generated name" in {
    resource.addUser()

    val created = allUsers.loneElement
    created.getRole shouldBe UserRoleEnum.INACTIVE
    localHandleOf(created.getUid) shouldBe created.getName
  }

  it should "generate distinct handles for accounts created in quick succession" in {
    resource.addUser()
    resource.addUser()
    resource.addUser()

    val handles = allUsers.map(u => localHandleOf(u.getUid))
    handles should have size 3
    handles.distinct should have size 3
    handles.foreach(_ should not be null)
  }

  it should "leave the handle alone when the display name is later changed" in {
    resource.addUser()
    val created = allUsers.loneElement
    val handle = created.getName

    val renamed = userDao.fetchOneByUid(created.getUid)
    renamed.setName("Friendly Name")
    renamed.setEmail("friendly@example.com")
    resource.updateUser(renamed)

    localHandleOf(created.getUid) shouldBe handle
    infoFor(created.getUid).name shouldBe "Friendly Name"
    infoFor(created.getUid).localHandle shouldBe handle
  }

  // ---- list ----------------------------------------------------------------

  behavior of "list"

  it should "report the local handle for a password account" in {
    val user = seedUser("Local Only", "local@example.com")
    seedProvider(user.getUid, ProviderTypeEnum.LOCAL, "local-handle", password = "hashed")

    val info = infoFor(user.getUid)
    info.localHandle shouldBe "local-handle"
    info.googleId shouldBe null
  }

  it should "report the google id for an external account" in {
    val user = seedUser("Google Only", "google@example.com")
    seedProvider(user.getUid, ProviderTypeEnum.GOOGLE, "google-sub-1")

    val info = infoFor(user.getUid)
    info.googleId shouldBe "google-sub-1"
    info.localHandle shouldBe null
  }

  it should "keep the two handles in their own columns for an account holding both" in {
    val user = seedUser("Both", "both@example.com")
    seedProvider(user.getUid, ProviderTypeEnum.LOCAL, "both-handle", password = "hashed")
    seedProvider(user.getUid, ProviderTypeEnum.GOOGLE, "google-sub-2")

    val info = infoFor(user.getUid)
    info.localHandle shouldBe "both-handle"
    info.googleId shouldBe "google-sub-2"
    info.name shouldBe "Both"
    info.email shouldBe "both@example.com"
  }

  // Two left joins against the same table are what make this a risk: without the
  // provider_type predicates a user with several providers would fan out into one row each.
  it should "return one row per user even when a user has several providers" in {
    val user = seedUser("Both", "both@example.com")
    seedProvider(user.getUid, ProviderTypeEnum.LOCAL, "both-handle-2", password = "hashed")
    seedProvider(user.getUid, ProviderTypeEnum.GOOGLE, "google-sub-3")

    resource.list().asScala.count(_.uid == user.getUid) shouldBe 1
  }
}
