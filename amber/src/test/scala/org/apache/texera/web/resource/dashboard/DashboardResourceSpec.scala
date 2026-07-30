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

package org.apache.texera.web.resource.dashboard

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.USER
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

/**
  * Spec for [[DashboardResource.resultsOwnersInfo]] against embedded Postgres.
  *
  * The avatar it returns is read straight out of `"user".avatar` (formerly `google_avatar`), so
  * the point here is that the column still round-trips: a rename that compiles but selects the
  * wrong column would hand the frontend a null avatar for every owner instead of failing.
  */
class DashboardResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val emailDomain = "@dashboard-test.com"

  private var userDao: UserDao = _
  private var resource: DashboardResource = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    resource = new DashboardResource()
  }

  override protected def afterAll(): Unit = shutdownDB()

  override protected def beforeEach(): Unit = cleanup()
  override protected def afterEach(): Unit = cleanup()

  private def cleanup(): Unit =
    getDSLContext.deleteFrom(USER).where(USER.EMAIL.like("%" + emailDomain)).execute()

  private def seedUser(name: String, localPart: String, avatar: String = null): User = {
    val user = new User
    user.setName(name)
    user.setEmail(localPart + emailDomain)
    user.setRole(UserRoleEnum.REGULAR)
    if (avatar != null) user.setAvatar(avatar)
    userDao.insert(user)
    user
  }

  private def ownersInfo(uids: Integer*): Map[Integer, DashboardResource.UserInfo] =
    resource.resultsOwnersInfo(uids.asJava).asScala.toMap

  behavior of "resultsOwnersInfo"

  it should "return each owner's name and avatar" in {
    val withAvatar = seedUser("Has Avatar", "has-avatar", avatar = "AVATAR-ID")

    val info = ownersInfo(withAvatar.getUid).apply(withAvatar.getUid)

    info.userId shouldBe withAvatar.getUid
    info.userName shouldBe "Has Avatar"
    info.googleAvatar shouldBe Some("AVATAR-ID")
  }

  it should "report no avatar for an owner that has none" in {
    val without = seedUser("No Avatar", "no-avatar")

    ownersInfo(without.getUid).apply(without.getUid).googleAvatar shouldBe None
  }

  it should "return one entry per requested owner" in {
    val first = seedUser("First", "first", avatar = "one")
    val second = seedUser("Second", "second", avatar = "two")

    val info = ownersInfo(first.getUid, second.getUid)

    info.keySet shouldBe Set(first.getUid, second.getUid)
    info(first.getUid).googleAvatar shouldBe Some("one")
    info(second.getUid).googleAvatar shouldBe Some("two")
  }

  it should "skip a uid that matches no user rather than failing" in {
    val known = seedUser("Known", "known")

    val info = ownersInfo(known.getUid, Integer.valueOf(-1))

    info.keySet shouldBe Set(known.getUid)
  }
}
