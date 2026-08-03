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

package org.apache.texera.web.resource

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{USER, USER_CONFIG}
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec

import javax.ws.rs.BadRequestException

class UserConfigResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testUid = 4000 + scala.util.Random.nextInt(1000)
  private val otherUid = testUid + 1

  private var userDao: UserDao = _
  private var testUser: User = _
  private var otherUser: User = _
  private val resource = new UserConfigResource

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = shutdownDB()

  // Every test starts from a clean slate for the ids under test, then seeds only
  // the two users it needs, so tests are order-independent.
  override protected def beforeEach(): Unit = resetFixtures()

  private def resetFixtures(): Unit = {
    getDSLContext.deleteFrom(USER_CONFIG).where(USER_CONFIG.UID.in(testUid, otherUid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.in(testUid, otherUid)).execute()

    testUser = newUser(testUid, "config_user", "config@example.com")
    otherUser = newUser(otherUid, "other_user", "other@example.com")
  }

  private def newUser(uid: Int, name: String, email: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(email)
    user.setPassword("password")
    userDao.insert(user)
    user
  }

  private def session(user: User): SessionUser = new SessionUser(user)

  // ─── getEntry / setEntry ───────────────────────────────────────────────────

  "getEntry" should "return null for a key that was never set" in {
    assert(resource.getEntry("missing", session(testUser)) == null)
  }

  "setEntry" should "store a value that getEntry then returns" in {
    resource.setEntry("theme", "dark", session(testUser))
    assert(resource.getEntry("theme", session(testUser)) == "dark")
  }

  it should "overwrite the value when the same key is set again" in {
    resource.setEntry("theme", "dark", session(testUser))
    resource.setEntry("theme", "light", session(testUser))
    assert(resource.getEntry("theme", session(testUser)) == "light")
    // still a single entry, not two
    assert(resource.getAllDict(session(testUser)).size == 1)
  }

  // ─── getAllDict ────────────────────────────────────────────────────────────

  "getAllDict" should "return an empty map when the user has no entries" in {
    assert(resource.getAllDict(session(testUser)).isEmpty)
  }

  it should "return every key/value pair the user has set" in {
    resource.setEntry("a", "1", session(testUser))
    resource.setEntry("b", "2", session(testUser))
    resource.setEntry("c", "3", session(testUser))

    val dict = resource.getAllDict(session(testUser))
    assert(dict == Map("a" -> "1", "b" -> "2", "c" -> "3"))
  }

  it should "be scoped to the requesting user (another user's entries are not returned)" in {
    resource.setEntry("shared", "mine", session(testUser))
    resource.setEntry("shared", "theirs", session(otherUser))
    resource.setEntry("other-only", "x", session(otherUser))

    assert(resource.getAllDict(session(testUser)) == Map("shared" -> "mine"))
    assert(resource.getEntry("other-only", session(testUser)) == null)
    assert(
      resource.getAllDict(session(otherUser)) == Map("shared" -> "theirs", "other-only" -> "x")
    )
  }

  // ─── deleteEntry ───────────────────────────────────────────────────────────

  "deleteEntry" should "remove the key so getEntry and getAllDict no longer show it" in {
    resource.setEntry("k1", "v1", session(testUser))
    resource.setEntry("k2", "v2", session(testUser))

    resource.deleteEntry("k1", session(testUser))

    assert(resource.getEntry("k1", session(testUser)) == null)
    assert(resource.getAllDict(session(testUser)) == Map("k2" -> "v2"))
  }

  it should "be a no-op (no error) when the key does not exist" in {
    resource.deleteEntry("never-set", session(testUser))
    assert(resource.getAllDict(session(testUser)).isEmpty)
  }

  // ─── key validation ────────────────────────────────────────────────────────

  "getEntry" should "reject a blank key with a BadRequestException" in {
    assertThrows[BadRequestException](resource.getEntry("  ", session(testUser)))
  }

  "setEntry" should "reject a blank key with a BadRequestException" in {
    assertThrows[BadRequestException](resource.setEntry("", "v", session(testUser)))
  }

  "deleteEntry" should "reject a blank key with a BadRequestException" in {
    assertThrows[BadRequestException](resource.deleteEntry("", session(testUser)))
  }
}
