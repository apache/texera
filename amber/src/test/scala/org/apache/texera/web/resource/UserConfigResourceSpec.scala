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
import org.apache.texera.dao.DatabaseFlatSpec
import org.apache.texera.dao.jooq.generated.Tables.{USER, USER_CONFIG}
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User

import javax.ws.rs.BadRequestException

class UserConfigResourceSpec extends DatabaseFlatSpec() {

  private val testUid = 4000 + scala.util.Random.nextInt(1000)
  private val otherUid = testUid + 1

  private var userDao: UserDao = _
  private var testUser: SessionUser = _
  private var otherUser: SessionUser = _
  private val resource = new UserConfigResource

  override protected def onBeforeAll(): Unit =
    userDao = new UserDao(getDSLContext.configuration())

  // Every test starts from a clean slate for the ids under test, then seeds only
  // the two users it needs, so tests are order-independent.
  override protected def cleanup(): Unit = {
    getDSLContext.deleteFrom(USER_CONFIG).where(USER_CONFIG.UID.in(testUid, otherUid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.in(testUid, otherUid)).execute()
  }

  override protected def onBeforeEach(): Unit = {
    testUser = seedSession(testUid, "config_user", "config@example.com")
    otherUser = seedSession(otherUid, "other_user", "other@example.com")
  }

  private def seedSession(uid: Int, name: String, email: String): SessionUser = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(email)
    userDao.insert(user)
    new SessionUser(user)
  }

  // ─── getEntry / setEntry ───────────────────────────────────────────────────

  "getEntry" should "return null for a key that was never set" in {
    assert(resource.getEntry("missing", testUser) == null)
  }

  "setEntry" should "store a value that getEntry then returns" in {
    resource.setEntry("theme", "dark", testUser)
    assert(resource.getEntry("theme", testUser) == "dark")
  }

  it should "overwrite the value when the same key is set again" in {
    resource.setEntry("theme", "dark", testUser)
    resource.setEntry("theme", "light", testUser)
    assert(resource.getEntry("theme", testUser) == "light")
    // still a single entry, not two
    assert(resource.getAllDict(testUser).size == 1)
  }

  // ─── getAllDict ────────────────────────────────────────────────────────────

  "getAllDict" should "return an empty map when the user has no entries" in {
    assert(resource.getAllDict(testUser).isEmpty)
  }

  it should "return every key/value pair the user has set" in {
    resource.setEntry("a", "1", testUser)
    resource.setEntry("b", "2", testUser)
    resource.setEntry("c", "3", testUser)

    val dict = resource.getAllDict(testUser)
    assert(dict == Map("a" -> "1", "b" -> "2", "c" -> "3"))
  }

  it should "be scoped to the requesting user (another user's entries are not returned)" in {
    resource.setEntry("shared", "mine", testUser)
    resource.setEntry("shared", "theirs", otherUser)
    resource.setEntry("other-only", "x", otherUser)

    assert(resource.getAllDict(testUser) == Map("shared" -> "mine"))
    assert(resource.getEntry("other-only", testUser) == null)
    assert(
      resource.getAllDict(otherUser) == Map("shared" -> "theirs", "other-only" -> "x")
    )
  }

  // ─── deleteEntry ───────────────────────────────────────────────────────────

  "deleteEntry" should "remove the key so getEntry and getAllDict no longer show it" in {
    resource.setEntry("k1", "v1", testUser)
    resource.setEntry("k2", "v2", testUser)

    resource.deleteEntry("k1", testUser)

    assert(resource.getEntry("k1", testUser) == null)
    assert(resource.getAllDict(testUser) == Map("k2" -> "v2"))
  }

  it should "be a no-op (no error) when the key does not exist" in {
    resource.deleteEntry("never-set", testUser)
    assert(resource.getAllDict(testUser).isEmpty)
  }

  // ─── key validation ────────────────────────────────────────────────────────

  "getEntry" should "reject a blank key with a BadRequestException" in {
    assertThrows[BadRequestException](resource.getEntry("  ", testUser))
  }

  "setEntry" should "reject a blank key with a BadRequestException" in {
    assertThrows[BadRequestException](resource.setEntry("", "v", testUser))
  }

  "deleteEntry" should "reject a blank key with a BadRequestException" in {
    assertThrows[BadRequestException](resource.deleteEntry("", testUser))
  }
}
