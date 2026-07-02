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

import org.apache.texera.auth.JwtParser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.apache.texera.web.resource.dashboard.admin.user.AdminUserResource
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

class AdminUserResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val regularUid = 9000 + scala.util.Random.nextInt(1000)
  private val inactiveUid = regularUid + 1
  private val missingUid = regularUid + 999
  private val resource = new AdminUserResource

  private def makeUser(uid: Int, name: String, role: UserRoleEnum): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    user.setPassword("password")
    user.setRole(role)
    user
  }

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(makeUser(regularUid, "impersonate_spec_regular_user", UserRoleEnum.REGULAR))
    userDao.insert(makeUser(inactiveUid, "impersonate_spec_inactive_user", UserRoleEnum.INACTIVE))
  }

  override protected def afterAll(): Unit = shutdownDB()

  "impersonate" should "issue a token whose claims identify the target user" in {
    val token = resource.impersonate(regularUid).accessToken

    val parsed = JwtParser.parseToken(token)
    parsed.isPresent shouldBe true
    val user = parsed.get().getUser
    user.getUid.intValue() shouldBe regularUid
    user.getName shouldBe "impersonate_spec_regular_user"
    user.getRole shouldBe UserRoleEnum.REGULAR
  }

  it should "reject impersonating a non-existent user with 404" in {
    val thrown = the[WebApplicationException] thrownBy resource.impersonate(missingUid)
    thrown.getResponse.getStatus shouldBe Response.Status.NOT_FOUND.getStatusCode
  }

  it should "reject impersonating an inactive user with 400" in {
    val thrown = the[WebApplicationException] thrownBy resource.impersonate(inactiveUid)
    thrown.getResponse.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }
}
