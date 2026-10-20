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

package org.apache.texera.web.resource.dashboard.user

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.USER
import org.apache.texera.dao.jooq.generated.enums.UserRoleEnum
import org.apache.texera.dao.jooq.generated.tables.daos.UserDao
import org.apache.texera.dao.jooq.generated.tables.pojos.User
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import javax.ws.rs.WebApplicationException
import javax.ws.rs.core.Response

/**
  * Covers the joining-reason endpoints against embedded Postgres: whether a user still has to be
  * prompted, and that submitting a reason persists and flips that answer.
  */
class UserResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testUid = 8000 + scala.util.Random.nextInt(1000)
  private val unknownUid = testUid + 1

  private var userDao: UserDao = _
  private var resource: UserResource = _

  override protected def beforeAll(): Unit = initializeDBAndReplaceDSLContext()

  override protected def afterAll(): Unit = {
    cleanup()
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    resource = new UserResource()
    cleanup()
    userDao.insert(seedUser())
  }

  override protected def afterEach(): Unit = cleanup()

  private def cleanup(): Unit =
    getDSLContext.deleteFrom(USER).where(USER.UID.in(testUid, unknownUid)).execute()

  /** A freshly registered user: joining reason not yet supplied. */
  private def seedUser(): User = {
    val user = new User
    user.setUid(testUid)
    user.setName(s"joining_reason_user_$testUid")
    user.setEmail(s"joining_reason_$testUid@example.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  "isJoiningReasonRequired" should "be true while the user has not supplied one" in {
    resource.isJoiningReasonRequired(testUid) shouldBe true
  }

  it should "be false once a joining reason is stored" in {
    val user = userDao.fetchOneByUid(testUid)
    user.setJoiningReason("research")
    userDao.update(user)

    resource.isJoiningReasonRequired(testUid) shouldBe false
  }

  it should "report 404 for a user that does not exist" in {
    val ex = intercept[WebApplicationException](resource.isJoiningReasonRequired(unknownUid))
    ex.getResponse.getStatus shouldBe Response.Status.NOT_FOUND.getStatusCode
  }

  "updateJoiningReason" should "persist the affiliation and reason, and stop the prompt" in {
    resource.updateJoiningReason(RegistrationUpdateRequest(testUid, "UC Irvine", "research"))

    val stored = userDao.fetchOneByUid(testUid)
    stored.getAffiliation shouldBe "UC Irvine"
    stored.getJoiningReason shouldBe "research"
    resource.isJoiningReasonRequired(testUid) shouldBe false
  }

  it should "trim the submitted values" in {
    resource.updateJoiningReason(
      RegistrationUpdateRequest(testUid, "  UC Irvine  ", "  research  ")
    )

    val stored = userDao.fetchOneByUid(testUid)
    stored.getAffiliation shouldBe "UC Irvine"
    stored.getJoiningReason shouldBe "research"
  }

  it should "default a null affiliation to an empty string" in {
    resource.updateJoiningReason(RegistrationUpdateRequest(testUid, null, "research"))

    userDao.fetchOneByUid(testUid).getAffiliation shouldBe ""
  }

  it should "reject a blank reason with 400 and leave the user untouched" in {
    val ex = intercept[WebApplicationException](
      resource.updateJoiningReason(RegistrationUpdateRequest(testUid, "UC Irvine", "   "))
    )

    ex.getResponse.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
    userDao.fetchOneByUid(testUid).getJoiningReason shouldBe null
  }

  it should "reject a null reason with 400" in {
    val ex = intercept[WebApplicationException](
      resource.updateJoiningReason(RegistrationUpdateRequest(testUid, "UC Irvine", null))
    )

    ex.getResponse.getStatus shouldBe Response.Status.BAD_REQUEST.getStatusCode
  }
}
