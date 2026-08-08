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

package org.apache.texera.service.resource

import jakarta.ws.rs.NotFoundException
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}
import org.apache.texera.service.resource.ComputingUnitManagingResource.WorkflowComputingUnitMetrics
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// Drives the per-user computing-unit endpoints against the embedded database using
// local units (so no Kubernetes calls are made).
class ComputingUnitManagingResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val uid = 800
  private lazy val user: SessionUser = {
    val u = new User()
    u.setUid(uid)
    u.setName("owner")
    u.setEmail("owner@example.com")
    u.setRole(UserRoleEnum.REGULAR)
    u.setPassword("password")
    u.setGoogleAvatar("owner-avatar")
    new SessionUser(u)
  }

  // Fixtures for the terminate tests. The units acted on are owned by "victim" (uid 901), not by
  // `user` (uid 800), so they never show up in listComputingUnits(user)'s exact-set assertion.
  private def makeUser(id: Int, name: String, role: UserRoleEnum): User = {
    val u = new User()
    u.setUid(id)
    u.setName(name)
    u.setEmail(s"$name@example.com")
    u.setRole(role)
    u.setPassword("password")
    u
  }
  private lazy val adminUser: SessionUser =
    new SessionUser(makeUser(900, "admin", UserRoleEnum.ADMIN))
  private lazy val strangerUser: SessionUser =
    new SessionUser(makeUser(902, "stranger", UserRoleEnum.REGULAR))

  private def localUnit(cuid: Int, name: String): WorkflowComputingUnit =
    localUnitOwnedBy(cuid, uid, name)

  private def localUnitOwnedBy(cuid: Int, ownerUid: Int, name: String): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit()
    unit.setCuid(cuid)
    unit.setUid(ownerUid)
    unit.setName(name)
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit
  }

  private def insertLocalUnit(cuid: Int, ownerUid: Int, name: String): Unit =
    new WorkflowComputingUnitDao(getDSLContext.configuration())
      .insert(localUnitOwnedBy(cuid, ownerUid, name))

  private def isTerminated(cuid: Int): Boolean =
    new WorkflowComputingUnitDao(getDSLContext.configuration())
      .fetchOneByCuid(cuid)
      .getTerminateTime != null

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(user.getUser)
    userDao.insert(adminUser.getUser)
    userDao.insert(makeUser(901, "victim", UserRoleEnum.REGULAR))
    userDao.insert(strangerUser.getUser)
    val unitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    unitDao.insert(localUnit(800, "cu-a"))
    unitDao.insert(localUnit(801, "cu-b"))
  }

  override protected def afterAll(): Unit =
    try shutdownDB()
    finally super.afterAll()

  private val resource = new ComputingUnitManagingResource

  "getComputingUnitInfo" should "return the owner's local unit with WRITE access and Running status" in {
    val info = resource.getComputingUnitInfo(800, user)

    info.computingUnit.getCuid shouldBe 800
    info.status shouldBe "Running"
    info.metrics shouldBe WorkflowComputingUnitMetrics("NaN", "NaN")
    info.isOwner shouldBe true
    info.accessPrivilege shouldBe PrivilegeEnum.WRITE
    info.ownerName shouldBe "owner"
  }

  "getComputingUnitMetricsEndpoint" should "return NaN metrics for an owned local unit" in {
    resource.getComputingUnitMetricsEndpoint("800", user) shouldBe
      WorkflowComputingUnitMetrics("NaN", "NaN")
  }

  "listComputingUnits" should "return the caller's owned, non-terminated units" in {
    val result = resource.listComputingUnits(user)

    result.map(_.computingUnit.getCuid.intValue()) should contain theSameElementsAs Seq(800, 801)
    all(result.map(_.isOwner)) shouldBe true
    all(result.map(_.accessPrivilege)) shouldBe PrivilegeEnum.WRITE
    all(result.map(_.status)) shouldBe "Running"
  }

  "terminateComputingUnit" should "let an admin terminate a unit it does not own" in {
    insertLocalUnit(cuid = 910, ownerUid = 901, name = "victim-cu")

    val response = resource.terminateComputingUnit(910, adminUser)

    response.getStatus shouldBe 200
    isTerminated(910) shouldBe true
  }

  it should "reject a non-admin acting on a unit it does not own with 400 and not terminate it" in {
    insertLocalUnit(cuid = 911, ownerUid = 901, name = "victim-cu-2")

    val response = resource.terminateComputingUnit(911, strangerUser)

    response.getStatus shouldBe 400
    isTerminated(911) shouldBe false
  }

  it should "let an owner terminate its own unit" in {
    insertLocalUnit(cuid = 912, ownerUid = 902, name = "stranger-own-cu")

    val response = resource.terminateComputingUnit(912, strangerUser)

    response.getStatus shouldBe 200
    isTerminated(912) shouldBe true
  }

  it should "return 404 when an admin terminates a nonexistent unit" in {
    a[NotFoundException] should be thrownBy resource.terminateComputingUnit(99999, adminUser)
  }
}
