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

import jakarta.annotation.security.RolesAllowed
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW_COMPUTING_UNIT
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowComputingUnitDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowComputingUnit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp

class AdminComputingUnitResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()
  }

  // MockTexeraDB does not truncate between tests, and this endpoint lists every unit in the
  // database, so each case must start from a known-empty table — otherwise the exact-match
  // assertions below depend on the order ScalaTest happens to run the cases in. Safe to wipe
  // unqualified: MockTexeraDB provisions a fresh database per suite, so these are the only rows.
  override protected def beforeEach(): Unit = {
    super.beforeEach()
    getDSLContext.deleteFrom(WORKFLOW_COMPUTING_UNIT).execute()
  }

  override protected def afterAll(): Unit =
    try shutdownDB()
    finally super.afterAll()

  private def makeUser(uid: Int, name: String): User = {
    val u = new User()
    u.setUid(uid)
    u.setName(name)
    u.setEmail(s"user$uid@example.com")
    u.setRole(UserRoleEnum.ADMIN)
    // Credentials live in auth_provider now, and this spec exercises the listing rather than
    // login, so the user needs none. The avatar column is provider-neutral; the DTO field it
    // feeds is still named ownerGoogleAvatar.
    u.setAvatar(s"avatar-$uid")
    u
  }

  // Only local units are used here: a kubernetes unit would make the listing reach for the real
  // KubernetesClient singleton, and its stub seam is private[util].
  private def localUnit(cuid: Int, uid: Int, name: String): WorkflowComputingUnit = {
    val u = new WorkflowComputingUnit()
    u.setCuid(cuid)
    u.setUid(uid)
    u.setName(name)
    u.setType(WorkflowComputingUnitTypeEnum.local)
    u
  }

  // The class-level @RolesAllowed(ADMIN) is what makes Jersey reject non-ADMIN callers; that this
  // spec calls the method directly is exactly why the annotation has to be asserted here. The
  // enforcement half (Jersey actually honouring it) is pinned by
  // ComputingUnitManagingServiceRunSpec, which verifies RolesAllowedDynamicFeature is registered.
  "AdminComputingUnitResource" should "declare @RolesAllowed(ADMIN) at the class level" in {
    val annotation = classOf[AdminComputingUnitResource].getAnnotation(classOf[RolesAllowed])
    annotation should not be null
    annotation.value.toSeq shouldBe Seq("ADMIN")
  }

  "listAllComputingUnits" should "return every non-terminated unit across users, marked WRITE" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val unitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    val admin = makeUser(700, "admin")
    userDao.insert(admin)
    userDao.insert(makeUser(701, "other"))
    unitDao.insert(localUnit(cuid = 700, uid = 700, name = "admin-cu"))
    unitDao.insert(localUnit(cuid = 701, uid = 701, name = "other-cu"))
    // A terminated unit must be excluded by the SQL filter.
    val terminated = localUnit(cuid = 702, uid = 701, name = "terminated-cu")
    terminated.setTerminateTime(new Timestamp(0L))
    unitDao.insert(terminated)

    val result = new AdminComputingUnitResource().listAllComputingUnits(new SessionUser(admin))

    result.map(_.computingUnit.getCuid.intValue()) should contain theSameElementsAs Seq(700, 701)
    all(result.map(_.accessPrivilege)) shouldBe PrivilegeEnum.WRITE
    all(result.map(_.status)) shouldBe "Running" // local units
    val byCuid = result.map(r => r.computingUnit.getCuid.intValue() -> r).toMap
    // isOwner tracks the caller; owner name/avatar are joined from the user table.
    byCuid(700).isOwner shouldBe true
    byCuid(700).ownerName shouldBe "admin"
    byCuid(700).ownerGoogleAvatar shouldBe "avatar-700"
    byCuid(701).isOwner shouldBe false
    byCuid(701).ownerName shouldBe "other"
  }

  it should "return an empty list when every unit is terminated" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val unitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    val admin = makeUser(710, "lonely-admin")
    userDao.insert(admin)
    val terminated = localUnit(cuid = 710, uid = 710, name = "only-terminated-cu")
    terminated.setTerminateTime(new Timestamp(0L))
    unitDao.insert(terminated)

    new AdminComputingUnitResource().listAllComputingUnits(new SessionUser(admin)) shouldBe empty
  }

  it should "return an empty list when no unit exists at all" in {
    val admin = makeUser(720, "empty-admin")
    new UserDao(getDSLContext.configuration()).insert(admin)

    new AdminComputingUnitResource().listAllComputingUnits(new SessionUser(admin)) shouldBe empty
  }

  // A caller whose own user row was deleted (or who is an admin from another realm) still sees
  // every unit, but owns none of them — isOwner must not accidentally default to true.
  it should "mark no row as owned when the caller owns none of the units" in {
    val userDao = new UserDao(getDSLContext.configuration())
    val unitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    val admin = makeUser(730, "outsider-admin")
    userDao.insert(admin)
    userDao.insert(makeUser(731, "owner"))
    unitDao.insert(localUnit(cuid = 731, uid = 731, name = "someone-elses-cu"))

    val result = new AdminComputingUnitResource().listAllComputingUnits(new SessionUser(admin))

    result should have size 1
    result.head.isOwner shouldBe false
    result.head.ownerName shouldBe "owner"
  }
}
