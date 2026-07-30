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
import org.apache.texera.dao.jooq.generated.Tables.COMPUTING_UNIT_USER_ACCESS
import org.apache.texera.dao.jooq.generated.enums.{
  PrivilegeEnum,
  UserRoleEnum,
  WorkflowComputingUnitTypeEnum
}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  UserDao,
  WorkflowComputingUnitDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  ComputingUnitUserAccess,
  User,
  WorkflowComputingUnit
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

/**
  * Spec for [[ComputingUnitManagingResource.getComputingUnitInfo]], backed by embedded Postgres
  * (via [[MockTexeraDB]]). Units are `local`, which keeps the status and metrics helpers away
  * from Kubernetes — `local` is always Running with NaN metrics.
  *
  * The owner's avatar is read off `"user".avatar` (formerly `google_avatar`) and normalised so
  * that "no avatar" reaches the frontend as null rather than an empty string; both halves of
  * that are pinned here, since a wrong column would silently null every owner's avatar.
  */
class ComputingUnitManagingResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val ownerUser: User = {
    val user = new User
    user.setName("info_owner")
    user.setEmail("info_owner@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user.setAvatar("OWNER-AVATAR-ID")
    user
  }

  private val blankAvatarUser: User = {
    val user = new User
    user.setName("info_blank_avatar")
    user.setEmail("info_blank_avatar@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user.setAvatar("")
    user
  }

  private val strangerUser: User = {
    val user = new User
    user.setName("info_stranger")
    user.setEmail("info_stranger@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val ownedUnit: WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit
    unit.setName("info-unit")
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit.setUri("")
    unit
  }

  private val blankAvatarUnit: WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit
    unit.setName("info-unit-blank-avatar")
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit.setUri("")
    unit
  }

  private lazy val resource = new ComputingUnitManagingResource()

  private lazy val ownerSession = new SessionUser(ownerUser)
  private lazy val strangerSession = new SessionUser(strangerUser)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(blankAvatarUser)
    userDao.insert(strangerUser)

    val wcDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    ownedUnit.setUid(ownerUser.getUid)
    wcDao.insert(ownedUnit)
    blankAvatarUnit.setUid(blankAvatarUser.getUid)
    wcDao.insert(blankAvatarUnit)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // every test starts with no explicit grants
    getDSLContext.deleteFrom(COMPUTING_UNIT_USER_ACCESS).execute()
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  private def grantDirectly(cuid: Integer, uid: Integer, privilege: PrivilegeEnum): Unit = {
    val access = new ComputingUnitUserAccess
    access.setCuid(cuid)
    access.setUid(uid)
    access.setPrivilege(privilege)
    new ComputingUnitUserAccessDao(getDSLContext.configuration()).insert(access)
  }

  behavior of "getComputingUnitInfo"

  it should "report the owner's name and avatar" in {
    val info = resource.getComputingUnitInfo(ownedUnit.getCuid, ownerSession)

    info.ownerName shouldBe "info_owner"
    info.ownerGoogleAvatar shouldBe "OWNER-AVATAR-ID"
    info.computingUnit.getCuid shouldBe ownedUnit.getCuid
  }

  // Empty and absent are the same thing to the frontend, so an empty column becomes null
  // rather than being passed through as "".
  it should "report no avatar as null rather than an empty string" in {
    val info = resource.getComputingUnitInfo(blankAvatarUnit.getCuid, ownerSession)

    info.ownerGoogleAvatar shouldBe null
    info.ownerName shouldBe "info_blank_avatar"
  }

  it should "grant the owner write access without an explicit access row" in {
    val info = resource.getComputingUnitInfo(ownedUnit.getCuid, ownerSession)

    info.isOwner shouldBe true
    info.accessPrivilege shouldBe PrivilegeEnum.WRITE
    info.status shouldBe "Running"
  }

  it should "report no privilege for a stranger with no access row" in {
    val info = resource.getComputingUnitInfo(ownedUnit.getCuid, strangerSession)

    info.isOwner shouldBe false
    info.accessPrivilege shouldBe PrivilegeEnum.NONE
  }

  it should "report the granted privilege for a non-owner that has one" in {
    grantDirectly(ownedUnit.getCuid, strangerUser.getUid, PrivilegeEnum.READ)

    val info = resource.getComputingUnitInfo(ownedUnit.getCuid, strangerSession)

    info.isOwner shouldBe false
    info.accessPrivilege shouldBe PrivilegeEnum.READ
  }

  it should "reject a cuid that does not exist" in {
    a[NotFoundException] should be thrownBy
      resource.getComputingUnitInfo(Integer.valueOf(999999), ownerSession)
  }
}
