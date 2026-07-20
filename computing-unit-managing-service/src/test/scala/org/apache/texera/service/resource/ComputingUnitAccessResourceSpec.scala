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

import jakarta.ws.rs.{BadRequestException, ForbiddenException, NotFoundException}
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.config.ComputingUnitConfig
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.COMPUTING_UNIT_USER_ACCESS
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

/**
  * Spec for [[ComputingUnitAccessResource]]'s share/revoke endpoints, backed by an
  * embedded Postgres (via [[MockTexeraDB]]).
  *
  * The suite runs with COMPUTING_UNIT_SHARING_ENABLED=true (set in build.sbt), which
  * `ensureSharingIsEnabled()` requires; this is asserted below so a missing env var
  * fails loudly instead of silently short-circuiting every case with a ForbiddenException.
  *
  * The key regression these tests guard: granting/revoking to an unknown email must
  * surface a clear IllegalArgumentException, not a NullPointerException (500), because
  * `userDao.fetchOneByEmail` returns null for an address with no account.
  */
class ComputingUnitAccessResourceSpec
    extends AnyFlatSpec
    with Matchers
    with MockTexeraDB
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private val ownerUser: User = {
    val user = new User
    user.setName("cu_owner")
    user.setPassword("123")
    user.setEmail("cu_owner@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val granteeUser: User = {
    val user = new User
    user.setName("cu_grantee")
    user.setPassword("123")
    user.setEmail("cu_grantee@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val strangerUser: User = {
    val user = new User
    user.setName("cu_stranger")
    user.setPassword("123")
    user.setEmail("cu_stranger@test.com")
    user.setRole(UserRoleEnum.REGULAR)
    user
  }

  private val ownedUnit: WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit
    unit.setName("owned-unit")
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit.setUri("")
    unit
  }

  private val nonExistentEmail: String = "nobody@test.com"
  private val nonExistentCuid: Integer = 999999

  lazy val accessResource = new ComputingUnitAccessResource()

  lazy val ownerSession = new SessionUser(ownerUser)
  lazy val strangerSession = new SessionUser(strangerUser)

  private def cuid: Integer = ownedUnit.getCuid

  private def accessEmails(cuid: Integer): List[String] =
    accessResource.getComputingUnitAccessList(ownerSession, cuid).map(_.email)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()

    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(ownerUser)
    userDao.insert(granteeUser)
    userDao.insert(strangerUser)

    ownedUnit.setUid(ownerUser.getUid)
    new WorkflowComputingUnitDao(getDSLContext.configuration()).insert(ownedUnit)
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    // every test starts with no explicit grants on the unit
    getDSLContext.deleteFrom(COMPUTING_UNIT_USER_ACCESS).execute()
  }

  override protected def afterAll(): Unit = {
    try shutdownDB()
    finally super.afterAll()
  }

  "the test environment" should "have computing-unit sharing enabled" in {
    // guards against a silent misconfiguration that would make every case below
    // short-circuit in ensureSharingIsEnabled rather than test the intended paths
    ComputingUnitConfig.sharingComputingUnitEnabled shouldBe true
  }

  // ===========================================================================
  // grantAccess
  // ===========================================================================

  "grantAccess" should "add a grantee that appears in the access list" in {
    accessResource.grantAccess(ownerSession, cuid, granteeUser.getEmail, PrivilegeEnum.READ)

    val entries = accessResource.getComputingUnitAccessList(ownerSession, cuid)
    entries should have size 1
    entries.head.email shouldEqual granteeUser.getEmail
    entries.head.privilege shouldEqual PrivilegeEnum.READ
  }

  it should "reject an unknown email with a 400 instead of crashing" in {
    val ex = intercept[BadRequestException] {
      accessResource.grantAccess(ownerSession, cuid, nonExistentEmail, PrivilegeEnum.READ)
    }
    ex.getResponse.getStatus shouldEqual 400
    ex.getMessage should include("User with the given email does not exist")
    accessEmails(cuid) shouldBe empty
  }

  it should "update the privilege in place when re-granting with a different privilege" in {
    accessResource.grantAccess(ownerSession, cuid, granteeUser.getEmail, PrivilegeEnum.READ)
    accessResource.grantAccess(ownerSession, cuid, granteeUser.getEmail, PrivilegeEnum.WRITE)

    val entries = accessResource.getComputingUnitAccessList(ownerSession, cuid)
    entries should have size 1
    entries.head.email shouldEqual granteeUser.getEmail
    entries.head.privilege shouldEqual PrivilegeEnum.WRITE
  }

  it should "reject a caller without write access with a 403" in {
    val ex = intercept[ForbiddenException] {
      accessResource.grantAccess(strangerSession, cuid, granteeUser.getEmail, PrivilegeEnum.READ)
    }
    ex.getResponse.getStatus shouldEqual 403
    ex.getMessage should include("does not have permission to grant access")
  }

  // ===========================================================================
  // revokeAccess
  // ===========================================================================

  "revokeAccess" should "remove the grantee from the access list" in {
    accessResource.grantAccess(ownerSession, cuid, granteeUser.getEmail, PrivilegeEnum.READ)
    accessEmails(cuid) should contain(granteeUser.getEmail)

    accessResource.revokeAccess(ownerSession, cuid, granteeUser.getEmail)

    accessEmails(cuid) shouldBe empty
  }

  it should "reject an unknown email with a 400 instead of crashing" in {
    val ex = intercept[BadRequestException] {
      accessResource.revokeAccess(ownerSession, cuid, nonExistentEmail)
    }
    ex.getResponse.getStatus shouldEqual 400
    ex.getMessage should include("User with the given email does not exist")
  }

  it should "reject a caller without write access with a 403" in {
    accessResource.grantAccess(ownerSession, cuid, granteeUser.getEmail, PrivilegeEnum.READ)

    val ex = intercept[ForbiddenException] {
      accessResource.revokeAccess(strangerSession, cuid, granteeUser.getEmail)
    }
    ex.getResponse.getStatus shouldEqual 403
    ex.getMessage should include("does not have permission to revoke access")
  }

  // ===========================================================================
  // getOwner
  // ===========================================================================

  "getOwner" should "reject a nonexistent computing unit with a 404 instead of crashing" in {
    val ex = intercept[NotFoundException] {
      accessResource.getOwner(ownerSession, nonExistentCuid)
    }
    ex.getResponse.getStatus shouldEqual 404
    ex.getMessage should include(s"Computing unit with cuid=$nonExistentCuid does not exist")
  }
}
