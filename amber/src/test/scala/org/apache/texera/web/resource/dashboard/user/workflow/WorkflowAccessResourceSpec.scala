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

package org.apache.texera.web.resource.dashboard.user.workflow

import org.apache.texera.auth.SessionUser
import org.apache.texera.auth.util.WorkflowAccess
import org.apache.texera.dao.WorkflowAccessDatabaseSpec
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{User, WorkflowUserAccess}
import org.apache.texera.dao.jooq.generated.tables.records.WorkflowUserAccessRecord

import javax.ws.rs.{BadRequestException, ForbiddenException}
import scala.jdk.CollectionConverters._

class WorkflowAccessResourceSpec extends WorkflowAccessDatabaseSpec(withTargetUser = true) {

  private var owner: User = _
  private var userWithWrite: User = _
  private var userWithRead: User = _
  private var targetUser: User = _

  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowAccessResource: WorkflowAccessResource = _

  override protected def beforeEach(): Unit = {
    // Initialize DAOs
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    super.beforeEach()
    workflowAccessResource = new WorkflowAccessResource()

    owner = newUser(ownerUid, "owner", "owner@test.com")
    userWithWrite = newUser(userWithWriteUid, "user_with_write", "write@test.com")
    userWithRead = newUser(userWithReadUid, "user_with_read", "read@test.com")
    targetUser = newUser(targetUserUid, "target_user", "target@test.com")
    val testWorkflow = newWorkflow()

    // Clean up before each test
    cleanupTestData()

    // Insert test data
    userDao.insert(owner)
    userDao.insert(userWithWrite)
    userDao.insert(userWithRead)
    userDao.insert(targetUser)
    workflowDao.insert(testWorkflow)

    seedOwnerAndGrantees()
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(targetUserUid, testWorkflowWid, PrivilegeEnum.WRITE)
    )
  }

  private def grant(email: String, privilege: String, user: SessionUser): Unit =
    workflowAccessResource.grantAccess(testWorkflowWid, email, privilege, user)

  private def accessRow(uid: Int): WorkflowUserAccessRecord =
    getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWorkflowWid).and(WORKFLOW_USER_ACCESS.UID.eq(uid)))
      .fetchOne()

  "WorkflowAccessResource.revokeAccess" should "successfully revoke access when user has WRITE permission" in {
    val sessionUser = new SessionUser(userWithWrite)

    // Verify target user has access before revocation
    val accessBefore = accessRow(targetUserUid)
    assert(accessBefore != null, "Target user should have access before revocation")

    // Revoke access
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify access has been revoked
    val accessAfter = accessRow(targetUserUid)

    assert(accessAfter == null, "Target user's access should be revoked")
  }

  it should "successfully allow user to revoke their own access" in {
    val sessionUser = new SessionUser(userWithRead)

    // Verify user has access before revocation
    val accessBefore = accessRow(userWithReadUid)
    assert(accessBefore != null, "User should have access before revocation")

    // User revokes their own access
    workflowAccessResource.revokeAccess(testWorkflowWid, "read@test.com", sessionUser)

    // Verify access has been revoked
    val accessAfter = accessRow(userWithReadUid)

    assert(accessAfter == null, "User's own access should be revoked")
  }

  it should "throw ForbiddenException when user without WRITE permission tries to revoke others' access" in {
    val sessionUser = new SessionUser(userWithRead)

    assertThrows[ForbiddenException] {
      workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)
    }

    // Verify target user's access is still intact
    val access = accessRow(targetUserUid)

    assert(access != null, "Target user's access should remain intact")
  }

  it should "throw ForbiddenException when trying to revoke owner's access" in {
    val sessionUser = new SessionUser(userWithWrite)

    val exception = intercept[ForbiddenException] {
      workflowAccessResource.revokeAccess(testWorkflowWid, "owner@test.com", sessionUser)
    }

    assert(
      exception.getMessage.contains("owner cannot revoke their own access"),
      "Exception message should indicate owner cannot revoke their own access"
    )
  }

  it should "throw ForbiddenException when owner tries to revoke their own access" in {
    val sessionUser = new SessionUser(owner)

    val exception = intercept[ForbiddenException] {
      workflowAccessResource.revokeAccess(testWorkflowWid, "owner@test.com", sessionUser)
    }

    assert(
      exception.getMessage.contains("owner cannot revoke their own access"),
      "Exception message should indicate owner cannot revoke their own access"
    )
  }

  it should "throw BadRequestException when email does not exist" in {
    val sessionUser = new SessionUser(userWithWrite)

    assertThrows[BadRequestException] {
      workflowAccessResource.revokeAccess(
        testWorkflowWid,
        "nonexistent@test.com",
        sessionUser
      )
    }
  }

  it should "not affect other users' access when revoking one user's access" in {
    val sessionUser = new SessionUser(userWithWrite)

    // Verify both users have access before revocation
    val readAccessBefore = accessRow(userWithReadUid)
    assert(readAccessBefore != null, "Read user should have access before revocation")

    val targetAccessBefore = accessRow(targetUserUid)
    assert(targetAccessBefore != null, "Target user should have access before revocation")

    // Revoke only target user's access
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify read user's access is still intact
    val readAccessAfter = accessRow(userWithReadUid)
    assert(readAccessAfter != null, "Read user's access should remain intact")

    // Verify target user's access has been revoked
    val targetAccessAfter = accessRow(targetUserUid)
    assert(targetAccessAfter == null, "Target user's access should be revoked")
  }

  it should "handle revoking access for a user who already has no access gracefully" in {
    val sessionUser = new SessionUser(userWithWrite)

    // First revocation
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify access has been revoked
    val accessAfterFirst = accessRow(targetUserUid)
    assert(accessAfterFirst == null, "Target user's access should be revoked")

    // Second revocation attempt (should not throw an error, just do nothing)
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify access is still revoked
    val accessAfterSecond = accessRow(targetUserUid)
    assert(accessAfterSecond == null, "Target user's access should still be revoked")
  }

  // Always inserts at newGranteeUid (the only extra uid cleanupTestData removes),
  // so callers can't leak a user row across tests.
  private def seedUserWithoutAccess(name: String, email: String): User = {
    val user = newUser(newGranteeUid, name, email)
    userDao.insert(user)
    user
  }

  "WorkflowAccessResource.grantAccess" should "reject granting to a placeholder account" in {
    val placeholder = seedUserWithoutAccess("wf_placeholder", "wf-placeholder@test.com")
    // a placeholder has no credential at all; under auth_provider that means no row, which
    // seedUserWithoutAccess already leaves it with
    placeholder.setIsPlaceholder(true)
    userDao.update(placeholder)

    assertThrows[BadRequestException] {
      grant("wf-placeholder@test.com", "READ", new SessionUser(userWithWrite))
    }
  }

  "WorkflowAccessResource.getOwner" should "return the workflow owner's email" in {
    assert(workflowAccessResource.getOwner(testWorkflowWid) == "owner@test.com")
  }

  "WorkflowAccessResource.getAccessList" should "list the granted users with their privilege, excluding the owner" in {
    val byEmail =
      workflowAccessResource
        .getAccessList(testWorkflowWid)
        .asScala
        .map(e => e.email -> e.privilege.getLiteral)
        .toMap

    assert(byEmail.size == 3)
    assert(byEmail("write@test.com") == "WRITE")
    assert(byEmail("read@test.com") == "READ")
    assert(byEmail("target@test.com") == "WRITE")
    assert(!byEmail.contains("owner@test.com"))
  }

  it should "return an empty list when the workflow has no shared access" in {
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWorkflowWid))
      .execute()

    assert(workflowAccessResource.getAccessList(testWorkflowWid).isEmpty)
  }

  "WorkflowAccessResource.grantAccess" should "grant READ access to a new user, reflected in getPrivilege" in {
    seedUserWithoutAccess("new_grantee", "newgrantee@test.com")

    grant("newgrantee@test.com", "READ", new SessionUser(userWithWrite))

    assert(
      WorkflowAccess.getPrivilege(testWorkflowWid, newGranteeUid) == PrivilegeEnum.READ
    )
  }

  it should "change an existing grantee's privilege on re-grant (READ -> WRITE)" in {
    grant("read@test.com", "WRITE", new SessionUser(userWithWrite))

    assert(
      WorkflowAccess.getPrivilege(testWorkflowWid, userWithReadUid) == PrivilegeEnum.WRITE
    )
  }

  it should "throw ForbiddenException when the caller lacks WRITE access" in {
    assertThrows[ForbiddenException] {
      grant("target@test.com", "READ", new SessionUser(userWithRead))
    }
  }

  it should "throw ForbiddenException when granting access to the owner" in {
    assertThrows[ForbiddenException] {
      grant("owner@test.com", "READ", new SessionUser(userWithWrite))
    }
  }

  it should "throw BadRequestException when a user without access grants to themselves" in {
    val noAccessUser = seedUserWithoutAccess("no_access", "noaccess@test.com")

    assertThrows[BadRequestException] {
      grant("noaccess@test.com", "READ", new SessionUser(noAccessUser))
    }
  }
}
