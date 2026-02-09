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
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowOfUserDao,
  WorkflowUserAccessDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.sql.Timestamp
import javax.ws.rs.{BadRequestException, ForbiddenException}

class WorkflowAccessResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val ownerUid = 1000 + scala.util.Random.nextInt(1000)
  private val userWithWriteUid = 2000 + scala.util.Random.nextInt(1000)
  private val userWithReadUid = 3000 + scala.util.Random.nextInt(1000)
  private val targetUserUid = 4000 + scala.util.Random.nextInt(1000)
  private val testWorkflowWid = 5000 + scala.util.Random.nextInt(1000)

  private var owner: User = _
  private var userWithWrite: User = _
  private var userWithRead: User = _
  private var targetUser: User = _
  private var testWorkflow: Workflow = _

  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _
  private var workflowAccessResource: WorkflowAccessResource = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    // Initialize DAOs
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
    workflowAccessResource = new WorkflowAccessResource()

    // Create test users
    owner = new User
    owner.setUid(ownerUid)
    owner.setName("owner")
    owner.setEmail("owner@test.com")
    owner.setPassword("password")

    userWithWrite = new User
    userWithWrite.setUid(userWithWriteUid)
    userWithWrite.setName("user_with_write")
    userWithWrite.setEmail("write@test.com")
    userWithWrite.setPassword("password")

    userWithRead = new User
    userWithRead.setUid(userWithReadUid)
    userWithRead.setName("user_with_read")
    userWithRead.setEmail("read@test.com")
    userWithRead.setPassword("password")

    targetUser = new User
    targetUser.setUid(targetUserUid)
    targetUser.setName("target_user")
    targetUser.setEmail("target@test.com")
    targetUser.setPassword("password")

    // Create test workflow
    testWorkflow = new Workflow
    testWorkflow.setWid(testWorkflowWid)
    testWorkflow.setName("test_workflow")
    testWorkflow.setContent("{}")
    testWorkflow.setDescription("test description")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))

    // Clean up before each test
    cleanupTestData()

    // Insert test data
    userDao.insert(owner)
    userDao.insert(userWithWrite)
    userDao.insert(userWithRead)
    userDao.insert(targetUser)
    workflowDao.insert(testWorkflow)

    // Set up workflow ownership
    val workflowOfUser = new WorkflowOfUser
    workflowOfUser.setUid(ownerUid)
    workflowOfUser.setWid(testWorkflowWid)
    workflowOfUserDao.insert(workflowOfUser)

    // Grant write access to userWithWrite
    val writeAccess = new WorkflowUserAccess
    writeAccess.setUid(userWithWriteUid)
    writeAccess.setWid(testWorkflowWid)
    writeAccess.setPrivilege(PrivilegeEnum.WRITE)
    workflowUserAccessDao.insert(writeAccess)

    // Grant read access to userWithRead
    val readAccess = new WorkflowUserAccess
    readAccess.setUid(userWithReadUid)
    readAccess.setWid(testWorkflowWid)
    readAccess.setPrivilege(PrivilegeEnum.READ)
    workflowUserAccessDao.insert(readAccess)

    // Grant write access to targetUser
    val targetAccess = new WorkflowUserAccess
    targetAccess.setUid(targetUserUid)
    targetAccess.setWid(testWorkflowWid)
    targetAccess.setPrivilege(PrivilegeEnum.WRITE)
    workflowUserAccessDao.insert(targetAccess)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  private def cleanupTestData(): Unit = {
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW_OF_USER)
      .where(WORKFLOW_OF_USER.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW)
      .where(WORKFLOW.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(USER)
      .where(
        USER.UID.in(ownerUid, userWithWriteUid, userWithReadUid, targetUserUid)
      )
      .execute()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  "WorkflowAccessResource.revokeAccess" should "successfully revoke access when user has WRITE permission" in {
    val sessionUser = new SessionUser(userWithWrite)

    // Verify target user has access before revocation
    val accessBefore = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(targetUserUid)
          )
      )
      .fetchOne()
    assert(accessBefore != null, "Target user should have access before revocation")

    // Revoke access
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify access has been revoked
    val accessAfter = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(targetUserUid)
          )
      )
      .fetchOne()

    assert(accessAfter == null, "Target user's access should be revoked")
  }

  it should "successfully allow user to revoke their own access" in {
    val sessionUser = new SessionUser(userWithRead)

    // Verify user has access before revocation
    val accessBefore = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(userWithReadUid)
          )
      )
      .fetchOne()
    assert(accessBefore != null, "User should have access before revocation")

    // User revokes their own access
    workflowAccessResource.revokeAccess(testWorkflowWid, "read@test.com", sessionUser)

    // Verify access has been revoked
    val accessAfter = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(userWithReadUid)
          )
      )
      .fetchOne()

    assert(accessAfter == null, "User's own access should be revoked")
  }

  it should "throw ForbiddenException when user without WRITE permission tries to revoke others' access" in {
    val sessionUser = new SessionUser(userWithRead)

    assertThrows[ForbiddenException] {
      workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)
    }

    // Verify target user's access is still intact
    val access = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(targetUserUid)
          )
      )
      .fetchOne()

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
    val readAccessBefore = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(userWithReadUid)
          )
      )
      .fetchOne()
    assert(readAccessBefore != null, "Read user should have access before revocation")

    val targetAccessBefore = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(targetUserUid)
          )
      )
      .fetchOne()
    assert(targetAccessBefore != null, "Target user should have access before revocation")

    // Revoke only target user's access
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify read user's access is still intact
    val readAccessAfter = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(userWithReadUid)
          )
      )
      .fetchOne()
    assert(readAccessAfter != null, "Read user's access should remain intact")

    // Verify target user's access has been revoked
    val targetAccessAfter = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(targetUserUid)
          )
      )
      .fetchOne()
    assert(targetAccessAfter == null, "Target user's access should be revoked")
  }

  it should "handle revoking access for a user who already has no access gracefully" in {
    val sessionUser = new SessionUser(userWithWrite)

    // First revocation
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify access has been revoked
    val accessAfterFirst = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(targetUserUid)
          )
      )
      .fetchOne()
    assert(accessAfterFirst == null, "Target user's access should be revoked")

    // Second revocation attempt (should not throw an error, just do nothing)
    workflowAccessResource.revokeAccess(testWorkflowWid, "target@test.com", sessionUser)

    // Verify access is still revoked
    val accessAfterSecond = getDSLContext
      .selectFrom(WORKFLOW_USER_ACCESS)
      .where(
        WORKFLOW_USER_ACCESS.WID
          .eq(testWorkflowWid)
          .and(
            WORKFLOW_USER_ACCESS.UID.eq(targetUserUid)
          )
      )
      .fetchOne()
    assert(accessAfterSecond == null, "Target user's access should still be revoked")
  }
}
