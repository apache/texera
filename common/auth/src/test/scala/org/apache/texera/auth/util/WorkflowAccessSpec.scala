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

package org.apache.texera.auth.util

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
import scala.jdk.CollectionConverters._

class WorkflowAccessSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val ownerUid = 1000 + scala.util.Random.nextInt(1000)
  private val userWithWriteUid = 2000 + scala.util.Random.nextInt(1000)
  private val userWithReadUid = 3000 + scala.util.Random.nextInt(1000)
  private val testWorkflowWid = 5000 + scala.util.Random.nextInt(1000)
  private val newGranteeUid = 6000 + scala.util.Random.nextInt(1000)

  private var workflowOfUserDao: WorkflowOfUserDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    val userDao = new UserDao(getDSLContext.configuration())
    val workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())

    cleanupTestData()

    // The workflow's owner, a WRITE grantee and a READ grantee.
    Seq(
      (ownerUid, "owner", "owner@test.com"),
      (userWithWriteUid, "user_with_write", "write@test.com"),
      (userWithReadUid, "user_with_read", "read@test.com")
    ).foreach {
      case (uid, name, email) =>
        val user = new User
        user.setUid(uid)
        user.setName(name)
        user.setEmail(email)
        userDao.insert(user)
    }

    val testWorkflow = new Workflow
    testWorkflow.setWid(testWorkflowWid)
    testWorkflow.setName("test_workflow")
    testWorkflow.setContent("{}")
    testWorkflow.setDescription("test description")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(testWorkflow)

    workflowOfUserDao.insert(new WorkflowOfUser(ownerUid, testWorkflowWid))
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(userWithWriteUid, testWorkflowWid, PrivilegeEnum.WRITE)
    )
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(userWithReadUid, testWorkflowWid, PrivilegeEnum.READ)
    )
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
      .where(USER.UID.in(ownerUid, userWithWriteUid, userWithReadUid, newGranteeUid))
      .execute()
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  "WorkflowAccess.getPrivilege" should "report the grantees' privileges and NONE for anyone else" in {
    assert(
      WorkflowAccess.getPrivilege(testWorkflowWid, userWithWriteUid) == PrivilegeEnum.WRITE
    )
    assert(
      WorkflowAccess.getPrivilege(testWorkflowWid, userWithReadUid) == PrivilegeEnum.READ
    )
    assert(
      WorkflowAccess.getPrivilege(testWorkflowWid, newGranteeUid) == PrivilegeEnum.NONE
    )
  }

  it should "give the owner WRITE without an access row" in {
    assert(WorkflowAccess.getPrivilege(testWorkflowWid, ownerUid) == PrivilegeEnum.WRITE)
    assert(WorkflowAccess.hasWriteAccess(testWorkflowWid, ownerUid))
    assert(WorkflowAccess.hasGrantedAccess(testWorkflowWid, ownerUid))
  }

  it should "give the owner WRITE even when their access row says READ" in {
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(ownerUid, testWorkflowWid, PrivilegeEnum.READ)
    )
    assert(WorkflowAccess.getPrivilege(testWorkflowWid, ownerUid) == PrivilegeEnum.WRITE)
  }

  it should "not fail when the workflow has several owner rows" in {
    workflowOfUserDao.insert(new WorkflowOfUser(userWithReadUid, testWorkflowWid))
    assert(WorkflowAccess.getPrivilege(testWorkflowWid, ownerUid) == PrivilegeEnum.WRITE)
    assert(
      WorkflowAccess.getPrivilege(testWorkflowWid, userWithReadUid) == PrivilegeEnum.WRITE
    )
    assert(
      WorkflowAccess.getPrivilege(testWorkflowWid, userWithWriteUid) == PrivilegeEnum.WRITE
    )
  }

  "WorkflowAccess.accessibleWids" should "list owned and granted workflows, once each" in {
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(ownerUid, testWorkflowWid, PrivilegeEnum.WRITE)
    )
    def wids(uid: Integer) =
      getDSLContext
        .fetch(WorkflowAccess.accessibleWids(uid))
        .getValues(0, classOf[Integer])
        .asScala
        .toList
    assert(wids(ownerUid) == List(testWorkflowWid))
    assert(wids(userWithReadUid) == List(testWorkflowWid))
    assert(wids(newGranteeUid).isEmpty)
  }

  "WorkflowAccess.hasReadAccess" should "allow the owner and grantees, and anyone once the workflow is public" in {
    assert(WorkflowAccess.hasReadAccess(testWorkflowWid, ownerUid))
    assert(WorkflowAccess.hasReadAccess(testWorkflowWid, userWithReadUid))
    assert(!WorkflowAccess.isPublic(testWorkflowWid))
    assert(!WorkflowAccess.hasReadAccess(testWorkflowWid, newGranteeUid))

    getDSLContext
      .update(WORKFLOW)
      .set(WORKFLOW.IS_PUBLIC, java.lang.Boolean.TRUE)
      .where(WORKFLOW.WID.eq(testWorkflowWid))
      .execute()
    assert(WorkflowAccess.isPublic(testWorkflowWid))
    assert(WorkflowAccess.hasReadAccess(testWorkflowWid, newGranteeUid))
  }

  "WorkflowAccess.isOwner" should "hold for the owner only" in {
    assert(WorkflowAccess.isOwner(testWorkflowWid, ownerUid))
    assert(!WorkflowAccess.isOwner(testWorkflowWid, userWithWriteUid))
  }
}
