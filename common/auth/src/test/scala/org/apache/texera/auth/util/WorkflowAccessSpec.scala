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

import org.apache.texera.dao.WorkflowAccessDatabaseSpec
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{UserDao, WorkflowDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{WorkflowOfUser, WorkflowUserAccess}

import scala.jdk.CollectionConverters._

class WorkflowAccessSpec extends WorkflowAccessDatabaseSpec(withTargetUser = false) {

  override protected def beforeEach(): Unit = {
    val userDao = new UserDao(getDSLContext.configuration())
    val workflowDao = new WorkflowDao(getDSLContext.configuration())
    super.beforeEach()

    cleanupTestData()

    // The workflow's owner, a WRITE grantee and a READ grantee.
    Seq(
      (ownerUid, "owner", "owner@test.com"),
      (userWithWriteUid, "user_with_write", "write@test.com"),
      (userWithReadUid, "user_with_read", "read@test.com")
    ).foreach {
      case (uid, name, email) => userDao.insert(newUser(uid, name, email))
    }

    workflowDao.insert(newWorkflow())
    seedOwnerAndGrantees()
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
