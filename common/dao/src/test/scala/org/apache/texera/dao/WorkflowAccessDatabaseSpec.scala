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

package org.apache.texera.dao

import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{WorkflowOfUserDao, WorkflowUserAccessDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowOfUser,
  WorkflowUserAccess
}

import java.sql.Timestamp

abstract class WorkflowAccessDatabaseSpec(withTargetUser: Boolean)
    extends DatabaseFlatSpec(cleanupBeforeEach = false, cleanupAfterEach = true) {

  protected val ownerUid = 1000 + scala.util.Random.nextInt(1000)
  protected val userWithWriteUid = 2000 + scala.util.Random.nextInt(1000)
  protected val userWithReadUid = 3000 + scala.util.Random.nextInt(1000)
  protected val targetUserUid = if (withTargetUser) 4000 + scala.util.Random.nextInt(1000) else 0
  protected val testWorkflowWid = 5000 + scala.util.Random.nextInt(1000)
  protected val newGranteeUid = 6000 + scala.util.Random.nextInt(1000)

  protected var workflowOfUserDao: WorkflowOfUserDao = _
  protected var workflowUserAccessDao: WorkflowUserAccessDao = _

  override protected def onBeforeEach(): Unit = {
    workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
  }

  protected def newUser(uid: Int, name: String, email: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(email)
    user
  }

  protected def newAccessUsers(): (User, User, User) =
    (
      newUser(ownerUid, "owner", "owner@test.com"),
      newUser(userWithWriteUid, "user_with_write", "write@test.com"),
      newUser(userWithReadUid, "user_with_read", "read@test.com")
    )

  protected def newWorkflow(): Workflow = {
    val workflow = new Workflow
    workflow.setWid(testWorkflowWid)
    workflow.setName("test_workflow")
    workflow.setContent("{}")
    workflow.setDescription("test description")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflow
  }

  protected def seedOwnerAndGrantees(): Unit = {
    workflowOfUserDao.insert(new WorkflowOfUser(ownerUid, testWorkflowWid))
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(userWithWriteUid, testWorkflowWid, PrivilegeEnum.WRITE)
    )
    workflowUserAccessDao.insert(
      new WorkflowUserAccess(userWithReadUid, testWorkflowWid, PrivilegeEnum.READ)
    )
  }

  override protected def cleanup(): Unit = {
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

    val userIds = Seq(ownerUid, userWithWriteUid, userWithReadUid) ++
      (if (withTargetUser) Seq(targetUserUid) else Seq.empty) :+ newGranteeUid
    getDSLContext
      .deleteFrom(USER)
      .where(USER.UID.in(userIds.map(Int.box): _*))
      .execute()
  }
}
