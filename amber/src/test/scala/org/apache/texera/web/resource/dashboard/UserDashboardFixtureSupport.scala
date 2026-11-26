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

package org.apache.texera.web.resource.dashboard

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos._
import org.apache.texera.dao.jooq.generated.tables.pojos._
import org.jooq.{Condition, DSLContext}

import java.sql.Timestamp
import java.util.UUID

private[dashboard] trait DashboardWorkflowFixtureSupport { self: MockTexeraDB =>
  private lazy val workflowOfUserDao = new WorkflowOfUserDao(getDSLContext.configuration())
  private lazy val workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())

  protected lazy val workflowDao = new WorkflowDao(getDSLContext.configuration())

  protected def uniqueSuffix: String = UUID.randomUUID().toString.substring(0, 8)
  protected def now: Timestamp = new Timestamp(System.currentTimeMillis())

  protected def fixtureUser(uid: Int, name: String): User = {
    val user = new User
    user.setUid(uid)
    user.setName(name)
    user.setEmail(s"$name@test.com")
    user
  }

  protected def fixtureWorkflow(
      wid: Int,
      name: String,
      description: String,
      isPublic: java.lang.Boolean
  ): Workflow = {
    val workflow = new Workflow
    workflow.setWid(wid)
    workflow.setName(name)
    workflow.setContent("{}")
    workflow.setDescription(description)
    workflow.setIsPublic(isPublic)
    workflow.setCreationTime(now)
    workflow.setLastModifiedTime(now)
    workflow
  }

  protected def seedOwnership(uid: Int, wid: Int): Unit = {
    val ownership = new WorkflowOfUser
    ownership.setUid(uid)
    ownership.setWid(wid)
    workflowOfUserDao.insert(ownership)
  }

  protected def seedAccess(uid: Int, wid: Int, privilege: PrivilegeEnum): Unit = {
    val access = new WorkflowUserAccess
    access.setUid(uid)
    access.setWid(wid)
    access.setPrivilege(privilege)
    workflowUserAccessDao.insert(access)
  }
}

private[dashboard] trait UserDashboardFixtureSupport extends DashboardWorkflowFixtureSupport {
  self: MockTexeraDB =>
  protected lazy val userDao = new UserDao(getDSLContext.configuration())
  protected lazy val workflowExecutionsDao = new WorkflowExecutionsDao(
    getDSLContext.configuration()
  )
  private lazy val workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
  private lazy val datasetDao = new DatasetDao(getDSLContext.configuration())

  protected def seedWorkflow(wid: Int, namePrefix: String): Workflow = {
    val workflow = fixtureWorkflow(wid, namePrefix + uniqueSuffix, "desc", isPublic = null)
    workflowDao.insert(workflow)
    workflow
  }

  protected def seedVersion(wid: Int): WorkflowVersion = {
    val version = new WorkflowVersion
    version.setWid(wid)
    version.setContent("{}")
    version.setCreationTime(now)
    workflowVersionDao.insert(version)
    version
  }

  protected def seedDataset(uid: Int, namePrefix: String): Dataset = {
    val dataset = new Dataset
    dataset.setOwnerUid(uid)
    dataset.setName(namePrefix + uniqueSuffix)
    dataset.setRepositoryName("repo-" + uniqueSuffix)
    dataset.setIsPublic(false)
    dataset.setIsDownloadable(true)
    dataset.setDescription("")
    dataset.setCreationTime(now)
    datasetDao.insert(dataset)
    dataset
  }

  protected def seedExecution(vid: Integer, uid: Int, name: String): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    execution.setVid(vid)
    execution.setUid(uid)
    execution.setStatus(0.toByte)
    execution.setResult("")
    execution.setLogLocation("")
    execution.setStartingTime(now)
    execution.setBookmarked(false)
    execution.setName(name)
    execution.setEnvironmentVersion("test-env-1.0")
    workflowExecutionsDao.insert(execution)
    execution
  }

  protected def deleteWorkflow(
      wid: Int,
      accessFilter: Condition,
      ownershipFilter: Condition
  ): Unit = {
    getDSLContext.deleteFrom(WORKFLOW_VERSION).where(WORKFLOW_VERSION.WID.eq(wid)).execute()
    getDSLContext.deleteFrom(WORKFLOW_USER_ACCESS).where(accessFilter).execute()
    getDSLContext.deleteFrom(WORKFLOW_OF_USER).where(ownershipFilter).execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(wid)).execute()
  }
}

/** Creates the accounts used by warehouse endpoint and workflow selection tests. */
private[web] object WarehouseAccountFixture {
  def seed(context: DSLContext, name: String, avatar: Option[String] = None): User = {
    val user = new User
    user.setName(name)
    user.setEmail(s"user_${UUID.randomUUID()}@example.com")
    avatar.foreach(user.setAvatar)
    new UserDao(context.configuration()).insert(user)
    user
  }
}
