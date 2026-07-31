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

package org.apache.texera.web.resource.dashboard.admin.execution

import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowUserAccessDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowUserAccess,
  WorkflowVersion
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.UUID

class AdminExecutionResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Random ids keep parallel spec files from colliding on the shared embedded DB.
  private val testWid = 4000 + scala.util.Random.nextInt(1000)
  private val testUid = 4000 + scala.util.Random.nextInt(1000)

  private var testUser: User = _
  private var testWorkflow: Workflow = _
  private var testVersion: WorkflowVersion = _

  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _

  private val resource = new AdminExecutionResource()

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    userDao = new UserDao(getDSLContext.configuration())
    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())

    cleanupTestData()

    testUser = new User
    testUser.setUid(testUid)
    testUser.setName("test_user")
    testUser.setEmail("admin_exec_test@example.com")
    testUser.setPassword("password")

    testWorkflow = new Workflow
    testWorkflow.setWid(testWid)
    testWorkflow.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent("{}")
    testWorkflow.setDescription("test description")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))

    testVersion = new WorkflowVersion
    testVersion.setWid(testWid)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))

    userDao.insert(testUser)
    workflowDao.insert(testWorkflow)
    workflowVersionDao.insert(testVersion)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  private def cleanupTestData(): Unit = {
    val vidSubquery = getDSLContext
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWid))

    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.VID.in(vidSubquery))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW_VERSION).where(WORKFLOW_VERSION.WID.eq(testWid)).execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    getDSLContext.deleteFrom(USER).where(USER.UID.eq(testUid)).execute()
  }

  // ─── helpers ────────────────────────────────────────────────────────────────

  private def seedExecution(
      status: Byte = 3.toByte,
      name: String = "run",
      startOffsetMillis: Long = 0L,
      durationMillis: Long = 1000L
  ): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUser.getUid)
    execution.setStatus(status)
    execution.setResult("")
    execution.setLogLocation("")
    val now = System.currentTimeMillis()
    execution.setStartingTime(new Timestamp(now - startOffsetMillis))
    execution.setLastUpdateTime(new Timestamp(now - startOffsetMillis + durationMillis))
    execution.setBookmarked(false)
    execution.setName(name)
    execution.setEnvironmentVersion("test-env-1.0")
    workflowExecutionsDao.insert(execution)
    execution
  }

  private def grantAccess(uid: Int, privilege: PrivilegeEnum): Unit = {
    val access = new WorkflowUserAccess
    access.setUid(uid)
    access.setWid(testWid)
    access.setPrivilege(privilege)
    workflowUserAccessDao.insert(access)
  }

  private def emptyFilter: java.util.List[String] = new java.util.ArrayList[String]()

  private def filterOf(statuses: String*): java.util.List[String] = {
    val list = new java.util.ArrayList[String]()
    statuses.foreach(list.add)
    list
  }

  // Calls the admin listing as testUser with the default page window; keeps each
  // call site under the 100-column limit and centralizes the fixed arguments.
  private def list(
      sortField: String = "NO_SORTING",
      filter: java.util.List[String] = emptyFilter
  ): List[AdminExecutionResource.dashboardExecution] =
    resource.listWorkflows(new SessionUser(testUser), 20, 0, sortField, "desc", filter)

  // ─── pure status lookups (no DB) ─────────────────────────────────────────────

  "AdminExecutionResource.mapToName" should "map known status codes and default the rest" in {
    AdminExecutionResource.mapToName(0) shouldBe "READY"
    AdminExecutionResource.mapToName(1) shouldBe "RUNNING"
    AdminExecutionResource.mapToName(2) shouldBe "PAUSED"
    AdminExecutionResource.mapToName(3) shouldBe "COMPLETED"
    AdminExecutionResource.mapToName(4) shouldBe "FAILED"
    AdminExecutionResource.mapToName(5) shouldBe "KILLED"
    AdminExecutionResource.mapToName(99) shouldBe "UNKNOWN"
  }

  "AdminExecutionResource.mapToStatus" should "invert mapToName and map unknowns to -1" in {
    Seq("READY", "RUNNING", "PAUSED", "COMPLETED", "FAILED", "KILLED").zipWithIndex.foreach {
      case (name, code) => AdminExecutionResource.mapToStatus(name) shouldBe code
    }
    AdminExecutionResource.mapToStatus("NOT_A_STATUS") shouldBe -1
  }

  // ─── getTotalWorkflows ───────────────────────────────────────────────────────

  "getTotalWorkflows" should "count distinct workflows that have executions" in {
    resource.getTotalWorkflows shouldBe 0

    // two executions on the same workflow still count as one distinct workflow
    seedExecution()
    seedExecution()
    resource.getTotalWorkflows shouldBe 1
  }

  // ─── listWorkflows ───────────────────────────────────────────────────────────

  "listWorkflows" should "return the seeded execution with the mapped status and access flag" in {
    seedExecution(status = 3.toByte, name = "completed-run")
    grantAccess(testUser.getUid, PrivilegeEnum.READ)

    val rows = list()

    rows should have size 1
    val row = rows.head
    row.workflowId shouldBe testWid
    row.userName shouldBe "test_user"
    row.executionName shouldBe "completed-run"
    row.executionStatus shouldBe "COMPLETED"
    row.access shouldBe true
  }

  it should "report access = false when the current user has no WORKFLOW_USER_ACCESS row" in {
    seedExecution()

    val rows = list()

    rows should have size 1
    rows.head.access shouldBe false
  }

  it should "keep only the latest execution per workflow" in {
    seedExecution(name = "older")
    val latest = seedExecution(name = "newer")

    val rows = list()

    rows should have size 1
    rows.head.executionId shouldBe latest.getEid
    rows.head.executionName shouldBe "newer"
  }

  it should "narrow the result to executions whose status is in the filter" in {
    // The listing returns the latest execution per workflow; a RUNNING latest passes a
    // RUNNING filter, and is excluded by a COMPLETED-only filter.
    seedExecution(status = 1.toByte, name = "running-run")

    list(filter = filterOf("RUNNING")) should have size 1
    list(filter = filterOf("COMPLETED")) shouldBe empty
  }

  it should "apply sorting without error when a sort field is provided" in {
    seedExecution()

    val rows = list(sortField = "end_time")

    rows should have size 1
    rows.head.workflowId shouldBe testWid
  }
}
