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
import java.util.concurrent.TimeUnit

class AdminExecutionResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // MockTexeraDB's embedded Postgres is a JVM singleton shared by all suites, which
  // run sequentially. Random ids keep this suite's rows from colliding with data other
  // suites may have left behind in that shared DB.
  private val testWid = 4000 + scala.util.Random.nextInt(1000)
  private val testUid = 4000 + scala.util.Random.nextInt(1000)
  private val secondWid = testWid + 1

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
      .where(WORKFLOW_VERSION.WID.in(testWid, secondWid))

    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.VID.in(vidSubquery))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.in(testWid, secondWid))
      .execute()
    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.in(testWid, secondWid))
      .execute()
    getDSLContext.deleteFrom(WORKFLOW).where(WORKFLOW.WID.in(testWid, secondWid)).execute()
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

  // Seeds a full second workflow (workflow + version + one execution) owned by
  // testUser, whose latest execution ends `endOffsetMillis` before "now". Used to
  // give the ordering test two workflows with distinct end times.
  private def seedSecondWorkflow(endOffsetMillis: Long): Unit = {
    val workflow = new Workflow
    workflow.setWid(secondWid)
    workflow.setName("second_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    workflow.setContent("{}")
    workflow.setDescription("second")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    workflowDao.insert(workflow)

    val version = new WorkflowVersion
    version.setWid(secondWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflowVersionDao.insert(version)

    val execution = new WorkflowExecutions
    execution.setVid(version.getVid)
    execution.setUid(testUser.getUid)
    execution.setStatus(3.toByte)
    execution.setResult("")
    execution.setLogLocation("")
    val now = System.currentTimeMillis()
    execution.setStartingTime(new Timestamp(now - endOffsetMillis - 1000))
    execution.setLastUpdateTime(new Timestamp(now - endOffsetMillis))
    execution.setBookmarked(false)
    execution.setName("second-run")
    execution.setEnvironmentVersion("test-env-1.0")
    workflowExecutionsDao.insert(execution)
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

  it should "order the results by the sort field and direction" in {
    // testWid's latest execution ends ~now; the second workflow's ends 10 minutes earlier.
    seedExecution(name = "recent")
    seedSecondWorkflow(endOffsetMillis = TimeUnit.MINUTES.toMillis(10))

    // desc by end_time -> the more recently finished workflow (testWid) comes first
    list(sortField = "end_time").map(_.workflowId) shouldBe List(testWid, secondWid)

    // asc by end_time -> the older one comes first
    val ascending =
      resource.listWorkflows(new SessionUser(testUser), 20, 0, "end_time", "asc", emptyFilter)
    ascending.map(_.workflowId) shouldBe List(secondWid, testWid)
  }
}
