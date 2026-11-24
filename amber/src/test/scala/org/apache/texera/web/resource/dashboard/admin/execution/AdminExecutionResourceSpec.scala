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
import org.apache.texera.dao.jooq.generated.Tables.WORKFLOW_OF_USER
import org.apache.texera.dao.jooq.generated.enums.PrivilegeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.WorkflowUserAccessDao
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  WorkflowExecutions,
  WorkflowUserAccess,
  WorkflowVersion
}
import org.apache.texera.web.ExecutionDatabaseTestSupport
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID
import java.util.concurrent.TimeUnit

class AdminExecutionResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB
    with ExecutionDatabaseTestSupport {

  // MockTexeraDB's embedded Postgres is a JVM singleton shared by all suites, which
  // run sequentially. Random ids keep this suite's rows from colliding with data other
  // suites may have left behind in that shared DB.
  private val testWid = 4000 + scala.util.Random.nextInt(1000)
  private val testUid = 4000 + scala.util.Random.nextInt(1000)
  private val secondWid = testWid + 1

  private var testUser: User = _
  private var testVersion: WorkflowVersion = _
  private var workflowUserAccessDao: WorkflowUserAccessDao = _

  private val resource = new AdminExecutionResource()

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    workflowUserAccessDao = new WorkflowUserAccessDao(getDSLContext.configuration())
    cleanupTestData()

    testUser = insertUser(testUid, "test_user", "admin_exec_test@example.com")
    insertWorkflow(
      testWid,
      "test_workflow_" + UUID.randomUUID().toString.substring(0, 8),
      description = "test description"
    )
    testVersion = insertWorkflowVersion(testWid)
  }

  override protected def afterEach(): Unit = {
    cleanupTestData()
  }

  private def cleanupTestData(): Unit =
    cleanupWorkflowRecords(
      Seq(testWid, secondWid),
      uids = Seq(testUid),
      includeUserAccess = true
    )

  // ─── helpers ────────────────────────────────────────────────────────────────

  private def seedExecution(
      status: Byte = 3.toByte,
      name: String = "run",
      startOffsetMillis: Long = 0L,
      durationMillis: Long = 1000L
  ): WorkflowExecutions =
    insertWorkflowExecution(
      vid = testVersion.getVid,
      uid = testUser.getUid,
      status = status,
      logLocation = Some(""),
      envVersion = "test-env-1.0",
      name = name,
      startOffsetMillis = Some(startOffsetMillis),
      durationMillis = Some(durationMillis)
    )

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
    insertWorkflow(
      secondWid,
      "second_workflow_" + UUID.randomUUID().toString.substring(0, 8),
      description = "second"
    )
    val version = insertWorkflowVersion(secondWid)
    insertWorkflowExecution(
      vid = version.getVid,
      uid = testUser.getUid,
      status = 3.toByte,
      logLocation = Some(""),
      envVersion = "test-env-1.0",
      name = "second-run",
      startOffsetMillis = Some(endOffsetMillis + 1000),
      durationMillis = Some(1000L)
    )
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

  it should "report access = true for the owner without an access row" in {
    seedExecution()
    getDSLContext
      .insertInto(WORKFLOW_OF_USER)
      .set(WORKFLOW_OF_USER.UID, Integer.valueOf(testUid))
      .set(WORKFLOW_OF_USER.WID, Integer.valueOf(testWid))
      .execute()

    list().head.access shouldBe true
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
