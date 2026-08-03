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

import org.apache.texera.amber.core.storage.{VFSResourceType, VFSURIFactory}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity}
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.{PrivilegeEnum, WorkflowComputingUnitTypeEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  DatasetDao,
  UserDao,
  WorkflowComputingUnitDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  Dataset,
  User,
  Workflow,
  WorkflowComputingUnit,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.amber.engine.architecture.coordinator.OperatorPortResultUriAvailable
import org.apache.texera.web.service.ExecutionResultService
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, PrivateMethodTester}

import javax.ws.rs.{BadRequestException, ForbiddenException, WebApplicationException}
import java.net.URI
import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ArrayBuffer

class WorkflowExecutionsResourceSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB
    with PrivateMethodTester {

  private val testWorkflowWid = 3000 + scala.util.Random.nextInt(1000)
  private val testUserId = 1000 + scala.util.Random.nextInt(1000)

  private var testWorkflow: Workflow = _
  private var testVersion: WorkflowVersion = _
  private var testUser: User = _
  private var userDao: UserDao = _
  private var workflowDao: WorkflowDao = _
  private var workflowVersionDao: WorkflowVersionDao = _
  private var workflowExecutionsDao: WorkflowExecutionsDao = _
  private var datasetDao: DatasetDao = _
  private var computingUnitDao: WorkflowComputingUnitDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def beforeEach(): Unit = {
    testUser = new User
    testUser.setUid(testUserId)
    testUser.setName("test_user")
    testUser.setEmail("test@example.com")
    testUser.setAvatar("avatar_url")

    testWorkflow = new Workflow
    testWorkflow.setWid(testWorkflowWid)
    testWorkflow.setName("test_workflow_" + UUID.randomUUID().toString.substring(0, 8))
    testWorkflow.setContent("{}")
    testWorkflow.setDescription("test description")
    testWorkflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    testWorkflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))

    testVersion = new WorkflowVersion
    testVersion.setWid(testWorkflowWid)
    testVersion.setContent("{}")
    testVersion.setCreationTime(new Timestamp(System.currentTimeMillis()))

    workflowDao = new WorkflowDao(getDSLContext.configuration())
    workflowVersionDao = new WorkflowVersionDao(getDSLContext.configuration())
    userDao = new UserDao(getDSLContext.configuration())
    workflowExecutionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
    datasetDao = new DatasetDao(getDSLContext.configuration())
    computingUnitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())

    cleanupTestData()

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
      .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))

    // Child tables of WORKFLOW_EXECUTIONS must be wiped before the parent row.
    getDSLContext
      .deleteFrom(OPERATOR_PORT_EXECUTIONS)
      .where(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(
          getDSLContext
            .select(WORKFLOW_EXECUTIONS.EID)
            .from(WORKFLOW_EXECUTIONS)
            .where(WORKFLOW_EXECUTIONS.VID.in(vidSubquery))
        )
      )
      .execute()

    getDSLContext
      .deleteFrom(OPERATOR_EXECUTIONS)
      .where(
        OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(
          getDSLContext
            .select(WORKFLOW_EXECUTIONS.EID)
            .from(WORKFLOW_EXECUTIONS)
            .where(WORKFLOW_EXECUTIONS.VID.in(vidSubquery))
        )
      )
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.VID.in(vidSubquery))
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWorkflowWid))
      .execute()

    // Access grants seeded by the endpoint tests must go before the workflow row.
    getDSLContext
      .deleteFrom(WORKFLOW_USER_ACCESS)
      .where(WORKFLOW_USER_ACCESS.WID.eq(testWorkflowWid))
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW)
      .where(WORKFLOW.WID.eq(testWorkflowWid))
      .execute()

    // Datasets / computing units / extra users may be seeded by individual cases.
    getDSLContext
      .deleteFrom(DATASET)
      .where(DATASET.OWNER_UID.in(getDSLContext.select(USER.UID).from(USER).where(USER.UID.ne(0))))
      .execute()

    getDSLContext
      .deleteFrom(WORKFLOW_COMPUTING_UNIT)
      .where(WORKFLOW_COMPUTING_UNIT.UID.eq(testUserId))
      .execute()

    getDSLContext
      .deleteFrom(USER)
      .where(USER.UID.eq(testUserId))
      .execute()
  }

  override protected def afterAll(): Unit = {
    closeConnectionPool()
  }

  // ─── helpers ──────────────────────────────────────────────────────────────

  private def insertComputingUnit(): WorkflowComputingUnit = {
    val unit = new WorkflowComputingUnit
    unit.setUid(testUser.getUid)
    unit.setName("test-unit-" + UUID.randomUUID().toString.substring(0, 8))
    unit.setCreationTime(new Timestamp(System.currentTimeMillis()))
    unit.setType(WorkflowComputingUnitTypeEnum.local)
    unit.setUri("local://test")
    unit.setResource("{}")
    computingUnitDao.insert(unit)
    unit
  }

  private def insertExecution(
      name: String = s"Execution-${UUID.randomUUID().toString.substring(0, 8)}",
      status: Byte = 0.toByte,
      result: String = "",
      logLocation: String = "",
      startOffsetMillis: Long = 0L,
      lastUpdateOffsetMillis: Option[Long] = None,
      cuid: Integer = null,
      runtimeStatsUri: String = null
  ): WorkflowExecutions = {
    val execution = new WorkflowExecutions
    execution.setVid(testVersion.getVid)
    execution.setUid(testUser.getUid)
    execution.setStatus(status)
    execution.setResult(result)
    execution.setLogLocation(logLocation)
    val now = System.currentTimeMillis()
    execution.setStartingTime(new Timestamp(now - startOffsetMillis))
    lastUpdateOffsetMillis.foreach(off => execution.setLastUpdateTime(new Timestamp(now - off)))
    execution.setBookmarked(false)
    execution.setName(name)
    execution.setEnvironmentVersion("test-env-1.0")
    execution.setCuid(cuid)
    execution.setRuntimeStatsUri(runtimeStatsUri)
    workflowExecutionsDao.insert(execution)
    execution
  }

  // Local convenience over the production callback: fixture rows the
  // lookup specs below need go through the same insert prod uses, so a
  // regression in the column list shows up here too.
  private def insertOperatorPortResult(
      eid: ExecutionIdentity,
      globalPortId: GlobalPortIdentity,
      uri: URI
  ): Unit =
    ExecutionResultService.persistOperatorPortResultUri(
      eid,
      OperatorPortResultUriAvailable(globalPortId, uri)
    )

  // ─── existing tests (preserved) ───────────────────────────────────────────

  "WorkflowExecutionsResource.getWorkflowExecutions" should "return executions with EIDs in descending order" in {
    val numExecutions = 10
    val executionIds = ArrayBuffer.empty[Integer]

    for (i <- 1 to numExecutions) {
      val execution = insertExecution(
        name = s"Execution ${i}",
        startOffsetMillis = TimeUnit.DAYS.toMillis(numExecutions - i)
      )
      executionIds.append(execution.getEid)
    }

    val result = WorkflowExecutionsResource.getWorkflowExecutions(testWorkflowWid, getDSLContext)

    assert(result.nonEmpty, "Result should not be empty")
    assert(
      result.size == numExecutions,
      s"Expected $numExecutions executions, but got ${result.size}"
    )

    for (i <- 0 until result.size - 1) {
      assert(
        result(i).eId > result(i + 1).eId,
        s"Executions are not in descending order: ${result(i).eId} should be > ${result(i + 1).eId}"
      )
    }

    val returnedIds = result.map(_.eId).toSet
    assert(
      executionIds.toSet.subsetOf(returnedIds),
      "All inserted execution IDs should be returned"
    )
  }

  // (The production callback body that writes operator_port_executions is
  // covered by `ExecutionResultServiceSpec.persistOperatorPortResultUri`.)

  // ─── new: status-filtered execution listing ───────────────────────────────

  "getWorkflowExecutions with statusCodes" should "narrow results to the requested codes" in {
    insertExecution(status = 1.toByte)
    insertExecution(status = 2.toByte)
    insertExecution(status = 1.toByte)

    val onlyStatusOne =
      WorkflowExecutionsResource.getWorkflowExecutions(
        testWorkflowWid,
        getDSLContext,
        Set(1.toByte)
      )
    assert(onlyStatusOne.size == 2)
    assert(onlyStatusOne.forall(_.status == 1.toByte))
  }

  // ─── new: getLatestExecutionID ────────────────────────────────────────────

  "getLatestExecutionID" should "return None when no executions exist for the (wid, cuid) pair" in {
    val result =
      WorkflowExecutionsResource.getLatestExecutionID(testWorkflowWid, Integer.valueOf(999))
    assert(result.isEmpty)
  }

  it should "return the largest EID for matching (wid, cuid)" in {
    // cuid has an FK to WORKFLOW_COMPUTING_UNIT — seed two units.
    val unitA = insertComputingUnit()
    val unitB = insertComputingUnit()

    val a = insertExecution(cuid = unitA.getCuid)
    val b = insertExecution(cuid = unitA.getCuid)
    // Distractor with a different cuid — should be ignored.
    insertExecution(cuid = unitB.getCuid)

    val result = WorkflowExecutionsResource.getLatestExecutionID(testWorkflowWid, unitA.getCuid)
    assert(result.isDefined)
    assert(result.get == math.max(a.getEid, b.getEid))
  }

  // ─── new: getExpiredExecutionsWithResultOrLog ─────────────────────────────

  "getExpiredExecutionsWithResultOrLog" should "match rows that are stale by starting_time and have a result" in {
    // Stale-by-starting-time + has result → match.
    val expired = insertExecution(
      name = "expired-with-result",
      result = "some-result",
      startOffsetMillis = TimeUnit.SECONDS.toMillis(120)
    )
    // Fresh starting_time → must not match.
    insertExecution(name = "fresh", result = "some-result")
    // Stale but empty result+log → must not match.
    insertExecution(
      name = "stale-but-empty",
      startOffsetMillis = TimeUnit.SECONDS.toMillis(120)
    )

    val matched = WorkflowExecutionsResource.getExpiredExecutionsWithResultOrLog(60)

    val eids = matched.map(_.getEid).toSet
    assert(eids.contains(expired.getEid))
    assert(matched.forall(e => e.getResult.nonEmpty || Option(e.getLogLocation).exists(_.nonEmpty)))
  }

  it should "match rows that are stale by last_update_time and have a log_location" in {
    val expired = insertExecution(
      name = "log-stale",
      logLocation = "file:///tmp/log",
      lastUpdateOffsetMillis = Some(TimeUnit.SECONDS.toMillis(120))
    )
    insertExecution(
      name = "log-fresh",
      logLocation = "file:///tmp/log-2",
      lastUpdateOffsetMillis = Some(0L)
    )

    val matched = WorkflowExecutionsResource.getExpiredExecutionsWithResultOrLog(60)
    assert(matched.map(_.getEid).toSet.contains(expired.getEid))
  }

  // ─── new: insertOperatorExecutions ────────────────────────────────────────

  "insertOperatorExecutions" should "insert one OPERATOR_EXECUTIONS row" in {
    val execution = insertExecution()
    val uri = URI.create("vfs:///console-msg")

    WorkflowExecutionsResource.insertOperatorExecutions(
      execution.getEid.longValue(),
      "op-A",
      uri
    )

    val rows = getDSLContext
      .selectFrom(OPERATOR_EXECUTIONS)
      .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_EXECUTIONS.OPERATOR_ID.eq("op-A"))
      .fetch()

    assert(rows.size() == 1)
    assert(rows.get(0).getConsoleMessagesUri == uri.toString)
  }

  // ─── new: updateRuntimeStatsUri ───────────────────────────────────────────

  "updateRuntimeStatsUri" should "set the runtime_stats_uri on the matching execution" in {
    val execution = insertExecution()
    val uri = URI.create("vfs:///runtime-stats")

    WorkflowExecutionsResource.updateRuntimeStatsUri(
      testWorkflowWid.longValue(),
      execution.getEid.longValue(),
      uri
    )

    val refreshed = workflowExecutionsDao.fetchOneByEid(execution.getEid)
    assert(refreshed.getRuntimeStatsUri == uri.toString)
  }

  it should "leave executions belonging to other workflows untouched" in {
    val execution = insertExecution()
    val uri = URI.create("vfs:///runtime-stats")

    // wid that does not match the execution's WORKFLOW_VERSION row → no-op.
    WorkflowExecutionsResource.updateRuntimeStatsUri(
      (testWorkflowWid + 100000).longValue(),
      execution.getEid.longValue(),
      uri
    )

    val refreshed = workflowExecutionsDao.fetchOneByEid(execution.getEid)
    assert(refreshed.getRuntimeStatsUri == null)
  }

  // ─── new: URI fetchers ────────────────────────────────────────────────────

  "getResultUrisByExecutionId" should "return inserted URIs and filter out null/empty entries" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val opA = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("opA"), "main"),
      PortIdentity(),
      input = false
    )
    val opB = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("opB"), "main"),
      PortIdentity(),
      input = false
    )
    val opC = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("opC"), "main"),
      PortIdentity(),
      input = false
    )

    insertOperatorPortResult(eid, opA, URI.create("vfs:///A"))
    insertOperatorPortResult(eid, opB, URI.create("vfs:///B"))
    // Empty-string URI row — the helper should drop it from the returned list.
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .values(execution.getEid, opC.serializeAsString, "")
      .execute()

    val uris = WorkflowExecutionsResource.getResultUrisByExecutionId(eid)
    assert(uris.toSet == Set(URI.create("vfs:///A"), URI.create("vfs:///B")))
  }

  "getConsoleMessagesUriByExecutionId" should "return inserted URIs and filter empty entries" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())

    WorkflowExecutionsResource.insertOperatorExecutions(
      execution.getEid.longValue(),
      "op-A",
      URI.create("vfs:///console-A")
    )
    WorkflowExecutionsResource.insertOperatorExecutions(
      execution.getEid.longValue(),
      "op-B",
      URI.create("vfs:///console-B")
    )
    // Empty-URI row — must be filtered.
    getDSLContext
      .insertInto(OPERATOR_EXECUTIONS)
      .columns(
        OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_EXECUTIONS.OPERATOR_ID,
        OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI
      )
      .values(execution.getEid, "op-C", "")
      .execute()

    val uris = WorkflowExecutionsResource.getConsoleMessagesUriByExecutionId(eid)
    assert(uris.toSet == Set(URI.create("vfs:///console-A"), URI.create("vfs:///console-B")))
  }

  "getRuntimeStatsUriByExecutionId" should "return None when the stored URI is null or empty" in {
    val noUri = insertExecution()
    assert(
      WorkflowExecutionsResource
        .getRuntimeStatsUriByExecutionId(ExecutionIdentity(noUri.getEid.longValue()))
        .isEmpty
    )

    val emptyUri = insertExecution(runtimeStatsUri = "")
    assert(
      WorkflowExecutionsResource
        .getRuntimeStatsUriByExecutionId(ExecutionIdentity(emptyUri.getEid.longValue()))
        .isEmpty
    )
  }

  it should "return Some(URI) when the stored URI is non-empty" in {
    val withUri = insertExecution(runtimeStatsUri = "vfs:///stats")
    val result = WorkflowExecutionsResource.getRuntimeStatsUriByExecutionId(
      ExecutionIdentity(withUri.getEid.longValue())
    )
    assert(result.contains(URI.create("vfs:///stats")))
  }

  // ─── new: deleteConsoleMessageAndExecutionResultUris ──────────────────────

  "deleteConsoleMessageAndExecutionResultUris" should "purge both child tables for a given eid" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())

    val globalPortId = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("op-purge"), "main"),
      PortIdentity(),
      input = false
    )
    insertOperatorPortResult(
      eid,
      globalPortId,
      URI.create("vfs:///r")
    )
    WorkflowExecutionsResource.insertOperatorExecutions(
      execution.getEid.longValue(),
      "op-purge",
      URI.create("vfs:///c")
    )

    WorkflowExecutionsResource.deleteConsoleMessageAndExecutionResultUris(eid)

    val resultRows = getDSLContext
      .fetchCount(
        OPERATOR_PORT_EXECUTIONS,
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid)
      )
    val consoleRows = getDSLContext
      .fetchCount(
        OPERATOR_EXECUTIONS,
        OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid)
      )
    assert(resultRows == 0)
    assert(consoleRows == 0)
  }

  // ─── new: removeAllExecutionFiles (DB delete branch) ──────────────────────

  "removeAllExecutionFiles" should "delete the listed executions from WORKFLOW_EXECUTIONS" in {
    val a = insertExecution()
    val b = insertExecution()
    // Distractor that should survive.
    val survivor = insertExecution()

    WorkflowExecutionsResource.removeAllExecutionFiles(Array(a.getEid, b.getEid))

    val survivors = workflowExecutionsDao.findAll()
    val survivorEids = survivors.toArray.map(_.asInstanceOf[WorkflowExecutions].getEid).toSet
    assert(!survivorEids.contains(a.getEid))
    assert(!survivorEids.contains(b.getEid))
    assert(survivorEids.contains(survivor.getEid))
  }

  // ─── new: updateResultSize ────────────────────────────────────────────────

  "updateResultSize" should "set RESULT_SIZE on the matching (eid, globalPortId) row" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val globalPortId = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("op-size"), "main"),
      PortIdentity(),
      input = false
    )
    insertOperatorPortResult(
      eid,
      globalPortId,
      URI.create("vfs:///r")
    )

    WorkflowExecutionsResource.updateResultSize(eid, globalPortId, 4096L)

    val row = getDSLContext
      .selectFrom(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortId.serializeAsString))
      .fetchOne()
    assert(row.getResultSize == 4096)
  }

  it should "store a >2GiB size without truncation (#6978)" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val globalPortId = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("op-big-size"), "main"),
      PortIdentity(),
      input = false
    )
    insertOperatorPortResult(eid, globalPortId, URI.create("vfs:///big"))

    // 3 GiB exceeds Int.MaxValue; a Long->Int narrowing would wrap it negative.
    val threeGiB = 3L * 1024 * 1024 * 1024
    WorkflowExecutionsResource.updateResultSize(eid, globalPortId, threeGiB)

    val row = getDSLContext
      .selectFrom(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortId.serializeAsString))
      .fetchOne()
    assert(row.getResultSize.longValue() == threeGiB)
  }

  // ─── new: updateRuntimeStatsSize / updateConsoleMessageSize ───────────────

  "updateRuntimeStatsSize" should "store a >2GiB size on the matching execution" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val threeGiB = 3L * 1024 * 1024 * 1024

    WorkflowExecutionsResource.updateRuntimeStatsSize(eid, threeGiB)

    val row = getDSLContext
      .selectFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.EID.eq(execution.getEid))
      .fetchOne()
    assert(row.getRuntimeStatsSize.longValue() == threeGiB)
  }

  it should "leave the size untouched when the execution has no runtime stats URI" in {
    val execution = insertExecution(runtimeStatsUri = null)

    WorkflowExecutionsResource.updateRuntimeStatsSize(
      ExecutionIdentity(execution.getEid.longValue())
    )

    val row = getDSLContext
      .selectFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.EID.eq(execution.getEid))
      .fetchOne()
    // The fixture never set a size, so a no-op leaves the column as inserted.
    assert(row.getRuntimeStatsSize == null)
  }

  it should "open the stored document for measuring when a runtime stats URI is present" in {
    // A URI is present, so the method must reach the document-open call. No
    // document backend exists in this unit environment, so the open fails on
    // the unsupported scheme — proving the branch executed and that the
    // failure propagates instead of degrading into a silent no-op.
    val execution = insertExecution(runtimeStatsUri = "mock:///runtime-stats")

    val ex = intercept[UnsupportedOperationException] {
      WorkflowExecutionsResource.updateRuntimeStatsSize(
        ExecutionIdentity(execution.getEid.longValue())
      )
    }
    assert(ex.getMessage.contains("mock"))
  }

  "updateConsoleMessageSize" should "store a >2GiB size on the matching (eid, opId) row" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val opId = OperatorIdentity("op-console-size")
    WorkflowExecutionsResource.insertOperatorExecutions(
      execution.getEid.longValue(),
      opId.id,
      URI.create("vfs:///console-big")
    )

    val threeGiB = 3L * 1024 * 1024 * 1024
    WorkflowExecutionsResource.updateConsoleMessageSize(eid, opId, threeGiB)

    val row = getDSLContext
      .selectFrom(OPERATOR_EXECUTIONS)
      .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_EXECUTIONS.OPERATOR_ID.eq(opId.id))
      .fetchOne()
    assert(row.getConsoleMessagesSize.longValue() == threeGiB)
  }

  it should "leave the size untouched when the operator has no console messages URI" in {
    val execution = insertExecution()
    val opId = OperatorIdentity("op-no-console-uri")

    WorkflowExecutionsResource.updateConsoleMessageSize(
      ExecutionIdentity(execution.getEid.longValue()),
      opId
    )

    val row = getDSLContext
      .selectFrom(OPERATOR_EXECUTIONS)
      .where(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_EXECUTIONS.OPERATOR_ID.eq(opId.id))
      .fetchOne()
    assert(row == null)
  }

  it should "open the stored document for measuring when a console messages URI is present" in {
    // Same shape as the runtime-stats case above: the stored URI forces the
    // document-open call, whose unsupported-scheme failure propagates.
    val execution = insertExecution()
    val opId = OperatorIdentity("op-console-uri")
    WorkflowExecutionsResource.insertOperatorExecutions(
      execution.getEid.longValue(),
      opId.id,
      URI.create("mock:///console")
    )

    val ex = intercept[UnsupportedOperationException] {
      WorkflowExecutionsResource.updateConsoleMessageSize(
        ExecutionIdentity(execution.getEid.longValue()),
        opId
      )
    }
    assert(ex.getMessage.contains("mock"))
  }

  // ─── new: getResultUriByLogicalPortId ─────────────────────────────────────

  "getResultUriByLogicalPortId" should "match by logical operator id, port id, and resource type" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val wfId = WorkflowIdentity(testWorkflowWid.longValue())

    // Build a real VFS result URI that decodeURI can parse.
    val targetOpId = OperatorIdentity("target-op")
    val targetPortId = PortIdentity()
    val targetGlobalPort = GlobalPortIdentity(
      PhysicalOpIdentity(targetOpId, "main"),
      targetPortId,
      input = false
    )
    val targetUri = VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(wfId, eid, targetGlobalPort)
    )
    insertOperatorPortResult(eid, targetGlobalPort, targetUri)

    // Distractor: same workflow, different op id.
    val otherGlobalPort = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("other-op"), "main"),
      PortIdentity(),
      input = false
    )
    val otherUri = VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(wfId, eid, otherGlobalPort)
    )
    insertOperatorPortResult(eid, otherGlobalPort, otherUri)

    val found =
      WorkflowExecutionsResource.getResultUriByLogicalPortId(eid, targetOpId, targetPortId)
    assert(found.contains(targetUri))

    // Sanity-check: the decoded URI is RESULT-typed and matches the target ids.
    val components = VFSURIFactory.decodeURI(found.get)
    assert(components.resourceType == VFSResourceType.RESULT)
    assert(
      components.globalPortId
        .exists(gp => gp.opId.logicalOpId == targetOpId && gp.portId == targetPortId)
    )
  }

  it should "return None when no URI matches the requested op/port" in {
    val execution = insertExecution()
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val found =
      WorkflowExecutionsResource.getResultUriByLogicalPortId(
        eid,
        OperatorIdentity("nope"),
        PortIdentity()
      )
    assert(found.isEmpty)
  }

  // ─── new: getNonDownloadableOperatorMap (private — via PrivateMethodTester) ─

  "getNonDownloadableOperatorMap" should "flag operators reading non-downloadable datasets they don't own" in {
    // Owner of the non-downloadable dataset is a *different* user than testUser.
    val otherUser = new User
    val otherUid = testUserId + 1
    otherUser.setUid(otherUid)
    otherUser.setName("dataset-owner")
    otherUser.setEmail("owner@example.com")
    userDao.insert(otherUser)

    val dataset = new Dataset
    dataset.setOwnerUid(otherUid)
    dataset.setName("LockedDS")
    dataset.setRepositoryName("repo-locked")
    dataset.setIsPublic(false)
    dataset.setIsDownloadable(false)
    dataset.setDescription("")
    dataset.setCreationTime(new Timestamp(System.currentTimeMillis()))
    datasetDao.insert(dataset)

    // Workflow content: scan op A reading the locked dataset, then a downstream op B.
    val content =
      """{
        |  "operators": [
        |    {"operatorID": "scanA", "operatorProperties": {"fileName": "/owner@example.com/LockedDS/v1/data.csv"}},
        |    {"operatorID": "downstreamB", "operatorProperties": {}}
        |  ],
        |  "links": [
        |    {"source": {"operatorID": "scanA"}, "target": {"operatorID": "downstreamB"}}
        |  ]
        |}""".stripMargin
    testWorkflow.setContent(content)
    workflowDao.update(testWorkflow)

    val privateMethod =
      PrivateMethod[Map[String, Set[(String, String)]]](Symbol("getNonDownloadableOperatorMap"))
    val result = WorkflowExecutionsResource invokePrivate privateMethod(testWorkflowWid, testUser)

    assert(result.contains("scanA"))
    assert(result("scanA").contains(("owner@example.com", "LockedDS")))
    // BFS propagates the restriction to the downstream operator.
    assert(result.contains("downstreamB"))
  }

  it should "return an empty map when the workflow content is unparseable" in {
    testWorkflow.setContent("not-json")
    workflowDao.update(testWorkflow)

    val privateMethod =
      PrivateMethod[Map[String, Set[(String, String)]]](Symbol("getNonDownloadableOperatorMap"))
    val result = WorkflowExecutionsResource invokePrivate privateMethod(testWorkflowWid, testUser)
    assert(result.isEmpty)
  }

  it should "return an empty map when the workflow has no operators referencing datasets" in {
    val content =
      """{"operators": [{"operatorID": "x", "operatorProperties": {}}], "links": []}"""
    testWorkflow.setContent(content)
    workflowDao.update(testWorkflow)

    val privateMethod =
      PrivateMethod[Map[String, Set[(String, String)]]](Symbol("getNonDownloadableOperatorMap"))
    val result = WorkflowExecutionsResource invokePrivate privateMethod(testWorkflowWid, testUser)
    assert(result.isEmpty)
  }

  it should "skip restriction when the current user is the dataset owner" in {
    // The dataset is owned by testUser (test@example.com), and the operator points
    // to /test@example.com/MyDS/v1/file.csv → no restriction even though
    // is_downloadable=false.
    val dataset = new Dataset
    dataset.setOwnerUid(testUserId)
    dataset.setName("MyDS")
    dataset.setRepositoryName("repo-my")
    dataset.setIsPublic(false)
    dataset.setIsDownloadable(false)
    dataset.setDescription("")
    dataset.setCreationTime(new Timestamp(System.currentTimeMillis()))
    datasetDao.insert(dataset)

    val content =
      """{
        |  "operators": [
        |    {"operatorID": "scan", "operatorProperties": {"fileName": "/test@example.com/MyDS/v1/data.csv"}}
        |  ],
        |  "links": []
        |}""".stripMargin
    testWorkflow.setContent(content)
    workflowDao.update(testWorkflow)

    val privateMethod =
      PrivateMethod[Map[String, Set[(String, String)]]](Symbol("getNonDownloadableOperatorMap"))
    val result = WorkflowExecutionsResource invokePrivate privateMethod(testWorkflowWid, testUser)
    assert(result.isEmpty)
  }
  // ─── new: endpoint auth-annotation audit (#6977) ──────────────────────────

  "WorkflowExecutionsResource endpoints" should "all declare @RolesAllowed and take an @Auth user" in {
    val httpAnnotations: Seq[Class[_ <: java.lang.annotation.Annotation]] =
      Seq(
        classOf[javax.ws.rs.GET],
        classOf[javax.ws.rs.PUT],
        classOf[javax.ws.rs.POST],
        classOf[javax.ws.rs.DELETE]
      )
    val handlers = classOf[WorkflowExecutionsResource].getDeclaredMethods.toSeq
      .filter(m => httpAnnotations.exists(a => m.getAnnotation(a) != null))
    assert(handlers.nonEmpty)

    // exportResultToLocal authenticates manually: it serves a browser form-submit
    // download, which cannot carry an Authorization header, so the JWT arrives as
    // a form field and is verified in-method via JwtParser.parseToken (including
    // the role check). Any other handler must use the declarative annotations.
    val manuallyAuthenticated = Set("exportResultToLocal")

    val offenders = handlers.filterNot(m => manuallyAuthenticated.contains(m.getName)).filter { m =>
      val hasRoles =
        m.getAnnotation(classOf[javax.annotation.security.RolesAllowed]) != null
      val hasAuthParam = m.getParameterAnnotations.exists(
        _.exists(_.annotationType() == classOf[io.dropwizard.auth.Auth])
      )
      !(hasRoles && hasAuthParam)
    }
    assert(
      offenders.isEmpty,
      s"endpoints missing @RolesAllowed/@Auth: ${offenders.map(_.getName).sorted.mkString(", ")}"
    )
  }

  // ─── access-controlled instance endpoints (jOOQ metadata only) ─────────────
  // The result/log-URI and replay paths (DocumentFactory / ReplayLogRecord) are
  // out of scope; these cover the DB-metadata portion of each endpoint.

  private val resource = new WorkflowExecutionsResource

  private def session(user: User): SessionUser = new SessionUser(user)

  private def grantReadAccess(uid: Integer = testUserId): Unit =
    getDSLContext
      .insertInto(WORKFLOW_USER_ACCESS)
      .set(WORKFLOW_USER_ACCESS.WID, Integer.valueOf(testWorkflowWid))
      .set(WORKFLOW_USER_ACCESS.UID, uid)
      .set(WORKFLOW_USER_ACCESS.PRIVILEGE, PrivilegeEnum.READ)
      .execute()

  private def userWithoutAccess(): User = {
    val u = new User
    u.setUid(testUserId + 5000)
    u.setName("no_access_user")
    u.setEmail("noaccess@example.com")
    u
  }

  "retrieveExecutionsOfWorkflow" should "return an empty list when the user lacks read access" in {
    val result =
      resource.retrieveExecutionsOfWorkflow(testWorkflowWid, session(userWithoutAccess()), null)
    assert(result.isEmpty)
  }

  it should "return the workflow's executions for an authorized user" in {
    grantReadAccess()
    insertExecution()
    insertExecution()
    val result = resource.retrieveExecutionsOfWorkflow(testWorkflowWid, session(testUser), null)
    assert(result.size == 2)
  }

  // fetchInto maps onto WorkflowExecutionEntry POSITIONALLY (a case class has no no-arg
  // constructor, and jOOQ's mapConstructorParameterNames defaults to false), so `USER.AVATAR`
  // at projection position 5 lands on `googleAvatar` despite the names differing — exactly as
  // `last_update_time` at position 9 lands on `completionTime`. Nothing observed either before,
  // which is what makes an accidental column reorder silent. Pin both here.
  it should "map the owner's avatar and completion time onto the entry despite the name mismatch" in {
    grantReadAccess()
    insertExecution(lastUpdateOffsetMillis = Some(0L))
    val entry = resource.retrieveExecutionsOfWorkflow(testWorkflowWid, session(testUser), null).head
    assert(entry.userName == testUser.getName)
    assert(entry.googleAvatar == "avatar_url")
    // `last_update_time` is populated, so a null here would mean position 9 never reached
    // `completionTime` — i.e. the mapping had silently become name-based.
    assert(entry.completionTime != null)
  }

  it should "reject an invalid status filter with a BadRequestException" in {
    grantReadAccess()
    assertThrows[BadRequestException](
      resource.retrieveExecutionsOfWorkflow(
        testWorkflowWid,
        session(testUser),
        "definitely-not-a-status"
      )
    )
  }

  "retrieveLatestExecutionEntry" should "throw ForbiddenException when the workflow has no executions" in {
    grantReadAccess()
    assertThrows[ForbiddenException](
      resource.retrieveLatestExecutionEntry(testWorkflowWid, session(testUser))
    )
  }

  it should "return the most recently created execution entry" in {
    grantReadAccess()
    insertExecution(name = "first")
    val latest = insertExecution(name = "second")
    val entry = resource.retrieveLatestExecutionEntry(testWorkflowWid, session(testUser))
    // same VID, so the highest EID is the latest
    assert(entry.eId == latest.getEid)
    assert(entry.name == "second")
  }

  "retrieveInteractionHistory" should "return an empty list when the user lacks read access" in {
    val result =
      resource.retrieveInteractionHistory(
        testWorkflowWid,
        Integer.valueOf(1),
        session(userWithoutAccess())
      )
    assert(result.isEmpty)
  }

  "setExecutionAreBookmarked" should "reject a user without access" in {
    val exec = insertExecution()
    assertThrows[WebApplicationException](
      resource.setExecutionAreBookmarked(
        ExecutionGroupBookmarkRequest(testWorkflowWid, Array(exec.getEid), isBookmarked = false),
        session(userWithoutAccess())
      )
    )
  }

  it should "bookmark executions that are currently un-bookmarked" in {
    grantReadAccess()
    val exec = insertExecution() // bookmarked = false
    resource.setExecutionAreBookmarked(
      ExecutionGroupBookmarkRequest(testWorkflowWid, Array(exec.getEid), isBookmarked = false),
      session(testUser)
    )
    assert(workflowExecutionsDao.fetchOneByEid(exec.getEid).getBookmarked == true)
  }

  it should "un-bookmark executions that are currently bookmarked" in {
    grantReadAccess()
    val exec = insertExecution()
    resource.setExecutionAreBookmarked(
      ExecutionGroupBookmarkRequest(testWorkflowWid, Array(exec.getEid), isBookmarked = true),
      session(testUser)
    )
    assert(workflowExecutionsDao.fetchOneByEid(exec.getEid).getBookmarked == false)
  }

  "updateWorkflowExecutionsName" should "rename the execution" in {
    grantReadAccess()
    val exec = insertExecution(name = "old-name")
    resource.updateWorkflowExecutionsName(
      ExecutionRenameRequest(testWorkflowWid, exec.getEid, "new-name"),
      session(testUser)
    )
    assert(workflowExecutionsDao.fetchOneByEid(exec.getEid).getName == "new-name")
  }

  "groupDeleteExecutionsOfWorkflow" should "delete the execution rows" in {
    grantReadAccess()
    val e1 = insertExecution()
    val e2 = insertExecution()
    resource.groupDeleteExecutionsOfWorkflow(
      ExecutionGroupDeleteRequest(testWorkflowWid, Array(e1.getEid, e2.getEid)),
      session(testUser)
    )
    assert(workflowExecutionsDao.fetchOneByEid(e1.getEid) == null)
    assert(workflowExecutionsDao.fetchOneByEid(e2.getEid) == null)
  }

  "retrieveWorkflowRuntimeStatistics" should "throw when the execution has no runtime-stats URI" in {
    grantReadAccess()
    val exec = insertExecution() // runtimeStatsUri = null
    assertThrows[java.util.NoSuchElementException](
      resource.retrieveWorkflowRuntimeStatistics(testWorkflowWid, exec.getEid, session(testUser))
    )
  }

}
