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
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables._
import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
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
    testUser.setPassword("password")
    testUser.setGoogleAvatar("avatar_url")

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
    shutdownDB()
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
    val (_, _, gpOpt, resType) = VFSURIFactory.decodeURI(found.get)
    assert(resType == VFSResourceType.RESULT)
    assert(gpOpt.exists(gp => gp.opId.logicalOpId == targetOpId && gp.portId == targetPortId))
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
    otherUser.setPassword("password")
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

}
