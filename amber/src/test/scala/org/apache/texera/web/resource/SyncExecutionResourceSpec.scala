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

package org.apache.texera.web.resource

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.protobuf.timestamp.Timestamp
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
import org.apache.texera.amber.core.storage.model.{BufferedItemWriter, VirtualDocument}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PortIdentity,
  WorkflowContext,
  WorkflowSettings
}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.{
  COMPILATION_ERROR,
  EXECUTION_FAILURE
}
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.executionruntimestate.{
  ExecutionConsoleStore,
  ExecutionMetadataStore,
  ExecutionStatsStore,
  OperatorMetrics,
  OperatorStatistics
}
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.auth.SessionUser
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.dao.jooq.generated.Tables.OPERATOR_PORT_EXECUTIONS
import org.apache.texera.web.model.websocket.request.{LogicalPlanPojo, WorkflowExecuteRequest}
import org.apache.texera.web.storage.ExecutionStateStore
import org.apache.texera.web.service.{ConsoleMessageProcessor, WorkflowService}
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.sql.{Timestamp => SqlTimestamp}
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.UUID

/**
  * Unit tests for the pure parts of [[SyncExecutionResource]] that this PR introduced: the
  * `handleExecutionError` error-classification branch, and the behavior-preserving
  * extract-method helpers `sampleAndTruncateTuples` (the result sampling/truncation logic
  * lifted out of `collectOperatorResult`), `buildOperatorExecutionSummary` (the
  * per-operator summary construction lifted out of `collectOperatorInfos`), and
  * `assembleExecutionSummary` (the success/state derivation lifted out of
  * `executeWorkflowSync`).
  *
  * The remaining changed code paths (`executeWorkflowSync`, and the storage/DB plumbing in
  * `collectOperatorResult` / `collectOperatorInfos` around these helpers) require a live
  * Pekko execution engine, an Iceberg-backed result store, and DB-persisted port executions,
  * so they are exercised by integration tests rather than here.
  */
class SyncExecutionResourceSpec
    extends AnyFlatSpec
    with Matchers
    with PrivateMethodTester
    with BeforeAndAfterAll
    with MockTexeraDB {

  private val mapper = new ObjectMapper()
  private val resource = new SyncExecutionResource
  private var nextDbId = 900000

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

  private def sampleRow(idx: Int, k: String, v: String): SampledRow = {
    val node = mapper.createObjectNode()
    node.put(k, v)
    SampledRow(rowIndex = idx, node = node)
  }

  private def nextId(): Int = {
    nextDbId += 1
    nextDbId
  }

  private def insertExecutionRow(): ExecutionIdentity = {
    val now = new SqlTimestamp(System.currentTimeMillis())
    val suffix = UUID.randomUUID().toString.substring(0, 8)
    val uid = nextId()
    val wid = nextId()
    val vid = nextId()
    val eid = nextId()

    val user = new User
    user.setUid(uid)
    user.setName(s"sync-user-$suffix")
    user.setEmail(s"sync-user-$suffix@example.com")
    user.setPassword("password")
    new UserDao(getDSLContext.configuration()).insert(user)

    val workflow = new Workflow
    workflow.setWid(wid)
    workflow.setName(s"sync-workflow-$suffix")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(now)
    workflow.setLastModifiedTime(now)
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    val version = new WorkflowVersion
    version.setVid(vid)
    version.setWid(wid)
    version.setContent("{}")
    version.setCreationTime(now)
    new WorkflowVersionDao(getDSLContext.configuration()).insert(version)

    val execution = new WorkflowExecutions
    execution.setEid(eid)
    execution.setVid(vid)
    execution.setUid(uid)
    execution.setStatus(0.toByte)
    execution.setName(s"sync-execution-$suffix")
    execution.setEnvironmentVersion("test engine")
    execution.setStartingTime(now)
    new WorkflowExecutionsDao(getDSLContext.configuration()).insert(execution)

    ExecutionIdentity(eid.toLong)
  }

  private def buildExecutionService(
      stateStore: ExecutionStateStore
  ): org.apache.texera.web.service.WorkflowExecutionService = {
    val request = WorkflowExecuteRequest(
      executionName = "sync-test",
      engineVersion = "test",
      logicalPlan = LogicalPlanPojo(List.empty, List.empty, List.empty, List.empty),
      replayFromExecution = None,
      workflowSettings = WorkflowSettings(),
      emailNotificationEnabled = false,
      computingUnitId = 0
    )
    new org.apache.texera.web.service.WorkflowExecutionService(
      null,
      new WorkflowContext(),
      null,
      request,
      stateStore,
      (_: Throwable) => (),
      None,
      new URI("vfs:///sync-resource-test")
    )
  }

  private class StubWorkflowService(
      workflowId: WorkflowIdentity,
      computingUnitId: Int,
      initHook: StubWorkflowService => Unit
  ) extends WorkflowService(workflowId, computingUnitId, 1) {
    override def initExecutionService(
        req: WorkflowExecuteRequest,
        userOpt: Option[User],
        sessionUri: URI
    ): Unit = initHook(this)
  }

  private def workflowServiceMapping: ConcurrentHashMap[String, WorkflowService] = {
    val accessor = WorkflowService.getClass.getDeclaredMethod(
      "org$apache$texera$web$service$WorkflowService$$workflowServiceMapping"
    )
    accessor.setAccessible(true)
    accessor.invoke(WorkflowService).asInstanceOf[ConcurrentHashMap[String, WorkflowService]]
  }

  private def withRegisteredWorkflowService(
      service: WorkflowService
  )(body: => WorkflowExecutionSummary): WorkflowExecutionSummary = {
    val key = WorkflowService.mkWorkflowStateId(service.workflowId)
    workflowServiceMapping.put(key, service)
    try {
      body
    } finally {
      workflowServiceMapping.remove(key)
      service.unsubscribeAll()
    }
  }

  private def sessionUser(): SessionUser = {
    val user = new User
    user.setUid(nextId())
    user.setName("sync-session-user")
    user.setEmail("sync-session-user@example.com")
    new SessionUser(user)
  }

  private def syncRequest(timeoutSeconds: Int = 1): SyncExecutionRequest =
    SyncExecutionRequest(
      executionName = "sync-test",
      logicalPlan = LogicalPlanPojo(List.empty, List.empty, List.empty, List.empty),
      workflowSettings = None,
      targetOperatorIds = List.empty,
      timeoutSeconds = timeoutSeconds,
      maxOperatorResultCharLimit = 100000,
      maxOperatorResultCellCharLimit = 100000
    )

  private def runWithStubWorkflow(
      initHook: StubWorkflowService => Unit,
      request: SyncExecutionRequest = syncRequest()
  ): WorkflowExecutionSummary = {
    val workflowId = WorkflowIdentity(nextId().toLong)
    val service = new StubWorkflowService(workflowId, computingUnitId = 0, initHook)
    withRegisteredWorkflowService(service) {
      resource.executeWorkflowSync(workflowId.id, service.computingUnitId, request, sessionUser())
    }
  }

  private def failMetadataObservable(stateStore: ExecutionStateStore, error: Throwable): Unit = {
    val subjectField = stateStore.metadataStore.getClass.getDeclaredField("serializedSubject")
    subjectField.setAccessible(true)
    subjectField
      .get(stateStore.metadataStore)
      .asInstanceOf[io.reactivex.rxjava3.subjects.Subject[ExecutionMetadataStore]]
      .onError(error)
  }

  // handleExecutionError is private; reflectively invoke it (no production change needed).
  private val handleExecutionError =
    PrivateMethod[WorkflowExecutionSummary](Symbol("handleExecutionError"))

  private val collectOperatorInfos =
    PrivateMethod[Map[String, OperatorExecutionSummary]](Symbol("collectOperatorInfos"))

  private val collectOperatorResult =
    PrivateMethod[(String, Option[List[SampledRow]], Option[Int])](Symbol("collectOperatorResult"))

  private val symmetricTruncateCellValue =
    PrivateMethod[String](Symbol("symmetricTruncateCellValue"))

  private def classify(message: String): WorkflowExecutionSummary =
    resource invokePrivate handleExecutionError(new RuntimeException(message))

  "handleExecutionError" should "classify lowercase 'compilation' messages as CompilationFailed" in {
    val summary = classify("compilation failed for the plan")
    summary.success shouldBe false
    summary.state shouldBe "CompilationFailed"
    summary.operators shouldBe empty
    summary.errors should have size 1
    summary.errors.head.`type` shouldBe COMPILATION_ERROR
    summary.errors.head.message shouldBe "compilation failed for the plan"
  }

  it should "classify unrecognized messages as a generic Error" in {
    val summary = classify("something unexpected happened")
    summary.success shouldBe false
    summary.state shouldBe "Error"
    summary.operators shouldBe empty
    summary.errors should have size 1
    summary.errors.head.`type` shouldBe EXECUTION_FAILURE
    summary.errors.head.message shouldBe "something unexpected happened"
  }

  it should "fall back to 'Unknown error' when the exception has a null message" in {
    val summary = resource invokePrivate handleExecutionError(
      new RuntimeException(null.asInstanceOf[String])
    )
    summary.state shouldBe "Error"
    summary.errors should have size 1
    summary.errors.head.`type` shouldBe EXECUTION_FAILURE
    summary.errors.head.message shouldBe "Unknown error"
  }

  // --- sampleAndTruncateTuples (extracted from collectOperatorResult) ---------------------

  private val tableSchema = new Schema(List(new Attribute("col", AttributeType.STRING)))
  private def tableTuple(v: String): Tuple =
    Tuple.builder(tableSchema).add("col", AttributeType.STRING, v).build()

  private val mixedSchema = new Schema(
    List(
      new Attribute("col", AttributeType.STRING),
      new Attribute("number", AttributeType.INTEGER)
    )
  )
  private def mixedTuple(v: String, number: Int): Tuple =
    Tuple
      .builder(mixedSchema)
      .add("col", AttributeType.STRING, v)
      .add("number", AttributeType.INTEGER, number)
      .build()

  private def materializeResult(
      executionId: ExecutionIdentity,
      opId: String,
      tuples: List[Tuple]
  ): VirtualDocument[Tuple] = {
    val operatorId = OperatorIdentity(opId)
    val globalPort = GlobalPortIdentity(
      PhysicalOpIdentity(operatorId, "main"),
      PortIdentity(),
      input = false
    )
    val uri = VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(WorkflowIdentity(1L), executionId, globalPort)
    )
    val document =
      DocumentFactory.createDocument(uri, tableSchema).asInstanceOf[VirtualDocument[Tuple]]
    val writer =
      document
        .writer(s"sync-result-${executionId.id}-$opId")
        .asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
    tuples.foreach(writer.putOne)
    writer.close()
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .values(executionId.id.toInt, globalPort.serializeAsString, uri.toString)
      .execute()
    document
  }

  "collectOperatorResult" should "return no result summary when no result URI exists" in {
    val summary = resource invokePrivate collectOperatorResult(
      insertExecutionRow(),
      "missing-result-op",
      100000,
      100000
    )
    summary shouldBe (("table", None, None))
  }

  it should "sample rows from a materialized result document" in {
    val executionId = insertExecutionRow()
    val document =
      materializeResult(executionId, "result-op", List(tableTuple("a"), tableTuple("b")))
    try {
      val (mode, rows, total) = resource invokePrivate collectOperatorResult(
        executionId,
        "result-op",
        100000,
        100000
      )
      mode shouldBe "table"
      rows.get.map(_.rowIndex) shouldBe List(0, 1)
      rows.get.map(_.node.get("col").asText()) shouldBe List("a", "b")
      total shouldBe Some(2)
    } finally {
      document.clear()
    }
  }

  it should "fall back to no result summary when result lookup throws" in {
    val summary = resource invokePrivate collectOperatorResult(
      null.asInstanceOf[ExecutionIdentity],
      "bad-result-op",
      100000,
      100000
    )
    summary shouldBe (("table", None, None))

    val initializedLoggerMethod =
      classOf[SyncExecutionResource].getDeclaredMethod("logger$lzycompute")
    initializedLoggerMethod.setAccessible(true)
    initializedLoggerMethod.invoke(resource)

    val secondSummary = resource invokePrivate collectOperatorResult(
      null.asInstanceOf[ExecutionIdentity],
      "bad-result-op-again",
      100000,
      100000
    )
    secondSummary shouldBe (("table", None, None))

    val loggerField = classOf[SyncExecutionResource].getDeclaredField("logger")
    val loggerInitializedField = classOf[SyncExecutionResource].getDeclaredField("bitmap$trans$0")
    loggerField.setAccessible(true)
    loggerInitializedField.setAccessible(true)
    val originalLogger = loggerField.get(resource)
    val originalLoggerInitialized = loggerInitializedField.getBoolean(resource)
    try {
      loggerField.set(
        resource,
        com.typesafe.scalalogging.Logger(org.slf4j.helpers.NOPLogger.NOP_LOGGER)
      )
      loggerInitializedField.setBoolean(resource, true)
      val disabledLoggerSummary = resource invokePrivate collectOperatorResult(
        null.asInstanceOf[ExecutionIdentity],
        "bad-result-op-disabled-logger",
        100000,
        100000
      )
      disabledLoggerSummary shouldBe (("table", None, None))
    } finally {
      loggerField.set(resource, originalLogger)
      loggerInitializedField.setBoolean(resource, originalLoggerInitialized)
    }
  }

  "sampleAndTruncateTuples" should "report an empty table for a zero-count / empty iterator" in {
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(Iterator.empty, 0, 100000, 100000)
    mode shouldBe "table"
    rows shouldBe Some(List.empty[SampledRow])
    total shouldBe Some(0)
  }

  it should "report an empty table when the iterator is empty despite a positive count" in {
    // Exercises the `!tupleIterator.hasNext` half of the guard (count > 0, no rows).
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(Iterator.empty, 5, 100000, 100000)
    mode shouldBe "table"
    rows shouldBe Some(List.empty[SampledRow])
    total shouldBe Some(0)
  }

  it should "return visualization mode for a single visualization tuple" in {
    val vizSchema = new Schema(List(new Attribute("html-content", AttributeType.STRING)))
    val vizTuple =
      Tuple.builder(vizSchema).add("html-content", AttributeType.STRING, "<div>viz</div>").build()

    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(Iterator(vizTuple), 1, 100000, 100000)
    mode shouldBe "visualization"
    rows.get should have size 1
    rows.get.head.rowIndex shouldBe 0
    total shouldBe Some(1)
  }

  it should "return every row untruncated when the whole table fits (front-only path)" in {
    val tuples = List(tableTuple("a"), tableTuple("b"), tableTuple("c"))
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(tuples.iterator, tuples.size, 100000, 100000)
    mode shouldBe "table"
    rows.get should have size 3
    rows.get.map(_.rowIndex) shouldBe List(0, 1, 2)
    rows.get.map(_.node.get("col").asText()) shouldBe List("a", "b", "c")
    total shouldBe Some(3)
  }

  it should "truncate oversized text cells while preserving non-text fields" in {
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(Iterator(mixedTuple("abcdefghij", 42)), 1, 100000, 8)
    mode shouldBe "table"
    total shouldBe Some(1)
    rows.get.head.node.get("col").asText() shouldBe "abcdefgh"
    rows.get.head.node.get("number").asInt() shouldBe 42
  }

  it should "symmetrically truncate cells when enough room remains for both sides" in {
    val unchanged = resource invokePrivate symmetricTruncateCellValue("short", 10)
    val truncated = resource invokePrivate symmetricTruncateCellValue(
      "abcdefghijklmnopqrstuvwxyz",
      21
    )

    unchanged shouldBe "short"
    truncated shouldBe "ab...[truncated]...yz"
  }

  it should "keep only the first (truncated) row when a single tuple exceeds the char limit" in {
    // Tiny char limit so the first tuple's estimated size already exceeds it. The single
    // tuple is not a visualization tuple, so the oversized-first early return fires.
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(Iterator(tableTuple("hello world")), 1, 10, 100000)
    mode shouldBe "table"
    rows.get should have size 1
    rows.get.head.rowIndex shouldBe 0
    total shouldBe Some(1)
  }

  it should "drop the middle rows when the front fills and a sliding back-window runs" in {
    // ~31 chars/tuple, halfLimit = 100: the front fills after a few rows, then the inner
    // else switches to the sliding back-window and drops the middle rows.
    val tuples = (0 until 12).map(i => tableTuple("v" * 20)).toList
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(tuples.iterator, tuples.size, 200, 100000)
    mode shouldBe "table"
    total shouldBe Some(12)
    rows.get.size should be < 12
    // Front rows keep their original positions; the tail keeps the most recent rows.
    rows.get.head.rowIndex shouldBe 0
    rows.get.last.rowIndex shouldBe 11
  }

  it should "run the trailing back-window when the first row alone fills the front half" in {
    // First tuple's string is truncated by convertTuplesToJson to 103 chars, giving an
    // estimated size >= halfLimit (100) but < the char limit (200), so the front while-loop
    // never runs and the trailing back-window path handles the remaining rows.
    val tuples = tableTuple("x" * 150) :: (0 until 5).map(_ => tableTuple("y" * 20)).toList
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(tuples.iterator, tuples.size, 200, 100000)
    mode shouldBe "table"
    total shouldBe Some(6)
    rows.get.size should be < 6
    rows.get.head.rowIndex shouldBe 0
    rows.get.last.rowIndex shouldBe 5
  }

  it should "keep one oversized row in the trailing back-window" in {
    // The back-window limit is smaller than the first tail row. The sliding-window loop still
    // retains one row instead of dropping every row from the back sample.
    val tuples = List(tableTuple("x" * 150), tableTuple("y" * 80))
    val (mode, rows, total) =
      resource.sampleAndTruncateTuples(tuples.iterator, tuples.size, 200, 100000)
    mode shouldBe "table"
    total shouldBe Some(2)
    rows.get.map(_.rowIndex) shouldBe List(0, 1)
  }

  // --- buildOperatorExecutionSummary (extracted from collectOperatorInfos) ----------------

  "buildOperatorExecutionSummary" should "wire a result summary and no errors when only a result is present" in {
    val rows = List(sampleRow(0, "col", "v"))
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-1",
      state = "Completed",
      resultMode = "table",
      result = Some(rows),
      tuplesCount = Some(7),
      consoleLogs = None
    )
    summary.state shouldBe "Completed"
    summary.errorMessages shouldBe empty
    summary.consoleMessages shouldBe None
    summary.resultSummary.get.resultMode shouldBe "table"
    // sampled rows are emitted as (originalRowIndex, all-STRING Tuple) pairs
    val sampled = summary.resultSummary.get.sampleTuples
    sampled.map(_._1) shouldBe List(0)
    sampled.head._2.getField[String]("col") shouldBe "v"
    sampled.head._2.getSchema.getAttribute("col").getType shouldBe AttributeType.STRING
    summary.resultSummary.get.tuplesCount shouldBe 7
  }

  // Locks the wire contract the agent-service / frontend consumers depend on: each sampled
  // row serializes as a 2-element array [index, {schema:{attributes:[...]}, fields:[...]}].
  it should "serialize sampled rows as [index, {schema, fields}]" in {
    val scalaMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val tuple =
      Tuple(Schema(List(new Attribute("col", AttributeType.STRING))), Array[Any]("v"))
    val summary =
      OperatorResultSummary(resultMode = "table", sampleTuples = List((0, tuple)), tuplesCount = 1)
    val firstRow =
      scalaMapper.readTree(scalaMapper.writeValueAsString(summary)).get("sampleTuples").get(0)
    firstRow.get(0).asInt() shouldBe 0
    firstRow.get(1).get("fields").get(0).asText() shouldBe "v"
    val attr = firstRow.get(1).get("schema").get("attributes").get(0)
    attr.get("attributeName").asText() shouldBe "col"
    attr.get("attributeType").asText() shouldBe "string"
  }

  it should "default a present result summary to zero tuples when the count is absent" in {
    val rows = List(sampleRow(0, "col", "v"))
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-1",
      state = "Completed",
      resultMode = "table",
      result = Some(rows),
      tuplesCount = None,
      consoleLogs = None
    )
    summary.resultSummary.get.tuplesCount shouldBe 0
  }

  it should "surface a console ERROR as one EXECUTION_FAILURE error using the longer of title/message" in {
    val logs = List(
      ConsoleMessageSummary(msgType = "PRINT", title = "noise", message = "ignored"),
      ConsoleMessageSummary(msgType = "ERROR", title = "short", message = "a much longer message")
    )
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-9",
      state = "Failed",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = Some(logs)
    )
    summary.errorMessages should have size 1
    summary.errorMessages.head.`type` shouldBe EXECUTION_FAILURE
    summary.errorMessages.head.message shouldBe "a much longer message"
    summary.errorMessages.head.operatorId shouldBe "op-9"
    summary.consoleMessages.get should have size 2
  }

  it should "keep the ERROR title when it is longer than the message" in {
    val logs =
      List(
        ConsoleMessageSummary(msgType = "ERROR", title = "a long descriptive title", message = "")
      )
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-2",
      state = "Failed",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = Some(logs)
    )
    summary.errorMessages.head.message shouldBe "a long descriptive title"
  }

  it should "keep the ERROR title when the message is non-empty but shorter" in {
    // Exercises `message.nonEmpty` true AND `message.length > title.length` false.
    val logs =
      List(
        ConsoleMessageSummary(
          msgType = "ERROR",
          title = "a fairly long error title",
          message = "short"
        )
      )
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-5",
      state = "Failed",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = Some(logs)
    )
    summary.errorMessages.head.message shouldBe "a fairly long error title"
  }

  it should "leave the result summary empty when no result was materialized" in {
    val summary = resource.buildOperatorExecutionSummary(
      opId = "op-3",
      state = "Uninitialized",
      resultMode = "table",
      result = None,
      tuplesCount = None,
      consoleLogs = None
    )
    summary.resultSummary shouldBe None
    summary.consoleMessages shouldBe None
    summary.errorMessages shouldBe empty
  }

  // --- collectOperatorInfos (private wrapper around state/result/console collection) -------

  "collectOperatorInfos" should "return an empty map when there are no target or stats operators" in {
    val stateStore = new ExecutionStateStore
    val executionService = buildExecutionService(stateStore)

    val operatorInfos = resource invokePrivate collectOperatorInfos(
      ExecutionIdentity(1L),
      executionService,
      List.empty[String],
      100000,
      100000,
      None
    )

    operatorInfos shouldBe empty
  }

  it should "summarize an explicit target even when stats are absent" in {
    val stateStore = new ExecutionStateStore
    val executionService = buildExecutionService(stateStore)

    val operatorInfos = resource invokePrivate collectOperatorInfos(
      insertExecutionRow(),
      executionService,
      List("target-op"),
      100000,
      100000,
      None
    )

    operatorInfos.keySet shouldBe Set("target-op")
    operatorInfos("target-op").state shouldBe "Unknown"
    operatorInfos("target-op").resultSummary shouldBe None
    operatorInfos("target-op").consoleMessages shouldBe None
  }

  it should "include in-memory console-error operators that are not target operators" in {
    val stateStore = new ExecutionStateStore
    val executionService = buildExecutionService(stateStore)
    val consoleMessage = new ConsoleMessage(
      "worker-1",
      Timestamp(Instant.now),
      ConsoleMessageType.ERROR,
      "source",
      "short",
      "a longer in-memory console error"
    )
    val consoleState = ConsoleMessageProcessor.addMessageToOperatorConsole(
      new ExecutionConsoleStore(),
      "console-op",
      consoleMessage,
      10
    )

    val operatorInfos = resource invokePrivate collectOperatorInfos(
      insertExecutionRow(),
      executionService,
      List.empty[String],
      100000,
      100000,
      Some(consoleState)
    )

    operatorInfos.keySet shouldBe Set("console-op")
    operatorInfos("console-op").errorMessages should have size 1
    operatorInfos(
      "console-op"
    ).errorMessages.head.message shouldBe "a longer in-memory console error"
    operatorInfos("console-op").consoleMessages.get.map(_.msgType) shouldBe List("ERROR")
  }

  it should "map operator stats state when the stats store has target metrics" in {
    val stateStore = new ExecutionStateStore
    stateStore.statsStore.updateState(_ =>
      ExecutionStatsStore(
        operatorInfo = Map(
          "stats-op" -> OperatorMetrics(
            operatorState = WorkflowAggregatedState.KILLED,
            operatorStatistics = OperatorStatistics()
          )
        )
      )
    )
    val executionService = buildExecutionService(stateStore)

    val operatorInfos = resource invokePrivate collectOperatorInfos(
      insertExecutionRow(),
      executionService,
      List("stats-op"),
      100000,
      100000,
      None
    )

    operatorInfos("stats-op").state shouldBe "Killed"
  }

  // --- executeWorkflowSync public branches ------------------------------------------------

  "executeWorkflowSync" should "return an error when init does not publish an execution service" in {
    val summary = runWithStubWorkflow(_ => ())

    summary.success shouldBe false
    summary.state shouldBe "Error"
    summary.operators shouldBe empty
    summary.errors should have size 1
    summary.errors.head.`type` shouldBe EXECUTION_FAILURE
    summary.errors.head.message shouldBe "Failed to initialize execution service"
  }

  it should "return an error when waiting for execution state fails" in {
    val stateStore = new ExecutionStateStore
    val executionService = buildExecutionService(stateStore)

    val summary = runWithStubWorkflow(
      service => {
        service.executionService.onNext(executionService)
        val failThread = new Thread(() => {
          Thread.sleep(50)
          failMetadataObservable(stateStore, new RuntimeException("metadata stream failed"))
        })
        failThread.setDaemon(true)
        failThread.start()
      },
      syncRequest(timeoutSeconds = 1)
    )

    summary.success shouldBe false
    summary.state shouldBe "Error"
    summary.operators shouldBe empty
    summary.errors should have size 1
    summary.errors.head.`type` shouldBe EXECUTION_FAILURE
    summary.errors.head.message shouldBe "metadata stream failed"
  }

  it should "assemble a completed summary when execution is already terminal" in {
    val stateStore = new ExecutionStateStore
    stateStore.metadataStore.updateState(_.withState(WorkflowAggregatedState.COMPLETED))
    val executionService = buildExecutionService(stateStore)

    val summary = runWithStubWorkflow(_.executionService.onNext(executionService))

    summary.success shouldBe true
    summary.state shouldBe "Completed"
    summary.operators shouldBe empty
    summary.errors shouldBe empty
  }

  // --- assembleExecutionSummary (extracted from executeWorkflowSync) ----------------------

  private def metadataStore(
      state: WorkflowAggregatedState,
      fatalErrors: Seq[WorkflowFatalError] = Seq.empty
  ): ExecutionMetadataStore =
    ExecutionMetadataStore(
      state = state,
      fatalErrors = fatalErrors,
      executionId = ExecutionIdentity(0L)
    )

  private def failingOperatorSummary: OperatorExecutionSummary =
    OperatorExecutionSummary(
      state = "Failed",
      errorMessages =
        List(WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "err", "", "op1")),
      resultSummary = None,
      consoleMessages = None
    )

  "assembleExecutionSummary" should "report success for a COMPLETED run with no errors" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.COMPLETED),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.success shouldBe true
    summary.state shouldBe "Completed"
    summary.errors shouldBe empty
    summary.operators shouldBe empty
  }

  it should "map a non-terminal-success final state through stateToString" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.FAILED),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.success shouldBe false
    summary.state shouldBe "Failed"
  }

  it should "force a Failed state when terminated by a console error, regardless of final state" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.COMPLETED),
      operatorInfos = Map.empty,
      terminatedByConsoleError = true,
      terminatedByTargetResults = false
    )
    summary.state shouldBe "Failed"
    summary.success shouldBe false
  }

  it should "override to Completed/success when terminated by target results on a non-completed state" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.RUNNING),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = true
    )
    summary.state shouldBe "Completed"
    summary.success shouldBe true
  }

  it should "prefer console-error failure over target-results completion" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.RUNNING),
      operatorInfos = Map.empty,
      terminatedByConsoleError = true,
      terminatedByTargetResults = true
    )
    summary.state shouldBe "Failed"
    summary.success shouldBe false
  }

  it should "mark the run unsuccessful when any operator reports console errors" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.COMPLETED),
      operatorInfos = Map("op1" -> failingOperatorSummary),
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.success shouldBe false
    summary.state shouldBe "Completed"
  }

  it should "mark target-results completion unsuccessful when an operator reports console errors" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(WorkflowAggregatedState.RUNNING),
      operatorInfos = Map("op1" -> failingOperatorSummary),
      terminatedByConsoleError = false,
      terminatedByTargetResults = true
    )
    summary.success shouldBe false
    summary.state shouldBe "Completed"
  }

  it should "surface each final-state fatal error" in {
    val summary = resource.assembleExecutionSummary(
      finalState = metadataStore(
        WorkflowAggregatedState.COMPLETED,
        Seq(WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "boom", "", "op1"))
      ),
      operatorInfos = Map.empty,
      terminatedByConsoleError = false,
      terminatedByTargetResults = false
    )
    summary.errors should have size 1
    summary.errors.head.`type` shouldBe EXECUTION_FAILURE
    summary.errors.head.message shouldBe "boom"
  }
}
