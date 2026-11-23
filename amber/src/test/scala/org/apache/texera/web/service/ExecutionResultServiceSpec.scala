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

package org.apache.texera.web.service

import com.fasterxml.jackson.databind.node.ObjectNode
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.{ActorSystem, Cancellable}
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.storage.model.BufferedItemWriter
import org.apache.texera.amber.core.storage.result.{OperatorResultMetadata, WorkflowResultStore}
import org.apache.texera.amber.core.storage.{
  DocumentFactory,
  VFSURIFactory,
  WarehouseUnavailableException
}
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.OutputPort.OutputMode
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  OutputPort,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  ExecutionStateUpdate,
  FatalError,
  OperatorPortResultUriAvailable
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{
  OPERATOR_PORT_EXECUTIONS,
  USER,
  WORKFLOW,
  WORKFLOW_COMPUTING_UNIT,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.enums.{OutputModeEnum, WorkflowComputingUnitTypeEnum}
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowComputingUnitDao,
  WorkflowDao,
  WorkflowExecutionsDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowComputingUnit,
  WorkflowExecutions,
  WorkflowVersion
}
import org.apache.texera.web.model.websocket.event.{
  PaginatedResultEvent,
  TexeraWebSocketEvent,
  WebResultUpdateEvent
}
import org.apache.texera.web.model.websocket.request.ResultPaginationRequest
import org.apache.texera.web.service.ExecutionResultService.{
  PaginationMode,
  SetDeltaMode,
  SetSnapshotMode,
  WebDataUpdate,
  WebPaginationUpdate
}
import org.apache.texera.web.storage.{ExecutionStateStore, WorkflowStateStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import java.net.URI
import java.sql.Timestamp
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import java.util.UUID
import scala.concurrent.Await
import scala.concurrent.duration.DurationInt

class ExecutionResultServiceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  // Fixed (not random) so a failure replays identically across runs. The
  // spec owns its embedded DB so collision with other specs isn't a concern.
  private val testWid: Integer = 9001
  private val testUid: Integer = 9001
  private var executionsDao: WorkflowExecutionsDao = _
  private var testVid: Integer = _
  private var testCuid: Integer = _

  // AmberClient needs an ActorSystem to host its ClientActor. A bare one is enough:
  // the client is constructed over an empty PhysicalPlan, so its InitializeRequest
  // completes without an engine, and the subclass below never sends anything to it.
  private var system: ActorSystem = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    // Suffixed so two ActorSystems can never contend for the same name if this spec is ever run
    // alongside another in one JVM.
    system = ActorSystem(s"ExecutionResultServiceSpec-${UUID.randomUUID()}")
  }

  override protected def afterAll(): Unit = {
    // terminate() is asynchronous; without the await the JVM can move on to shutdownDB() while the
    // dispatcher threads are still running, which is how this kind of fixture starts hanging.
    Await.ready(system.terminate(), 30.seconds)
    shutdownDB()
  }

  override protected def beforeEach(): Unit = {
    val user = new User
    user.setUid(testUid)
    user.setName("execution-result-test-user")
    user.setEmail(s"u$testUid@example.com")
    new UserDao(getDSLContext.configuration()).insert(user)

    // getLatestExecutionId matches on cuid, and a NULL cuid matches no value, so
    // handleResultPagination can only find an execution that names a real unit.
    val computingUnit = new WorkflowComputingUnit
    computingUnit.setUid(testUid)
    computingUnit.setName("execution-result-test-unit")
    computingUnit.setCreationTime(new Timestamp(System.currentTimeMillis()))
    computingUnit.setType(WorkflowComputingUnitTypeEnum.local)
    computingUnit.setUri("local://execution-result-test")
    computingUnit.setResource("{}")
    new WorkflowComputingUnitDao(getDSLContext.configuration()).insert(computingUnit)
    testCuid = computingUnit.getCuid

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(s"execution-result-test-$testWid")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(getDSLContext.configuration()).insert(version)
    // The vid sequence isn't reset between tests, so capture the
    // generated key here instead of assuming it's `1` later.
    testVid = version.getVid

    executionsDao = new WorkflowExecutionsDao(getDSLContext.configuration())
  }

  /** Every ResultEvents subscription made during a test, disposed when that test ends. */
  private val openObservers = mutable.ArrayBuffer.empty[Disposable]

  override protected def afterEach(): Unit = {
    openObservers.foreach(_.dispose())
    openObservers.clear()

    val ctx = getDSLContext
    // Scope every delete to the test's own ids so this spec stays safe
    // if it ever shares a DB with another spec.
    ctx
      .deleteFrom(OPERATOR_PORT_EXECUTIONS)
      .where(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.in(
          ctx
            .select(WORKFLOW_EXECUTIONS.EID)
            .from(WORKFLOW_EXECUTIONS)
            .where(WORKFLOW_EXECUTIONS.UID.eq(testUid))
        )
      )
      .execute()
    ctx
      .deleteFrom(WORKFLOW_EXECUTIONS)
      .where(WORKFLOW_EXECUTIONS.UID.eq(testUid))
      .execute()
    ctx.deleteFrom(WORKFLOW_VERSION).where(WORKFLOW_VERSION.WID.eq(testWid)).execute()
    ctx.deleteFrom(WORKFLOW).where(WORKFLOW.WID.eq(testWid)).execute()
    ctx.deleteFrom(WORKFLOW_COMPUTING_UNIT).where(WORKFLOW_COMPUTING_UNIT.UID.eq(testUid)).execute()
    ctx.deleteFrom(USER).where(USER.UID.eq(testUid)).execute()
  }

  "persistOperatorPortResultUri" should
    "insert the URI carried by an OperatorPortResultUriAvailable event" in {
    val execution = new WorkflowExecutions
    execution.setVid(testVid)
    execution.setUid(testUid)
    execution.setStatus(0.toByte)
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("execution-result-callback-test")
    execution.setEnvironmentVersion("test-env")
    executionsDao.insert(execution)
    val eid = ExecutionIdentity(execution.getEid.longValue())
    val globalPortId = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity("op-X"), "main"),
      PortIdentity(),
      input = false
    )
    val uri = URI.create("vfs:///exec-result-callback")

    ExecutionResultService.persistOperatorPortResultUri(
      eid,
      OperatorPortResultUriAvailable(globalPortId, uri, OutputMode.SINGLE_SNAPSHOT)
    )

    val rows = getDSLContext
      .selectFrom(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(execution.getEid))
      .and(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(globalPortId.serializeAsString))
      .fetch()
    rows.size() shouldBe 1
    rows.get(0).getResultUri shouldBe uri.toString
    // The mode is recorded with the URI, so a finished execution's results can be
    // listed without the engine's physical plan (#8994).
    rows.get(0).getOutputMode shouldBe OutputModeEnum.SINGLE_SNAPSHOT
  }

  "toOutputModeEnum" should "map every declared output mode to the stored value of its name" in {
    // Walks the proto's values, so a new mode missing from the match fails here, not at runtime.
    OutputMode.values.foreach { mode =>
      ExecutionResultService.toOutputModeEnum(mode).getLiteral shouldBe mode.name
    }
  }

  it should "reject an unrecognized mode" in {
    a[RuntimeException] should be thrownBy
      ExecutionResultService.toOutputModeEnum(OutputMode.Unrecognized(99))
  }

  // The WebOutputMode / WebResultUpdate ADTs are serialized to the frontend over
  // the websocket; the "type" discriminator is the contract the UI dispatches on.

  "WebOutputMode serialization" should "tag each mode with its discriminator" in {
    objectMapper
      .valueToTree[ObjectNode](PaginationMode())
      .get("type")
      .asText() shouldBe "PaginationMode"
    objectMapper
      .valueToTree[ObjectNode](SetSnapshotMode())
      .get("type")
      .asText() shouldBe "SetSnapshotMode"
    objectMapper
      .valueToTree[ObjectNode](SetDeltaMode())
      .get("type")
      .asText() shouldBe "SetDeltaMode"
  }

  "WebPaginationUpdate serialization" should "carry the total count, dirty pages, and mode discriminator" in {
    val json =
      objectMapper.valueToTree[ObjectNode](WebPaginationUpdate(PaginationMode(), 7L, List(1, 3)))
    json.get("mode").get("type").asText() shouldBe "PaginationMode"
    json.get("totalNumTuples").asLong() shouldBe 7L
    json.get("dirtyPageIndices").elements().asScala.map(_.asInt()).toList shouldBe List(1, 3)
  }

  "WebDataUpdate serialization" should "carry the mode discriminator and the table rows" in {
    val row = objectMapper.createObjectNode()
    row.put("k", "v")
    val json = objectMapper.valueToTree[ObjectNode](WebDataUpdate(SetSnapshotMode(), List(row)))
    json.get("mode").get("type").asText() shouldBe "SetSnapshotMode"
    val table = json.get("table")
    table.size() shouldBe 1
    table.get(0).get("k").asText() shouldBe "v"
  }

  // ==========================================================================
  // The service instance: handleResultPagination, and the callbacks / result
  // diff handler that attachToExecution wires up.
  //
  // Two seams make this reachable without an engine:
  //   * `client` is a parameter of attachToExecution and `registerCallback` is
  //     overridable, so `RecordingAmberClient` captures the callbacks the
  //     service registers and each test fires one directly.
  //   * the result-polling `Cancellable` is created by AmberRuntime's scheduler
  //     once the execution reaches RUNNING. AmberRuntime holds its ActorSystem
  //     in a private var that only a started pekko cluster fills in, so the
  //     service's own private field is set directly (`setPollingCancellable`)
  //     to reproduce the post-RUNNING state that the terminal-state, fatal-error
  //     and re-attach paths all assume. Nothing global is mutated, and no
  //     RUNNING state is ever pushed through the metadata store.
  //
  // The result documents are real Iceberg tables. Their storage key is derived
  // from the URI path, and creating one overwrites whatever is already there, so
  // every logical operator id below carries an `erss-` prefix that is unique in
  // the repository -- sbt runs amber suites in parallel inside one JVM.
  // ==========================================================================

  private val paginationRequest =
    ResultPaginationRequest(requestID = "req-1", operatorID = "", pageIndex = 1, pageSize = 3)

  "handleResultPagination" should "return the rows of the requested page" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(new Attribute("id", AttributeType.INTEGER), new Attribute("label", AttributeType.STRING))
    )
    storeResult(
      executionId,
      "erss-page-rows",
      schema,
      (0 until 7).map(i =>
        Tuple
          .builder(schema)
          .add("id", AttributeType.INTEGER, i)
          .add("label", AttributeType.STRING, s"row-$i")
          .build()
      )
    )

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-page-rows", pageIndex = 2, pageSize = 3)
    )

    // Page 2 of size 3 is the half-open range [3, 6): both the `pageSize *
    // (pageIndex - 1)` origin and the `from + pageSize` bound are pinned by the
    // exact id list, which shifts under an off-by-one on either end.
    event.table.map(_.get("id").asInt()) shouldBe List(3, 4, 5)
    event.table.map(_.get("label").asText()) shouldBe List("row-3", "row-4", "row-5")
    // The request fields are echoed back so the frontend can match the reply to
    // the outstanding page request.
    event.requestID shouldBe "req-1"
    event.operatorID shouldBe "erss-page-rows"
    event.pageIndex shouldBe 2
    event.schema.map(_.getName) shouldBe List("id", "label")
  }

  it should "clamp the last page to the rows that exist" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-page-rows",
      schema,
      (0 until 7).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-page-rows", pageIndex = 3, pageSize = 3)
    )

    event.table.map(_.get("id").asInt()) shouldBe List(6)
  }

  it should "return an empty page with the stored schema past the end of the result" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-page-rows",
      schema,
      (0 until 7).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-page-rows", pageIndex = 4, pageSize = 3)
    )

    event.table shouldBe empty
    event.schema.map(_.getName) shouldBe List("id")
  }

  it should "report the stored columns of a result with no rows" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(new Attribute("id", AttributeType.INTEGER), new Attribute("label", AttributeType.STRING))
    )
    storeResult(executionId, "erss-no-rows", schema, List.empty)

    val event = paginate(paginationRequest.copy(operatorID = "erss-no-rows"))

    event.table shouldBe empty
    event.schema.map(a => (a.getName, a.getType)) shouldBe
      List(("id", AttributeType.INTEGER), ("label", AttributeType.STRING))
  }

  it should "match a column search case-insensitively" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(
        new Attribute("userName", AttributeType.STRING),
        new Attribute("UserAge", AttributeType.STRING),
        new Attribute("city", AttributeType.STRING)
      )
    )
    storeResult(
      executionId,
      "erss-col-search",
      schema,
      List(
        Tuple
          .builder(schema)
          .add("userName", AttributeType.STRING, "ada")
          .add("UserAge", AttributeType.STRING, "36")
          .add("city", AttributeType.STRING, "london")
          .build()
      )
    )

    // "eRNa" matches "userName" only when BOTH sides are lower-cased: dropping
    // the column's `.toLowerCase` leaves "userName".contains("erna") false, and
    // dropping the search term's leaves "username".contains("eRNa") false.
    val event = paginate(
      paginationRequest
        .copy(operatorID = "erss-col-search", columnSearch = Some("eRNa"))
    )

    event.table.map(_.fieldNames().asScala.toList) shouldBe List(List("userName"))
    event.table.head.get("userName").asText() shouldBe "ada"
    event.schema.map(_.getName) shouldBe List("userName")
  }

  it should "slice the projected columns by offset and limit" in {
    val executionId = newExecution()
    val columnNames = List("c0", "c1", "c2", "c3")
    val schema = new Schema(columnNames.map(new Attribute(_, AttributeType.STRING)))
    storeResult(
      executionId,
      "erss-col-slice",
      schema,
      List(
        columnNames
          .foldLeft(Tuple.builder(schema))((b, c) => b.add(c, AttributeType.STRING, s"v-$c"))
          .build()
      )
    )

    // slice's second argument is an end index, not a count: `slice(offset, limit)`
    // would return only "c1" here.
    val event = paginate(
      paginationRequest.copy(operatorID = "erss-col-slice", columnOffset = 1, columnLimit = 2)
    )

    event.table.map(_.fieldNames().asScala.toList) shouldBe List(List("c1", "c2"))
    event.schema.map(_.getName) shouldBe List("c1", "c2")
  }

  it should "read the output port the request names, and count that port's rows" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val rows = (n: Int, offset: Int) =>
      (0 until n).map(i =>
        Tuple.builder(schema).add("id", AttributeType.INTEGER, offset + i).build()
      )
    // Two output ports of one operator, as a Split keeps them, with different row counts.
    storeResult(executionId, "erss-two-ports", schema, rows(2, 0))
    storeResult(executionId, "erss-two-ports", schema, rows(5, 100), port = PortIdentity(1))

    val first = paginate(paginationRequest.copy(operatorID = "erss-two-ports", pageSize = 10))
    val second = paginate(
      paginationRequest.copy(operatorID = "erss-two-ports", pageSize = 3, portIndex = 1)
    )

    // Without a port, the first one is read, as before.
    first.table.map(_.get("id").asInt()) shouldBe List(0, 1)
    first.portIndex shouldBe 0
    first.totalTuples shouldBe 2
    // The second port's rows, and its own count, which pages the port past this first page.
    second.table.map(_.get("id").asInt()) shouldBe List(100, 101, 102)
    second.portIndex shouldBe 1
    second.totalTuples shouldBe 5
  }

  it should "return an empty page when the operator has no stored result" in {
    newExecution()

    val event = paginate(
      paginationRequest.copy(operatorID = "erss-never-stored", pageIndex = 2)
    )

    // The reply still has to identify the request it answers, otherwise the
    // frontend cannot retire the pending page.
    event.requestID shouldBe "req-1"
    event.operatorID shouldBe "erss-never-stored"
    event.pageIndex shouldBe 2
    event.table shouldBe empty
    event.schema shouldBe empty
  }

  it should "fail loudly when the workflow has never been executed" in {
    // No execution row at all: distinct from the case above, where an execution
    // exists but stored no result for the operator.
    val thrown = the[IllegalStateException] thrownBy paginate(
      paginationRequest.copy(operatorID = "erss-page-rows")
    )
    thrown.getMessage shouldBe "No execution is recorded"
  }

  it should "refuse to read a result whose warehouse the URI cannot resolve" in {
    val executionId = newExecution()
    // Only the URI row is needed: the guard has to reject before the document is
    // opened, and no table was ever created for this warehouse. An unresolvable
    // /wh/ prefix is what the guard refuses in *either* flag state, so this pins
    // the wiring — that paginate consults the guard at all — without depending on
    // the shipping default. WarehouseReadGuardSpec pins the enabled and disabled
    // behaviour of the guard itself.
    val resolvable = resultUriOf(executionId, "erss-warehouse", Some("byo"))
    insertResultUri(executionId, new URI(resolvable.toString.replace("/wh/byo/", "/wh/a%2Fb/")))

    a[WarehouseUnavailableException] should be thrownBy paginate(
      paginationRequest.copy(operatorID = "erss-warehouse")
    )
  }

  // -- attachToExecution: the result-store diff handler -----------------------

  "the result diff handler" should "send a pagination update for a SET_SNAPSHOT output port" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-snapshot",
      schema,
      (0 until 7).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )
    // The internal port is listed FIRST so that dropping the `!portId.internal`
    // filter would read SET_DELTA's mode instead of the external port's.
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-snapshot",
        PortIdentity(1, internal = true) -> OutputMode.SET_DELTA,
        PortIdentity() -> OutputMode.SET_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-snapshot" -> 7)

      // 7 rows at the default page size of 5 dirty two pages; the count sent to
      // the frontend is the new count, not the old one.
      events.updates.map(_.updates) shouldBe List(
        Map("erss-snapshot" -> Map(0 -> WebPaginationUpdate(PaginationMode(), 7L, List(1, 2))))
      )
    }
  }

  it should "send the per-column statistics of a table-mode result" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(new Attribute("id", AttributeType.INTEGER), new Attribute("name", AttributeType.STRING))
    )
    storeResult(
      executionId,
      "erss-snapshot-stats",
      schema,
      List(
        Tuple
          .builder(schema)
          .add("id", AttributeType.INTEGER, 1)
          .add("name", AttributeType.STRING, "ada")
          .build()
      )
    )
    val plan = planOf(
      physicalOp(executionId, "erss-snapshot-stats", PortIdentity() -> OutputMode.SET_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-snapshot-stats" -> 1)

      events.updates.head.tableStats.keySet shouldBe Set("erss-snapshot-stats")
      events.updates.head.tableStats("erss-snapshot-stats").keySet shouldBe Set(0)
      events.updates.head.tableStats("erss-snapshot-stats")(0).keySet shouldBe Set("id", "name")
    }
  }

  it should "record the size and row count of a table-mode result with no browser connected" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val uri = storeResult(
      executionId,
      "erss-snapshot-count",
      schema,
      (1 to 3).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )
    val plan = planOf(
      physicalOp(executionId, "erss-snapshot-count", PortIdentity() -> OutputMode.SET_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      // No ResultEvents: the run finishes after its browser tab has closed (#8994).
      setPollingCancellable(fixture.service, new RecordingCancellable)
      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.COMPLETED))

      storedResultCount(executionId, "erss-snapshot-count") shouldBe 3L
      // The document's own file size, not some other measure of the result. Nothing is
      // closed afterwards because VirtualDocument exposes no close(); its only teardown is
      // clear(), which deletes the data rather than releasing a handle.
      val expectedSize = DocumentFactory.openDocument(uri)._1.getTotalFileSize
      expectedSize should not be 0L
      storedResultSize(executionId, "erss-snapshot-count") shouldBe expectedSize
    }
  }

  it should "record no row count for a SINGLE_SNAPSHOT port" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("html", AttributeType.STRING)))
    storeResult(
      executionId,
      "erss-single-count",
      schema,
      List(Tuple.builder(schema).add("html", AttributeType.STRING, "<p/>").build())
    )
    val plan = planOf(
      physicalOp(executionId, "erss-single-count", PortIdentity() -> OutputMode.SINGLE_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      setPollingCancellable(fixture.service, new RecordingCancellable)
      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.COMPLETED))

      // A visualization is shown whole, never paged, so it has no count to record.
      storedResultCount(executionId, "erss-single-count") shouldBe null
    }
  }

  it should "send the whole untruncated snapshot, and no statistics, for a SINGLE_SNAPSHOT port" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("html", AttributeType.STRING)))
    val html = "a" * 150
    // Two rows, so that reading only the head of the document would be visible.
    storeResult(
      executionId,
      "erss-single",
      schema,
      List(html, "<p>second</p>")
        .map(Tuple.builder(schema).add("html", AttributeType.STRING, _).build())
    )
    val plan =
      planOf(physicalOp(executionId, "erss-single", PortIdentity() -> OutputMode.SINGLE_SNAPSHOT))

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-single" -> 2)

      val update = dataUpdate(events.updates.head.updates("erss-single")(0))
      update.mode shouldBe SetSnapshotMode()
      // SINGLE_SNAPSHOT carries rendered HTML, so the 100-character string
      // truncation must be switched off for it.
      update.table.map(_.get("html").asText()) shouldBe List(html, "<p>second</p>")
      // A SINGLE_SNAPSHOT port's document is not a table, so no statistics are
      // gathered and no size is recorded for it -- while the data update itself
      // is still sent.
      events.updates.head.tableStats shouldBe empty
      storedResultSize(executionId, "erss-single") shouldBe 0L
    }
  }

  it should "send only the rows added since the previous update for a SET_DELTA port" in {
    val executionId = newExecution()
    val schema = new Schema(
      List(new Attribute("id", AttributeType.INTEGER), new Attribute("note", AttributeType.STRING))
    )
    val longNote = "b" * 150
    storeResult(
      executionId,
      "erss-delta",
      schema,
      (0 until 5).map(i =>
        Tuple
          .builder(schema)
          .add("id", AttributeType.INTEGER, i)
          .add("note", AttributeType.STRING, longNote)
          .build()
      )
    )
    val plan = planOf(physicalOp(executionId, "erss-delta", PortIdentity() -> OutputMode.SET_DELTA))

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)

      fixture.setResultCounts("erss-delta" -> 2)
      fixture.setResultCounts("erss-delta" -> 5)

      val tables =
        events.updates.map(e =>
          dataUpdate(e.updates("erss-delta")(0)).table.map(_.get("id").asInt())
        )
      // The delta is read from the PREVIOUS count, so the second update repeats
      // nothing: reading from the new count would send an empty table, and
      // reading from zero would resend rows 0 and 1.
      tables shouldBe List(List(0, 1, 2, 3, 4), List(2, 3, 4))

      val deltaUpdate = dataUpdate(events.updates.head.updates("erss-delta")(0))
      deltaUpdate.mode shouldBe SetDeltaMode()
      // Unlike SINGLE_SNAPSHOT, a delta is table data, so long strings are
      // truncated for display.
      deltaUpdate.table.head.get("note").asText() shouldBe ("b" * 100 + "...")
    }
  }

  it should "send an empty pagination update when the result is not stored yet" in {
    val executionId = newExecution()
    val plan =
      planOf(physicalOp(executionId, "erss-unstored", PortIdentity() -> OutputMode.SET_SNAPSHOT))

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-unstored" -> 4)

      // Storage only exists once the operator's region has been scheduled. Until
      // then the reported count is zero, not the count the engine reported.
      events.updates.map(_.updates) shouldBe List(
        Map("erss-unstored" -> Map(0 -> WebPaginationUpdate(PaginationMode(), 0L, List.empty)))
      )
    }
  }

  it should "reject an output mode it does not recognize" in {
    val executionId = newExecution()
    val plan = planOf(
      physicalOp(executionId, "erss-bad-mode", PortIdentity() -> OutputMode.Unrecognized(99))
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-bad-mode" -> 1)

      events.updates shouldBe empty
      // The unrecognized enum value renders as "UNRECOGNIZED", so the workflow id
      // is the only thing in the message that identifies what failed.
      events.errors.map(_.getMessage) shouldBe List(
        s"Unrecognized output mode: UNRECOGNIZED for workflow $testWid"
      )
    }
  }

  it should "update only the operators whose tuple count changed" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val rows = List(Tuple.builder(schema).add("id", AttributeType.INTEGER, 1).build())
    storeResult(executionId, "erss-changed-a", schema, rows)
    storeResult(executionId, "erss-changed-b", schema, rows)
    val plan = planOf(
      physicalOp(executionId, "erss-changed-a", PortIdentity() -> OutputMode.SET_SNAPSHOT),
      physicalOp(executionId, "erss-changed-b", PortIdentity() -> OutputMode.SET_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setResultCounts("erss-changed-a" -> 1, "erss-changed-b" -> 1)
      fixture.setResultCounts("erss-changed-a" -> 1, "erss-changed-b" -> 2)

      // Recomputing an unchanged operator would re-read its whole document on
      // every poll, so the second diff must mention only the operator that moved.
      events.updates.map(_.updates.keySet) shouldBe List(
        Set("erss-changed-a", "erss-changed-b"),
        Set("erss-changed-b")
      )
    }
  }

  it should "send every output port's update and statistics, keyed by its port index" in {
    val executionId = newExecution()
    // Different columns per port, so statistics read off the wrong port are visible.
    val firstSchema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val secondSchema = new Schema(List(new Attribute("value", AttributeType.INTEGER)))
    storeResult(executionId, "erss-ports", firstSchema, intRows(firstSchema, "id", 0 until 3))
    storeResult(
      executionId,
      "erss-ports",
      secondSchema,
      intRows(secondSchema, "value", 0 until 12),
      port = PortIdentity(1)
    )
    // The internal port is listed first, as in the single-port test above.
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-ports",
        PortIdentity(1, internal = true) -> OutputMode.SET_DELTA,
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setPortResultCounts(("erss-ports", 0) -> 3, ("erss-ports", 1) -> 12)

      // Each port is reported from its own count and document: port 0 has 3 rows (one page),
      // port 1 has 12 (three pages).
      events.updates.map(_.updates) shouldBe List(
        Map(
          "erss-ports" -> Map(
            0 -> WebPaginationUpdate(PaginationMode(), 3L, List(1)),
            1 -> WebPaginationUpdate(PaginationMode(), 12L, List(1, 2, 3))
          )
        )
      )
      // Each port's statistics are read off its own document's columns.
      events.updates.head.tableStats.keySet shouldBe Set("erss-ports")
      events.updates.head.tableStats("erss-ports").keySet shouldBe Set(0, 1)
      events.updates.head.tableStats("erss-ports")(0).keySet shouldBe Set("id")
      events.updates.head.tableStats("erss-ports")(1).keySet shouldBe Set("value")
    }
  }

  it should "send only the output ports whose tuple count changed" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val rows = intRows(schema, "id", List(1))
    storeResult(executionId, "erss-port-diff", schema, rows)
    storeResult(executionId, "erss-port-diff", schema, rows, port = PortIdentity(1))
    storeResult(executionId, "erss-port-diff", schema, rows, port = PortIdentity(2))
    storeResult(executionId, "erss-port-diff-single", schema, rows)
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-port-diff",
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_SNAPSHOT,
        PortIdentity(2) -> OutputMode.SET_SNAPSHOT
      ),
      physicalOp(executionId, "erss-port-diff-single", PortIdentity() -> OutputMode.SET_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      // 1: everything is new. 2: only port 1 moves. 3: only port 0 moves (and the
      // single-port operator). 4: port 2 moves.
      fixture.setPortResultCounts(
        ("erss-port-diff", 0) -> 1,
        ("erss-port-diff", 1) -> 1,
        ("erss-port-diff", 2) -> 1,
        ("erss-port-diff-single", 0) -> 1
      )
      fixture.setPortResultCounts(
        ("erss-port-diff", 0) -> 1,
        ("erss-port-diff", 1) -> 2,
        ("erss-port-diff", 2) -> 1,
        ("erss-port-diff-single", 0) -> 1
      )
      fixture.setPortResultCounts(
        ("erss-port-diff", 0) -> 2,
        ("erss-port-diff", 1) -> 2,
        ("erss-port-diff", 2) -> 1,
        ("erss-port-diff-single", 0) -> 2
      )
      fixture.setPortResultCounts(
        ("erss-port-diff", 0) -> 2,
        ("erss-port-diff", 1) -> 2,
        ("erss-port-diff", 2) -> 3,
        ("erss-port-diff-single", 0) -> 2
      )

      // A move on one port is not a move on another, and an operator with no moved port has no
      // key at all, not an empty map.
      val movedPorts = List(
        Map("erss-port-diff" -> Set(0, 1, 2), "erss-port-diff-single" -> Set(0)),
        Map("erss-port-diff" -> Set(1)),
        Map("erss-port-diff" -> Set(0), "erss-port-diff-single" -> Set(0)),
        Map("erss-port-diff" -> Set(2))
      )
      events.updates.map(_.updates.view.mapValues(_.keySet).toMap) shouldBe movedPorts
      events.updates.map(_.tableStats.view.mapValues(_.keySet).toMap) shouldBe movedPorts
    }
  }

  it should "not resend a later port whose count is unchanged but whose change detector moved" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val rows = intRows(schema, "id", List(1))
    storeResult(executionId, "erss-port-detector", schema, rows)
    storeResult(executionId, "erss-port-detector", schema, rows, port = PortIdentity(1))
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-port-detector",
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setPortResults(
        ("erss-port-detector", 0) -> OperatorResultMetadata(1, "a"),
        ("erss-port-detector", 1) -> OperatorResultMetadata(1, "a")
      )
      fixture.setPortResults(
        ("erss-port-detector", 0) -> OperatorResultMetadata(1, "b"),
        ("erss-port-detector", 1) -> OperatorResultMetadata(1, "b")
      )

      // A port is changed when it is new or its count moved.
      events.updates.map(_.updates.view.mapValues(_.keySet).toMap) shouldBe List(
        Map("erss-port-detector" -> Set(0, 1)),
        Map.empty
      )
    }
  }

  it should "build each port's update with that port's own output mode and document" in {
    val executionId = newExecution()
    val tableSchema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val htmlSchema = new Schema(List(new Attribute("html", AttributeType.STRING)))
    val html = "a" * 150
    storeResult(executionId, "erss-two-modes", tableSchema, intRows(tableSchema, "id", 0 until 7))
    storeResult(
      executionId,
      "erss-two-modes",
      htmlSchema,
      List(Tuple.builder(htmlSchema).add("html", AttributeType.STRING, html).build()),
      port = PortIdentity(1)
    )
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-two-modes",
        PortIdentity(1, internal = true) -> OutputMode.SET_DELTA,
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SINGLE_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setPortResultCounts(("erss-two-modes", 0) -> 7, ("erss-two-modes", 1) -> 1)

      val ports = events.updates.head.updates("erss-two-modes")
      ports.keySet shouldBe Set(0, 1)
      ports(0) shouldBe WebPaginationUpdate(PaginationMode(), 7L, List(1, 2))
      // Port 1 is an HTML port: the whole, untruncated snapshot of ITS document.
      val second = dataUpdate(ports(1))
      second.mode shouldBe SetSnapshotMode()
      second.table.map(_.get("html").asText()) shouldBe List(html)
      // An HTML port beside it does not stop the table port's statistics, and has none itself.
      events.updates.head.tableStats.keySet shouldBe Set("erss-two-modes")
      events.updates.head.tableStats("erss-two-modes").keySet shouldBe Set(0)
    }
  }

  it should "page a later table port behind an HTML first port" in {
    val executionId = newExecution()
    val tableSchema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val htmlSchema = new Schema(List(new Attribute("html", AttributeType.STRING)))
    storeResult(
      executionId,
      "erss-html-first",
      htmlSchema,
      List(Tuple.builder(htmlSchema).add("html", AttributeType.STRING, "<p>chart</p>").build())
    )
    storeResult(
      executionId,
      "erss-html-first",
      tableSchema,
      intRows(tableSchema, "id", 0 until 12),
      port = PortIdentity(1)
    )
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-html-first",
        PortIdentity() -> OutputMode.SINGLE_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setPortResultCounts(("erss-html-first", 0) -> 1, ("erss-html-first", 1) -> 12)

      val ports = events.updates.head.updates("erss-html-first")
      val first = dataUpdate(ports(0))
      first.mode shouldBe SetSnapshotMode()
      first.table.map(_.get("html").asText()) shouldBe List("<p>chart</p>")
      ports(1) shouldBe WebPaginationUpdate(PaginationMode(), 12L, List(1, 2, 3))
      // Only the table port has statistics: an HTML first port does not hide port 1's.
      events.updates.head.tableStats.keySet shouldBe Set("erss-html-first")
      events.updates.head.tableStats("erss-html-first").keySet shouldBe Set(1)
      events.updates.head.tableStats("erss-html-first")(1).keySet shouldBe Set("id")
    }
  }

  it should "send a later SET_DELTA port only the rows added since that port's previous count" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(executionId, "erss-port-delta", schema, intRows(schema, "id", List(0)))
    storeResult(
      executionId,
      "erss-port-delta",
      schema,
      intRows(schema, "id", 100 until 105),
      port = PortIdentity(1)
    )
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-port-delta",
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_DELTA
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setPortResultCounts(("erss-port-delta", 0) -> 1, ("erss-port-delta", 1) -> 2)
      fixture.setPortResultCounts(("erss-port-delta", 0) -> 1, ("erss-port-delta", 1) -> 5)

      val tables = events.updates.map(e =>
        dataUpdate(e.updates("erss-port-delta")(1)).table.map(_.get("id").asInt())
      )
      // Read from port 1's own previous count (2); port 0's (1) would resend row 101.
      tables shouldBe List(List(100, 101, 102, 103, 104), List(102, 103, 104))
      dataUpdate(events.updates.head.updates("erss-port-delta")(1)).mode shouldBe
        SetDeltaMode()
    }
  }

  it should "send an empty pagination update for a later port that is not stored yet" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    // Only the first port is stored; reading it in port 1's place would report its pages.
    storeResult(executionId, "erss-port-unstored", schema, intRows(schema, "id", 0 until 3))
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-port-unstored",
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setPortResultCounts(("erss-port-unstored", 1) -> 4)

      // Port 1 is not stored yet, so it reports nothing rather than port 0's pages; port 0 did
      // not move, so it is not in the update at all.
      events.updates.map(_.updates) shouldBe List(
        Map("erss-port-unstored" -> Map(1 -> WebPaginationUpdate(PaginationMode(), 0L, List.empty)))
      )
    }
  }

  it should "fail loudly for a result entry whose output port is not in the plan" in {
    val executionId = newExecution()
    // The operator declares one output port; a result entry for a second one has no mode to
    // serve it with, and must not be served with port 0's.
    val plan = planOf(
      physicalOp(executionId, "erss-port-missing", PortIdentity() -> OutputMode.SET_SNAPSHOT)
    )

    withAttached(plan, executionId) { fixture =>
      val events = new ResultEvents(fixture.workflowStateStore)
      fixture.setPortResultCounts(("erss-port-missing", 1) -> 1)

      events.updates shouldBe empty
      events.errors.map(_.getMessage) shouldBe List(
        "Output port 1 of operator erss-port-missing is not in the physical plan"
      )
    }
  }

  it should "record each table port's size and row count, and none for an HTML port" in {
    val executionId = newExecution()
    val tableSchema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    val htmlSchema = new Schema(List(new Attribute("html", AttributeType.STRING)))
    val firstUri =
      storeResult(executionId, "erss-port-size", tableSchema, intRows(tableSchema, "id", 0 until 3))
    val secondUri = storeResult(
      executionId,
      "erss-port-size",
      tableSchema,
      intRows(tableSchema, "id", 0 until 6),
      port = PortIdentity(1)
    )
    storeResult(
      executionId,
      "erss-port-size",
      htmlSchema,
      List(Tuple.builder(htmlSchema).add("html", AttributeType.STRING, "<p>chart</p>").build()),
      port = PortIdentity(2)
    )
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-port-size",
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_SNAPSHOT,
        PortIdentity(2) -> OutputMode.SINGLE_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      // No ResultEvents: the sizes and counts are recorded with no browser connected.
      setPollingCancellable(fixture.service, new RecordingCancellable)
      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.COMPLETED))

      // Each table port under its own port id, from its own document.
      storedResultCount(executionId, "erss-port-size") shouldBe 3L
      storedResultCount(executionId, "erss-port-size", PortIdentity(1)) shouldBe 6L
      storedResultSize(executionId, "erss-port-size") shouldBe
        DocumentFactory.openDocument(firstUri)._1.getTotalFileSize
      storedResultSize(executionId, "erss-port-size", PortIdentity(1)) shouldBe
        DocumentFactory.openDocument(secondUri)._1.getTotalFileSize
      // The HTML port is shown whole, never paged, so nothing is recorded for it.
      storedResultCount(executionId, "erss-port-size", PortIdentity(2)) shouldBe null
      storedResultSize(executionId, "erss-port-size", PortIdentity(2)) shouldBe 0L
    }
  }

  // -- attachToExecution: the engine callbacks -------------------------------

  "the OperatorPortResultUriAvailable callback" should
    "persist the URI under the attached execution" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val globalPortId = globalPortIdOf("erss-callback")
      val uri = resultUriOf(executionId, "erss-callback")

      fixture.client.fire(
        OperatorPortResultUriAvailable(globalPortId, uri, OutputMode.SET_SNAPSHOT)
      )

      val rows = getDSLContext
        .selectFrom(OPERATOR_PORT_EXECUTIONS)
        .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(executionId.id.toInt))
        .fetch()
      rows.size() shouldBe 1
      rows.get(0).getGlobalPortId shouldBe globalPortId.serializeAsString
      rows.get(0).getResultUri shouldBe uri.toString
    }
  }

  "the ExecutionStateUpdate callback" should
    "stop polling and run one final update when the execution terminates" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(
      executionId,
      "erss-final-snapshot",
      schema,
      (0 until 3).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )
    storeResult(
      executionId,
      "erss-final-delta",
      schema,
      (0 until 2).map(i => Tuple.builder(schema).add("id", AttributeType.INTEGER, i).build())
    )
    val plan = planOf(
      physicalOp(executionId, "erss-final-snapshot", PortIdentity() -> OutputMode.SET_SNAPSHOT),
      physicalOp(executionId, "erss-final-delta", PortIdentity() -> OutputMode.SET_DELTA)
    )

    withAttached(plan, executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.COMPLETED))

      polling.cancelCount shouldBe 1
      val resultInfo = fixture.workflowStateStore.resultStore.getState.resultInfo
      resultInfo.keySet shouldBe Set(
        globalPortIdOf("erss-final-snapshot"),
        globalPortIdOf("erss-final-delta")
      )
      // Counts come from the documents themselves, so the two operators must not
      // be reported with the same count.
      resultInfo(globalPortIdOf("erss-final-snapshot")).tupleCount shouldBe 3
      resultInfo(globalPortIdOf("erss-final-delta")).tupleCount shouldBe 2
      // A SET_SNAPSHOT result gets a random change detector, so the stored state differs on
      // every poll and the diff handler runs. The handler still sends only the ports whose
      // row count changed. A delta gets no detector.
      resultInfo(globalPortIdOf("erss-final-snapshot")).changeDetector should not be empty
      resultInfo(globalPortIdOf("erss-final-delta")).changeDetector shouldBe ""
    }
  }

  it should "record one result entry per external output port, each with its own count" in {
    val executionId = newExecution()
    val schema = new Schema(List(new Attribute("id", AttributeType.INTEGER)))
    storeResult(executionId, "erss-final-split", schema, intRows(schema, "id", 0 until 3))
    storeResult(
      executionId,
      "erss-final-split",
      schema,
      intRows(schema, "id", 0 until 8),
      port = PortIdentity(1)
    )
    // An internal port's materialization is not a result the UI shows, and its index (0)
    // would collide with external port 0 if it were kept.
    storeResult(
      executionId,
      "erss-final-split",
      schema,
      intRows(schema, "id", 0 until 5),
      port = PortIdentity(0, internal = true)
    )
    // Only the second port of this one is stored (its first port's region never ran).
    storeResult(
      executionId,
      "erss-final-only-second",
      schema,
      intRows(schema, "id", 0 until 4),
      port = PortIdentity(1)
    )
    val plan = planOf(
      physicalOp(
        executionId,
        "erss-final-split",
        PortIdentity(0, internal = true) -> OutputMode.SET_SNAPSHOT,
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_DELTA
      ),
      physicalOp(
        executionId,
        "erss-final-only-second",
        PortIdentity() -> OutputMode.SET_SNAPSHOT,
        PortIdentity(1) -> OutputMode.SET_SNAPSHOT
      )
    )

    withAttached(plan, executionId) { fixture =>
      setPollingCancellable(fixture.service, new RecordingCancellable)

      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.COMPLETED))

      val resultInfo = fixture.workflowStateStore.resultStore.getState.resultInfo
      resultInfo.keySet shouldBe Set(
        globalPortIdOf("erss-final-split"),
        globalPortIdOf("erss-final-split", PortIdentity(1)),
        globalPortIdOf("erss-final-only-second", PortIdentity(1))
      )
      // Keyed by port, the second port's 8 rows no longer replace the first port's 3.
      resultInfo(globalPortIdOf("erss-final-split")).tupleCount shouldBe 3
      resultInfo(globalPortIdOf("erss-final-split", PortIdentity(1))).tupleCount shouldBe 8
      resultInfo(globalPortIdOf("erss-final-only-second", PortIdentity(1))).tupleCount shouldBe 4
      // The change detector follows each port's own mode: port 1 is SET_DELTA, so it gets
      // none even though port 0 (SET_SNAPSHOT) does.
      resultInfo(globalPortIdOf("erss-final-split")).changeDetector should not be empty
      resultInfo(globalPortIdOf("erss-final-split", PortIdentity(1))).changeDetector shouldBe ""
    }
  }

  it should "leave polling running while the execution has not terminated" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.client.fire(ExecutionStateUpdate(WorkflowAggregatedState.PAUSED))

      polling.cancelCount shouldBe 0
      fixture.workflowStateStore.resultStore.getState.resultInfo shouldBe empty
    }
  }

  "the FatalError callback" should "stop polling" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.client.fire(FatalError(new RuntimeException("boom")))

      polling.cancelCount shouldBe 1
    }
  }

  it should "tolerate a fatal error raised before polling ever started" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      // A workflow can fail during initialization, i.e. before it reaches RUNNING
      // and creates a polling handle. Without the null guard this is an NPE
      // inside the error path, which would mask the original failure.
      noException should be thrownBy fixture.client.fire(FatalError(new RuntimeException("boom")))
    }
  }

  "attachToExecution" should "cancel the previous execution's polling" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable
      setPollingCancellable(fixture.service, polling)

      fixture.service.attachToExecution(
        executionId,
        new ExecutionStateStore,
        planOf(),
        fixture.client
      )

      // Two cancels, from two different places: attachToExecution's own guard,
      // and then the freshly registered metadata subscription, which replays the
      // store's current (non-RUNNING) state immediately and cancels on it. The
      // next case pins the guard by removing its contribution.
      polling.cancelCount shouldBe 2
    }
  }

  it should "not re-cancel polling that is already cancelled" in {
    val executionId = newExecution()
    withAttached(planOf(), executionId) { fixture =>
      val polling = new RecordingCancellable(initiallyCancelled = true)
      setPollingCancellable(fixture.service, polling)

      fixture.service.attachToExecution(
        executionId,
        new ExecutionStateStore,
        planOf(),
        fixture.client
      )

      // Only the metadata subscription's cancel is left: attachToExecution's guard
      // skips a handle that reports itself already cancelled.
      polling.cancelCount shouldBe 1
    }
  }

  // ---------------------------------------------------------------- fixtures

  /** An AmberClient that records the callbacks the service registers, so tests can fire them. */
  private final class RecordingAmberClient
      extends AmberClient(
        system,
        new WorkflowContext(),
        PhysicalPlan(Set.empty, Set.empty),
        CoordinatorConfig(None, None, None, None),
        _ => ()
      ) {
    private val callbacks = mutable.Map.empty[Class[_], Any => Unit]

    override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
      callbacks(ct.runtimeClass) = callback.asInstanceOf[Any => Unit]
      Disposable.empty()
    }

    /** Delivers `event` to the callback registered for its type; fails if there is none. */
    def fire[T <: AnyRef](event: T): Unit =
      callbacks
        .getOrElse(
          event.getClass,
          fail(s"no callback registered for ${event.getClass.getSimpleName}")
        )
        .apply(event)
  }

  /** A Cancellable that counts cancellations, standing in for AmberRuntime's scheduled poll. */
  private final class RecordingCancellable(initiallyCancelled: Boolean = false)
      extends Cancellable {

    /** Counts calls, not successful cancellations -- the tests assert how often production asks. */
    var cancelCount = 0
    private var cancelled = initiallyCancelled

    // Follows Cancellable's contract: true only if THIS call did the cancelling. Returning true
    // unconditionally would quietly diverge from the real scheduler if production ever branches on
    // the result.
    override def cancel(): Boolean = {
      cancelCount += 1
      val didCancel = !cancelled
      cancelled = true
      didCancel
    }

    override def isCancelled: Boolean = cancelled
  }

  private final class Fixture(
      val client: RecordingAmberClient,
      val service: ExecutionResultService,
      val workflowStateStore: WorkflowStateStore
  ) {

    /** Publishes one engine-reported tuple count per operator, on port 0, as the poller does. */
    def setResultCounts(counts: (String, Int)*): Unit =
      setPortResultCounts(counts.map { case (opId, count) => (opId, 0) -> count }: _*)

    /** Publishes one engine-reported tuple count per (operator, external output port index). */
    def setPortResultCounts(counts: ((String, Int), Int)*): Unit =
      setPortResults(counts.map { case (port, count) => port -> OperatorResultMetadata(count) }: _*)

    /** Publishes whole result metadata per (operator, external output port index). */
    def setPortResults(entries: ((String, Int), OperatorResultMetadata)*): Unit =
      workflowStateStore.resultStore.updateState(_ =>
        WorkflowResultStore(entries.map {
          case ((opId, port), metadata) =>
            globalPortIdOf(opId, PortIdentity(port)) -> metadata
        }.toMap)
      )
  }

  /** Collects what the result store's diff handler emits, and any error it raises. */
  private final class ResultEvents(workflowStateStore: WorkflowStateStore) {
    private val collected = mutable.ArrayBuffer.empty[TexeraWebSocketEvent]
    val errors: mutable.ArrayBuffer[Throwable] = mutable.ArrayBuffer.empty[Throwable]

    // Held and disposed in afterEach: an observer left subscribed keeps collecting into this
    // buffer after its test has finished, and retains the buffer with it.
    private val subscription: Disposable =
      workflowStateStore.resultStore.getWebsocketEventObservable.subscribe(
        (evts: Iterable[TexeraWebSocketEvent]) => collected ++= evts,
        (err: Throwable) => errors += err
      )

    openObservers += subscription

    def updates: List[WebResultUpdateEvent] =
      collected.collect { case e: WebResultUpdateEvent => e }.toList
  }

  private def withAttached(physicalPlan: PhysicalPlan, executionId: ExecutionIdentity)(
      body: Fixture => Unit
  ): Unit = {
    val client = new RecordingAmberClient
    val workflowStateStore = new WorkflowStateStore
    val service = new ExecutionResultService(
      WorkflowIdentity(testWid.longValue()),
      testCuid,
      workflowStateStore
    )
    try {
      service.attachToExecution(executionId, new ExecutionStateStore, physicalPlan, client)
      body(new Fixture(client, service, workflowStateStore))
    } finally {
      service.unsubscribeAll()
      client.shutdown()
    }
  }

  /**
    * Runs `request` against a service bound to the seeded workflow and computing
    * unit, so getLatestExecutionId resolves against the rows this spec inserts.
    */
  private def paginate(request: ResultPaginationRequest): PaginatedResultEvent = {
    val service = new ExecutionResultService(
      WorkflowIdentity(testWid.longValue()),
      testCuid,
      new WorkflowStateStore
    )
    service.handleResultPagination(request) match {
      case event: PaginatedResultEvent => event
      case other                       => fail(s"expected a PaginatedResultEvent, got $other")
    }
  }

  private def dataUpdate(update: ExecutionResultService.WebResultUpdate): WebDataUpdate =
    update match {
      case dataUpdate: WebDataUpdate => dataUpdate
      case other                     => fail(s"expected a WebDataUpdate, got $other")
    }

  private def newExecution(): ExecutionIdentity = {
    val execution = new WorkflowExecutions
    execution.setVid(testVid)
    execution.setUid(testUid)
    execution.setCuid(testCuid)
    execution.setStatus(0.toByte)
    execution.setStartingTime(new Timestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("execution-result-instance-test")
    execution.setEnvironmentVersion("test-env")
    executionsDao.insert(execution)
    ExecutionIdentity(execution.getEid.longValue())
  }

  private def globalPortIdOf(
      operatorId: String,
      port: PortIdentity = PortIdentity()
  ): GlobalPortIdentity =
    GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity(operatorId), "main"),
      port,
      input = false
    )

  /** The external-output result URI shape that getResultUriByLogicalPortId decodes and matches. */
  private def resultUriOf(
      executionId: ExecutionIdentity,
      operatorId: String,
      warehouse: Option[String] = None,
      port: PortIdentity = PortIdentity()
  ): URI =
    VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(
        WorkflowIdentity(testWid.longValue()),
        executionId,
        globalPortIdOf(operatorId, port),
        warehouse
      )
    )

  private def insertResultUri(executionId: ExecutionIdentity, uri: URI): Unit =
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .columns(
        OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID,
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        OPERATOR_PORT_EXECUTIONS.RESULT_URI
      )
      .values(
        Integer.valueOf(executionId.id.toInt),
        VFSURIFactory.decodeURI(uri).globalPortId.get.serializeAsString,
        uri.toString
      )
      .execute()

  /** Creates a real Iceberg result table holding `tuples`, and records its URI for `executionId`. */
  private def storeResult(
      executionId: ExecutionIdentity,
      operatorId: String,
      schema: Schema,
      tuples: Seq[Tuple],
      port: PortIdentity = PortIdentity()
  ): URI = {
    val uri = resultUriOf(executionId, operatorId, port = port)
    val document = DocumentFactory.createDocument(uri, schema)
    val writer = document.writer("erss").asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
    tuples.foreach(writer.putOne)
    writer.close()
    insertResultUri(executionId, uri)
    uri
  }

  private def storedResultCount(
      executionId: ExecutionIdentity,
      operatorId: String,
      port: PortIdentity = PortIdentity()
  ): java.lang.Long =
    getDSLContext
      .select(OPERATOR_PORT_EXECUTIONS.RESULT_COUNT)
      .from(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(executionId.id.toInt))
      .and(
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(
          globalPortIdOf(operatorId, port).serializeAsString
        )
      )
      .fetchOne(OPERATOR_PORT_EXECUTIONS.RESULT_COUNT)

  private def storedResultSize(
      executionId: ExecutionIdentity,
      operatorId: String,
      port: PortIdentity = PortIdentity()
  ): Long =
    getDSLContext
      .select(OPERATOR_PORT_EXECUTIONS.RESULT_SIZE)
      .from(OPERATOR_PORT_EXECUTIONS)
      .where(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(executionId.id.toInt))
      .and(
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID.eq(
          globalPortIdOf(operatorId, port).serializeAsString
        )
      )
      .fetchOne(OPERATOR_PORT_EXECUTIONS.RESULT_SIZE)

  /** One integer-column row per id, in the given column. */
  private def intRows(schema: Schema, column: String, ids: Seq[Int]): Seq[Tuple] =
    ids.map(i => Tuple.builder(schema).add(column, AttributeType.INTEGER, i).build())

  /**
    * A single-layer physical op whose output ports are declared in the given order --
    * a ListMap, so a test can list an internal port before an external one and catch
    * a mode lookup that does not go by the served port's own id.
    */
  private def physicalOp(
      executionId: ExecutionIdentity,
      operatorId: String,
      ports: (PortIdentity, OutputMode)*
  ): PhysicalOp =
    PhysicalOp(
      id = PhysicalOpIdentity(OperatorIdentity(operatorId), "main"),
      workflowId = WorkflowIdentity(testWid.longValue()),
      executionId = executionId,
      opExecInitInfo = OpExecInitInfo.Empty,
      outputPorts = ListMap.from(ports.map {
        case (portId, mode) =>
          portId -> (OutputPort(id = portId, mode = mode), List.empty, Right(Schema()))
      })
    )

  private def planOf(ops: PhysicalOp*): PhysicalPlan = PhysicalPlan(ops.toSet, Set.empty)

  /**
    * Sets the private polling handle. AmberRuntime creates it once the execution
    * reaches RUNNING, from an ActorSystem it keeps in a private var that only a
    * started pekko cluster fills in; the terminal-state, fatal-error and re-attach
    * paths are all defined by what they do to an already-created handle.
    */
  private def setPollingCancellable(
      service: ExecutionResultService,
      cancellable: Cancellable
  ): Unit = {
    val field = classOf[ExecutionResultService].getDeclaredField("resultUpdateCancellable")
    field.setAccessible(true)
    field.set(service, cancellable)
  }
}
