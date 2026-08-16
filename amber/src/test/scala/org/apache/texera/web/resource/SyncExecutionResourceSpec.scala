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
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.storage.model.BufferedItemWriter
import org.apache.texera.amber.core.storage.{DocumentFactory, VFSURIFactory}
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
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import org.apache.texera.amber.engine.architecture.worker.statistics.{
  PortTupleMetricsMapping,
  TupleMetrics
}
import org.apache.texera.amber.engine.common.executionruntimestate.{
  ExecutionConsoleStore,
  OperatorConsole,
  OperatorMetrics,
  OperatorStatistics
}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpDesc
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.amber.util.serde.GlobalPortIdentitySerde.SerdeOps
import org.apache.texera.auth.SessionUser
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlanPojo}
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{OPERATOR_EXECUTIONS, OPERATOR_PORT_EXECUTIONS}
import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
import org.apache.texera.dao.jooq.generated.tables.daos.{
  UserDao,
  WorkflowComputingUnitDao,
  WorkflowDao,
  WorkflowVersionDao
}
import org.apache.texera.dao.jooq.generated.tables.pojos.{
  User,
  Workflow,
  WorkflowComputingUnit,
  WorkflowVersion
}
import org.apache.texera.web.model.websocket.request.WorkflowExecuteRequest
import org.apache.texera.web.service.{WarehouseUnavailableException, WorkflowExecutionService}
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester}

import java.net.URI
import java.sql.Timestamp
import scala.jdk.CollectionConverters._

/**
  * Unit tests for the synchronous "run this workflow and hand me the results" endpoint used by
  * agent-service.
  *
  * Two of the resource's methods are public (`executeWorkflowSync`, `healthCheck`); the other
  * sixteen are `private def`. `executeWorkflowSync` is driven for real here — one end-to-end case
  * that gets as far as a running engine can be taken without one — but the result-shaping helpers
  * it calls cannot be reached that way: every one of them only produces an observable difference
  * once the engine has populated the stats/console stores or written Iceberg results. They are
  * therefore driven through `PrivateMethodTester` over fixtures that stand in for that state, and
  * each such test says which engine state it is standing in for.
  *
  * `collectOperatorResult` needs the most of those fixtures, and gets a real one: `storeResult`
  * below writes an actual Iceberg result table through `DocumentFactory` on the ambient catalog
  * and registers its URI in `operator_port_executions`, which is all the method needs — it takes
  * the `ExecutionIdentity` as a parameter and resolves storage from the database, so no engine,
  * coordinator or client is involved. Two things to know before touching those tests: its
  * outermost `catch` turns any storage failure into a well-formed `("table", None, ...)`, so an
  * assertion weaker than the row contents passes on a fixture that never opened a document; and
  * its front/back split is exact arithmetic, so the char budgets are multiples of a measured
  * per-row size (`rowChars`) rather than round numbers, which land in a neighbouring branch.
  *
  * Deliberately NOT covered, and why:
  *   - `validateWorkflow` (lines 905-924). It has zero call sites: nothing in the repository, in
  *     any language, invokes it (agent-service has a same-named TypeScript function of its own,
  *     which is unrelated). It is reachable by reflection, but reflective reachability is not
  *     liveness — a test here would only cement dead code and make its deletion look like a
  *     regression. Reported as dead instead.
  *   - `shutdownPreviousExecution` (350-361) and `killExecution`'s `client.shutdown()` line (368).
  *     Both need an `AmberClient` test double; amber's test scope has no mocking library, and
  *     `AmberClient`'s constructor needs a started `AmberRuntime`. `shutdownPreviousExecution` is
  *     additionally unpinnable in principle: its whole body sits inside `catch (Exception) => warn`,
  *     so removing either null guard turns a no-op into a *swallowed* NPE — no observable change.
  *   - the document-reading half of `collectConsoleLogs`. Reachable by the same fixture technique
  *     (a console-messages document whose single column holds ASCII-serialized `ConsoleMessage`
  *     protos), just not done here; its database half, `getConsoleMessageUri`, is pinned below.
  *   - the `Observable.amb` wait and its timeout/error handlers (lines 231-277), plus the
  *     `ConsoleErrorDetected` / `TargetResultsReady` termination arms (284-298). Reaching them
  *     needs an execution that is still non-terminal when `executeWorkflowSync` looks at it, i.e.
  *     a live engine.
  */
class SyncExecutionResourceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB
    with PrivateMethodTester {

  /**
    * Repo-unique ids. `executeWorkflowSync` registers a `WorkflowService` in a JVM-wide map that
    * nothing in a test run ever removes, and result/console storage URIs are derived from the
    * workflow id, so a value another suite also uses could collide inside the single JVM sbt runs
    * amber's suites in. 9317 appears nowhere else in the repository.
    */
  private val testWid = 9317
  private val testUid = 9317
  private val testCuid = 9317

  /**
    * The console-URI fixtures below belong to a SECOND computing unit. `initExecutionService`
    * treats the newest execution of (workflow, computing unit) as "the previous run" and cleans
    * its registered storage up; parking the fixtures elsewhere keeps the `executeWorkflowSync`
    * case from tripping over URIs that point at documents no test ever created.
    */
  private val fixtureCuid = 9318

  private val resource = new SyncExecutionResource()

  private val stateToString = PrivateMethod[String](Symbol("stateToString"))
  private val isTerminalState = PrivateMethod[Boolean](Symbol("isTerminalState"))
  private val hasConsoleError = PrivateMethod[Boolean](Symbol("hasConsoleError"))
  private val symmetricTruncateCellValue =
    PrivateMethod[String](Symbol("symmetricTruncateCellValue"))
  private val truncateSingleTuple = PrivateMethod[ObjectNode](Symbol("truncateSingleTuple"))
  private val estimateTupleSize = PrivateMethod[Int](Symbol("estimateTupleSize"))
  private val isVisualizationTuple = PrivateMethod[Boolean](Symbol("isVisualizationTuple"))
  private val handleExecutionError =
    PrivateMethod[SyncExecutionResult](Symbol("handleExecutionError"))
  private val computeSubDAGIfNeeded =
    PrivateMethod[LogicalPlanPojo](Symbol("computeSubDAGIfNeeded"))
  private val collectOperatorInfos =
    PrivateMethod[Map[String, OperatorInfo]](Symbol("collectOperatorInfos"))
  private val getConsoleMessageUri = PrivateMethod[Option[URI]](Symbol("getConsoleMessageUri"))
  private val killExecution = PrivateMethod[Unit](Symbol("killExecution"))

  private type CollectedResult = (String, Option[Any], Option[Int], Option[Int], Option[Boolean])
  private val collectOperatorResult =
    PrivateMethod[CollectedResult](Symbol("collectOperatorResult"))

  /** eid of the row `getConsoleMessageUri`'s cases hang their operator_executions rows off. */
  private var consoleEid: Int = 0

  /** A second eid, so the query's execution-id predicate is not vacuous. */
  private var otherEid: Int = 0

  /** eid the `collectOperatorResult` fixtures register their result URIs under. */
  private var resultEid: Int = 0

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val cfg = getDSLContext.configuration()

    val user = new User
    user.setUid(testUid)
    user.setName("sync_exec_user")
    user.setEmail("sync-exec@example.com")
    new UserDao(cfg).insert(user)

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName("sync_exec_workflow")
    workflow.setContent("{}")
    workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowDao(cfg).insert(workflow)

    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new Timestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(cfg).insert(version)

    // workflow_executions.cuid is a foreign key; without these rows initExecutionService cannot
    // insert its execution and never reaches the engine at all.
    val unitDao = new WorkflowComputingUnitDao(cfg)
    List(testCuid -> "sync_exec_unit", fixtureCuid -> "sync_exec_fixture_unit").foreach {
      case (cuid, name) =>
        val unit = new WorkflowComputingUnit
        unit.setCuid(cuid)
        unit.setUid(testUid)
        unit.setName(name)
        unit.setType(WorkflowComputingUnitTypeEnum.local)
        unit.setCreationTime(new Timestamp(System.currentTimeMillis()))
        unitDao.insert(unit)
    }

    consoleEid = insertExecution("console-fixture")
    otherEid = insertExecution("console-fixture-other")
    resultEid = insertExecution("result-fixture")
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  private def insertExecution(name: String): Int = {
    import org.apache.texera.dao.jooq.generated.Tables.{WORKFLOW_EXECUTIONS, WORKFLOW_VERSION}
    val vid = getDSLContext
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(testWid))
      .fetchOneInto(classOf[Integer])
    getDSLContext
      .insertInto(WORKFLOW_EXECUTIONS)
      .set(WORKFLOW_EXECUTIONS.VID, vid)
      .set(WORKFLOW_EXECUTIONS.UID, Integer.valueOf(testUid))
      .set(WORKFLOW_EXECUTIONS.CUID, Integer.valueOf(fixtureCuid))
      .set(WORKFLOW_EXECUTIONS.NAME, name)
      .set(WORKFLOW_EXECUTIONS.ENVIRONMENT_VERSION, "test")
      .returning(WORKFLOW_EXECUTIONS.EID)
      .fetchOne()
      .getEid
  }

  private def insertConsoleUri(eid: Int, opId: String, uri: String): Unit = {
    getDSLContext
      .insertInto(OPERATOR_EXECUTIONS)
      .set(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID, Integer.valueOf(eid))
      .set(OPERATOR_EXECUTIONS.OPERATOR_ID, opId)
      .set(OPERATOR_EXECUTIONS.CONSOLE_MESSAGES_URI, uri)
      .execute()
  }

  private def globalPortIdOf(opId: String): GlobalPortIdentity =
    GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity(opId), "main"),
      PortIdentity(),
      input = false
    )

  /** The external-output result URI shape `getResultUriByLogicalPortId` decodes and matches. */
  private def resultUriOf(opId: String): URI =
    VFSURIFactory.resultURI(
      VFSURIFactory.createPortBaseURI(
        WorkflowIdentity(testWid.toLong),
        ExecutionIdentity(resultEid.toLong),
        globalPortIdOf(opId)
      )
    )

  private def registerResultUri(uri: URI): Unit =
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .set(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID, Integer.valueOf(resultEid))
      .set(
        OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID,
        VFSURIFactory.decodeURI(uri).globalPortId.get.serializeAsString
      )
      .set(OPERATOR_PORT_EXECUTIONS.RESULT_URI, uri.toString)
      .execute()

  private val rowSchema = new Schema(List(new Attribute("v", AttributeType.STRING)))

  private def row(index: Int): Tuple =
    Tuple.builder(rowSchema).add("v", AttributeType.STRING, s"r$index").build()

  /**
    * The size `collectOperatorResult` charges for one `row(i)` with a single-digit index: the 28
    * characters of `{"v":"rN","__row_index__":N}` — pinned as a literal by the first test below —
    * plus the one it reserves for the comma that separates it from the next row of the array.
    */
  private val rowChars = 29

  /**
    * Creates a real Iceberg result table holding `tuples`, and registers its URI so
    * `getResultUriByLogicalPortId` finds it. Operator ids are prefixed `serc-` and hang off
    * workflow 9317, because an Iceberg table's identity derives from its URI path and sbt runs
    * amber's suites in one unforked JVM: an id another suite also uses would truncate its table.
    */
  private def storeResult(opId: String, tuples: Seq[Tuple], schema: Schema = rowSchema): Unit = {
    val uri = resultUriOf(opId)
    val writer = DocumentFactory
      .createDocument(uri, schema)
      .writer(opId)
      .asInstanceOf[BufferedItemWriter[Tuple]]
    writer.open()
    tuples.foreach(writer.putOne)
    writer.close()
    registerResultUri(uri)
  }

  private def collect(opId: String, charLimit: Int, cellCharLimit: Int): CollectedResult =
    resource invokePrivate collectOperatorResult(
      ExecutionIdentity(resultEid.toLong),
      opId,
      charLimit,
      cellCharLimit
    )

  /**
    * The rows of a collected result. Both shapes are accepted because the method really does
    * return both: the visualization branch hands back a Scala `List`, every other branch converts
    * to a `java.util.List` first, and the shared `Option[Any]` return type hides the difference.
    */
  private def rowsOf(result: CollectedResult): List[ObjectNode] =
    result._2.getOrElse(fail("expected a result payload, not an absent one")) match {
      case rows: java.util.List[_] => rows.asScala.toList.map(_.asInstanceOf[ObjectNode])
      case rows: List[_]           => rows.map(_.asInstanceOf[ObjectNode])
      case other                   => fail(s"unexpected result payload: ${other.getClass.getName}")
    }

  private def rowIndexOf(jsonRow: ObjectNode): Int = jsonRow.get("__row_index__").asInt()

  /**
    * An execution with no coordinator, no result service and no client — the pattern
    * `WorkflowExecutionServiceSpec` and `WorkflowServiceSpec` establish: construction does no
    * external work, and `client` stays null until `executeWorkflow()` builds one.
    */
  private def newExecution(stateStore: ExecutionStateStore): WorkflowExecutionService =
    new WorkflowExecutionService(
      null,
      new WorkflowContext(),
      null,
      WorkflowExecuteRequest(
        executionName = "test",
        engineVersion = "test",
        logicalPlan = LogicalPlanPojo(List.empty, List.empty, List.empty, List.empty),
        replayFromExecution = None,
        workflowSettings = WorkflowSettings(),
        emailNotificationEnabled = false,
        computingUnitId = testCuid,
        warehouseId = None
      ),
      stateStore,
      (_: Throwable) => (),
      None,
      new URI("vfs:///sync-execution-spec")
    )

  private def metrics(
      state: WorkflowAggregatedState,
      inputs: Seq[(Int, Long)],
      outputs: Seq[(Int, Long)]
  ): OperatorMetrics =
    OperatorMetrics(
      operatorState = state,
      operatorStatistics = OperatorStatistics(
        inputMetrics = inputs.map {
          case (port, count) => PortTupleMetricsMapping(PortIdentity(port), TupleMetrics(count, 0))
        },
        outputMetrics = outputs.map {
          case (port, count) => PortTupleMetricsMapping(PortIdentity(port), TupleMetrics(count, 0))
        }
      )
    )

  private def console(
      messages: (ConsoleMessageType, String, String)*
  ): OperatorConsole =
    OperatorConsole(consoleMessages = messages.map {
      case (msgType, title, message) =>
        ConsoleMessage.defaultInstance
          .withMsgType(msgType)
          .withTitle(title)
          .withMessage(message)
    })

  private def op(id: String): LogicalOp = {
    val logicalOp = new KeywordSearchOpDesc()
    logicalOp.setOperatorId(id)
    logicalOp
  }

  private def link(from: String, to: String): LogicalLink =
    LogicalLink(OperatorIdentity(from), PortIdentity(), OperatorIdentity(to), PortIdentity())

  /** 30 characters, so cell budgets below it actually truncate. */
  private val thirtyChars = "0123456789ABCDEFGHIJKLMNOPQRST"

  "healthCheck" should "report ok under the key the caller polls" in {
    resource.healthCheck shouldBe Map("status" -> "ok")
  }

  "stateToString" should "name every aggregated state and fall back to Unknown" in {
    // The whole mapping in one assertion: a swapped or dropped case shows up as a diff.
    val named = List(
      UNINITIALIZED,
      READY,
      RUNNING,
      PAUSING,
      PAUSED,
      RESUMING,
      COMPLETED,
      FAILED,
      KILLED,
      TERMINATED
    ).map(state => state -> (resource invokePrivate stateToString(state))).toMap

    named shouldBe Map(
      UNINITIALIZED -> "Uninitialized",
      READY -> "Ready",
      RUNNING -> "Running",
      PAUSING -> "Pausing",
      PAUSED -> "Paused",
      RESUMING -> "Resuming",
      COMPLETED -> "Completed",
      FAILED -> "Failed",
      KILLED -> "Killed",
      TERMINATED -> "Terminated"
    )

    // WorkflowAggregatedState also declares UNKNOWN, and protobuf can deliver a value this build
    // does not know; both must land on the catch-all rather than crash the response.
    resource invokePrivate stateToString(WorkflowAggregatedState.UNKNOWN) shouldBe "Unknown"
    resource invokePrivate stateToString(
      WorkflowAggregatedState.Unrecognized(99)
    ) shouldBe "Unknown"
  }

  "isTerminalState" should "treat exactly the four finished states as terminal" in {
    // Partitioning every declared state, so neither adding nor dropping a case survives: a state
    // wrongly called terminal makes executeWorkflowSync skip waiting for the run to finish.
    val terminal = List(
      UNINITIALIZED,
      READY,
      RUNNING,
      PAUSING,
      PAUSED,
      RESUMING,
      COMPLETED,
      FAILED,
      WorkflowAggregatedState.UNKNOWN,
      KILLED,
      TERMINATED
    ).filter(state => resource invokePrivate isTerminalState(state))

    terminal should contain theSameElementsAs List(COMPLETED, FAILED, KILLED, TERMINATED)
  }

  "hasConsoleError" should "find an ERROR anywhere in any operator's console" in {
    resource invokePrivate hasConsoleError(ExecutionConsoleStore()) shouldBe false

    val printsOnly = ExecutionConsoleStore(operatorConsole =
      Map("op-1" -> console((ConsoleMessageType.PRINT, "hello", "")))
    )
    resource invokePrivate hasConsoleError(printsOnly) shouldBe false

    // The ERROR is the SECOND message of the operator, so reading only the first message survives
    // neither this case nor production: a Python operator prints before it raises.
    val errorAfterPrint = ExecutionConsoleStore(operatorConsole =
      Map(
        "op-1" -> console(
          (ConsoleMessageType.PRINT, "hello", ""),
          (ConsoleMessageType.ERROR, "boom", "")
        )
      )
    )
    resource invokePrivate hasConsoleError(errorAfterPrint) shouldBe true

    // ... and it belongs to the SECOND operator, so scanning only one entry of the map fails too.
    val errorInSecondOperator = ExecutionConsoleStore(operatorConsole =
      Map(
        "op-1" -> console((ConsoleMessageType.PRINT, "hello", "")),
        "op-2" -> console((ConsoleMessageType.ERROR, "boom", ""))
      )
    )
    resource invokePrivate hasConsoleError(errorInSecondOperator) shouldBe true

    // COMMAND and DEBUGGER must not be mistaken for failures.
    val nonErrorTypes = ExecutionConsoleStore(operatorConsole =
      Map(
        "op-1" -> console(
          (ConsoleMessageType.COMMAND, "cmd", ""),
          (ConsoleMessageType.DEBUGGER, "dbg", "")
        )
      )
    )
    resource invokePrivate hasConsoleError(nonErrorTypes) shouldBe false
  }

  "symmetricTruncateCellValue" should "keep both ends of an oversized cell" in {
    // 30 chars, budget 27: the notice costs 17, leaving 10 to split 5/5.
    resource invokePrivate symmetricTruncateCellValue(
      thirtyChars,
      27
    ) shouldBe "01234...[truncated]...PQRST"

    // An odd leftover budget is floored, not rounded: 28 - 17 = 11 -> 5 per side, same output.
    resource invokePrivate symmetricTruncateCellValue(
      thirtyChars,
      28
    ) shouldBe "01234...[truncated]...PQRST"
  }

  it should "leave a cell that fits alone, including at exactly the budget" in {
    resource invokePrivate symmetricTruncateCellValue("short", 27) shouldBe "short"
    // Length == budget must NOT be truncated; `<` instead of `<=` would mangle it.
    resource invokePrivate symmetricTruncateCellValue(thirtyChars, 30) shouldBe thirtyChars
  }

  it should "hard-cut when the budget cannot even hold the notice" in {
    // "...[truncated]..." is 17 chars, so a 17-char budget leaves nothing to keep on either side.
    resource invokePrivate symmetricTruncateCellValue(
      thirtyChars,
      17
    ) shouldBe "0123456789ABCDEFG"
    resource invokePrivate symmetricTruncateCellValue(thirtyChars, 5) shouldBe "01234"
  }

  "truncateSingleTuple" should "shorten only textual cells over the budget" in {
    val mapper = new ObjectMapper()
    val tuple = mapper.createObjectNode()
    tuple.put("fits", "0123456789")
    // Exactly at the budget: must survive untouched. Note that this pins the *behaviour*, not the
    // comparison operator — flipping this method's `>` to `>=` changes nothing, because
    // symmetricTruncateCellValue re-checks the same bound and returns the cell as-is. The two
    // spellings are equivalent, so no input can tell them apart.
    tuple.put("exact", "01234567890123456789")
    tuple.put("oversized", "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    tuple.put("count", 42)
    tuple.put("flag", true)
    tuple.putNull("missing")

    val truncated = resource invokePrivate truncateSingleTuple(tuple, 20)

    // Asserted as serialized JSON so field order — which decides what the caller renders as
    // columns — is pinned along with the values.
    truncated.toString shouldBe
      """{"fits":"0123456789","exact":"01234567890123456789",""" +
        """"oversized":"0...[truncated]...Z","count":42,"flag":true,"missing":null}"""

    // A copy, not an edit in place: the caller still holds the untruncated tuple.
    tuple.get("oversized").asText() shouldBe "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  }

  "estimateTupleSize" should "count the serialized cell plus its array separator" in {
    val mapper = new ObjectMapper()
    val tuple = mapper.createObjectNode()
    tuple.put("a", 1)
    // {"a":1} is 7 characters; the extra 1 is the comma this tuple will need in the JSON array,
    // and dropping it makes every truncation budget quietly too generous.
    resource invokePrivate estimateTupleSize(tuple, mapper) shouldBe 8
  }

  "isVisualizationTuple" should "recognize exactly the two visualization payload columns" in {
    def tupleWith(names: String*): Tuple = {
      val schema = new Schema(names.map(new Attribute(_, AttributeType.STRING)).toList)
      names
        .foldLeft(Tuple.builder(schema))((builder, name) =>
          builder.add(name, AttributeType.STRING, "x")
        )
        .build()
    }

    resource invokePrivate isVisualizationTuple(tupleWith("html-content")) shouldBe true
    resource invokePrivate isVisualizationTuple(tupleWith("json-content")) shouldBe true
    // Present alongside ordinary columns, and not first, so a head-only check fails.
    resource invokePrivate isVisualizationTuple(tupleWith("region", "json-content")) shouldBe true

    resource invokePrivate isVisualizationTuple(tupleWith("region", "sales")) shouldBe false
    // Near-misses: the match is on the whole column name, not a substring of it — a table with a
    // "content" or "html" column must still render as a table.
    resource invokePrivate isVisualizationTuple(tupleWith("content", "html")) shouldBe false
    resource invokePrivate isVisualizationTuple(tupleWith("my-html-content")) shouldBe false

    // The catch: a malformed tuple must degrade to "not a visualization" rather than fail the
    // whole result assembly. Nothing else reaches it, since getSchema cannot throw on its own.
    resource invokePrivate isVisualizationTuple(null.asInstanceOf[Tuple]) shouldBe false
  }

  "handleExecutionError" should "report a compilation-flavoured failure separately" in {
    // The four keywords the classifier looks for, each on its own message.
    val compilationMessages = List(
      "compilation of the plan failed",
      "Compilation error at op-3",
      "unknown operator scan-1",
      "schema mismatch on port 0"
    )

    compilationMessages.foreach { message =>
      val result = resource invokePrivate handleExecutionError(new Exception(message))
      withClue(s"for message '$message': ") {
        result shouldBe SyncExecutionResult(
          success = false,
          state = "CompilationFailed",
          operators = Map.empty,
          compilationErrors = Some(Map("error" -> message)),
          errors = Some(List(message))
        )
      }
    }
  }

  it should "report any other failure as a plain error, with no compilation detail" in {
    resource invokePrivate handleExecutionError(
      new Exception("connection reset by peer")
    ) shouldBe SyncExecutionResult(
      success = false,
      state = "Error",
      operators = Map.empty,
      compilationErrors = None,
      errors = Some(List("connection reset by peer"))
    )
  }

  it should "survive an exception with no message" in {
    // A null message must take the plain-error arm (the keyword scan would NPE on it) and still
    // produce something the caller can display.
    resource invokePrivate handleExecutionError(new Exception()) shouldBe SyncExecutionResult(
      success = false,
      state = "Error",
      operators = Map.empty,
      compilationErrors = None,
      errors = Some(List("Unknown error"))
    )
  }

  "computeSubDAGIfNeeded" should "run only the target's upstream sub-DAG" in {
    // a -> b -> c, d -> c, c -> z, and an unrelated e.
    val plan = LogicalPlanPojo(
      operators = List(op("a"), op("b"), op("c"), op("d"), op("e"), op("z")),
      links = List(link("a", "b"), link("b", "c"), link("d", "c"), link("c", "z")),
      opsToViewResult = List("z"),
      opsToReuseResult = List("a", "e")
    )

    val subDag = resource invokePrivate computeSubDAGIfNeeded(plan, List("c"))

    // "z" is downstream of the target and "e" is unrelated: both must be dropped, which is the
    // whole point of "Execute To". Ids, not instances, because dfs order is map-iteration order.
    subDag.operators.map(_.operatorIdentifier.id) should contain theSameElementsAs
      List("a", "b", "c", "d")
    subDag.links should contain theSameElementsAs List(
      link("a", "b"),
      link("b", "c"),
      link("d", "c")
    )
    subDag.opsToViewResult shouldBe List("c")
    // "e" was never visited, so reusing its cached result would reference an operator that is not
    // in the plan handed to the compiler.
    subDag.opsToReuseResult shouldBe List("a")
  }

  it should "visit a diamond's shared ancestor exactly once" in {
    // a -> b -> d and a -> c -> d: without the visited set, "a" is collected twice and the
    // compiler sees a duplicate operator.
    val plan = LogicalPlanPojo(
      operators = List(op("a"), op("b"), op("c"), op("d")),
      links = List(link("a", "b"), link("a", "c"), link("b", "d"), link("c", "d")),
      opsToViewResult = List.empty,
      opsToReuseResult = List.empty
    )

    val subDag = resource invokePrivate computeSubDAGIfNeeded(plan, List("d"))

    subDag.operators.map(_.operatorIdentifier.id) should contain theSameElementsAs
      List("a", "b", "c", "d")
    subDag.operators should have size 4
    subDag.links should have size 4
  }

  it should "keep the full plan unless exactly one target is named" in {
    val plan = LogicalPlanPojo(
      operators = List(op("a"), op("b")),
      links = List(link("a", "b")),
      opsToViewResult = List("b"),
      opsToReuseResult = List("a")
    )

    // No target: a plain "run everything" request.
    (resource invokePrivate computeSubDAGIfNeeded(
      plan,
      List.empty
    )) should be theSameInstanceAs plan
    // Two targets: sub-DAG semantics are undefined, so the full DAG runs.
    (resource invokePrivate computeSubDAGIfNeeded(
      plan,
      List("a", "b")
    )) should be theSameInstanceAs plan
    // One target the plan does not contain: fall back rather than run an empty plan.
    (resource invokePrivate computeSubDAGIfNeeded(
      plan,
      List("ghost")
    )) should be theSameInstanceAs plan
  }

  "collectOperatorInfos" should "report per-operator stats, summed across ports" in {
    val stateStore = new ExecutionStateStore()
    stateStore.statsStore.updateState(
      _.withOperatorInfo(
        Map(
          "op-with-stats" -> metrics(
            COMPLETED,
            inputs = Seq(0 -> 3L, 1 -> 4L),
            outputs = Seq(0 -> 5L, 1 -> 6L)
          ),
          "op-no-ports" -> metrics(RUNNING, inputs = Seq.empty, outputs = Seq.empty)
        )
      )
    )
    val execution = newExecution(stateStore)

    val infos = resource invokePrivate collectOperatorInfos(
      ExecutionIdentity(consoleEid.toLong),
      execution,
      List("op-with-stats", "op-no-ports", "op-never-ran"),
      100000,
      20000,
      None
    )

    infos.keySet shouldBe Set("op-with-stats", "op-no-ports", "op-never-ran")

    val withStats = infos("op-with-stats")
    withStats.state shouldBe "Completed"
    // Distinct totals on purpose: 3+4 and 5+6 cannot be produced by summing the wrong side, by
    // taking a single port, or by counting ports instead of tuples.
    withStats.inputTuples shouldBe 7L
    withStats.outputTuples shouldBe 11L
    withStats.inputPortShapes shouldBe Some(List(PortShape(0, 3L), PortShape(1, 4L)))

    // An operator the engine knows but that reported no ports: an empty shape list is reported as
    // absent, so the caller does not render a zero-column table header.
    val noPorts = infos("op-no-ports")
    noPorts.state shouldBe "Running"
    noPorts.inputTuples shouldBe 0L
    noPorts.outputTuples shouldBe 0L
    noPorts.inputPortShapes shouldBe None

    // A requested operator with no stats at all — an upstream target that never started.
    val neverRan = infos("op-never-ran")
    neverRan.state shouldBe "Unknown"
    neverRan.inputTuples shouldBe 0L
    neverRan.outputTuples shouldBe 0L
    neverRan.inputPortShapes shouldBe None

    // No result URI is registered for this execution, so every entry reports an absent table
    // rather than an empty one — the caller distinguishes the two.
    infos.values.foreach { info =>
      info.resultMode shouldBe "table"
      info.result shouldBe None
      info.totalRowCount shouldBe None
      info.consoleLogs shouldBe None
      info.error shouldBe None
      info.warnings shouldBe None
    }
  }

  it should "report every operator the engine knows when no target is named" in {
    val stateStore = new ExecutionStateStore()
    stateStore.statsStore.updateState(
      _.withOperatorInfo(
        Map(
          "op-1" -> metrics(COMPLETED, Seq(0 -> 1L), Seq(0 -> 2L)),
          "op-2" -> metrics(FAILED, Seq(0 -> 3L), Seq(0 -> 4L))
        )
      )
    )

    val infos = resource invokePrivate collectOperatorInfos(
      ExecutionIdentity(consoleEid.toLong),
      newExecution(stateStore),
      List.empty,
      100000,
      20000,
      None
    )

    // Two operators, not one: "all of them" has to mean the whole stats map.
    infos.keySet shouldBe Set("op-1", "op-2")
    infos("op-1").state shouldBe "Completed"
    infos("op-2").state shouldBe "Failed"
  }

  it should "surface console errors, warnings and operators that are not targets" in {
    val stateStore = new ExecutionStateStore()
    stateStore.statsStore.updateState(
      _.withOperatorInfo(Map("target" -> metrics(FAILED, Seq(0 -> 1L), Seq.empty)))
    )
    val consoleState = ExecutionConsoleStore(operatorConsole =
      Map(
        "target" -> console(
          (ConsoleMessageType.PRINT, "WARNING: input looked empty", ""),
          // A print that merely mentions the word: `contains` instead of `startsWith` would
          // wrongly promote it to a warning.
          (ConsoleMessageType.PRINT, "note: WARNING: appears mid-line", ""),
          (ConsoleMessageType.ERROR, "short title", "a much longer python traceback body")
        ),
        // Not a target: an upstream operator whose failure is the reason the target failed. Its
        // console must still reach the caller.
        "upstream" -> console((ConsoleMessageType.ERROR, "scala style error", "")),
        "chatty" -> console((ConsoleMessageType.PRINT, "just progress", "")),
        // Present in the map but with no messages at all. ExecutionConsoleService creates exactly
        // this shape -- `getOrElse(opId, OperatorConsole())` -- before it has anything to add, so
        // without it nothing distinguishes "no console" (None) from "an empty console" (Some(Nil)).
        "silent" -> OperatorConsole()
      )
    )

    val infos = resource invokePrivate collectOperatorInfos(
      ExecutionIdentity(consoleEid.toLong),
      newExecution(stateStore),
      List("target"),
      100000,
      20000,
      Some(consoleState)
    )

    infos.keySet shouldBe Set("target", "upstream", "chatty", "silent")

    val target = infos("target")
    target.consoleLogs shouldBe Some(
      List(
        ConsoleMessageInfo("PRINT", "WARNING: input looked empty", ""),
        ConsoleMessageInfo("PRINT", "note: WARNING: appears mid-line", ""),
        ConsoleMessageInfo("ERROR", "short title", "a much longer python traceback body")
      )
    )
    // Python puts the full error in `message`; the longer of the two fields wins.
    target.error shouldBe Some("a much longer python traceback body")
    target.warnings shouldBe Some(List("WARNING: input looked empty"))

    // Scala puts the error in `title` and a stack trace in `message`; here `message` is empty, so
    // the title has to be used instead of an empty string.
    infos("upstream").error shouldBe Some("scala style error")
    infos("upstream").state shouldBe "Unknown"

    // Prints only: logs travel, but no error and — critically — no empty warnings list.
    infos("chatty").consoleLogs shouldBe Some(
      List(ConsoleMessageInfo("PRINT", "just progress", ""))
    )
    infos("chatty").error shouldBe None
    infos("chatty").warnings shouldBe None

    // An operator whose console exists but is empty must report None, not Some(Nil): the frontend
    // renders a console pane for Some and nothing for None, so Some(Nil) is an empty pane.
    infos("silent").consoleLogs shouldBe None
    infos("silent").error shouldBe None
    infos("silent").warnings shouldBe None
  }

  it should "let a disabled-warehouse refusal reach the caller" in {
    // #6930: every other storage failure degrades into an absent result, but a kill-switch
    // refusal must not — "no data" is indistinguishable from data loss. The URI carries an
    // unresolvable warehouse name, which WarehouseReadGuard refuses in either flag state, so this
    // does not depend on how the deployment has the switch set.
    insertConsoleUri(
      otherEid,
      "guarded",
      s"vfs:///wh/a%2Fb/wid/$testWid/eid/$otherEid/opid/guarded/consolemessages"
    )
    val stateStore = new ExecutionStateStore()
    stateStore.statsStore.updateState(
      _.withOperatorInfo(Map("guarded" -> metrics(COMPLETED, Seq.empty, Seq.empty)))
    )

    a[WarehouseUnavailableException] should be thrownBy {
      resource invokePrivate collectOperatorInfos(
        ExecutionIdentity(otherEid.toLong),
        newExecution(stateStore),
        List("guarded"),
        100000,
        20000,
        None
      )
    }
  }

  "collectOperatorResult" should "return every row of a result that fits the char budget" in {
    storeResult("serc-all-fit", (0 until 3).map(row))

    val result = collect("serc-all-fit", 100000, 20000)

    result._1 shouldBe "table"
    // Asserted as serialized JSON rather than by row count: the outermost catch turns any storage
    // failure into a well-formed ("table", None, ...), so a fixture that never opened a document
    // would satisfy anything weaker. It also pins __row_index__, the column order the caller
    // renders, and — at 28 characters — the `rowChars` the budgets below are multiples of.
    rowsOf(result).map(_.toString) shouldBe List(
      """{"v":"r0","__row_index__":0}""",
      """{"v":"r1","__row_index__":1}""",
      """{"v":"r2","__row_index__":2}"""
    )
    result._3 shouldBe Some(3)
    result._4 shouldBe Some(3)
    result._5 shouldBe Some(false)
  }

  it should "report an empty result as zero rows rather than as an absent one" in {
    storeResult("serc-empty", Seq.empty)

    val result = collect("serc-empty", 100000, 20000)

    result._1 shouldBe "table"
    // Some(empty) and not None: the caller renders "0 rows" for an empty payload and "no result"
    // for an absent one, and this early return is the only thing keeping the two apart — without
    // it the walk asks the exhausted iterator for a first tuple and degrades into None.
    rowsOf(result) shouldBe empty
    result._3 shouldBe Some(0)
    result._4 shouldBe Some(0)
    result._5 shouldBe Some(false)
  }

  it should "hand a lone html-content row back as a visualization payload" in {
    // 120 characters, so this also pins the `isVisualization = true` argument: ordinary STRING
    // cells are capped at 100 and suffixed "..." on the way to JSON, and a visualization payload
    // must arrive whole. The cell budget is 20 for the same reason — per-cell truncation must not
    // be applied to it either.
    val html = "<p>" + ("h" * 113) + "</p>"
    html should have length 120
    val vizSchema = new Schema(List(new Attribute("html-content", AttributeType.STRING)))
    storeResult(
      "serc-viz",
      Seq(Tuple.builder(vizSchema).add("html-content", AttributeType.STRING, html).build()),
      vizSchema
    )

    val result = collect("serc-viz", 100000, 20)

    result._1 shouldBe "visualization"
    val rows = rowsOf(result)
    rows should have size 1
    rows.head.get("html-content").asText() shouldBe html
    // The flag the frontend switches on to render an iframe instead of a table.
    rows.head.get("__is_visualization__").asBoolean() shouldBe true
    result._3 shouldBe Some(1)
    result._4 shouldBe Some(1)
    result._5 shouldBe Some(false)
  }

  it should "keep a single truncated row when the first row alone fills the budget" in {
    storeResult("serc-first-fills", (0 until 3).map(row))

    // Exactly at the budget, where the guard has to fire: with `>` instead of `>=` the walk falls
    // through into the sliding window and reports two rows.
    val result = collect("serc-first-fills", rowChars, 20000)

    result._1 shouldBe "table"
    rowsOf(result).map(_.toString) shouldBe List("""{"v":"r0","__row_index__":0}""")
    // Three rows exist but one is shown: the caller needs both numbers to say "1 of 3".
    result._3 shouldBe Some(3)
    result._4 shouldBe Some(1)
    result._5 shouldBe Some(true)
  }

  it should "drop the middle of an oversized result and keep the newest rows" in {
    storeResult("serc-mid-walk-window", (0 until 10).map(row))

    // The front half holds three rows and then rejects the fourth; that rejection is what opens
    // the sliding window mid-walk. The +1 keeps the budget off an exact multiple of rowChars,
    // where the front loop would end on its own condition and take the other window block instead.
    val halfLimit = 3 * rowChars + 1
    val result = collect("serc-mid-walk-window", 2 * halfLimit, 20000)

    result._1 shouldBe "table"
    // Non-contiguous on purpose. Rows 3-8 are gone, so the window really slid rather than just
    // stopping; and the survivor is 9 rather than 3, so it evicted the OLDEST of the tail.
    rowsOf(result).map(rowIndexOf) shouldBe List(0, 1, 2, 9)
    result._3 shouldBe Some(10)
    result._4 shouldBe Some(4)
    result._5 shouldBe Some(true)
  }

  it should "walk the tail through the back window once the front half is exactly full" in {
    storeResult("serc-front-exact", (0 until 4).map(row))

    // The front loop only ends on its own condition when frontSize lands exactly on halfLimit,
    // which needs a budget that is an exact multiple of rowChars; any round char limit rejects a
    // row first and covers the mid-walk window above instead. Three rows fill the front half, and
    // the fourth fits the back half's budget (halfLimit less the 50 reserved for the notice).
    //
    // This does NOT pin the loop's bound itself: relaxing `frontSize < halfLimit` to `<=` is an
    // equivalent mutant. Once the front is exactly full no tuple can fit it (sizes are positive),
    // so the relaxed loop enters, immediately takes its else branch, and runs the same window over
    // the same remaining iterator. No input can tell the two spellings apart.
    val result = collect("serc-front-exact", 6 * rowChars, 20000)

    result._1 shouldBe "table"
    rowsOf(result).map(rowIndexOf) shouldBe List(0, 1, 2, 3)
    result._3 shouldBe Some(4)
    result._4 shouldBe Some(4)
    // Nothing was dropped, so this block must not raise the flag merely because it ran.
    result._5 shouldBe Some(false)
  }

  it should "slide the back window on the tail as well, not only mid-walk" in {
    storeResult("serc-tail-window", (0 until 5).map(row))

    // Two rows fill the front half exactly, so the tail is walked by the same block as above — but
    // its budget (halfLimit less the 50-char notice) is now under one row, so every append there
    // has to evict. Without the eviction all five rows come back.
    val result = collect("serc-tail-window", 4 * rowChars, 20000)

    result._1 shouldBe "table"
    rowsOf(result).map(rowIndexOf) shouldBe List(0, 1, 4)
    result._3 shouldBe Some(5)
    result._4 shouldBe Some(3)
    result._5 shouldBe Some(true)
  }

  it should "shorten an oversized cell in every row, not only in the first" in {
    // 60 characters: under convertTuplesToJson's own 100-char cap, so the only thing that can
    // shorten it is the cell budget this method passes down.
    val wide = "0123456789" * 6
    val wideSchema = new Schema(List(new Attribute("wide", AttributeType.STRING)))
    storeResult(
      "serc-wide-cells",
      Seq.fill(2)(Tuple.builder(wideSchema).add("wide", AttributeType.STRING, wide).build()),
      wideSchema
    )

    val result = collect("serc-wide-cells", 100000, 30)

    // 30 less the 17-char notice, floored to 6 per side. Asserted for BOTH rows: the first row and
    // the loop body pass the cell budget down at separate call sites, so one of them handed the
    // whole-result budget by mistake would otherwise stay green.
    rowsOf(result).map(_.get("wide").asText()) shouldBe
      List.fill(2)("012345...[truncated]...456789")
    result._3 shouldBe Some(2)
    result._4 shouldBe Some(2)
    result._5 shouldBe Some(false)
  }

  it should "let a disabled-warehouse refusal reach the caller" in {
    // #6930, on the result path this time: every other storage failure degrades into an absent
    // result, but a kill-switch refusal must not — "no data" is indistinguishable from data loss.
    // The URI carries an unresolvable warehouse name, which WarehouseReadGuard refuses in either
    // flag state, so this does not depend on how the deployment has the switch set. Built by hand
    // because VFSURIFactory rejects a name it could not parse back.
    val opId = "serc-guarded"
    registerResultUri(
      new URI(
        s"vfs:///wh/a%2Fb/wid/$testWid/eid/$resultEid" +
          s"/globalportid/${globalPortIdOf(opId).serializeAsString}/result"
      )
    )

    a[WarehouseUnavailableException] should be thrownBy collect(opId, 100000, 20000)
  }

  it should "degrade to an absent result when the registered URI has no document behind it" in {
    // The execution row records a result URI before anything is written to it, so a report asked
    // for in that window must still come back — as an absent result, not as an exception.
    val opId = "serc-no-document"
    registerResultUri(resultUriOf(opId))

    val result = collect(opId, 100000, 20000)

    result._1 shouldBe "table"
    result._2 shouldBe None
    result._3 shouldBe None
    result._4 shouldBe None
    result._5 shouldBe None
  }

  "getConsoleMessageUri" should "find the console URI for one operator of one execution" in {
    def consoleUri(eid: Int, opId: String) =
      s"vfs:///wid/$testWid/eid/$eid/opid/$opId/consolemessages"

    insertConsoleUri(consoleEid, "op-a", consoleUri(consoleEid, "op-a"))
    insertConsoleUri(consoleEid, "op-b", consoleUri(consoleEid, "op-b"))
    // Same operator id under a different execution: the execution predicate is what keeps this
    // out of the answer, and a run's console must never leak into an earlier run's report.
    insertConsoleUri(otherEid, "op-a", consoleUri(otherEid, "op-a"))
    // A row that exists but was never given a URI (the operator produced no console output).
    insertConsoleUri(consoleEid, "op-empty", "")

    resource invokePrivate getConsoleMessageUri(
      ExecutionIdentity(consoleEid.toLong),
      OperatorIdentity("op-a")
    ) shouldBe Some(URI.create(consoleUri(consoleEid, "op-a")))
    // A second operator of the SAME execution: without the operator predicate the query matches
    // two rows and cannot answer at all.
    resource invokePrivate getConsoleMessageUri(
      ExecutionIdentity(consoleEid.toLong),
      OperatorIdentity("op-b")
    ) shouldBe Some(URI.create(consoleUri(consoleEid, "op-b")))
    resource invokePrivate getConsoleMessageUri(
      ExecutionIdentity(otherEid.toLong),
      OperatorIdentity("op-a")
    ) shouldBe Some(URI.create(consoleUri(otherEid, "op-a")))

    // An empty stored URI is "nothing to read", not a URI that resolves to the storage root.
    resource invokePrivate getConsoleMessageUri(
      ExecutionIdentity(consoleEid.toLong),
      OperatorIdentity("op-empty")
    ) shouldBe None
    resource invokePrivate getConsoleMessageUri(
      ExecutionIdentity(consoleEid.toLong),
      OperatorIdentity("op-absent")
    ) shouldBe None
  }

  "killExecution" should "stamp an end time and mark the execution killed" in {
    val stateStore = new ExecutionStateStore()
    val execution = newExecution(stateStore)
    stateStore.statsStore.getState.endTimeStamp shouldBe 0L

    val before = System.currentTimeMillis()
    resource invokePrivate killExecution(execution)
    val after = System.currentTimeMillis()

    // Bounded on both sides: a constant, or a stamp taken from somewhere other than the wall
    // clock, would fall outside the window. The end time is what the UI shows as the run's
    // duration, so it has to be the moment of the kill.
    val endTimeStamp = stateStore.statsStore.getState.endTimeStamp
    endTimeStamp should be >= before
    endTimeStamp should be <= after
    stateStore.metadataStore.getState.state shouldBe KILLED
  }

  "executeWorkflowSync" should "report a run whose plan cannot compile as a failure" in {
    // A scan source with no file selected: the engine rejects it during compilation, so this
    // reaches initExecutionService, the execution row, the engine, and the whole result-assembly
    // tail without needing a coordinator to come up.
    //
    // What this does NOT pin, deliberately: which arm produced `state`. On this path
    // `terminatedByConsoleError` is false and `stateToString(finalState.state)` is also "Failed",
    // so inverting the console-error branch at SyncExecutionResource.scala:328 leaves the suite
    // green. Distinguishing the two arms needs a run whose final state is not FAILED, which is
    // impossible without a live engine here -- with no coordinator, initExecutionService always
    // absorbs the failure and stamps FAILED. Recorded rather than papered over.
    val scan = new CSVScanSourceOpDesc()
    scan.setOperatorId("scan-op")
    val user = new User
    user.setUid(testUid)
    user.setEmail("sync-exec@example.com")

    val result = resource.executeWorkflowSync(
      testWid.toLong,
      testCuid,
      SyncExecutionRequest(
        executionName = "sync-spec-run",
        logicalPlan = LogicalPlanPojo(List(scan), List.empty, List.empty, List.empty),
        workflowSettings = None,
        targetOperatorIds = List("scan-op"),
        timeoutSeconds = 5,
        maxOperatorResultCharLimit = 100000,
        maxOperatorResultCellCharLimit = 20000
      ),
      new SessionUser(user)
    )

    result.success shouldBe false
    result.state shouldBe "Failed"
    // compilationErrors is always None on this path — the endpoint reports compile failures that
    // happen inside the engine as fatal errors, not as the compilationErrors map.
    result.compilationErrors shouldBe None
    val errors =
      result.errors.getOrElse(fail("expected the fatal compilation error to be reported"))
    errors should have size 1
    // The fatal error is rendered as "<type>: <message>"; dropping the type loses the only
    // machine-readable part of it.
    errors.head should startWith("EXECUTION_FAILURE: ")
    errors.head should include("No file selected")

    // The requested target is still reported, with no stats, so the caller learns which operator
    // it asked about rather than getting an empty map.
    result.operators.keySet shouldBe Set("scan-op")
    result.operators("scan-op").state shouldBe "Unknown"
    result.operators("scan-op").error shouldBe None
  }
}
