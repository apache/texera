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

import io.reactivex.rxjava3.disposables.Disposable
import org.apache.texera.amber.core.storage.result.{OperatorResultMetadata, WorkflowResultStore}
import org.apache.texera.amber.core.virtualidentity.{OperatorIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{WorkflowContext, WorkflowSettings}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  PAUSED,
  RUNNING
}
import org.apache.texera.amber.engine.common.Utils
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{
  OPERATOR_EXECUTIONS,
  OPERATOR_PORT_EXECUTIONS,
  WORKFLOW_EXECUTIONS,
  WORKFLOW_VERSION
}
import org.apache.texera.dao.jooq.generated.enums.WorkflowComputingUnitTypeEnum
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
import org.apache.texera.web.model.websocket.event.ExecutionDurationUpdateEvent
import org.apache.texera.common.compiler.model.LogicalPlanPojo
import org.apache.texera.web.WorkflowLifecycleManager
import org.apache.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WebResultUpdateEvent,
  WorkflowStateEvent
}
import org.apache.texera.web.model.websocket.request.WorkflowExecuteRequest
import org.apache.texera.web.storage.{ExecutionStateStore, WorkflowStateStore}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.sql.Timestamp
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

/**
  * Unit tests for the per-workflow service that outlives a single execution: it owns the
  * cross-execution state store, republishes each new execution to every connected client, and
  * decides what becomes of the previous execution's subscriptions when a new one arrives.
  *
  * Two collaborators are stubbed, both for reasons that would otherwise make these paths
  * untestable rather than merely inconvenient:
  *
  *   - `lifeCycleManager`. The real one schedules the clean-up through `AmberRuntime`'s global
  *     actor system, which no unit test starts, and its callback reads `workflow_executions` from
  *     the database and shuts down the execution's Amber client. Stubbing it also makes the two
  *     things `WorkflowService` actually contributes here observable: that `connect` registers a
  *     user, and that `disconnect` reports the *current execution's* state (the value that decides
  *     whether clean-up is postponed).
  *   - `resultService`, so that the third line of `unsubscribeAll` is pinned. A real
  *     `ExecutionResultService` holds no subscriptions until `attachToExecution` gives it a live
  *     Amber client, so "unsubscribed" would be indistinguishable from "never called".
  *
  * Executions are real `WorkflowExecutionService` instances with `coordinatorConfig`/`resultService`
  * passed as `null` (the pattern `WorkflowExecutionServiceSpec` establishes: construction does no
  * external work), so the events observed here travel through the production diff handler.
  *
  * Two cases run `initExecutionService` end to end against `MockTexeraDB`, for the previous-run
  * clean-up it performs before inserting the new execution row. Both are steered into a
  * compilation failure (a CSV scan with no file selected, the recipe `SyncExecutionResourceSpec`
  * establishes) so the method stops inside `executeWorkflow` and never reaches
  * `ComputingUnitMaster.createAmberRuntime`, which builds an `AmberClient` on
  * `AmberRuntime.actorSystem` — null outside a started coordinator. That stopping point is
  * asserted rather than assumed: it rests entirely on `executeWorkflow`'s early `return`, and
  * without the assertion, removing that `return` would send this "unit" test into
  * `createAmberRuntime` on whatever actor system another suite left installed in the shared JVM,
  * still green, because `initExecutionService`'s catch-all absorbs the fallout.
  *
  * `clearExecutionResources` reaches real storage, so only part of it is observable in process.
  * Pinned here: which execution it selects (workflow *and* computing unit, newest only), that it
  * deletes exactly that execution's registry rows, and that it reads the registered URIs *before*
  * deleting the rows that hold them. Admitted survivors, listed so the coverage numbers are not
  * mistaken for behaviour:
  *   - `LargeBinaryManager.deleteByExecution` (the last line). Its injectable overload is
  *     `private[util]` and the call site uses the one-arg one, so deleting the line, or handing
  *     it a different execution id, is invisible here. It also issues a REAL S3 delete against
  *     the configured endpoint on every run of this suite (`http://localhost:9000` by default).
  *     That is why the fixtures below pin explicit eids in the 941100 range instead of taking the
  *     `SERIAL`: a fresh `MockTexeraDB` database hands out eid 1, and `objects/1/` is the prefix
  *     `LargeBinaryManager` shares with its DEFAULT_EXECUTION_ID sentinel — a developer with
  *     `bin/local-dev.sh up` running would have that prefix recursively deleted for real.
  *   - the runtime-statistics block. Its URI column is NULL in every fixture, so the loop body
  *     runs zero times; a non-NULL value there would abort the whole method (see below) before
  *     the execution insert this suite also pins.
  *   - which arm of `WarehouseReadGuard.skipWhileDisabled` either loop takes: both arms end in
  *     `DocumentFactory`, which needs a live Iceberg/LakeFS backend, and the loop body swallows
  *     every `Throwable`.
  *   - swapping the two URI reads with each other. That is an equivalent mutant, not a gap: the
  *     two lists are concatenated and every element is treated identically.
  *
  * Not a contract, and deliberately not pinned as one: `WarehouseReadGuard.skipWhileDisabled` and
  * `WorkflowExecutionsResource`'s `URI.create` both sit *outside* the per-URI catch, so a single
  * registry row whose URI does not decode aborts `initExecutionService` before the new execution
  * row is even inserted — after which the user can start no run at all of that workflow.
  * Best-effort clean-up should survive an undecodable row; this is a robustness gap worth fixing.
  * The read-order case below uses it only as the one in-process witness that the reads happen
  * while the rows still exist. If the gap is ever closed, re-point that case at whatever new
  * evidence shows the read happened — do not delete it, the ordering it pins is real.
  *
  * Deliberately not covered:
  *   - the replay block of `initExecutionService`. It only mutates a local `CoordinatorConfig`
  *     whose `stateRestoreConfOpt` is read past the compilation failure, so on every path a unit
  *     test can reach it has no observable effect at all; pinning it would mean reflecting into
  *     `WorkflowExecutionService`'s private field.
  *   - the fault-tolerance block of `initExecutionService`, gated on
  *     `ApplicationConfig.faultToleranceLogRootFolder`: a `val` on a Scala `object` with no
  *     override seam, read at class-initialization time in a JVM shared with every other suite.
  *   - `resolveLakekeeperWarehouseName`, owned by `WorkflowServiceWarehouseSpec`.
  */
class WorkflowServiceSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  /** Long enough that a stray clean-up deadline could never fire inside a test run. */
  private val cleanUpTimeoutSecs = 3600

  private class TrackingDisposable extends Disposable {
    private var disposed = false

    override def dispose(): Unit = disposed = true

    override def isDisposed: Boolean = disposed
  }

  /** Records what `WorkflowService` reports, and never schedules the real clean-up. */
  private final class RecordingLifecycleManager
      extends WorkflowLifecycleManager("WorkflowServiceSpec", cleanUpTimeoutSecs, () => ()) {
    var increaseCount = 0
    val reportedStates: ArrayBuffer[Option[WorkflowAggregatedState]] = ArrayBuffer.empty

    override def increaseUserCount(): Unit = increaseCount += 1

    override def decreaseUserCount(currentWorkflowState: Option[WorkflowAggregatedState]): Unit =
      reportedStates += currentWorkflowState

    val registeredStores: ArrayBuffer[ExecutionStateStore] = ArrayBuffer.empty

    /**
      * The real one subscribes to the store's state observable, which replays the current state
      * to a new subscriber straight away; the resulting deadline is scheduled through
      * `AmberRuntime`'s actor system, null outside a started coordinator. Left in place, every
      * state update during `initExecutionService` raises an NPE inside an RxJava consumer.
      *
      * Recorded rather than discarded, because this call is the single statement that arms the
      * whole deferred clean-up: stubbed to a no-op it would also be unkillable, and a build in
      * which `initExecutionService` never registers the deadline would look identical.
      */
    override def registerCleanUpOnStateChange(stateStore: ExecutionStateStore): Unit =
      registeredStores += stateStore
  }

  private final class RecordingResultService(
      workflowId: WorkflowIdentity,
      computingUnitId: Int,
      stateStore: WorkflowStateStore
  ) extends ExecutionResultService(workflowId, computingUnitId, stateStore) {
    var unsubscribeCount = 0

    override def unsubscribeAll(): Unit = {
      unsubscribeCount += 1
      super.unsubscribeAll()
    }
  }

  private final class TestWorkflowService(id: Long, cuid: Int = 1)
      extends WorkflowService(WorkflowIdentity(id), computingUnitId = cuid, cleanUpTimeoutSecs) {
    val lifecycle = new RecordingLifecycleManager
    override val lifeCycleManager: WorkflowLifecycleManager = lifecycle
    val results = new RecordingResultService(workflowId, computingUnitId, stateStore)
    override val resultService: ExecutionResultService = results
  }

  // ---------------------------------------------------------------------------
  // Fixtures for the previous-run clean-up case. `initExecutionService` needs the whole
  // user / workflow / workflow_version / workflow_computing_unit chain before it can insert
  // its own execution row: workflow_executions.uid, .vid and .cuid are all foreign keys.
  // ---------------------------------------------------------------------------

  // Four id domains, four different literals. `getLatestExecutionID(wid, cuid)` binds two bare
  // `Integer`s into one predicate and `insertNewExecution` takes wid and uid side by side, so a
  // fixture that reused one number for all of them would make every transposition of those
  // arguments produce byte-identical SQL.
  private val testWid = 9411

  /** A second workflow that shares `testCuid`, so the WID leg of the predicate is not vacuous. */
  private val otherWid = 9412
  private val testUid = 9413

  /** The computing unit the run under test executes on. */
  private val testCuid = 9414

  /** A second computing unit of the same workflow, so the clean-up's scope is not vacuous. */
  private val otherCuid = 9415

  /** The unit the read-before-delete case runs on, kept apart from every other case's rows. */
  private val probeCuid = 9416

  // Executions carry explicit eids rather than the SERIAL's: see the header on
  // LargeBinaryManager. The relative order is load-bearing and is stated here rather than
  // inherited from insertion order.

  /** An older, superseded execution of (`testWid`, `testCuid`). Must survive untouched. */
  private val olderEid = 941100

  /** The newest execution of (`testWid`, `testCuid`): the run whose registry must be cleared. */
  private val previousEid = 941101

  /**
    * A LATER execution of the same workflow on a different computing unit. Its eid is larger than
    * `previousEid` on purpose: `getLatestExecutionID` picks the maximum eid *among the rows of
    * the requested computing unit*, so without the CUID leg this row would be chosen instead.
    */
  private val otherUnitEid = 941102

  /**
    * A LATER execution of a *different* workflow on the SAME computing unit. `workflow_computing_unit`
    * is keyed by uid and carries no wid, so one unit legitimately runs many workflows; without the
    * WID leg this row is what `getLatestExecutionID` returns, and a new run of `testWid` would
    * wipe an unrelated workflow's registry.
    */
  private val otherWorkflowEid = 941103

  /** The read-before-delete case's execution; the only fixture with a non-NULL URI column. */
  private val probeEid = 941104

  private val fixtureEids: Seq[Integer] =
    Seq(olderEid, previousEid, otherUnitEid, otherWorkflowEid, probeEid).map(Integer.valueOf)

  private var executingUser: User = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    val cfg = getDSLContext.configuration()

    executingUser = new User
    executingUser.setUid(testUid)
    executingUser.setName("workflow_service_spec_user")
    executingUser.setEmail(s"workflow-service-${UUID.randomUUID()}@example.com")
    new UserDao(cfg).insert(executingUser)

    val workflowDao = new WorkflowDao(cfg)
    val versionDao = new WorkflowVersionDao(cfg)
    List(testWid -> "workflow_service_spec_workflow", otherWid -> "workflow_service_spec_other")
      .foreach {
        case (wid, name) =>
          val workflow = new Workflow
          workflow.setWid(wid)
          workflow.setName(name)
          workflow.setContent("{}")
          workflow.setCreationTime(new Timestamp(System.currentTimeMillis()))
          workflow.setLastModifiedTime(new Timestamp(System.currentTimeMillis()))
          workflowDao.insert(workflow)

          val version = new WorkflowVersion
          version.setWid(wid)
          version.setContent("{}")
          version.setCreationTime(new Timestamp(System.currentTimeMillis()))
          versionDao.insert(version)
      }

    val unitDao = new WorkflowComputingUnitDao(cfg)
    List(
      testCuid -> "workflow_service_spec_unit",
      otherCuid -> "workflow_service_spec_other_unit",
      probeCuid -> "workflow_service_spec_probe_unit"
    ).foreach {
      case (cuid, name) =>
        val unit = new WorkflowComputingUnit
        unit.setCuid(cuid)
        unit.setUid(testUid)
        unit.setName(name)
        unit.setType(WorkflowComputingUnitTypeEnum.local)
        unit.setCreationTime(new Timestamp(System.currentTimeMillis()))
        unitDao.insert(unit)
    }

    insertExecution(olderEid, testWid, testCuid, "older-run")
    insertExecution(previousEid, testWid, testCuid, "previous-run")
    insertExecution(otherUnitEid, testWid, otherCuid, "other-unit-run")
    insertExecution(otherWorkflowEid, otherWid, testCuid, "other-workflow-run")
    insertExecution(probeEid, testWid, probeCuid, "probe-run")

    // Keep the SERIAL sequence ahead of our explicit fixture ids, so the execution that
    // production inserts below cannot land on eid 1 -- LargeBinaryManager's DEFAULT_EXECUTION_ID
    // sentinel, whose `objects/1/` prefix is shared rather than owned (see the header).
    //
    // The table name is qualified from the generated metadata rather than written bare:
    // `pg_get_serial_sequence` resolves its argument through the connection's search_path, and
    // only the throwaway connection that ran the DDL had `texera_db` on it. Every other statement
    // in this fixture goes through jOOQ, which qualifies for us -- a bare name here raises
    // `relation "workflow_executions" does not exist` and aborts the suite.
    getDSLContext.execute(
      "select setval(pg_get_serial_sequence(?, ?), ?, true)",
      s"${WORKFLOW_EXECUTIONS.getSchema.getName}.${WORKFLOW_EXECUTIONS.getName}",
      WORKFLOW_EXECUTIONS.EID.getName,
      Integer.valueOf(probeEid)
    )

    // A different row count per execution, so a clean-up that took the wrong one cannot be
    // mistaken for one that took the right one.
    (0 until 3).foreach { i =>
      registerOperator(olderEid, s"older-op-$i")
      registerPort(olderEid, s"older-port-$i")
    }
    (0 until 2).foreach { i =>
      registerOperator(previousEid, s"prev-op-$i")
      registerPort(previousEid, s"prev-port-$i")
    }
    registerOperator(otherUnitEid, "other-op")
    registerPort(otherUnitEid, "other-port")
    (0 until 4).foreach { i =>
      registerOperator(otherWorkflowEid, s"other-wf-op-$i")
      registerPort(otherWorkflowEid, s"other-wf-port-$i")
    }
    registerOperator(probeEid, "probe-op")
    // The one non-NULL URI in the fixture: syntactically a URI, but not a `vfs` one, so
    // `VFSURIFactory.decodeURI` rejects it. See the header -- this is the witness that the
    // registry was read, not an assertion that malformed rows ought to be fatal.
    registerPort(probeEid, "probe-port", resultUri = "bogus:///stranded")
  }

  override protected def afterAll(): Unit = closeConnectionPool()

  private def versionIdOf(wid: Int): Integer =
    getDSLContext
      .select(WORKFLOW_VERSION.VID)
      .from(WORKFLOW_VERSION)
      .where(WORKFLOW_VERSION.WID.eq(wid))
      .fetchOneInto(classOf[Integer])

  private def insertExecution(eid: Int, wid: Int, cuid: Int, name: String): Unit =
    getDSLContext
      .insertInto(WORKFLOW_EXECUTIONS)
      .set(WORKFLOW_EXECUTIONS.EID, Integer.valueOf(eid))
      .set(WORKFLOW_EXECUTIONS.VID, versionIdOf(wid))
      .set(WORKFLOW_EXECUTIONS.UID, Integer.valueOf(testUid))
      .set(WORKFLOW_EXECUTIONS.CUID, Integer.valueOf(cuid))
      // Spelled out rather than left to the DDL's default, which is RUNNING: what this suite
      // pins is a *finished* previous run being cleared. Whether a still-live execution's
      // registry may be wiped while `ExecutionResultService` is still serving from it is a
      // separate question production leaves open ("TODO: change this behavior after enabling
      // cache."), and nothing here should decide it.
      .set(
        WORKFLOW_EXECUTIONS.STATUS,
        java.lang.Short.valueOf(Utils.maptoStatusCode(COMPLETED).toShort)
      )
      .set(WORKFLOW_EXECUTIONS.NAME, name)
      .set(WORKFLOW_EXECUTIONS.ENVIRONMENT_VERSION, "test")
      .execute()

  /** A console-message registry row. The URI column stays NULL: see the header comment. */
  private def registerOperator(eid: Int, operatorId: String): Unit =
    getDSLContext
      .insertInto(OPERATOR_EXECUTIONS)
      .set(OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID, Integer.valueOf(eid))
      .set(OPERATOR_EXECUTIONS.OPERATOR_ID, operatorId)
      .execute()

  /** A result registry row. The URI column stays NULL unless a case needs otherwise. */
  private def registerPort(eid: Int, globalPortId: String, resultUri: String = null): Unit =
    getDSLContext
      .insertInto(OPERATOR_PORT_EXECUTIONS)
      .set(OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID, Integer.valueOf(eid))
      .set(OPERATOR_PORT_EXECUTIONS.GLOBAL_PORT_ID, globalPortId)
      .set(OPERATOR_PORT_EXECUTIONS.RESULT_URI, resultUri)
      .execute()

  /** Executions this fixture did not seed: after a run, exactly the ones production inserted. */
  private def executionsOutsideFixture: Int =
    getDSLContext.fetchCount(WORKFLOW_EXECUTIONS, WORKFLOW_EXECUTIONS.EID.notIn(fixtureEids: _*))

  private def insertedExecution(): WorkflowExecutions =
    new WorkflowExecutionsDao(getDSLContext.configuration()).fetchOneByEid(
      getDSLContext
        .select(WORKFLOW_EXECUTIONS.EID)
        .from(WORKFLOW_EXECUTIONS)
        .where(WORKFLOW_EXECUTIONS.EID.notIn(fixtureEids: _*))
        .fetchOneInto(classOf[Integer])
    )

  private def operatorRowCount(eid: Int): Int =
    getDSLContext.fetchCount(
      OPERATOR_EXECUTIONS,
      OPERATOR_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid)
    )

  private def portRowCount(eid: Int): Int =
    getDSLContext.fetchCount(
      OPERATOR_PORT_EXECUTIONS,
      OPERATOR_PORT_EXECUTIONS.WORKFLOW_EXECUTION_ID.eq(eid)
    )

  private def executionRowCount(eid: Int): Int =
    getDSLContext.fetchCount(WORKFLOW_EXECUTIONS, WORKFLOW_EXECUTIONS.EID.eq(eid))

  private def newExecution(): WorkflowExecutionService = {
    val request = WorkflowExecuteRequest(
      executionName = "test",
      engineVersion = "test",
      logicalPlan = LogicalPlanPojo(List.empty, List.empty, List.empty, List.empty),
      replayFromExecution = None,
      workflowSettings = WorkflowSettings(),
      emailNotificationEnabled = false,
      computingUnitId = 1,
      warehouseId = None
    )
    new WorkflowExecutionService(
      null,
      new WorkflowContext(),
      null,
      request,
      new ExecutionStateStore(),
      (_: Throwable) => (),
      None,
      new URI("vfs:///test")
    )
  }

  /** Collects the events an execution's metadata store emits, in subscription order. */
  private def collectExecutionEvents(
      execution: WorkflowExecutionService
  ): ArrayBuffer[TexeraWebSocketEvent] = {
    val events = ArrayBuffer.empty[TexeraWebSocketEvent]
    execution.executionStateStore.metadataStore.getWebsocketEventObservable.subscribe {
      (evts: Iterable[TexeraWebSocketEvent]) => events ++= evts
    }
    events
  }

  "WorkflowService" should "forward workflow-level events to a connected client until the subscription is disposed" in {
    val service = new TestWorkflowService(2L)
    val events = ArrayBuffer.empty[TexeraWebSocketEvent]
    // The production diff handler on the result store is registered by
    // ExecutionResultService.attachToExecution, which needs a live client; register one here so the
    // store has something to emit and the wiring `connect` sets up is observable.
    // TWO events from a single diff on purpose: the forwarding closure does `evts.foreach(onNext)`,
    // and a one-event diff cannot tell that from `evts.headOption.foreach(onNext)`. Multi-event
    // diffs are real -- WorkflowExecutionService's own handler appends a state event and an error
    // event together.
    service.stateStore.resultStore.registerDiffHandler((_, _) =>
      Iterable(
        WebResultUpdateEvent(Map.empty, Map.empty),
        WebResultUpdateEvent(Map.empty, Map.empty)
      )
    )

    val disposable = service.connect(events += _)
    service.lifecycle.increaseCount shouldBe 1

    val update: WorkflowResultStore => WorkflowResultStore =
      _ => WorkflowResultStore(Map(OperatorIdentity("op") -> OperatorResultMetadata(1)))
    service.stateStore.resultStore.updateState(update)
    events should have size 2

    disposable.dispose()
    service.stateStore.resultStore.updateState(_ => WorkflowResultStore(Map.empty))
    events should have size 2
  }

  it should "forward events from the newest execution only, and stop when disposed" in {
    val service = new TestWorkflowService(3L)
    val events = ArrayBuffer.empty[TexeraWebSocketEvent]
    val disposable = service.connectToExecution(events += _)

    val first = newExecution()
    service.executionService.onNext(first)
    first.executionStateStore.metadataStore.updateState(_.withState(RUNNING))
    events.collect { case e: WorkflowStateEvent => e.state } should contain("Running")

    // Publishing a second execution must drop the first execution's subscriptions; otherwise a
    // stale run keeps pushing state events into a client that has moved on.
    val second = newExecution()
    service.executionService.onNext(second)
    events.clear()
    first.executionStateStore.metadataStore.updateState(_.withState(PAUSED))
    events shouldBe empty

    second.executionStateStore.metadataStore.updateState(_.withState(RUNNING))
    events should have size 1

    // A second store, because the subscription is built by mapping over getAllStores: driving only
    // the metadata store would let the fan-out be narrowed to that one store, silently cutting a
    // connected client off from stats, console, breakpoint and reconfiguration events. The stats
    // store's production diff handler is registered by ExecutionStatsService, which is not attached
    // here, so one is registered directly to give the store something to emit.
    events.clear()
    // Two events from the one diff, for the same reason as the workflow-level test above: the
    // execution-side closure also does `events.foreach(onNext)`, and a single-event diff cannot
    // distinguish that from `events.headOption.foreach(onNext)`.
    second.executionStateStore.statsStore.registerDiffHandler((_, _) =>
      Iterable(
        ExecutionDurationUpdateEvent(7L, isRunning = true),
        ExecutionDurationUpdateEvent(8L, isRunning = true)
      )
    )
    second.executionStateStore.statsStore.updateState(_.withStartTimeStamp(1234L))
    events.collect { case e: ExecutionDurationUpdateEvent => e } should have size 2

    // Cleared so the assertion below counts only what arrives AFTER dispose, rather than being
    // sensitive to how many events the checks above happened to accumulate.
    events.clear()
    disposable.dispose()
    second.executionStateStore.metadataStore.updateState(_.withState(PAUSED))
    events shouldBe empty

    // Not asserted here, deliberately: dropping the outer `disposable` (the executionService
    // handle) from the "DO NOT OPTIMIZE" composite leaks that subscription but changes no
    // observable behaviour, because the already-disposed localDisposable immediately disposes
    // anything a later callback adds to it. A test for it would assert on subscriber bookkeeping
    // rather than on behaviour.
  }

  it should "report the current execution's state when a client disconnects" in {
    val service = new TestWorkflowService(4L)

    service.disconnect()
    service.lifecycle.reportedStates shouldBe Seq(None)

    val execution = newExecution()
    service.executionService.onNext(execution)
    execution.executionStateStore.metadataStore.updateState(_.withState(RUNNING))
    service.disconnect()
    // A running execution is what postpones the clean-up deadline, so the state has to travel.
    service.lifecycle.reportedStates.last shouldBe Some(RUNNING)

    // A second, different state. With RUNNING as the only non-empty case, the read could be
    // replaced by the constant RUNNING and this test would still pass -- and that constant would
    // postpone clean-up for every workflow that ever held an execution, since
    // WorkflowLifecycleManager.decreaseUserCount branches on exactly this value.
    execution.executionStateStore.metadataStore.updateState(_.withState(COMPLETED))
    service.disconnect()
    service.lifecycle.reportedStates.last shouldBe Some(COMPLETED)
  }

  it should "dispose its own, the execution's and the result service's subscriptions" in {
    val service = new TestWorkflowService(5L)
    val ownSubscription = new TrackingDisposable
    service.addSubscription(ownSubscription)
    val execution = newExecution()
    service.executionService.onNext(execution)
    val events = collectExecutionEvents(execution)

    service.unsubscribeAll()

    ownSubscription.isDisposed shouldBe true
    service.results.unsubscribeCount shouldBe 1
    execution.executionStateStore.metadataStore.updateState(_.withState(RUNNING))
    events shouldBe empty
  }

  it should "refuse to start an execution with no user id, after detaching the previous one" in {
    val service = new TestWorkflowService(6L)
    val previous = newExecution()
    service.executionService.onNext(previous)
    val events = collectExecutionEvents(previous)
    val request = WorkflowExecuteRequest(
      executionName = "test",
      engineVersion = "test",
      logicalPlan = LogicalPlanPojo(List.empty, List.empty, List.empty, List.empty),
      replayFromExecution = None,
      workflowSettings = WorkflowSettings(),
      emailNotificationEnabled = false,
      computingUnitId = 1,
      warehouseId = None
    )

    // uid is NOT NULL in the DB, so this has to fail before the execution row is inserted.
    val error = intercept[IllegalArgumentException] {
      service.initExecutionService(request, None, new URI("vfs:///session"))
    }
    error.getMessage should include("user id")

    // The previous execution is detached first, whatever happens next.
    previous.executionStateStore.metadataStore.updateState(_.withState(RUNNING))
    events shouldBe empty
  }

  it should "clear the previous run's storage registry, on its computing unit only, before starting a new execution" in {
    // Registered result and console documents outlive the run that produced them; starting a new
    // execution is what drops them (there is no cache yet), so if the registry rows survive here
    // every re-run leaks a result table and a console-message document with no owner left to
    // delete them.
    //
    // The service is built on `otherCuid` while the request names `testCuid`. Production reads
    // the REQUEST's unit here, and the two really do diverge in the field: `getOrCreate` keys its
    // registry on the workflow id alone, so a second opener's computing unit is dropped and the
    // cached service's field goes stale while the request stays right (#7676). Collapsing them
    // into one value would make the scoping this test is named for unobservable.
    val service = new TestWorkflowService(testWid.toLong, otherCuid)
    val scan = new CSVScanSourceOpDesc()
    scan.setOperatorId("scan-op")
    val request = WorkflowExecuteRequest(
      executionName = "cleanup-spec-run",
      engineVersion = "test",
      // A CSV scan with no file selected: compilation rejects it, so this reaches the clean-up
      // and the execution insert for real, then stops inside executeWorkflow without needing a
      // coordinator to come up.
      logicalPlan = LogicalPlanPojo(List(scan), List.empty, List.empty, List.empty),
      replayFromExecution = None,
      workflowSettings = WorkflowSettings(),
      emailNotificationEnabled = false,
      computingUnitId = testCuid,
      warehouseId = None
    )

    operatorRowCount(previousEid) shouldBe 2
    portRowCount(previousEid) shouldBe 2
    executionsOutsideFixture shouldBe 0

    service.initExecutionService(request, Some(executingUser), new URI("vfs:///session"))

    val execution = service.executionService.getValue
    // Stopped exactly where the header says it stops: one fatal error, the compiler's. A second
    // one would mean the run fell through into `createAmberRuntime` on a null workflow.
    val fatalErrors = execution.executionStateStore.metadataStore.getState.fatalErrors
    fatalErrors should have size 1
    fatalErrors.head.message should include("No file selected")

    // The clean-up deadline is armed, with this execution's own store. Nothing else in the method
    // touches `lifeCycleManager`, so without this the one statement that wires deferred clean-up
    // could be deleted from production unnoticed.
    service.lifecycle.registeredStores should have size 1
    service.lifecycle.registeredStores.head should be theSameInstanceAs
      execution.executionStateStore

    operatorRowCount(previousEid) shouldBe 0
    portRowCount(previousEid) shouldBe 0
    // The execution record itself stays. Both registry tables cascade on workflow_executions, so
    // deleting the row would empty them too -- while also erasing the run from the user's history.
    executionRowCount(previousEid) shouldBe 1

    // Scoped to the computing unit this REQUEST runs on. `otherUnitEid` is a newer execution of
    // the same workflow on another unit, so a clean-up that ignored the unit -- or that followed
    // the service's stale field instead -- would take these rows and leave the previous run's.
    operatorRowCount(otherUnitEid) shouldBe 1
    portRowCount(otherUnitEid) shouldBe 1

    // Scoped to this workflow too. `otherWorkflowEid` is the newest execution on `testCuid`, so
    // dropping the workflow leg of the predicate destroys an unrelated workflow's registry
    // instead -- a unit hosts many workflows, it is keyed by user, not by workflow.
    operatorRowCount(otherWorkflowEid) shouldBe 4
    portRowCount(otherWorkflowEid) shouldBe 4

    // Only the newest run of this (workflow, unit) is cleared, and only that one: `olderEid` is
    // superseded, and its rows witness both halves -- selecting the oldest instead of the newest
    // would strand `previousEid`'s documents, and widening the delete from "this execution" to
    // "up to this execution" would take every earlier run's registry with it.
    operatorRowCount(olderEid) shouldBe 3
    portRowCount(olderEid) shouldBe 3

    // The new execution row, recorded after the clean-up rather than deleted by it, and derived
    // from the request throughout.
    executionsOutsideFixture shouldBe 1
    val newRow = insertedExecution()
    newRow.getName shouldBe "cleanup-spec-run"
    newRow.getCuid.intValue() shouldBe testCuid
    newRow.getUid.intValue() shouldBe testUid
    newRow.getVid shouldBe versionIdOf(testWid)
    newRow.getEnvironmentVersion shouldBe """{"engine_version":"test"}"""
    // Past every fixture id, i.e. the sequence bump in `beforeAll` reached the sequence. A
    // `setval` that quietly resolved to NULL would leave the SERIAL at 1 -- the id whose S3
    // prefix `LargeBinaryManager` treats as a shared sentinel (see the header).
    newRow.getEid.intValue() should be > fixtureEids.map(_.intValue()).max
  }

  it should "read the previous run's registered URIs before it deletes the rows that hold them" in {
    // `clearExecutionResources` collects the result and console URIs and only then drops the
    // registry rows. Reversed, the collection comes back empty and every document the previous
    // run wrote is stranded with nothing left pointing at it -- and no assertion on row counts
    // can tell the two orders apart, because the rows are gone either way.
    //
    // The witness is `probeEid`'s registry row, whose URI the decoder rejects: reaching it at all
    // proves the read happened while the row still existed. That the rejection escapes the whole
    // method is a robustness gap in production, not a contract -- see the header.
    val service = new TestWorkflowService(testWid.toLong, probeCuid)
    val scan = new CSVScanSourceOpDesc()
    scan.setOperatorId("scan-op")
    val request = WorkflowExecuteRequest(
      executionName = "probe-spec-run",
      engineVersion = "test",
      logicalPlan = LogicalPlanPojo(List(scan), List.empty, List.empty, List.empty),
      replayFromExecution = None,
      workflowSettings = WorkflowSettings(),
      emailNotificationEnabled = false,
      computingUnitId = probeCuid,
      warehouseId = None
    )

    portRowCount(probeEid) shouldBe 1
    operatorRowCount(probeEid) shouldBe 1

    val error = intercept[IllegalArgumentException] {
      service.initExecutionService(request, Some(executingUser), new URI("vfs:///session"))
    }
    // From the decoder, not from the uid check the sibling case above exercises.
    error.getMessage should include("Invalid URI scheme")

    // ...and the registry rows are already gone by then, which is what places the read before the
    // delete rather than merely somewhere in the same method.
    portRowCount(probeEid) shouldBe 0
    operatorRowCount(probeEid) shouldBe 0
  }

  it should "record the engine version as a JSON object" in {
    // Persisted as the execution's environment version, so the key is part of the stored format.
    new TestWorkflowService(7L).convertToJson("1.2.3") shouldBe """{"engine_version":"1.2.3"}"""
  }

  "WorkflowService.getOrCreate" should "reuse one service per workflow and publish it in the registry" in {
    // The services created here stay in the JVM-wide registry (nothing but the DB-backed clean-up
    // callback removes them). That is safe as long as no execution is published into them:
    // ClusterListener's node-failure recovery skips services whose executionService has no value.
    val workflowId = WorkflowIdentity(8L)
    val service = WorkflowService.getOrCreate(workflowId, computingUnitId = 2, cleanUpTimeoutSecs)

    service.workflowId shouldBe workflowId
    service.computingUnitId shouldBe 2
    WorkflowService.getOrCreate(workflowId, computingUnitId = 3, cleanUpTimeoutSecs) should
      be theSameInstanceAs service
    WorkflowService.getAllWorkflowServices should contain(service)

    val other =
      WorkflowService.getOrCreate(WorkflowIdentity(9L), computingUnitId = 2, cleanUpTimeoutSecs)
    other should not be theSameInstanceAs(service)
  }

  "WorkflowService.mkWorkflowStateId" should "key the registry by workflow id alone" in {
    val stateId = WorkflowService.mkWorkflowStateId(WorkflowIdentity(42L))
    stateId should include("42")
    stateId shouldBe WorkflowService.mkWorkflowStateId(WorkflowIdentity(42L))
    stateId should not be WorkflowService.mkWorkflowStateId(WorkflowIdentity(43L))
  }
}
