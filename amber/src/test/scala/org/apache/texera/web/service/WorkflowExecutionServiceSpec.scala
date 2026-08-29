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

import com.google.protobuf.timestamp.Timestamp
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext, WorkflowSettings}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.EXECUTION_FAILURE
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorConfig
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  RUNNING
}
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.operator.source.scan.csv.CSVScanSourceOpDesc
import org.apache.texera.dao.MockTexeraDB
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
import org.apache.texera.web.WebsocketInput
import org.apache.texera.web.model.websocket.event.{
  TexeraWebSocketEvent,
  WorkflowErrorEvent,
  WorkflowStateEvent
}
import org.apache.texera.common.compiler.model.LogicalPlanPojo
import org.apache.texera.web.model.websocket.request.WorkflowExecuteRequest
import org.apache.texera.web.storage.ExecutionStateStore
import org.apache.texera.web.storage.ExecutionStateStore.updateWorkflowState
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.URI
import java.sql.{Timestamp => SqlTimestamp}
import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * Regression guard for the consolidated init-error reporting path (#5921):
  * `WorkflowExecutionService` registers its metadata-store diff handler at
  * construction, so a fatalErrors update -- e.g. the one `errorHandler` records
  * when `executeWorkflow` fails -- surfaces as a `WorkflowErrorEvent` through the
  * normal websocket-event observable.
  *
  * The unused `coordinatorConfig` / `resultService` are passed as `null` on
  * purpose: construction must stay side-effect-free (all throwing work is in
  * `executeWorkflow`), so a future change that dereferences them during
  * construction would fail here.
  *
  * The suite also owns two paths that need more scaffolding than that:
  *
  *   - `executeWorkflow`'s compilation-failure arm, driven for real (not simulated
  *     through `errorHandler`) with a CSV scan that has no file selected -- the
  *     recipe `SyncExecutionResourceSpec` establishes and `WorkflowServiceSpec`
  *     reuses. Compilation rejects it inside `LogicalPlan.resolveScanSourceOpFileName`
  *     before `FileResolver` is reached, so no storage is involved, and the early
  *     `return` stops the method before `ComputingUnitMaster.createAmberRuntime`,
  *     which would build an `AmberClient` on `AmberRuntime.actorSystem` -- null
  *     outside a started coordinator, and whatever another suite left behind inside
  *     the shared serial JVM.
  *
  *   - `unsubscribeAll`, the session-teardown contract. This is what makes the
  *     suite carry a TestKit `ActorSystem` and `MockTexeraDB`: the method walks the
  *     four runtime services, and `ExecutionStatsService`'s constructor creates its
  *     iceberg runtime-statistics table and stamps the URI onto a `workflow_executions`
  *     row, so a non-null value for that field cannot be had more cheaply. There is
  *     no second home for the test -- one spec file per source class -- and stubbing
  *     the services out would leave the four teardown calls unobserved, which is the
  *     entire content of the method.
  *
  * The runtime half of `executeWorkflow` (everything from `createAmberRuntime` on) is
  * out of reach here for the reason above and is exercised by the integration suites.
  *
  * Known, pre-existing: `ExecutionStatsService` never shuts down its private
  * `metricsPersistThread` (it does not override `unsubscribeAll`), so the teardown
  * test below leaks one single-thread executor into the shared JVM, as every
  * construction of that service already does.
  *
  * Three further gaps are deliberately described and NOT pinned, because a test that
  * froze the current behaviour would cement it:
  *
  *   - Partial construction. `executeWorkflow` assigns `client` first and the four
  *     services after it, and `WorkflowService` catches whatever they throw while
  *     leaving the half-built service published. `unsubscribeAll` then passes its
  *     `client != null` guard and NPEs on the first null service, aborting teardown.
  *
  *   - A recovery that ends in failure. `createStateEvent` carves out COMPLETED only,
  *     while the flag itself is lowered only by a `WorkflowRecoveryStatus(false)` from
  *     the engine, which an execution that dies mid-recovery need not ever send. Such an
  *     execution reaches FAILED with the flag still raised and is announced as
  *     "Recovering" from then on.
  *
  *   - `ExecutionReconfigurationService.registerWorkerCompletionCallback` discards the
  *     `Disposable` its `registerCallback` returns instead of handing it to
  *     `addSubscription`, so that one callback outlives the teardown below. Not this
  *     class's defect, and not this suite's to pin.
  */
class WorkflowExecutionServiceSpec
    extends TestKit(ActorSystem("WorkflowExecutionServiceSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll
    with MockTexeraDB {

  // Distinct from every other suite's ids: the statistics URI is derived from wid/eid and
  // `createDocument` truncates whatever table already sits at it (ExecutionStatsServiceSpec
  // owns 9107/9108).
  private val testUid: Integer = 9207
  private val testWid: Integer = 9207
  private val testEid: Integer = 9208

  /**
    * The computing unit the fixture execution ran on. A different literal from `testWid` on
    * purpose: `getLatestExecutionID(wid, cuid)` binds two bare `Integer`s into one predicate, so a
    * fixture that reused one number for both would make a transposition of the two arguments
    * produce byte-identical SQL.
    */
  private val testCuid: Integer = 9209

  /** A computing unit with no executions, so the cuid leg of the predicate is not vacuous. */
  private val otherCuid: Integer = 9210

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    initializeDBAndReplaceDSLContext()

    val user = new User
    user.setName("workflow-execution-test-user")
    user.setEmail(s"u$testUid@example.com")
    new UserDao(getDSLContext.configuration()).insert(user)

    val workflow = new Workflow
    workflow.setWid(testWid)
    workflow.setName(s"workflow-execution-test-$testWid")
    workflow.setContent("{}")
    workflow.setDescription("")
    workflow.setCreationTime(new SqlTimestamp(System.currentTimeMillis()))
    workflow.setLastModifiedTime(new SqlTimestamp(System.currentTimeMillis()))
    new WorkflowDao(getDSLContext.configuration()).insert(workflow)

    val version = new WorkflowVersion
    version.setWid(testWid)
    version.setContent("{}")
    version.setCreationTime(new SqlTimestamp(System.currentTimeMillis()))
    new WorkflowVersionDao(getDSLContext.configuration()).insert(version)

    val computingUnitDao = new WorkflowComputingUnitDao(getDSLContext.configuration())
    List(testCuid -> "workflow-execution-test-unit", otherCuid -> "workflow-execution-idle-unit")
      .foreach {
        case (cuid, name) =>
          val unit = new WorkflowComputingUnit
          unit.setCuid(cuid)
          unit.setUid(testUid)
          unit.setName(name)
          unit.setCreationTime(new SqlTimestamp(System.currentTimeMillis()))
          unit.setType(WorkflowComputingUnitTypeEnum.local)
          unit.setUri("local://test")
          unit.setResource("{}")
          computingUnitDao.insert(unit)
      }

    // `ExecutionStatsService`'s constructor stamps the runtime-statistics URI onto this row,
    // reaching it from the workflow through workflow_version. `cuid` is set because
    // `getLatestExecutionID` matches on it and a NULL cuid matches no value at all.
    val execution = new WorkflowExecutions
    execution.setEid(testEid)
    execution.setVid(version.getVid)
    execution.setUid(testUid)
    execution.setCuid(testCuid)
    execution.setStatus(0.toByte)
    execution.setStartingTime(new SqlTimestamp(System.currentTimeMillis()))
    execution.setBookmarked(false)
    execution.setName("workflow-execution-test-execution")
    execution.setEnvironmentVersion("test-env")
    new WorkflowExecutionsDao(getDSLContext.configuration()).insert(execution)
  }

  override protected def afterAll(): Unit = {
    try {
      TestKit.shutdownActorSystem(system)
    } finally {
      closeConnectionPool()
      super.afterAll()
    }
  }

  private def buildService(
      store: ExecutionStateStore,
      errorHandler: Throwable => Unit = (_: Throwable) => (),
      logicalPlan: LogicalPlanPojo =
        LogicalPlanPojo(List.empty, List.empty, List.empty, List.empty),
      settings: WorkflowSettings = WorkflowSettings()
  ): WorkflowExecutionService = {
    val request = WorkflowExecuteRequest(
      executionName = "test",
      engineVersion = "test",
      logicalPlan = logicalPlan,
      replayFromExecution = None,
      workflowSettings = settings,
      emailNotificationEnabled = false,
      computingUnitId = 0,
      warehouseId = None
    )
    new WorkflowExecutionService(
      null,
      new WorkflowContext(),
      null,
      request,
      store,
      errorHandler,
      None,
      new URI("vfs:///test")
    )
  }

  /** Subscribe to the metadata store's websocket-event stream and collect events. */
  private def collectEvents(
      store: ExecutionStateStore
  ): mutable.ArrayBuffer[TexeraWebSocketEvent] = {
    val events = mutable.ArrayBuffer.empty[TexeraWebSocketEvent]
    store.metadataStore.getWebsocketEventObservable.subscribe {
      (evts: Iterable[TexeraWebSocketEvent]) => events ++= evts
    }
    events
  }

  /** A subscription whose only job is to report whether the manager holding it let it go. */
  private final class Tracker {
    var disposed = false
    val disposable: Disposable = Disposable.fromAction(() => disposed = true)
  }

  /**
    * Empty-plan client, the shape both `ExecutionRuntimeServiceSpec` and
    * `ExecutionStatsServiceSpec` use: real enough for the services' constructors, with the
    * engine-facing callbacks stubbed so nothing is registered against a live coordinator.
    */
  private final class TestAmberClient
      extends AmberClient(
        system,
        new WorkflowContext(),
        PhysicalPlan(Set.empty, Set.empty),
        CoordinatorConfig(None, None, None, None),
        _ => ()
      ) {
    var shutdownCount = 0

    override def shutdown(): Unit = {
      shutdownCount += 1
      super.shutdown()
    }

    override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable =
      Disposable.empty()
  }

  "WorkflowExecutionService" should
    "surface a recorded fatal error as a WorkflowErrorEvent via the metadata-store handler" in {
    val store = new ExecutionStateStore()
    buildService(store) // registers the diff handler at construction
    val events = collectEvents(store)

    val err =
      WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), "boom during init", "", "", "")
    store.metadataStore.updateState(_.addFatalErrors(err))

    val errorEvents = events.collect { case e: WorkflowErrorEvent => e }
    errorEvents should have size 1
    errorEvents.head.fatalErrors should contain(err)
    // The other half of the handler's contract: an update that moves only `fatalErrors` must not
    // also announce a state. `StateStore` filters out updates that change nothing at all, so the
    // guard's whole job is this case -- a write that moves some other field while `state` and
    // `isRecovering` stand still. Without it, recording an error also republishes a redundant
    // WorkflowStateEvent to the session.
    events.collect { case e: WorkflowStateEvent => e } shouldBe empty
  }

  it should "apply the request's workflow settings to the shared workflow context" in {
    // The one thing construction does besides wiring the handler, and it is not inert: the
    // context is what the compiler and the schedule generator read later, so a dropped assignment
    // silently runs every execution on WorkflowContext's defaults while the request's settings --
    // batch size, execution mode, and the output ports that need materialized storage -- are
    // ignored. The value has to be a non-default one, or the context's own default would answer.
    val settings = WorkflowSettings(dataTransferBatchSize = 137)
    val service = buildService(new ExecutionStateStore(), settings = settings)

    service.workflowContext.workflowSettings shouldBe settings
  }

  it should "report fatal errors recorded at successive phases through the same handler" in {
    val store = new ExecutionStateStore()
    // Mirror WorkflowService's real errorHandler, which records into the
    // metadata store. The service invokes this same handler at every phase
    // (compile, runtime creation, startWorkflow failure), so invoking it
    // repeatedly here stands in for failures arising at different phases.
    val recordError: Throwable => Unit = t =>
      store.metadataStore.updateState(metadataStore =>
        updateWorkflowState(FAILED, metadataStore).addFatalErrors(
          WorkflowFatalError(EXECUTION_FAILURE, Timestamp(Instant.now), t.toString, "", "", "")
        )
      )
    buildService(store, recordError)
    val events = collectEvents(store)

    recordError(new RuntimeException("init phase"))
    recordError(new RuntimeException("runtime phase"))

    val errorEvents = events.collect { case e: WorkflowErrorEvent => e }
    errorEvents should have size 2
    errorEvents.last.fatalErrors.map(_.message) should contain allOf (
      "java.lang.RuntimeException: init phase",
      "java.lang.RuntimeException: runtime phase"
    )
  }

  it should "emit a WorkflowStateEvent when the execution state changes" in {
    val store = new ExecutionStateStore()
    buildService(store)
    val events = collectEvents(store)

    store.metadataStore.updateState(_.withState(RUNNING))

    events.collect { case e: WorkflowStateEvent => e } should not be empty
    // The mirror of the assertion in the fatal-error test: a state-only update must not push an
    // empty WorkflowErrorEvent at the frontend. This is the suite's only update that leaves
    // `fatalErrors` alone, so it is the only place the errors guard can be observed suppressing.
    events.collect { case e: WorkflowErrorEvent => e } shouldBe empty
  }

  it should "report a recovering execution as Recovering instead of as its aggregated state" in {
    // Recovery is a banner the frontend keeps up over whatever the engine is otherwise doing:
    // the execution really is RUNNING throughout, and the three updates are kept apart so that the
    // recovery flag has to carry the second and third events on its own -- the aggregated state
    // does not move with it, and a handler that only watched `state` would go quiet exactly when
    // the banner needs to go up.
    //
    // The flag is driven in BOTH directions on purpose. Raising it is what a guard that merely
    // read `newState.isRecovering` would also do; only clearing it separates that from the
    // `!=` the guard actually needs, and a missing third event is the banner never coming down.
    val store = new ExecutionStateStore()
    buildService(store)
    val events = collectEvents(store)

    store.metadataStore.updateState(_.withState(RUNNING))
    store.metadataStore.updateState(_.withIsRecovering(true))
    store.metadataStore.updateState(_.withIsRecovering(false))

    events.collect { case e: WorkflowStateEvent => e } shouldBe
      Seq(
        WorkflowStateEvent("Running"),
        WorkflowStateEvent("Recovering"),
        WorkflowStateEvent("Running")
      )
  }

  it should "report the aggregated state of a completed execution still marked as recovering" in {
    // The recovery flag is not cleared on the way to COMPLETED, so without the COMPLETED
    // carve-out a finished execution would be announced as still recovering and the frontend
    // would never take the banner down.
    //
    // COMPLETED is the only sentinel this pins, and deliberately so. `createStateEvent`'s
    // carve-out lists exactly that one terminal state, so an execution that dies mid-recovery
    // (FAILED / KILLED with the flag still raised) is still announced as "Recovering" -- see the
    // note in the class header. Adding FAILED to the expectation here would cement that; adding
    // it to the guard is a production question, not this suite's to answer.
    val store = new ExecutionStateStore()
    buildService(store)
    val events = collectEvents(store)

    store.metadataStore.updateState(_.withState(COMPLETED).withIsRecovering(true))

    events.collect { case e: WorkflowStateEvent => e } shouldBe
      Seq(WorkflowStateEvent("Completed"))
  }

  it should "report a compilation failure and leave the workflow and the client unset" in {
    val store = new ExecutionStateStore()
    val errors = ListBuffer.empty[Throwable]
    val scan = new CSVScanSourceOpDesc()
    scan.setOperatorId("scan-op")
    val service = buildService(
      store,
      errors += _,
      LogicalPlanPojo(List(scan), List.empty, List.empty, List.empty)
    )

    // This is the assertion that pins the early `return`, and it has to wrap the call itself:
    // without the `return` the run falls through to `createAmberRuntime`, dereferences the still
    // null `workflow` on the way in, and throws out of `executeWorkflow` -- which is also what
    // keeps a "unit" test out of the live-runtime half on whatever actor system the shared JVM
    // holds.
    noException should be thrownBy service.executeWorkflow()

    errors should have size 1
    errors.head.getMessage should include("No file selected")
    // Documentation, not a pin: both fields are still at their `_` defaults on this path, because
    // the only assignments to them sit after the throw and after the `return`. Neither assertion
    // can fail while the two above hold -- they record the post-condition the early return leaves
    // behind, and they are the suite's only read of `workflow`.
    service.workflow shouldBe null
    service.client shouldBe null
  }

  it should "shut the client down and unsubscribe every runtime service on teardown" in {
    // The end of a websocket session. Everything the execution owns has to be let go here:
    // the engine client, and the four services that hold callbacks on it and diff handlers on
    // the shared state store. A survivor keeps publishing into a store nobody reads.
    //
    // What is asserted is the SET of teardown effects, not their order. `AmberClient.shutdown`
    // only flips a flag and posts a PoisonPill, so whether it runs before or after the four
    // unsubscribes changes only a race against the client actor's mailbox -- nothing a
    // deterministic test can observe, and not an order any contract here states.
    val store = new ExecutionStateStore()
    val service = buildService(store)
    val events = collectEvents(store)

    val client = new TestAmberClient
    val wsInput = new WebsocketInput(_ => ())
    // `workflow` is only dereferenced by the reconfiguration diff handler when a
    // reconfiguration actually completes, which is not what this test drives.
    val reconfigurationService = new ExecutionReconfigurationService(client, store, workflow = null)
    val statsService = new ExecutionStatsService(
      client,
      store,
      new WorkflowContext(
        workflowId = WorkflowIdentity(testWid.longValue()),
        executionId = ExecutionIdentity(testEid.longValue())
      )
    )
    val runtimeService = new ExecutionRuntimeService(
      client,
      store,
      wsInput,
      reconfigurationService,
      logConf = None,
      workflowId = testWid.longValue(),
      emailNotificationEnabled = false,
      userEmailOpt = None,
      sessionUri = new URI("https://texera.example/session")
    )
    val consoleService = new ExecutionConsoleService(client, store, wsInput, new WorkflowContext())

    service.client = client
    service.executionReconfigurationService = reconfigurationService
    service.executionStatsService = statsService
    service.executionRuntimeService = runtimeService
    service.executionConsoleService = consoleService

    // One tracker per manager, so a teardown call that goes missing names its own service.
    val ownTracker = new Tracker
    val reconfigurationTracker = new Tracker
    val statsTracker = new Tracker
    val runtimeTracker = new Tracker
    val consoleTracker = new Tracker
    service.addSubscription(ownTracker.disposable)
    reconfigurationService.addSubscription(reconfigurationTracker.disposable)
    statsService.addSubscription(statsTracker.disposable)
    runtimeService.addSubscription(runtimeTracker.disposable)
    consoleService.addSubscription(consoleTracker.disposable)

    service.unsubscribeAll()

    withClue("the engine client: ") { client.shutdownCount shouldBe 1 }
    withClue("the execution's own subscriptions: ") { ownTracker.disposed shouldBe true }
    withClue("executionRuntimeService: ") { runtimeTracker.disposed shouldBe true }
    withClue("executionConsoleService: ") { consoleTracker.disposed shouldBe true }
    withClue("executionStatsService: ") { statsTracker.disposed shouldBe true }
    withClue("executionReconfigurationService: ") {
      reconfigurationTracker.disposed shouldBe true
    }

    // The constructor-registered diff handler is part of "its own subscriptions": after teardown
    // the store is inert, which is what stops a closed session from still producing websocket
    // events for its (now disconnected) client.
    events.clear()
    store.metadataStore.updateState(_.withState(RUNNING))
    events shouldBe empty
  }

  it should "leave a never-started execution alone on teardown" in {
    // `unsubscribeAll` also runs for an execution that failed to compile, where the client and all
    // four service fields are still null: teardown of that execution must not throw, and its own
    // subscriptions must still be let go.
    //
    // Which field the guard reads is NOT observed here, and cannot honestly be. The two states
    // that would separate the five candidates are (client set, services null) -- whose current
    // behaviour is an NPE, so pinning it would cement the partial-construction defect noted in the
    // class header -- and (client null, services set), which the assignment order in
    // `executeWorkflow` makes unreachable.
    val store = new ExecutionStateStore()
    val service = buildService(store)
    val events = collectEvents(store)
    val ownTracker = new Tracker
    service.addSubscription(ownTracker.disposable)

    noException should be thrownBy service.unsubscribeAll()

    ownTracker.disposed shouldBe true
    events.clear()
    store.metadataStore.updateState(_.withState(RUNNING))
    events shouldBe empty
  }

  // The companion's one expression, exercised directly rather than only through the two result
  // services that call it. Transposing its two arguments -- both bare `Integer`s bound into one
  // predicate -- does reach those siblings, and hard (30 of their tests fail on it), so this is not
  // a hole in the module's coverage. It is the class that owns the method stating its own contract,
  // and failing first and by name when that contract breaks.
  "WorkflowExecutionService.getLatestExecutionId" should
    "resolve the newest execution of a workflow on a given computing unit" in {
    WorkflowExecutionService.getLatestExecutionId(
      WorkflowIdentity(testWid.longValue()),
      testCuid.intValue()
    ) shouldBe Some(ExecutionIdentity(testEid.longValue()))
  }

  it should "find no execution for a computing unit the workflow has not run on" in {
    // Same workflow row as the case above, so only the cuid leg can produce this None -- which is
    // what keeps that leg from being vacuous. The transposition of the two arguments dies in the
    // case above, where the swapped pair selects no row at all.
    WorkflowExecutionService.getLatestExecutionId(
      WorkflowIdentity(testWid.longValue()),
      otherCuid.intValue()
    ) shouldBe None
  }
}
