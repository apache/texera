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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI
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
  * Deliberately not covered:
  *   - `initExecutionService` past its user-id check, and `createWorkflowContext` with it. The rest
  *     of the method inserts an execution row, then hands a compiled plan to
  *     `ComputingUnitMaster.createAmberRuntime`, which builds an `AmberClient` on
  *     `AmberRuntime.actorSystem` — null outside a started coordinator. Reaching the code past the
  *     insert would mean asserting on that NPE, i.e. pinning an accident rather than a contract.
  *   - `clearExecutionResources` and the clean-up callback that calls it: they resolve result and
  *     console URIs out of the database and open Iceberg documents.
  *   - `lastCompletedLogicalPlan` and the constructor's `executionService.subscribe` block that
  *     maintains it. Nothing in the repository ever reads that field, so a test could only assert
  *     which plan a write-only var holds — cementing code that should be deleted instead. Worth
  *     knowing if it ever acquires a reader: the completion diff handler runs only while something
  *     consumes the metadata store's websocket-event stream, so the snapshot silently does not
  *     happen when no client is connected.
  *   - `resolveWarehouseName`, owned by `WorkflowServiceWarehouseSpec`.
  */
class WorkflowServiceSpec extends AnyFlatSpec with Matchers {

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

  private final class TestWorkflowService(id: Long)
      extends WorkflowService(WorkflowIdentity(id), computingUnitId = 1, cleanUpTimeoutSecs) {
    val lifecycle = new RecordingLifecycleManager
    override val lifeCycleManager: WorkflowLifecycleManager = lifecycle
    val results = new RecordingResultService(workflowId, computingUnitId, stateStore)
    override val resultService: ExecutionResultService = results
  }

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
