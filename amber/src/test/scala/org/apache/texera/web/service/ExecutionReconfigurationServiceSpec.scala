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

import com.twitter.util.Future
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  PhysicalPlan,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  UpdateExecutorCompleted,
  Workflow
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.WorkflowReconfigureRequest
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceFs2Grpc
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, StateTransferFunc}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.common.compiler.model.LogicalPlan
import org.apache.texera.web.model.websocket.event.TexeraWebSocketEvent
import org.apache.texera.web.model.websocket.request.ModifyLogicRequest
import org.apache.texera.web.model.websocket.response.{
  ModifyLogicCompletedEvent,
  ModifyLogicResponse
}
import org.apache.texera.web.storage.{
  ExecutionReconfigurationStore,
  ExecutionStateStore,
  StateStore
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.{InvocationHandler, Method, Proxy}
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/**
  * Web-service-layer tests for ExecutionReconfigurationService.
  *
  * The end-to-end engine path (reconfigureWorkflow → Fries algorithm →
  * UpdateExecutor on workers) is covered by ReconfigurationSpec. This spec covers the
  * service's own wiring:
  *
  *   - `modifyOperatorLogic`, validating a frontend edit against the operator that is
  *     currently deployed and queueing the resulting physical op,
  *   - `performReconfigurationOnResume`, i.e. empty short-circuit, request construction
  *     and store reset semantics,
  *   - the two constructor-time subscriptions: the engine's UpdateExecutorCompleted
  *     callback, and the reconfiguration-store diff handler that announces newly
  *     reconfigured operators to the frontend.
  *
  * The tests that need a real `AmberClient` share a single one (see `sharedClient`): its
  * constructor spawns a ClientActor, which spawns a real engine Coordinator whose `initState`
  * runs a schedule search, probes `SqlServer` and iterates the JVM-global `SessionState` map.
  * One such engine per suite, torn down and awaited in `afterAll`, keeps that work from leaking
  * into whichever sibling suite this shared JVM runs next.
  */
class ExecutionReconfigurationServiceSpec
    extends TestKit(ActorSystem("ExecutionReconfigurationServiceSpec"))
    with AnyFlatSpecLike
    with Matchers
    with BeforeAndAfterAll {

  private var sharedClient: TestAmberClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sharedClient = new TestAmberClient
  }

  override def afterAll(): Unit = {
    try {
      // Shutting the client down stops the ClientActor and, with it, the Coordinator it spawned;
      // awaiting full system termination is what actually keeps that Coordinator's schedule
      // search / DB probe / SessionState iteration from running as a straggler in a later suite.
      if (sharedClient != null) {
        sharedClient.dispose()
      }
      TestKit.shutdownActorSystem(system, 60.seconds, verifySystemShutdown = true)
    } finally super.afterAll()
  }

  // Deliberately different from WorkflowContext's defaults (1L / 1L) and from each other, so an
  // op built from the wrong identity is a visibly different op.
  private val testWorkflowId: WorkflowIdentity = WorkflowIdentity(9231L)
  private val testExecutionId: ExecutionIdentity = ExecutionIdentity(9232L)

  /** The message the test-only descriptor below rejects a reconfiguration with. */
  private val rejectionMessage = "this operator refuses to be reconfigured at runtime"

  private def mkPhysicalOp(name: String): PhysicalOp =
    PhysicalOp(
      id = PhysicalOpIdentity(OperatorIdentity(name), "main"),
      workflowId = WorkflowIdentity(0L),
      executionId = ExecutionIdentity(0L),
      opExecInitInfo = OpExecWithClassName(s"$name.Class", "")
    )

  /** A worker of `opName`; the name shape is what VirtualIdentityUtils parses back into a
    * PhysicalOpIdentity, which is how the diff handler maps a worker to its logical operator.
    */
  private def mkWorker(opName: String, workerIndex: Int): ActorVirtualIdentity =
    ActorVirtualIdentity(s"Worker:WF1-$opName-main-$workerIndex")

  private def mkLimitOp(opId: String, limit: Int): LimitOpDesc = {
    val op = new LimitOpDesc
    op.setOperatorId(opId)
    op.limit = limit
    op
  }

  /**
    * No production descriptor ever returns `Failure` from `runtimeReconfiguration`: the six
    * that override it all return `Success`, and the `LogicalOp` base throws
    * `UnsupportedOperationException` instead. The failure arm therefore needs a test-only
    * descriptor. It is invisible to `OperatorMetadataGenerator`, which resolves subtypes from
    * the `@JsonSubTypes` annotation rather than by scanning the classpath.
    */
  private class RejectingReconfigurationOpDesc extends LogicalOp {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Rejecting Reconfiguration",
        "test-only operator whose runtime reconfiguration always fails",
        OperatorGroupConstants.CLEANING_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort()),
        supportReconfiguration = true
      )

    override def runtimeReconfiguration(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity,
        oldOpDesc: LogicalOp,
        newOpDesc: LogicalOp
    ): Try[(PhysicalOp, Option[StateTransferFunc])] =
      Failure(new RuntimeException(rejectionMessage))
  }

  /**
    * Records the arguments `modifyOperatorLogic` hands to `runtimeReconfiguration`. Every
    * production override ignores the `oldOpDesc` parameter (FilterOpDesc, MapOpDesc, LimitOpDesc,
    * JavaUDFOpDesc, PythonUDFOpDescV2, RUDFOpDesc), so the service's choice of which descriptor
    * is the deployed one is unobservable without a descriptor that writes it down.
    */
  private class RecordingReconfigurationOpDesc extends LogicalOp {
    var seenWorkflowId: WorkflowIdentity = _
    var seenExecutionId: ExecutionIdentity = _
    var seenOld: LogicalOp = _
    var seenNew: LogicalOp = _

    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        "Recording Reconfiguration",
        "test-only operator that records the reconfiguration arguments it was given",
        OperatorGroupConstants.CLEANING_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort()),
        supportReconfiguration = true
      )

    override def runtimeReconfiguration(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity,
        oldOpDesc: LogicalOp,
        newOpDesc: LogicalOp
    ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
      seenWorkflowId = workflowId
      seenExecutionId = executionId
      seenOld = oldOpDesc
      seenNew = newOpDesc
      Success((mkPhysicalOp(operatorIdentifier.id), None))
    }
  }

  private def mkWorkflow(
      logicalOps: List[LogicalOp] = List(),
      physicalOps: Set[PhysicalOp] = Set()
  ): Workflow =
    Workflow(
      new WorkflowContext(workflowId = testWorkflowId, executionId = testExecutionId),
      LogicalPlan(logicalOps, List()),
      PhysicalPlan(physicalOps, Set.empty)
    )

  /** Service variant that records dispatched requests and skips the AmberClient
    * registration / workflow-dependent diff handler so it can be constructed
    * without a live engine.
    */
  private class RecordingService(stateStore: ExecutionStateStore)
      extends ExecutionReconfigurationService(client = null, stateStore, workflow = null) {
    val captured: mutable.ArrayBuffer[WorkflowReconfigureRequest] = mutable.ArrayBuffer.empty
    override protected def dispatch(request: WorkflowReconfigureRequest): Unit =
      captured += request
    override protected def registerWorkerCompletionCallback(): Unit = ()
    override protected def registerCompletionDiffHandler(): Unit = ()
  }

  /** Service over a real workflow, with only the client-dependent seam disabled: the diff
    * handler stays live because it needs the workflow, not the client.
    */
  private class WorkflowOnlyService(stateStore: ExecutionStateStore, workflow: Workflow)
      extends ExecutionReconfigurationService(client = null, stateStore, workflow) {
    override protected def registerWorkerCompletionCallback(): Unit = ()
  }

  /** Real empty-plan client for constructor compatibility; calls made through its grpc interface
    * are recorded instead of reaching the engine, and the engine callbacks are captured instead
    * of being wired to actors. One instance per suite -- see the class comment.
    */
  private final class TestAmberClient
      extends AmberClient(
        system,
        new WorkflowContext(),
        PhysicalPlan(Set.empty, Set.empty),
        CoordinatorConfig(None, None, None, None),
        _ => ()
      ) {
    private val callbacks = mutable.Map.empty[Class[_], Any => Unit]

    /** (method name, request) of every call made through `coordinatorInterface`. */
    val coordinatorCalls: mutable.ArrayBuffer[(String, Any)] = mutable.ArrayBuffer.empty

    override val coordinatorInterface: CoordinatorServiceFs2Grpc[Future, Unit] =
      Proxy
        .newProxyInstance(
          classOf[CoordinatorServiceFs2Grpc[Future, Unit]].getClassLoader,
          Array(classOf[CoordinatorServiceFs2Grpc[Future, Unit]]),
          new InvocationHandler {
            override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
              if (args == null || args.isEmpty) {
                // Object methods the proxy also routes here; never part of a dispatch.
                if (method.getName == "hashCode") Integer.valueOf(System.identityHashCode(proxy))
                else if (method.getName == "toString") "TestCoordinatorInterface"
                else null
              } else {
                coordinatorCalls += ((method.getName, args(0)))
                Future.value(EmptyReturn())
              }
            }
          }
        )
        .asInstanceOf[CoordinatorServiceFs2Grpc[Future, Unit]]

    /**
      * Honours the Disposable it hands out. Production
      * (`ExecutionReconfigurationService.registerWorkerCompletionCallback`) currently DISCARDS
      * it, so `unsubscribeAll()` does not release the engine callback -- the only
      * `registerCallback` call site in this package that is not wrapped in `addSubscription`
      * (contrast ExecutionStatsService, ExecutionConsoleService, ExecutionResultService and
      * ExecutionRuntimeService). A fake that ignored disposal would silently absolve that line.
      * No test below fires an engine event after `unsubscribeAll`, so the suite neither depends
      * on the leak nor breaks when it is fixed.
      */
    override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
      val clazz = ct.runtimeClass
      callbacks(clazz) = callback.asInstanceOf[Any => Unit]
      Disposable.fromAction(() => callbacks.remove(clazz))
    }

    /** Delivers an engine event the way the client's observable would. */
    def fire[T <: AnyRef](event: T)(implicit ct: ClassTag[T]): Unit =
      callbacks.getOrElse(
        ct.runtimeClass,
        fail(s"no callback is registered for ${ct.runtimeClass.getSimpleName}")
      )(event)

    /** Per-test isolation for the one client the suite shares. */
    def reset(): Unit = {
      callbacks.clear()
      coordinatorCalls.clear()
    }

    def dispose(): Unit = super.shutdown()
  }

  private final class LiveFixture(
      val client: TestAmberClient,
      val stateStore: ExecutionStateStore,
      val service: ExecutionReconfigurationService
  )

  /** Builds a service with every seam left in place, so the constructor really registers the
    * engine callback and the diff handler and `dispatch` really goes through the client.
    */
  private def withLiveService(
      logicalOps: List[LogicalOp] = List(),
      physicalOps: Set[PhysicalOp] = Set()
  )(body: LiveFixture => Unit): Unit = {
    sharedClient.reset()
    val stateStore = new ExecutionStateStore
    val service = new ExecutionReconfigurationService(
      sharedClient,
      stateStore,
      mkWorkflow(logicalOps, physicalOps)
    )
    try body(new LiveFixture(sharedClient, stateStore, service))
    finally service.unsubscribeAll()
  }

  /**
    * The store's diff subject is cold: handlers only run once something subscribes to the
    * websocket observable, and an update whose result equals the previous state is dropped
    * before the handlers see it. So a test that cares what a state change publishes has to
    * subscribe first and count batches, not just look for events.
    *
    * Subscribing is itself observable: StateStore prepends the default state and buffers pairs,
    * so a subscriber that attaches to an already-advanced store is handed a
    * (default, current) catch-up pair before anything else -- exactly what a browser
    * reconnecting to a running execution produces.
    */
  private def recordBatches(
      store: StateStore[ExecutionReconfigurationStore]
  )(body: => Unit): Seq[Iterable[TexeraWebSocketEvent]] = {
    val batches = mutable.ListBuffer.empty[Iterable[TexeraWebSocketEvent]]
    val subscription = store.getWebsocketEventObservable
      .subscribe((batch: Iterable[TexeraWebSocketEvent]) => batches += batch)
    try body
    finally subscription.dispose()
    batches.toSeq
  }

  private def announcedOps(batch: Iterable[TexeraWebSocketEvent]): List[String] =
    batch.collect { case e: ModifyLogicCompletedEvent => e.opIds }.flatten.toList

  private def limitOf(op: PhysicalOp): Int =
    objectMapper
      .readTree(op.opExecInitInfo.asInstanceOf[OpExecWithClassName].descString)
      .get("limit")
      .asInt()

  "modifyOperatorLogic" should
    "queue every edited operator's physical op in request order and report each edit as valid" in {
    // The operators in the plan and the ones in the requests differ only in `limit`, so the
    // queued physical ops show which of the two each reconfiguration was built from.
    val stateStore = new ExecutionStateStore
    val service = new WorkflowOnlyService(
      stateStore,
      mkWorkflow(List(mkLimitOp("limit-a", 1), mkLimitOp("limit-b", 2)))
    )

    service.modifyOperatorLogic(ModifyLogicRequest(mkLimitOp("limit-a", 7))) shouldBe
      ModifyLogicResponse("limit-a", isValid = true, "")
    service.modifyOperatorLogic(ModifyLogicRequest(mkLimitOp("limit-b", 11))) shouldBe
      ModifyLogicResponse("limit-b", isValid = true, "")

    // A user can edit several operators during one pause, so the queue has to accumulate:
    // overwriting would silently drop every edit but the last, and prepending would reverse the
    // order the engine reconfigures them in on resume.
    val queued = stateStore.reconfigurationStore.getState.unscheduledReconfigurations
    queued should have size 2
    queued.map(_._1.id) shouldBe List(
      PhysicalOpIdentity(OperatorIdentity("limit-a"), "main"),
      PhysicalOpIdentity(OperatorIdentity("limit-b"), "main")
    )
    // The engine is asked to install the NEW logic; installing the deployed one would be a
    // silently successful no-op reconfiguration.
    queued.map { case (op, _) => limitOf(op) } shouldBe List(7, 11)
    // The workflow's identities are threaded into the reconfiguration, not left at the
    // WorkflowContext defaults the executor would otherwise be addressed with.
    queued.map(_._1.workflowId).distinct shouldBe List(testWorkflowId)
    queued.map(_._1.executionId).distinct shouldBe List(testExecutionId)
    // The queued tuple keeps the descriptor's state-transfer closure -- for LimitOpDesc the one
    // that carries the old executor's row count into the new one. Storing None instead would
    // restart counting on resume.
    queued.map { case (_, stateTransfer) => stateTransfer.isDefined } shouldBe List(true, true)
  }

  it should "reconfigure the deployed descriptor, giving it itself as old logic and the request as new" in {
    val opId = "recording-op"
    val deployedOp = new RecordingReconfigurationOpDesc
    deployedOp.setOperatorId(opId)
    val requestOp = mkLimitOp(opId, 7)
    val stateStore = new ExecutionStateStore
    val service = new WorkflowOnlyService(stateStore, mkWorkflow(List(deployedOp)))

    service.modifyOperatorLogic(ModifyLogicRequest(requestOp))

    // `theSameInstanceAs` rather than equality: both descriptors carry the same operator id, so
    // only identity distinguishes the deployed one from the one the frontend sent. The service is
    // the only place in the system that makes this choice -- every production override of
    // `runtimeReconfiguration` ignores the old descriptor, so a swap is invisible everywhere else.
    deployedOp.seenOld should be theSameInstanceAs deployedOp
    deployedOp.seenNew should be theSameInstanceAs requestOp
    deployedOp.seenWorkflowId shouldBe testWorkflowId
    deployedOp.seenExecutionId shouldBe testExecutionId
  }

  it should "report the deployed operator's rejection and leave the pending queue untouched" in {
    // The request carries a LimitOpDesc, which would reconfigure happily. Only the operator
    // that is actually deployed refuses, so an invalid response proves the service asked the
    // plan's operator rather than the one the frontend sent.
    val opId = "rejecting-op"
    val deployedOp = new RejectingReconfigurationOpDesc
    deployedOp.setOperatorId(opId)
    val stateStore = new ExecutionStateStore
    val service = new WorkflowOnlyService(stateStore, mkWorkflow(List(deployedOp)))

    // An edit the user already made earlier in the same pause.
    val alreadyQueued = mkPhysicalOp("already-queued")
    stateStore.reconfigurationStore.updateState(_ =>
      ExecutionReconfigurationStore(unscheduledReconfigurations = List((alreadyQueued, None)))
    )

    val response = service.modifyOperatorLogic(ModifyLogicRequest(mkLimitOp(opId, 7)))

    response shouldBe a[ModifyLogicResponse]
    val rejected = response.asInstanceOf[ModifyLogicResponse]
    rejected.opId shouldBe opId
    rejected.isValid shouldBe false
    // The frontend shows this string in the operator's error box, so it has to be the reason the
    // operator gave -- not an empty string, which is also what the valid arm reports.
    rejected.errorMessage shouldBe rejectionMessage

    // A rejected edit neither queues itself nor discards work that was already pending.
    val queued = stateStore.reconfigurationStore.getState.unscheduledReconfigurations
    queued.map(_._1.id) shouldBe List(alreadyQueued.id)
  }

  "performReconfigurationOnResume" should
    "return without dispatching when no reconfigurations are pending" in {
    val stateStore = new ExecutionStateStore()
    val service = new RecordingService(stateStore)

    noException should be thrownBy service.performReconfigurationOnResume()

    service.captured shouldBe empty
    val state = stateStore.reconfigurationStore.getState
    state.unscheduledReconfigurations shouldBe empty
    state.currentReconfigId shouldBe None
    state.completedReconfigurations shouldBe empty
  }

  it should "dispatch one request carrying every pending reconfiguration and reset the store" in {
    val stateStore = new ExecutionStateStore()
    val service = new RecordingService(stateStore)

    val op1 = mkPhysicalOp("op-1")
    val op2 = mkPhysicalOp("op-2")
    // A worker that completed in the PREVIOUS round, i.e. the state a second pause/resume cycle
    // actually starts from.
    val staleWorker = mkWorker("op-1", 0)
    stateStore.reconfigurationStore.updateState(_ =>
      ExecutionReconfigurationStore(
        unscheduledReconfigurations = List((op1, None), (op2, None)),
        completedReconfigurations = Set(staleWorker)
      )
    )

    service.performReconfigurationOnResume()

    service.captured should have size 1
    val request = service.captured.head
    request.reconfigurationId should not be empty
    request.reconfiguration.map(_.targetOpId) should contain theSameElementsInOrderAs Seq(
      op1.id,
      op2.id
    )
    request.reconfiguration.map(_.newExecInitInfo) should contain theSameElementsInOrderAs Seq(
      op1.opExecInitInfo,
      op2.opExecInitInfo
    )

    val state = stateStore.reconfigurationStore.getState
    state.unscheduledReconfigurations shouldBe empty
    state.currentReconfigId shouldBe Some(request.reconfigurationId)
    // The new round starts from an empty completed set. Carrying the previous round's workers
    // over would make their re-completion `old + worker` produce an equal store, which
    // StateStore drops before any diff handler runs -- the frontend would never be told the
    // second round finished.
    state.completedReconfigurations shouldBe empty
  }

  it should "use a fresh reconfigurationId on each dispatch" in {
    val stateStore = new ExecutionStateStore()
    val service = new RecordingService(stateStore)

    def queueAndDispatch(opName: String): String = {
      stateStore.reconfigurationStore.updateState(old =>
        old.copy(unscheduledReconfigurations = List((mkPhysicalOp(opName), None)))
      )
      service.performReconfigurationOnResume()
      service.captured.last.reconfigurationId
    }

    val firstId = queueAndDispatch("op-a")
    val secondId = queueAndDispatch("op-b")

    firstId should not be secondId
    stateStore.reconfigurationStore.getState.currentReconfigId shouldBe Some(secondId)
  }

  it should "send the request to the coordinator over the client's grpc interface" in {
    // The tests above run against an overridden `dispatch`, which leaves the one line that
    // actually reaches the engine untested. This one keeps the real dispatch.
    withLiveService() { f =>
      val op1 = mkPhysicalOp("dispatched-op-1")
      val op2 = mkPhysicalOp("dispatched-op-2")
      f.stateStore.reconfigurationStore.updateState(_ =>
        ExecutionReconfigurationStore(unscheduledReconfigurations = List((op1, None), (op2, None)))
      )

      f.service.performReconfigurationOnResume()

      f.client.coordinatorCalls should have size 1
      val (calledMethod, payload) = f.client.coordinatorCalls.head
      // Named explicitly: the coordinator interface carries every control command, and reaching
      // the engine through any other one would not start a Fries round.
      calledMethod shouldBe "reconfigureWorkflow"
      val captured = payload.asInstanceOf[WorkflowReconfigureRequest]
      captured.reconfiguration.map(_.targetOpId) should contain theSameElementsInOrderAs Seq(
        op1.id,
        op2.id
      )
      captured.reconfiguration.map(_.newExecInitInfo) should contain theSameElementsInOrderAs Seq(
        op1.opExecInitInfo,
        op2.opExecInitInfo
      )
      // The id the engine is told to reconfigure under is the one the store now tracks; a
      // mismatch would leave the completion diff handler waiting on a round that never reports.
      f.stateStore.reconfigurationStore.getState.currentReconfigId shouldBe
        Some(captured.reconfigurationId)
    }
  }

  "onWorkerReconfigured" should
    "add the worker id to completedReconfigurations so the diff handler can fire" in {
    val stateStore = new ExecutionStateStore()
    val service = new RecordingService(stateStore)

    val w1 = ActorVirtualIdentity("Worker:WF1-E1-op-main-0")
    val w2 = ActorVirtualIdentity("Worker:WF1-E1-op-main-1")
    service.onWorkerReconfigured(w1)
    service.onWorkerReconfigured(w2)
    // duplicate completion is idempotent (Set semantics).
    service.onWorkerReconfigured(w1)

    stateStore.reconfigurationStore.getState.completedReconfigurations should contain theSameElementsAs Set(
      w1,
      w2
    )
  }

  "the worker completion callback" should
    "announce the logical operator of every worker that reports in" in {
    // Driven only through the engine event, so it also pins that the constructor registered the
    // callback for UpdateExecutorCompleted and passed the reported worker on.
    withLiveService(physicalOps = Set(mkPhysicalOp("opA"), mkPhysicalOp("opB"))) { f =>
      val batches = recordBatches(f.stateStore.reconfigurationStore) {
        f.client.fire(UpdateExecutorCompleted(mkWorker("opA", 0)))
        f.client.fire(UpdateExecutorCompleted(mkWorker("opB", 0)))
        f.client.fire(UpdateExecutorCompleted(mkWorker("opA", 1)))
      }

      batches should have size 3
      announcedOps(batches.head) shouldBe List("opA")
      // Only the worker that just reported is announced; publishing the whole completed set
      // would re-announce opA here.
      announcedOps(batches(1)) shouldBe List("opB")
      // opA's SECOND worker announces opA a second time. Recorded as OBSERVED behaviour, not as
      // an endorsed contract: the engine emits one UpdateExecutorCompleted per worker
      // (ReconfigurationHandler.notifyOnComplete) and the handler maps each straight to its
      // logical operator, so an N-worker operator produces N ModifyLogicCompletedEvents and the
      // frontend shows N "reconfiguration on operator(s) opA complete" toasts -- the first while
      // opA's remaining workers are still running the old executor. That contradicts the comment
      // above ExecutionReconfigurationService.registerCompletionDiffHandler, which says the
      // frontend is notified "when all workers of an operator complete reconfiguration". One of
      // the two is wrong; whichever is corrected, this assertion has to be revisited.
      announcedOps(batches(2)) shouldBe List("opA")

      f.stateStore.reconfigurationStore.getState.completedReconfigurations shouldBe
        Set(mkWorker("opA", 0), mkWorker("opB", 0), mkWorker("opA", 1))
    }
  }

  it should "announce every operator that completed before the client attached" in {
    // WorkflowService re-subscribes the websocket observable on every browser attach, and
    // StateStore replays a (default, current) pair to each new subscriber, so a client joining a
    // running execution is handed one diff carrying every worker that has finished so far. It is
    // the only path that produces a multi-worker diff, and therefore the only thing that
    // distinguishes announcing the whole diff from announcing one of its members.
    withLiveService(physicalOps = Set(mkPhysicalOp("opA"), mkPhysicalOp("opB"))) { f =>
      f.client.fire(UpdateExecutorCompleted(mkWorker("opA", 0)))
      f.client.fire(UpdateExecutorCompleted(mkWorker("opB", 0)))

      val batches = recordBatches(f.stateStore.reconfigurationStore) {
        // nothing to drive: the catch-up pair is delivered synchronously on subscribe.
      }

      batches should have size 1
      // Order-insensitive: the diff is a Set.
      announcedOps(batches.head) should contain theSameElementsAs List("opA", "opB")
    }
  }

  it should "announce nothing to a client that attaches after a new round has started" in {
    // Same catch-up pair, but this time it straddles a round boundary (reconfigId None -> Some).
    // It is not a completion report, so the joining client must not be told that opA finished.
    withLiveService(physicalOps = Set(mkPhysicalOp("opA"), mkPhysicalOp("opB"))) { f =>
      f.stateStore.reconfigurationStore.updateState(_ =>
        ExecutionReconfigurationStore(unscheduledReconfigurations =
          List((mkPhysicalOp("opA"), None))
        )
      )
      f.service.performReconfigurationOnResume()
      f.client.fire(UpdateExecutorCompleted(mkWorker("opA", 0)))

      val batches = recordBatches(f.stateStore.reconfigurationStore) {
        f.client.fire(UpdateExecutorCompleted(mkWorker("opB", 0)))
      }

      batches should have size 2
      batches.head shouldBe empty
      // ...and the subscription really is live, so the empty batch above is a decision, not a
      // dead handler: the next genuine completion is announced.
      announcedOps(batches.last) shouldBe List("opB")
    }
  }

  it should "publish nothing to announce when the completed set is untouched" in {
    val opId = "queued-limit"
    withLiveService(
      logicalOps = List(mkLimitOp(opId, 1)),
      physicalOps = Set(mkPhysicalOp("opA"))
    ) { f =>
      val batches = recordBatches(f.stateStore.reconfigurationStore) {
        // A real announcement first, so the empty batch below cannot be explained by a dead
        // subscription, a handler that was never registered, or a state that never changed.
        f.client.fire(UpdateExecutorCompleted(mkWorker("opA", 0)))
        // Queueing a reconfiguration changes the store without completing anything.
        f.service.modifyOperatorLogic(ModifyLogicRequest(mkLimitOp(opId, 7)))
      }

      batches should have size 2
      announcedOps(batches.head) shouldBe List("opA")
      batches.last shouldBe empty
    }
  }

  it should "stop announcing completions once the service is unsubscribed" in {
    val workerA = mkWorker("opA", 0)
    val workerB = mkWorker("opB", 0)
    withLiveService(physicalOps = Set(mkPhysicalOp("opA"), mkPhysicalOp("opB"))) { f =>
      val batches = recordBatches(f.stateStore.reconfigurationStore) {
        f.client.fire(UpdateExecutorCompleted(workerA))
        f.service.unsubscribeAll()
        // Driven through the service's own entry point rather than the engine event on purpose.
        // What this test is about is the diff handler's disposable reaching the
        // SubscriptionManager; the engine callback has an independent lifetime (see
        // TestAmberClient.registerCallback), and routing through it would make this test's
        // verdict depend on that unrelated, currently broken, seam.
        f.service.onWorkerReconfigured(workerB)
      }

      batches should have size 2
      announcedOps(batches.head) shouldBe List("opA")
      // The store still publishes -- the second completion did change the state, as the
      // assertion below confirms -- but the handler is gone, which is only true if the
      // constructor handed its disposable to the subscription manager.
      batches.last shouldBe empty
      f.stateStore.reconfigurationStore.getState.completedReconfigurations shouldBe
        Set(workerA, workerB)
    }
  }
}
