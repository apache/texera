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

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.executor.{OpExecInitInfo, OpExecWithClassName}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  ExecutionTimeBinding,
  InputPort,
  OutputPort,
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.coordinator.{UpdateExecutorCompleted, Workflow}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.WorkflowReconfigureRequest
import org.apache.texera.amber.operator.dummy.DummyOpDesc
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.limit.LimitOpDesc
import org.apache.texera.amber.operator.loop.{LoopEndOpDesc, LoopStartOpDesc}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.{LogicalOp, StateTransferFunc}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.common.compiler.model.{LogicalLink, LogicalPlan}
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

import scala.collection.mutable
import scala.concurrent.duration.DurationInt
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

  private var sharedClient: RecordingExecutionClient = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    sharedClient = new RecordingExecutionClient(system)
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

  /** The message the test-only descriptor below throws from a reconfiguration with. */
  private val thrownMessage = "this operator blew up while being reconfigured"

  /**
    * Minimal test-only [[LogicalOp]] supporting runtime reconfiguration, with ports pre-wired so
    * subclasses only define `runtimeReconfiguration`.
    */
  private abstract class StubReconfigurationOpDesc(name: String, desc: String) extends LogicalOp {
    override def operatorInfo: OperatorInfo =
      OperatorInfo(
        name,
        desc,
        OperatorGroupConstants.CLEANING_GROUP,
        inputPorts = List(InputPort()),
        outputPorts = List(OutputPort()),
        supportReconfiguration = true
      )
  }

  /**
    * Test-only reconfiguration descriptor evaluating `outcome` on `runtimeReconfiguration` (to
    * return `Failure` or throw). Invisible to `OperatorMetadataGenerator`, which scans `@JsonSubTypes`.
    */
  private class StaticReconfigurationOpDesc(
      outcome: => Try[(PhysicalOp, Option[StateTransferFunc])]
  ) extends StubReconfigurationOpDesc(
        "Stub Reconfiguration",
        "test-only operator returning a configured reconfiguration outcome"
      ) {
    override def runtimeReconfiguration(
        workflowId: WorkflowIdentity,
        executionId: ExecutionIdentity,
        oldOpDesc: LogicalOp,
        newOpDesc: LogicalOp
    ): Try[(PhysicalOp, Option[StateTransferFunc])] = outcome
  }

  /**
    * Records the arguments `modifyOperatorLogic` hands to `runtimeReconfiguration`. Every
    * production override ignores the `oldOpDesc` parameter (FilterOpDesc, MapOpDesc, LimitOpDesc,
    * JavaUDFOpDesc, PythonUDFOpDescV2, RUDFOpDesc), so the service's choice of which descriptor
    * is the deployed one is unobservable without a descriptor that writes it down.
    */
  private class RecordingReconfigurationOpDesc
      extends StubReconfigurationOpDesc(
        "Recording Reconfiguration",
        "test-only operator that records the reconfiguration arguments it was given"
      ) {
    var seenWorkflowId: WorkflowIdentity = _
    var seenExecutionId: ExecutionIdentity = _
    var seenOld: LogicalOp = _
    var seenNew: LogicalOp = _

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
    val mounted: mutable.ArrayBuffer[Set[String]] = mutable.ArrayBuffer.empty
    override protected def dispatch(request: WorkflowReconfigureRequest): Unit =
      captured += request
    override protected def ensureMounted(locators: Set[String]): Unit = mounted += locators
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

  private final class LiveFixture(
      val client: RecordingExecutionClient,
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

  private def withLiveOpPair(logicalOps: List[LogicalOp] = Nil)(body: LiveFixture => Unit): Unit =
    withLiveService(logicalOps, physicalOps = Set(mkPhysicalOp("opA"), mkPhysicalOp("opB")))(body)

  private def enqueueReconfigs(
      stateStore: ExecutionStateStore,
      ops: (PhysicalOp, Option[StateTransferFunc])*
  ): Unit =
    stateStore.reconfigurationStore.updateState(old =>
      old.copy(unscheduledReconfigurations = ops.toList)
    )

  private def queuedReconfigs(stateStore: ExecutionStateStore) =
    stateStore.reconfigurationStore.getState.unscheduledReconfigurations

  private def completedWorkers(stateStore: ExecutionStateStore): Set[ActorVirtualIdentity] =
    stateStore.reconfigurationStore.getState.completedReconfigurations

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
    val queued = queuedReconfigs(stateStore)
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

  /** Sends an edit to a deployed `deployedOp` with one reconfiguration already pending and
    * returns the response together with the ids left in the pending queue.
    */
  private def modifyWithOnePending(
      deployedOp: LogicalOp,
      request: LogicalOp
  ): (TexeraWebSocketEvent, List[PhysicalOpIdentity]) = {
    val stateStore = new ExecutionStateStore
    val service = new WorkflowOnlyService(stateStore, mkWorkflow(List(deployedOp)))
    // An edit the user already made earlier in the same pause.
    enqueueReconfigs(stateStore, (mkPhysicalOp("already-queued"), None))
    val response = service.modifyOperatorLogic(ModifyLogicRequest(request))
    (response, queuedReconfigs(stateStore).map(_._1.id))
  }

  private def assertRejectedEdit(
      opId: String,
      deployedOp: LogicalOp
  )(checkMessage: String => Unit): Unit = {
    deployedOp.setOperatorId(opId)
    val (response, queued) = modifyWithOnePending(deployedOp, mkLimitOp(opId, 7))
    response shouldBe a[ModifyLogicResponse]
    val rejected = response.asInstanceOf[ModifyLogicResponse]
    rejected.opId shouldBe opId
    rejected.isValid shouldBe false
    checkMessage(rejected.errorMessage)
    // A rejected edit neither queues itself nor discards work that was already pending.
    queued shouldBe List(mkPhysicalOp("already-queued").id)
  }

  it should "report the deployed operator's rejection and leave the pending queue untouched" in {
    // The request carries a LimitOpDesc, which would reconfigure happily. Only the operator
    // that is actually deployed refuses, so an invalid response proves the service asked the
    // plan's operator rather than the one the frontend sent.
    // The frontend shows this string in an error notification, so it has to be the reason the
    // operator gave -- not an empty string, which is also what the valid arm reports.
    assertRejectedEdit(
      "rejecting-op",
      new StaticReconfigurationOpDesc(Failure(new RuntimeException(rejectionMessage)))
    )(_ shouldBe rejectionMessage)
  }

  it should "report an operator without a reconfiguration override as a rejected edit" in {
    // DummyOpDesc advertises supportReconfiguration but inherits the LogicalOp base, which
    // throws UnsupportedOperationException instead of returning Failure.
    assertRejectedEdit("dummy-op", new DummyOpDesc)(
      _ should include("DummyOpDesc does not support reconfiguration")
    )
  }

  it should "report an operator whose reconfiguration override throws as a rejected edit" in {
    assertRejectedEdit(
      "throwing-op",
      new StaticReconfigurationOpDesc(throw new IllegalStateException(thrownMessage))
    )(_ shouldBe thrownMessage)
  }

  it should "describe a rejection by its exception type when the exception has no message" in {
    assertRejectedEdit(
      "throwing-op",
      new StaticReconfigurationOpDesc(throw new IllegalStateException())
    )(_ shouldBe "java.lang.IllegalStateException")
  }

  it should "let a fatal reconfiguration exception escape instead of reporting a rejection" in {
    val opId = "throwing-op"
    val deployedOp = new StaticReconfigurationOpDesc(throw new InterruptedException("stop"))
    deployedOp.setOperatorId(opId)

    an[InterruptedException] should be thrownBy modifyWithOnePending(deployedOp, mkLimitOp(opId, 7))
  }

  // ---------------------------------------------------------------------------
  // An operator that refers to loop variables
  // ---------------------------------------------------------------------------

  private def loopStartOp(): LoopStartOpDesc = {
    val op = new LoopStartOpDesc
    op.initialization = "i = 0"
    op.output = "table.iloc[i]"
    op
  }

  private def loopEndOp(): LoopEndOpDesc = {
    val op = new LoopEndOpDesc
    op.update = "i += 1"
    op.condition = "i < len(table)"
    op
  }

  /** A service over a workflow whose plan is LoopStart -> `deployedOp` -> LoopEnd. */
  private def insideLoopBlock(deployedOp: LogicalOp): (ExecutionStateStore, WorkflowOnlyService) = {
    val start = loopStartOp()
    val end = loopEndOp()
    def link(from: LogicalOp, to: LogicalOp): LogicalLink =
      LogicalLink(from.operatorIdentifier, PortIdentity(), to.operatorIdentifier, PortIdentity())
    val workflow = Workflow(
      new WorkflowContext(workflowId = testWorkflowId, executionId = testExecutionId),
      LogicalPlan(
        List(start, deployedOp, end),
        List(link(start, deployedOp), link(deployedOp, end))
      ),
      PhysicalPlan(Set.empty, Set.empty)
    )
    val stateStore = new ExecutionStateStore
    (stateStore, new WorkflowOnlyService(stateStore, workflow))
  }

  private def filterOp(opId: String, value: String): SpecializedFilterOpDesc = {
    val op = new SpecializedFilterOpDesc
    op.setOperatorId(opId)
    op.predicates = List(new FilterPredicate("line", ComparisonType.EQUAL_TO, value))
    op
  }

  private def refused(name: String): String =
    s"$name refers to loop variables, so it can't be changed while the workflow runs."

  private def assertLoopBlocked(
      deployedOp: LogicalOp,
      edit: LogicalOp,
      opId: String,
      opLabel: String
  ): Unit = {
    val (store, service) = insideLoopBlock(deployedOp)
    service.modifyOperatorLogic(ModifyLogicRequest(edit)) shouldBe
      ModifyLogicResponse(opId, isValid = false, refused(opLabel))
    queuedReconfigs(store) shouldBe empty
  }

  "modifyOperatorLogic" should
    "refuse to modify an operator whose deployed setting refers to a loop variable" in {
    // The references the compiler left on the deployed operator, as it does inside a loop block.
    val deployedOp = mkLimitOp("limit-a", 0)
    deployedOp.stateReferences = Map("/limit" -> "n")
    assertLoopBlocked(deployedOp, mkLimitOp("limit-a", 7), "limit-a", "Limit")
  }

  it should "refuse an edit that makes an operator inside a loop block refer to a loop variable" in {
    // As the frontend sends it: "$n" in the Int property.
    val typedEdit = objectMapper.readValue(
      """{"operatorID":"limit-a","limit":"$n","operatorType":"Limit"}""",
      classOf[LogicalOp]
    )
    assertLoopBlocked(mkLimitOp("limit-a", 1), typedEdit, "limit-a", "Limit")

    // A whole-string "$x" refers to a loop variable inside a loop block; the compiler never saw it.
    assertLoopBlocked(
      filterOp("filter-a", "Asia"),
      filterOp("filter-a", "$x"),
      "filter-a",
      "Filter"
    )
  }

  it should "still modify an operator inside a loop block that refers to no loop variable, and a '$x' literal outside every block" in {
    val (insideStore, inside) = insideLoopBlock(mkLimitOp("limit-a", 1))
    inside.modifyOperatorLogic(ModifyLogicRequest(mkLimitOp("limit-a", 7))) shouldBe
      ModifyLogicResponse("limit-a", isValid = true, "")
    queuedReconfigs(insideStore).map { case (op, _) => limitOf(op) } shouldBe List(7)

    // Outside every block "$x" is the literal it is on main.
    val outsideStore = new ExecutionStateStore
    val outside =
      new WorkflowOnlyService(outsideStore, mkWorkflow(List(filterOp("filter-a", "Asia"))))
    outside.modifyOperatorLogic(ModifyLogicRequest(filterOp("filter-a", "$x"))) shouldBe
      ModifyLogicResponse("filter-a", isValid = true, "")
    queuedReconfigs(outsideStore) should have size 1
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

  private def assertDispatchedOps(request: WorkflowReconfigureRequest, ops: PhysicalOp*): Unit = {
    request.reconfiguration.map(_.targetOpId) should contain theSameElementsInOrderAs ops.map(_.id)
    request.reconfiguration.map(_.newExecInitInfo) should contain theSameElementsInOrderAs
      ops.map(_.opExecInitInfo)
  }

  it should "mount what an edited operator names and send its bound code" in {
    val stateStore = new ExecutionStateStore()
    val service = new RecordingService(stateStore)
    val bound = OpExecWithClassName("bound.Class", "")
    val op = mkPhysicalOp("op-1").withExecutionTimeBinding(Some(new ExecutionTimeBinding {
      override def opExecInitInfo: OpExecInitInfo = bound
      override def mountLocators: Set[String] = Set("dataset-1:abc123")
    }))
    enqueueReconfigs(stateStore, (op, None))

    service.performReconfigurationOnResume()

    service.mounted shouldBe Seq(Set("dataset-1:abc123"))
    service.captured.head.reconfiguration.map(_.newExecInitInfo) shouldBe Seq(bound)
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
    assertDispatchedOps(request, op1, op2)

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
      enqueueReconfigs(stateStore, (mkPhysicalOp(opName), None))
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
      enqueueReconfigs(f.stateStore, (op1, None), (op2, None))

      f.service.performReconfigurationOnResume()

      f.client.coordinatorCalls should have size 1
      val (calledMethod, payload) = f.client.coordinatorCalls.head
      // Named explicitly: the coordinator interface carries every control command, and reaching
      // the engine through any other one would not start a Fries round.
      calledMethod shouldBe "reconfigureWorkflow"
      val captured = payload.asInstanceOf[WorkflowReconfigureRequest]
      assertDispatchedOps(captured, op1, op2)
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

    completedWorkers(stateStore) should contain theSameElementsAs Set(w1, w2)
  }

  "the worker completion callback" should
    "announce the logical operator of every worker that reports in" in {
    // Driven only through the engine event, so it also pins that the constructor registered the
    // callback for UpdateExecutorCompleted and passed the reported worker on.
    withLiveOpPair() { f =>
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

      completedWorkers(f.stateStore) shouldBe
        Set(mkWorker("opA", 0), mkWorker("opB", 0), mkWorker("opA", 1))
    }
  }

  it should "announce every operator that completed before the client attached" in {
    // WorkflowService re-subscribes the websocket observable on every browser attach, and
    // StateStore replays a (default, current) pair to each new subscriber, so a client joining a
    // running execution is handed one diff carrying every worker that has finished so far. It is
    // the only path that produces a multi-worker diff, and therefore the only thing that
    // distinguishes announcing the whole diff from announcing one of its members.
    withLiveOpPair() { f =>
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
    withLiveOpPair() { f =>
      enqueueReconfigs(f.stateStore, (mkPhysicalOp("opA"), None))
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
    withLiveOpPair() { f =>
      val batches = recordBatches(f.stateStore.reconfigurationStore) {
        f.client.fire(UpdateExecutorCompleted(workerA))
        f.service.unsubscribeAll()
        // Driven through the service's own entry point rather than the engine event on purpose.
        // What this test is about is the diff handler's disposable reaching the
        // SubscriptionManager; the engine callback is a separate subscription with its own
        // disposable (covered by the test below), and routing through it would leave the empty
        // batch explainable by either one being released.
        f.service.onWorkerReconfigured(workerB)
      }

      batches should have size 2
      announcedOps(batches.head) shouldBe List("opA")
      // The store still publishes -- the second completion did change the state, as the
      // assertion below confirms -- but the handler is gone, which is only true if the
      // constructor handed its disposable to the subscription manager.
      batches.last shouldBe empty
      completedWorkers(f.stateStore) shouldBe Set(workerA, workerB)
    }
  }

  it should "release the engine subscription once the service is unsubscribed" in {
    val workerA = mkWorker("opA", 0)
    val workerB = mkWorker("opB", 0)
    withLiveOpPair() { f =>
      // A live registration first, so a disposed one below cannot be confused with one that was
      // never made.
      f.client.fire(UpdateExecutorCompleted(workerA))
      completedWorkers(f.stateStore) shouldBe Set(workerA)

      f.service.unsubscribeAll()

      // The Disposable `registerCallback` handed back has to reach the SubscriptionManager, the
      // same way the diff handler's does; this is the only thing that can release it. The
      // client's own `shutdown` is not an alternative: it flips `isActive` and poison-pills the
      // ClientActor but never touches `registeredObservables`, so the subject keeps the
      // subscriber -- and with it this service, its state store and its Workflow -- attached for
      // as long as the AmberClient is reachable, which WorkflowExecutionService keeps it.
      f.client.disposedCallbacks should contain(classOf[UpdateExecutorCompleted])

      // ...and the detached callback is inert: a late engine event does not write into the store
      // of a finished execution.
      f.client.fireIfRegistered(UpdateExecutorCompleted(workerB))
      completedWorkers(f.stateStore) shouldBe Set(workerA)
    }
  }
}
