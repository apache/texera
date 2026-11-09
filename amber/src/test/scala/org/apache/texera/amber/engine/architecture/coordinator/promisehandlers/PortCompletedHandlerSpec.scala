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

package org.apache.texera.amber.engine.architecture.coordinator.promisehandlers

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.{ImplicitSender, TestKit}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  PortCompletedRequest,
  ControlInvocation => ControlInvocationPayload
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  ControlError,
  ControlReturn,
  EmptyReturn,
  ReturnInvocation
}
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceGrpc.{
  METHOD_COORDINATOR_INITIATE_ADVANCE_REGION_EXECUTIONS,
  METHOD_PORT_COMPLETED
}
import org.apache.texera.amber.engine.architecture.scheduling.RegionExecutionManagerTestSupport.{
  createSingleWorkerRegion,
  createSourceOp,
  createWorkerId,
  createWorkerIds,
  seedReusableWorkerExecution
}
import org.apache.texera.amber.engine.architecture.scheduling.{
  RegionExecutionManagerTestSupport,
  RegionIdentity,
  Schedule
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.{COORDINATOR, SELF}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable

/**
  * `portCompleted` is the request whose handling completes a region: once the last port of a
  * region is done, the region is terminated, which sends `EndWorker` to each of its workers.
  *
  * The reply to `portCompleted` and that `EndWorker` travel the same FIFO control channel to the
  * sending worker, so the region advance must NOT happen inline in this handler — it would put
  * `EndWorker` on the wire ahead of the reply, and the worker would then process `EndWorker` with
  * the reply still queued behind it and reject the termination (#6891). The advance is instead
  * requested as a coordinator-to-coordinator control message, which the coordinator can only
  * process in a later round — by which time this round's reply has been sent.
  */
class PortCompletedHandlerSpec
    extends TestKit(ActorSystem("PortCompletedHandlerSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionExecutionManagerTestSupport {

  private val physicalOp = createSourceOp("port-completed-op")
  private val workerId = createWorkerId(physicalOp)
  private val outputPortId = PortIdentity()
  private val portCompletedCommandId = 7L

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /**
    * A real `CoordinatorProcessor` with a single-region schedule already in flight: the region
    * owns the port the worker will report, and its worker execution is pre-seeded so starting the
    * region does not create real workers. `advanceRegionExecutions` is called once to move the
    * region into the executing set (its `startWorker` RPCs are captured and never answered, so
    * the region stays executing); the messages it produced are then dropped so each test only
    * observes what handling `portCompleted` emits.
    */
  private def coordinatorWithRegionInFlight()
      : (CoordinatorProcessor, mutable.ListBuffer[WorkflowFIFOMessage]) = {
    val captured = mutable.ListBuffer[WorkflowFIFOMessage]()
    val cp = new CoordinatorProcessor(
      new WorkflowContext(),
      CoordinatorConfig(None, None, None, None),
      COORDINATOR,
      {
        case Right(msg) => captured += msg
        case Left(_)    => ()
      }
    )
    val harness = createCoordinatorHarness()
    registerLiveWorker(harness.actorRefService, workerId)
    cp.setupActorService(harness.actorService)
    cp.workflowExecutionManager.setupActorRefService(harness.actorRefService)

    val region = createSingleWorkerRegion(1, physicalOp, workerId).copy(
      ports = Set(GlobalPortIdentity(physicalOp.id, outputPortId, input = false))
    )
    seedReusableWorkerExecution(cp.workflowExecution, seedRegionId = 101, physicalOp, workerId)
    cp.workflowExecutionManager.schedule = Schedule(Map(0 -> Set(region)))
    cp.workflowExecutionManager.advanceRegionExecutions(cp.actorService)

    captured.clear()
    (cp, captured)
  }

  /** Deliver a worker's `portCompleted` through the real RPC server, as production does. */
  private def receivePortCompleted(
      cp: CoordinatorProcessor,
      portId: PortIdentity,
      sender: ActorVirtualIdentity = workerId
  ): Unit = {
    cp.processDCM(
      ChannelIdentity(sender, COORDINATOR, isControl = true),
      ControlInvocation(
        METHOD_PORT_COMPLETED,
        PortCompletedRequest(portId, input = false),
        AsyncRPCContext(sender, COORDINATOR),
        portCompletedCommandId
      )
    )
  }

  private def messagesTo(
      captured: mutable.ListBuffer[WorkflowFIFOMessage],
      receiver: ActorVirtualIdentity
  ): Seq[WorkflowFIFOMessage] =
    captured.filter(_.channelId.toWorkerId == receiver).toSeq

  private def repliesTo(
      captured: mutable.ListBuffer[WorkflowFIFOMessage],
      receiver: ActorVirtualIdentity
  ): Seq[ReturnInvocation] =
    messagesTo(captured, receiver).collect {
      case WorkflowFIFOMessage(_, _, ret: ReturnInvocation) =>
        ret
    }

  private def selfInvocations(
      captured: mutable.ListBuffer[WorkflowFIFOMessage]
  ): Seq[ControlInvocationPayload] =
    messagesTo(captured, COORDINATOR).collect {
      case WorkflowFIFOMessage(_, _, inv: ControlInvocationPayload) => inv
    }

  /** The coordinator-addressed messages asking for region executions to be advanced. */
  private def advanceRequestsIn(
      captured: mutable.ListBuffer[WorkflowFIFOMessage]
  ): Seq[ControlInvocationPayload] =
    // The RPC proxy sends the reflected Java method name, which differs from the generated
    // constant only in the leading case; the server matches case-insensitively too.
    selfInvocations(captured).filter(
      _.methodName.equalsIgnoreCase(
        METHOD_COORDINATOR_INITIATE_ADVANCE_REGION_EXECUTIONS.getBareMethodName
      )
    )

  /** Resolve the statistics query the handler awaits, so its continuation runs. */
  private def resolveStatisticsQuery(
      cp: CoordinatorProcessor,
      captured: mutable.ListBuffer[WorkflowFIFOMessage],
      returnValue: ControlReturn = EmptyReturn()
  ): Unit = {
    val statsCommandId = selfInvocations(captured).head.commandId
    cp.asyncRPCClient.fulfillPromise(ReturnInvocation(statsCommandId, returnValue))
  }

  "PortCompletedHandler" should
    "request the region advance as a separate control message instead of advancing inline" in {
    val (cp, captured) = coordinatorWithRegionInFlight()

    receivePortCompleted(cp, outputPortId)
    resolveStatisticsQuery(cp, captured)

    // The advance leaves as a coordinator-to-coordinator control message, which the coordinator
    // can only process in a later round — after this round's reply below has been sent.
    assert(advanceRequestsIn(captured).size == 1)
    assert(
      repliesTo(captured, workerId) == Seq(
        ReturnInvocation(portCompletedCommandId, EmptyReturn())
      )
    )
  }

  it should "send nothing but the reply to the reporting worker while handling portCompleted" in {
    val (cp, captured) = coordinatorWithRegionInFlight()

    receivePortCompleted(cp, outputPortId)
    resolveStatisticsQuery(cp, captured)

    // Guards against a regression to inline advancing: an `EndWorker` (or any other control
    // invocation) emitted to this worker in this round would be ordered ahead of the reply.
    assert(messagesTo(captured, workerId).size == 1)
    assert(
      !captured.exists(msg =>
        msg.channelId.toWorkerId == workerId && msg.payload.isInstanceOf[ControlInvocationPayload]
      )
    )
  }

  it should "mark the reported port completed before requesting the advance" in {
    val (cp, captured) = coordinatorWithRegionInFlight()

    receivePortCompleted(cp, outputPortId)
    resolveStatisticsQuery(cp, captured)

    // The advance runs in a later round, so the bookkeeping it depends on must already be
    // recorded in the execution state by the time the request goes out.
    val operatorExecution = cp.workflowExecution
      .getRegionExecution(RegionIdentity(1))
      .getOperatorExecution(physicalOp.id)
    assert(operatorExecution.isOutputPortCompleted(outputPortId))
    assert(advanceRequestsIn(captured).size == 1)
  }

  it should "not request an advance for a port that belongs to no executing region" in {
    val (cp, captured) = coordinatorWithRegionInFlight()

    // "start"/"end" ports are not part of any region, so no region resolves and nothing advances.
    receivePortCompleted(cp, PortIdentity(9))
    resolveStatisticsQuery(cp, captured)

    assert(advanceRequestsIn(captured).isEmpty)
    assert(
      repliesTo(captured, workerId) == Seq(
        ReturnInvocation(portCompletedCommandId, EmptyReturn())
      )
    )
  }

  it should "request the advance without waiting for the sender's workerExecutionCompleted" in {
    val (cp, captured) = coordinatorWithRegionInFlight()

    // The worker emits `workerExecutionCompleted` right after its last `portCompleted`
    // (`OutputManager.finalizeOutput` appends FinalizePort before FinalizeExecutor). Region
    // completion keys only on port completion, so booking the last port requests the advance
    // while that request may still be queued at the coordinator — and the coordinator selects
    // input channels out of a HashMap, so it may run the advance first and reply afterwards.
    receivePortCompleted(cp, outputPortId)
    resolveStatisticsQuery(cp, captured)

    assert(advanceRequestsIn(captured).size == 1)
    // Nothing here has replied to a `workerExecutionCompleted`, so the resulting `EndWorker` can
    // legitimately reach the worker before that reply does. `EndHandler` closes this by not
    // counting a queued reply as work — the coordinator cannot order it, so the worker tolerates
    // it (see EndHandlerSpec, "reply successfully when only a coordinator reply is queued").
    assert(repliesTo(captured, workerId).map(_.commandId) == Seq(portCompletedCommandId))
  }

  it should "complete the port and request the advance even when the statistics query fails" in {
    val (cp, captured) = coordinatorWithRegionInFlight()
    receivePortCompleted(cp, outputPortId)

    // The statistics refresh is best-effort (#6924): if it fails, the port bookkeeping must
    // still run, or the region it belongs to would never advance. Worker-side failures are
    // absorbed inside the query, so a failure here stands for one in the coordinator itself.
    resolveStatisticsQuery(cp, captured, ControlError.defaultInstance)

    val operatorExecution = cp.workflowExecution
      .getRegionExecution(RegionIdentity(1))
      .getOperatorExecution(physicalOp.id)
    assert(operatorExecution.isOutputPortCompleted(outputPortId))
    assert(advanceRequestsIn(captured).size == 1)
    assert(
      repliesTo(captured, workerId) == Seq(
        ReturnInvocation(portCompletedCommandId, EmptyReturn())
      )
    )
  }

  it should "still return a failed reply to the worker when its own bookkeeping fails" in {
    val (cp, captured) = coordinatorWithRegionInFlight()
    // A worker of the region's operator that the coordinator holds no execution for: booking
    // its port fails inside the handler's continuation, after the statistics refresh.
    val unknownWorker = createWorkerIds(physicalOp, 2).last
    receivePortCompleted(cp, outputPortId, sender = unknownWorker)
    resolveStatisticsQuery(cp, captured)

    // Only the statistics refresh is best-effort; a failure in the bookkeeping itself is still
    // reported back to the worker that sent `portCompleted`.
    val reply = repliesTo(captured, unknownWorker).head
    assert(reply.commandId == portCompletedCommandId)
    assert(reply.returnValue.isInstanceOf[ControlError])
    assert(advanceRequestsIn(captured).isEmpty)
  }

  it should "complete the port and request the advance when the worker never answers the statistics refresh" in {
    // Built with the shared fixture rather than `coordinatorWithRegionInFlight()`: this test needs
    // the deadline the query schedules to be captured, so it can fire it.
    // The scenario of #6924: the statistics reply is lost, so without a deadline the bookkeeping
    // chained on it never runs and the region never advances.
    val f = newCoordinatorInFlight(
      physicalOp,
      workerId,
      regionPorts = Set(GlobalPortIdentity(physicalOp.id, outputPortId, input = false))
    )
    receivePortCompleted(f.cp, outputPortId)

    // Run the statistics refresh the handler requested of the coordinator itself.
    val refresh = selfInvocations(f.captured).head
    f.cp.processDCM(ChannelIdentity(COORDINATOR, COORDINATOR, isControl = true), refresh)
    // The worker never replies; the refresh's deadline fires instead.
    val deadline =
      f.actorService.scheduledToSelf.collect { case (_, inv: ControlInvocationPayload) => inv }
    f.cp.processDCM(ChannelIdentity(SELF, SELF, isControl = true), deadline.head)
    // The refresh then completes and replies to the coordinator, which resumes the handler.
    val refreshReply = messagesTo(f.captured, COORDINATOR).collectFirst {
      case WorkflowFIFOMessage(_, _, ret: ReturnInvocation) if ret.commandId == refresh.commandId =>
        ret
    }
    f.cp.asyncRPCClient.fulfillPromise(refreshReply.get)

    assert(advanceRequestsIn(f.captured).size == 1)
    assert(
      repliesTo(f.captured, workerId) == Seq(
        ReturnInvocation(portCompletedCommandId, EmptyReturn())
      )
    )
  }
}
