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
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
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
  private def newHarness(): (CoordinatorProcessor, mutable.ListBuffer[WorkflowFIFOMessage]) = {
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
  private def receivePortCompleted(cp: CoordinatorProcessor, portId: PortIdentity): Unit = {
    cp.processDCM(
      ChannelIdentity(workerId, COORDINATOR, isControl = true),
      ControlInvocation(
        METHOD_PORT_COMPLETED,
        PortCompletedRequest(portId, input = false),
        AsyncRPCContext(workerId, COORDINATOR),
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

  private def advanceRequests(
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
    val (cp, captured) = newHarness()

    receivePortCompleted(cp, outputPortId)
    resolveStatisticsQuery(cp, captured)

    // The advance leaves as a coordinator-to-coordinator control message, which the coordinator
    // can only process in a later round — after this round's reply below has been sent.
    assert(advanceRequests(captured).size == 1)
    assert(
      repliesTo(captured, workerId) == Seq(
        ReturnInvocation(portCompletedCommandId, EmptyReturn())
      )
    )
  }

  it should "send nothing but the reply to the reporting worker while handling portCompleted" in {
    val (cp, captured) = newHarness()

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
    val (cp, captured) = newHarness()

    receivePortCompleted(cp, outputPortId)
    resolveStatisticsQuery(cp, captured)

    // The advance runs in a later round, so the bookkeeping it depends on must already be
    // recorded in the execution state by the time the request goes out.
    val operatorExecution = cp.workflowExecution
      .getRegionExecution(RegionIdentity(1))
      .getOperatorExecution(physicalOp.id)
    assert(operatorExecution.isOutputPortCompleted(outputPortId))
    assert(advanceRequests(captured).size == 1)
  }

  it should "not request an advance for a port that belongs to no executing region" in {
    val (cp, captured) = newHarness()

    // "start"/"end" ports are not part of any region, so no region resolves and nothing advances.
    receivePortCompleted(cp, PortIdentity(9))
    resolveStatisticsQuery(cp, captured)

    assert(advanceRequests(captured).isEmpty)
    assert(
      repliesTo(captured, workerId) == Seq(
        ReturnInvocation(portCompletedCommandId, EmptyReturn())
      )
    )
  }

  it should "return a failed reply to the worker when its bookkeeping fails" in {
    val (cp, captured) = newHarness()
    receivePortCompleted(cp, outputPortId)

    // Failing the statistics query fails the handler's continuation. The reply is still chained
    // on that continuation, so the error is reported back to the worker that sent
    // `portCompleted` — the contract documented for this handler, which deferring the advance
    // must not break.
    resolveStatisticsQuery(cp, captured, ControlError.defaultInstance)

    val reply = repliesTo(captured, workerId).head
    assert(reply.commandId == portCompletedCommandId)
    assert(reply.returnValue.isInstanceOf[ControlError])
    assert(advanceRequests(captured).isEmpty)
  }
}
