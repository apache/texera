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

import com.twitter.util.{Await, Duration, Future, Promise}
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.engine.architecture.common.PekkoActorService
import org.apache.texera.amber.engine.architecture.coordinator.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.coordinator.{
  ClientEvent,
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor,
  FatalError
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  ReturnInvocation
}
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceGrpc.METHOD_COORDINATOR_INITIATE_ADVANCE_REGION_EXECUTIONS
import org.apache.texera.amber.engine.architecture.scheduling.{
  RegionExecutionManagerTestSupport,
  WorkflowExecutionManager
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import scala.collection.mutable.ArrayBuffer

/**
  * `coordinatorInitiateAdvanceRegionExecutions` is how the coordinator advances its region
  * executions in a control round of its own: a handler that must not advance inline (see
  * `PortCompletedHandler`) sends this request to the coordinator itself, so that the `EndWorker`
  * messages a completed region produces cannot overtake the replies that round still owed.
  *
  * The behaviors this spec pins down:
  *   - the advance is delegated to the coordinator's own `WorkflowExecutionManager`;
  *   - the reply is produced synchronously, WITHOUT awaiting the advance. The requester is the
  *     coordinator itself and discards the reply, so awaiting it would only hold the control round
  *     open;
  *   - because nothing awaits the reply, a failing advance has no caller to propagate to and is
  *     reported to the client as a `FatalError` instead — carrying the related worker id when the
  *     failure came from a worker RPC (`WorkflowRuntimeException`) and `None` for anything else.
  */
class AdvanceRegionExecutionsHandlerSpec
    extends TestKit(ActorSystem("AdvanceRegionExecutionsHandlerSpec", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionExecutionManagerTestSupport {

  private val awaitTimeout = Duration.fromSeconds(5)
  private val coordinatorConfig = CoordinatorConfig(None, None, None, None)
  private val ctx: AsyncRPCContext = AsyncRPCContext(COORDINATOR, COORDINATOR)
  private val advanceCommandId = 11L
  private val relatedWorker = ActorVirtualIdentity("Worker:unschedulable-region-worker")

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  /**
    * Stands in for the real `WorkflowExecutionManager`: driving its advance to a *failed* future
    * from the outside means staging a whole region launch or termination, and the causes that
    * reaches the handler with are `IllegalStateException`s (unresolved output schema, termination
    * retries exhausted) or the bare `Throwable` that `ErrorUtils.reconstructThrowable` builds for a
    * worker RPC error — never a `WorkflowRuntimeException` carrying a worker id. This stub records
    * the advance calls and hands back a future the test controls, so the handler can be observed
    * both while the advance is still pending and after it has failed with either kind of cause.
    */
  private class PendingAdvanceExecutionManager(
      workflowExecution: WorkflowExecution,
      asyncRPCClient: AsyncRPCClient
  ) extends WorkflowExecutionManager(workflowExecution, coordinatorConfig, asyncRPCClient) {
    val advanceCalls: ArrayBuffer[PekkoActorService] = ArrayBuffer()
    private val advance: Promise[Unit] = Promise[Unit]()

    override def advanceRegionExecutions(actorService: PekkoActorService): Future[Unit] = {
      advanceCalls += actorService
      advance
    }

    /** Twitter futures run their continuations on the thread that satisfies them, so the
      * handler's `onFailure` has already run once this returns — no polling needed.
      */
    def failAdvance(cause: Throwable): Unit = advance.setException(cause)

    def finishAdvance(): Unit = advance.setValue(())
  }

  private class TestCoordinatorProcessor(
      outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
  ) extends CoordinatorProcessor(
        new WorkflowContext(),
        coordinatorConfig,
        COORDINATOR,
        outputHandler
      ) {
    override val workflowExecutionManager: PendingAdvanceExecutionManager =
      new PendingAdvanceExecutionManager(workflowExecution, asyncRPCClient)
  }

  private case class Fixture(
      cp: TestCoordinatorProcessor,
      init: CoordinatorAsyncRPCHandlerInitializer,
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ) {
    def manager: PendingAdvanceExecutionManager = cp.workflowExecutionManager
  }

  /**
    * A real `CoordinatorProcessor` (and therefore the real RPC layer) with the execution manager
    * replaced by the controllable stub, and a real `PekkoActorService` set up so the argument the
    * handler forwards is something other than `null`.
    */
  private def newFixture(): Fixture = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val cp = new TestCoordinatorProcessor({
      case Right(msg) => sent += msg
      case Left(_)    => ()
    })
    cp.setupActorService(createCoordinatorHarness().actorService)
    Fixture(cp, new CoordinatorAsyncRPCHandlerInitializer(cp), sent)
  }

  private def clientEvents(sent: ArrayBuffer[WorkflowFIFOMessage]): Seq[ClientEvent] =
    sent.toSeq.filter(_.channelId.toWorkerId == CLIENT).map(_.payload).collect {
      case event: ClientEvent => event
    }

  private def repliesToCoordinator(sent: ArrayBuffer[WorkflowFIFOMessage]): Seq[ReturnInvocation] =
    sent.toSeq.filter(_.channelId.toWorkerId == COORDINATOR).map(_.payload).collect {
      case ret: ReturnInvocation => ret
    }

  behavior of "AdvanceRegionExecutionsHandler"

  it should "advance the region executions with the coordinator's own actor service" in {
    val fixture = newFixture()

    fixture.init.coordinatorInitiateAdvanceRegionExecutions(EmptyRequest(), ctx)

    // The actor service is what the advance needs to create the next region's workers on, so the
    // coordinator's own one must be forwarded; guarded against a vacuous null == null match.
    assert(fixture.cp.actorService != null)
    assert(fixture.manager.advanceCalls.toSeq == Seq(fixture.cp.actorService))
  }

  it should "reply immediately instead of awaiting the advance" in {
    val fixture = newFixture()

    val response = fixture.init.coordinatorInitiateAdvanceRegionExecutions(EmptyRequest(), ctx)

    // The advance was started but is still pending, and the reply is already satisfied: this
    // handler deliberately does not chain its reply on the advance. Chaining it would keep the
    // control round open for the whole advance, which itself waits on region termination RPCs.
    assert(fixture.manager.advanceCalls.size == 1)
    assert(response.isDefined)
    assert(Await.result(response, awaitTimeout) == EmptyReturn())
  }

  it should "reply to the requesting round while the advance is still running" in {
    val fixture = newFixture()

    // Delivered the way production does it: as a coordinator-to-coordinator control message,
    // dispatched by the real RPC server. No other spec drives this handler that way —
    // `PortCompletedHandlerSpec` only asserts that the invocation is sent.
    fixture.cp.processDCM(
      ChannelIdentity(COORDINATOR, COORDINATOR, isControl = true),
      ControlInvocation(
        METHOD_COORDINATOR_INITIATE_ADVANCE_REGION_EXECUTIONS,
        EmptyRequest(),
        ctx,
        advanceCommandId
      )
    )

    assert(fixture.manager.advanceCalls.size == 1)
    assert(
      repliesToCoordinator(fixture.sent) == Seq(
        ReturnInvocation(advanceCommandId, EmptyReturn())
      )
    )
  }

  it should "notify the client of a failed advance with the related worker id" in {
    val fixture = newFixture()
    fixture.init.coordinatorInitiateAdvanceRegionExecutions(EmptyRequest(), ctx)

    val failure = new WorkflowRuntimeException("region cannot be scheduled", Some(relatedWorker))
    fixture.manager.failAdvance(failure)

    // A `WorkflowRuntimeException` from a worker RPC knows which worker it came from, and the
    // client needs that id to attribute the error to an operator.
    assert(clientEvents(fixture.sent) == Seq(FatalError(failure, Some(relatedWorker))))
  }

  it should "notify the client of a failed advance without a worker id for other failures" in {
    val fixture = newFixture()
    fixture.init.coordinatorInitiateAdvanceRegionExecutions(EmptyRequest(), ctx)

    val failure = new IllegalStateException("no resource config for the next region")
    fixture.manager.failAdvance(failure)

    // Not every advance failure comes from a worker, so there is no id to attribute.
    assert(clientEvents(fixture.sent) == Seq(FatalError(failure, None)))
  }

  it should "not notify the client when the advance succeeds" in {
    val fixture = newFixture()
    fixture.init.coordinatorInitiateAdvanceRegionExecutions(EmptyRequest(), ctx)

    fixture.manager.finishAdvance()

    assert(clientEvents(fixture.sent).isEmpty)
  }
}
