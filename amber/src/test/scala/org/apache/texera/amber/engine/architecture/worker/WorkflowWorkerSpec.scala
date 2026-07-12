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

package org.apache.texera.amber.engine.architecture.worker

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{ImplicitSender, TestActorRef, TestKit, TestProbe}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.coordinator.ReplayStatusUpdate
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest,
  InitializeExecutorRequest
}
import org.apache.texera.amber.engine.architecture.scheduling.config.WorkerConfig
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  ActorCommandElement,
  DPInputQueueElement,
  FaultToleranceConfig,
  MainThreadDelegateMessage,
  StateRestoreConfig,
  TimerBasedControlElement,
  TriggerSend,
  WorkerReplayInitialization
}
import org.apache.texera.amber.engine.common.actormessage.Backpressure
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.common.{AmberRuntime, CheckpointState, SerializedState}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URI
import java.util.concurrent.{CompletableFuture, LinkedBlockingQueue, TimeUnit}
import scala.collection.mutable

class WorkflowWorkerSpec
    extends TestKit(ActorSystem("WorkflowWorkerSpec", AmberRuntime.pekkoConfig))
    with ImplicitSender
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  // The "Worker:...-<idx>" form is required so VirtualIdentityUtils.getWorkerIndex
  // resolves an index; otherwise loadFromCheckpoint's restoreExecutorState throws.
  private val identifier1 = ActorVirtualIdentity("Worker:WF1-E1-op-layer-1")

  override def beforeAll(): Unit = {
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private def mkWorker(): TestActorRef[WorkflowWorker] =
    TestActorRef(
      new WorkflowWorker(WorkerConfig(identifier1), WorkerReplayInitialization())
    )

  "WorkflowWorker" should "enqueue backpressure and preRestart" in {
    val worker = mkWorker()
    // Stop the DP thread first: it blocks on inputQueue.take and would otherwise
    // steal the element we enqueue below before we can poll it.
    worker.underlyingActor.dpThread.stop()

    // (a) handleBackpressure enqueues an ActorCommandElement(Backpressure(true)).
    worker.underlyingActor.handleBackpressure(true)
    assert(worker.underlyingActor.inputQueue.poll() == ActorCommandElement(Backpressure(true)))

    // (b) preRestart tears the worker down; safe because initState already ran (dpThread
    // is non-null) during synchronous TestActorRef construction.
    worker.underlyingActor.preRestart(new RuntimeException("x"), None)
  }

  "WorkflowWorker" should "run a MainThreadDelegateMessage closure via handleTriggerClosure" in {
    val worker = mkWorker()
    val f = new CompletableFuture[Boolean]()
    // TestActorRef dispatches synchronously, so receive -> handleTriggerClosure runs inline.
    worker ! MainThreadDelegateMessage(_ => f.complete(true))
    assert(f.get(5, TimeUnit.SECONDS))
    worker.underlyingActor.dpThread.stop()
  }

  "WorkflowWorker" should "restore worker state via loadFromCheckpoint" in {
    val dp = new DataProcessor(
      identifier1,
      msg => {},
      new LinkedBlockingQueue[DPInputQueueElement]()
    )
    // The restored executor is rebuilt reflectively from this init message, which
    // must be present on the serialization manager before the DP is checkpointed.
    dp.serializationManager.setOpInitialization(
      InitializeExecutorRequest(
        1,
        OpExecWithClassName(
          "org.apache.texera.amber.engine.architecture.worker.DummyOperatorExecutor"
        ),
        isSource = false
      )
    )

    val chkpt = new CheckpointState()
    chkpt.save(SerializedState.DP_STATE_KEY, dp)
    // Keep every message collection empty: transferService.send (network) is then never
    // invoked, and DummyOperatorExecutor is not CheckpointSupport so restoreExecutorState
    // yields an empty iterator.
    chkpt.save(SerializedState.IN_FLIGHT_MSG_KEY, mutable.ArrayBuffer.empty[WorkflowFIFOMessage])
    chkpt.save(SerializedState.DP_QUEUED_MSG_KEY, mutable.ArrayBuffer.empty[WorkflowFIFOMessage])
    chkpt.save(SerializedState.OUTPUT_MSG_KEY, Array.empty[WorkflowFIFOMessage])

    val parent = TestProbe()
    val worker = TestActorRef[WorkflowWorker](
      Props(new WorkflowWorker(WorkerConfig(identifier1), WorkerReplayInitialization())),
      parent.ref,
      "worker-load-checkpoint"
    )

    worker.underlyingActor.loadFromCheckpoint(chkpt)

    // preStart first sends a RegisterActorRef to the parent; fish past it for the
    // ReplayStatusUpdate emitted at the end of loadFromCheckpoint.
    parent.fishForMessage() {
      case r: ReplayStatusUpdate => r == ReplayStatusUpdate(identifier1, status = false)
      case _                     => false
    }

    worker.underlyingActor.dpThread.stop()
  }

  "WorkflowWorker companion" should "construct its message and config case classes" in {
    val control = ControlInvocation(
      "test",
      EmptyRequest(),
      AsyncRPCContext(COORDINATOR, identifier1),
      0
    )
    val fifoMsg = WorkflowFIFOMessage(
      ChannelIdentity(COORDINATOR, identifier1, isControl = true),
      0,
      control
    )

    assert(TriggerSend(fifoMsg).msg == fifoMsg)
    assert(TimerBasedControlElement(control).control == control)
    assert(ActorCommandElement(Backpressure(true)).cmd == Backpressure(true))

    val restore =
      StateRestoreConfig(new URI("ram:///read"), EmbeddedControlMessageIdentity("cp"))
    assert(restore.replayDestination == EmbeddedControlMessageIdentity("cp"))

    val ft = FaultToleranceConfig(new URI("ram:///write"))
    assert(ft.writeTo == new URI("ram:///write"))

    val init = WorkerReplayInitialization(Some(restore), Some(ft))
    assert(init.restoreConfOpt.contains(restore))
    assert(init.faultToleranceConfOpt.contains(ft))
  }

}
