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

package org.apache.texera.amber.engine.architecture.scheduling

import com.twitter.util.{Await, Duration, Future}
import org.apache.pekko.actor.{Actor, ActorRef, Props}
import org.apache.pekko.testkit.{TestActorRef, TestKit}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.engine.architecture.common.{
  AkkaActorRefMappingService,
  AkkaActorService,
  WorkflowActor
}
import org.apache.texera.amber.engine.architecture.controller.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.messaginglayer.{
  NetworkInputGateway,
  NetworkOutputGateway
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.ControlInvocation
import org.apache.texera.amber.engine.architecture.rpc.controlreturns._
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  OperatorConfig,
  ResourceConfig,
  WorkerConfig
}
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState
import org.apache.texera.amber.engine.common.CheckpointState
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient
import org.apache.texera.amber.engine.common.virtualidentity.util.CONTROLLER
import org.apache.texera.amber.util.VirtualIdentityUtils

import scala.collection.mutable

object RegionCoordinatorTestSupport {
  val InitializeExecutor = "initializeExecutor"
  val OpenExecutor = "openExecutor"
  val StartWorker = "startWorker"
  val EndWorker = "endWorker"

  // Generous deadline for the polling helpers below. Production timing under test (notably the
  // 200 ms `killRetryDelay` in `RegionExecutionCoordinator`) fits comfortably; the rest is
  // headroom for slow CI.
  val testTimeout: Duration = Duration.fromSeconds(5)

  case class WorkerRpcCall(
      methodName: String,
      receiver: ActorVirtualIdentity,
      commandId: Long
  )

  case class ControllerHarnessFixture(
      actorService: AkkaActorService,
      actorRefService: AkkaActorRefMappingService
  )

  /**
    * Captures controller-to-worker RPCs at the same boundary used by production
    * `AsyncRPCClient.workerInterface`.
    *
    * Non-termination RPCs are completed immediately because these tests focus on termination
    * ordering. `endWorker` responses are controlled by `endWorkerResponse`, allowing each test to
    * hold termination pending, fail an attempt, or allow it to succeed.
    */
  class ControllerRpcProbe(endWorkerResponse: WorkerRpcCall => Option[ControlReturn]) {
    val calls: mutable.ArrayBuffer[WorkerRpcCall] = mutable.ArrayBuffer()
    val inputGateway = new NetworkInputGateway(CONTROLLER)
    val outputGateway = new NetworkOutputGateway(CONTROLLER, handleOutput)
    val asyncRPCClient = new AsyncRPCClient(inputGateway, outputGateway, CONTROLLER)

    def methodTrace: Seq[String] = calls.map(_.methodName).toSeq

    def initializedWorkers: Seq[ActorVirtualIdentity] =
      calls.filter(_.methodName == InitializeExecutor).map(_.receiver).toSeq

    def startedWorkers: Seq[ActorVirtualIdentity] =
      calls.filter(_.methodName == StartWorker).map(_.receiver).toSeq

    def endWorkerCalls: Seq[WorkerRpcCall] =
      calls.filter(_.methodName == EndWorker).toSeq

    def onlyEndWorkerCall: WorkerRpcCall = {
      assert(endWorkerCalls.size == 1)
      endWorkerCalls.head
    }

    def fulfill(call: WorkerRpcCall, returnValue: ControlReturn): Unit = {
      asyncRPCClient.fulfillPromise(ReturnInvocation(call.commandId, returnValue))
    }

    private def handleOutput(message: WorkflowFIFOMessage): Unit = {
      message.payload match {
        case invocation: ControlInvocation =>
          recordAndMaybeFulfill(invocation)
        case _ =>
        // Client events and stats updates are irrelevant to the coordinator lifecycle assertions.
      }
    }

    private def recordAndMaybeFulfill(invocation: ControlInvocation): Unit = {
      val call = WorkerRpcCall(
        methodName = invocation.methodName,
        receiver = invocation.context.receiver,
        commandId = invocation.commandId
      )
      calls += call
      immediateReturn(call).foreach(fulfill(call, _))
    }

    private def immediateReturn(call: WorkerRpcCall): Option[ControlReturn] = {
      call.methodName match {
        case InitializeExecutor | OpenExecutor =>
          Some(EmptyReturn())
        case StartWorker =>
          Some(WorkerStateResponse(WorkerState.RUNNING))
        case EndWorker =>
          endWorkerResponse(call)
        case other =>
          throw new AssertionError(s"Unexpected worker RPC in test: $other")
      }
    }
  }

  class IdleActor extends Actor {
    override def receive: Receive = { case _ => () }
  }

  class ControllerHarness extends WorkflowActor(None, CONTROLLER) {
    override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = ()

    override def getQueuedCredit(channelId: ChannelIdentity): Long = 0

    override def handleBackpressure(isBackpressured: Boolean): Unit = ()

    override def initState(): Unit = ()

    override def loadFromCheckpoint(chkpt: CheckpointState): Unit = ()
  }

  def createSourceOp(logicalOpId: String): PhysicalOp =
    PhysicalOp.sourcePhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(logicalOpId), "main"),
      DEFAULT_WORKFLOW_ID,
      DEFAULT_EXECUTION_ID,
      OpExecWithClassName("unused")
    )

  def createWorkerId(physicalOp: PhysicalOp): ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, physicalOp.id, 0)

  def createSingleWorkerRegion(
      regionId: Long,
      physicalOp: PhysicalOp,
      workerId: ActorVirtualIdentity
  ): Region =
    Region(
      RegionIdentity(regionId),
      physicalOps = Set(physicalOp),
      physicalLinks = Set.empty,
      resourceConfig = Some(
        ResourceConfig(
          operatorConfigs = Map(physicalOp.id -> OperatorConfig(List(WorkerConfig(workerId))))
        )
      )
    )

  def seedReusableWorkerExecution(
      workflowExecution: WorkflowExecution,
      seedRegionId: Long,
      physicalOp: PhysicalOp,
      workerId: ActorVirtualIdentity
  ): Unit = {
    // RegionExecutionCoordinator skips real worker creation when an execution for this operator
    // already exists.
    workflowExecution
      .initRegionExecution(createSingleWorkerRegion(seedRegionId, physicalOp, workerId))
      .initOperatorExecution(physicalOp.id)
      .initWorkerExecution(workerId)
  }

  def await[T](future: Future[T]): T = Await.result(future, testTimeout)

  def waitUntil(condition: => Boolean): Unit = {
    val deadline = System.nanoTime() + testTimeout.inNanoseconds
    while (!condition && System.nanoTime() < deadline) {
      Thread.sleep(20)
    }
    assert(condition, s"condition not satisfied within $testTimeout")
  }
}

trait RegionCoordinatorTestSupport { self: TestKit =>
  import RegionCoordinatorTestSupport._

  protected def createControllerHarness(): ControllerHarnessFixture = {
    val controllerRef = TestActorRef(new ControllerHarness)
    controllerRef.underlyingActor.actorService.getAvailableNodeAddressesFunc = () =>
      Array(controllerRef.path.address)
    ControllerHarnessFixture(
      actorService = controllerRef.underlyingActor.actorService,
      actorRefService = controllerRef.underlyingActor.actorRefMappingService
    )
  }

  protected def registerLiveWorker(
      actorRefService: AkkaActorRefMappingService,
      workerId: ActorVirtualIdentity
  ): ActorRef = {
    val workerRef = system.actorOf(Props(new IdleActor), s"worker-${System.nanoTime()}")
    actorRefService.registerActorRef(workerId, workerRef)
    workerRef
  }
}
