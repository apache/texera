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

import com.twitter.util.Future
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  OutputPort,
  PhysicalOp,
  PortIdentity
}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.engine.architecture.common.PekkoActorRefMappingService
import org.apache.texera.amber.engine.architecture.controller.ControllerConfig
import org.apache.texera.amber.engine.architecture.controller.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.rpc.controlreturns._
import org.apache.texera.amber.engine.architecture.scheduling.RegionCoordinatorTestSupport._
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  OperatorConfig,
  OutputPortConfig,
  ResourceConfig,
  WorkerConfig
}
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.virtualidentity.util.CONTROLLER
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic

/**
  * Tests the real region-coordination lifecycle around synchronous region kill.
  *
  * The tests let the coordinator call the real `AsyncRPCClient.workerInterface`, capture the generated
  * `ControlInvocation`s at the controller output gateway, and fulfill those RPC promises
  * explicitly. This keeps the important production behavior under test:
  *
  *  - regular launch RPCs (`initializeExecutor`, `openExecutor`, `startWorker`) are allowed to
  *    complete immediately;
  *  - `endWorker` can be held pending or failed to model worker-side drain/termination behavior;
  *  - the real coordinator then decides when to remove actor refs, clean control channels, mark
  *    workers terminated, and allow the next region to start.
  */
class RegionExecutionCoordinatorSpec
    extends TestKit(ActorSystem("RegionExecutionCoordinatorSpec", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll
    with RegionCoordinatorTestSupport {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "RegionExecutionCoordinator" should "send gracefulStop only after EndWorker succeeds" in {
    val fixture = createSingleRegionFixture(endWorkerResponse = _ => None)

    launchRegion(fixture.coordinator)
    val completion = requestRegionCompletion(fixture.coordinator)

    assert(
      fixture.rpcProbe.methodTrace == Seq(InitializeExecutor, OpenExecutor, StartWorker, EndWorker)
    )
    assert(completion.poll.isEmpty)
    assert(!fixture.coordinator.isCompleted)
    assert(fixture.actorRefService.hasActorRef(fixture.workerId))

    fixture.rpcProbe.fulfill(fixture.rpcProbe.onlyEndWorkerCall, EmptyReturn())
    await(completion)

    assert(fixture.coordinator.isCompleted)
    assert(!fixture.actorRefService.hasActorRef(fixture.workerId))
    assert(workerState(fixture) == WorkerState.TERMINATED)
    assertControlChannelsAreRemoved(fixture)
  }

  it should "retry EndWorker failures and delay gracefulStop until a retry succeeds" in {
    val attempts = new atomic.AtomicInteger(0)
    // The first EndWorker is sent on the test thread; the retry is sent later from the coordinator's
    // kill-retry timer thread. Block on this latch — counted down from the probe callback once the
    // retry's call has been recorded — instead of polling `endWorkerCalls` from the test thread, so
    // the test never iterates the call buffer while the timer thread is appending to it.
    val retryAttempted = new CountDownLatch(1)
    val fixture = createSingleRegionFixture(endWorkerResponse =
      _ =>
        if (attempts.incrementAndGet() == 1) {
          Some(transientEndWorkerFailure)
        } else {
          retryAttempted.countDown()
          None
        }
    )

    launchRegion(fixture.coordinator)
    val completion = requestRegionCompletion(fixture.coordinator)

    assert(
      retryAttempted.await(testTimeout.inMilliseconds, TimeUnit.MILLISECONDS),
      "EndWorker was not retried within the deadline"
    )
    assert(completion.poll.isEmpty)
    assert(!fixture.coordinator.isCompleted)
    assert(fixture.actorRefService.hasActorRef(fixture.workerId))

    fixture.rpcProbe.fulfill(fixture.rpcProbe.endWorkerCalls.last, EmptyReturn())
    await(completion)

    assert(fixture.coordinator.isCompleted)
    assert(fixture.rpcProbe.endWorkerCalls.size == 2)
    assert(!fixture.actorRefService.hasActorRef(fixture.workerId))
    assert(workerState(fixture) == WorkerState.TERMINATED)
  }

  it should "surface the underlying cause when an output port schema is unavailable" in {
    // Reproduces issue #3546: when schema inference for an output port fails (e.g. because a
    // dataset used by the workflow has not been shared with the running user), the port's
    // schema is stored as a `Left(cause)`. The coordinator must surface that real cause rather
    // than discarding it behind a generic "Schema is missing" message.
    val cause = new RuntimeException("User texera1 has no access to dataset 'iris'")
    val coordinator = coordinatorWithUnresolvedOutputSchema(cause)

    val thrown = intercept[IllegalStateException] {
      await(coordinator.syncStatusAndTransitionRegionExecutionPhase())
    }
    assert(thrown.getCause eq cause)
    assert(thrown.getMessage.contains(cause.getMessage))
  }

  it should "fall back to the throwable's string form when the cause has no message" in {
    // Some throwables (e.g. NullPointerException) carry a null message; the surfaced text must
    // not read "...: null".
    val cause = new NullPointerException()
    assert(cause.getMessage == null)
    val coordinator = coordinatorWithUnresolvedOutputSchema(cause)

    val thrown = intercept[IllegalStateException] {
      await(coordinator.syncStatusAndTransitionRegionExecutionPhase())
    }
    assert(thrown.getCause eq cause)
    assert(thrown.getMessage.contains(cause.toString))
    assert(!thrown.getMessage.endsWith("null"))
  }

  /**
    * Builds a coordinator for a single-source region whose only output port has an unresolved
    * schema (`Left(cause)`) and a configured output storage, so that the non-dependee phase
    * reaches `createOutputPortStorageObjects` and attempts to read that schema.
    */
  private def coordinatorWithUnresolvedOutputSchema(
      cause: Throwable
  ): RegionExecutionCoordinator = {
    val portId = PortIdentity(0)
    val baseOp = createSourceOp("schema-missing-op").withOutputPorts(List(OutputPort(portId)))
    val (outPort, links, _) = baseOp.outputPorts(portId)
    val physicalOp =
      baseOp.copy(outputPorts = baseOp.outputPorts.updated(portId, (outPort, links, Left(cause))))

    val workerId = createWorkerId(physicalOp)
    val globalPortId = GlobalPortIdentity(physicalOp.id, portId)
    val storageBase =
      VFSURIFactory.createPortBaseURI(DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID, globalPortId)
    val region = Region(
      RegionIdentity(1),
      physicalOps = Set(physicalOp),
      physicalLinks = Set.empty,
      resourceConfig = Some(
        ResourceConfig(
          operatorConfigs = Map(physicalOp.id -> OperatorConfig(List(WorkerConfig(workerId)))),
          portConfigs = Map(globalPortId -> OutputPortConfig(storageBase))
        )
      )
    )

    val workflowExecution = WorkflowExecution()
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 0, physicalOp, workerId)
    workflowExecution.initRegionExecution(region)

    val rpcProbe = new ControllerRpcProbe(_ => None)
    val controller = createControllerHarness()
    registerLiveWorker(controller.actorRefService, workerId)

    new RegionExecutionCoordinator(
      region,
      isRestart = false,
      workflowExecution,
      rpcProbe.asyncRPCClient,
      ControllerConfig(None, None, None, None),
      controller.actorService,
      controller.actorRefService
    )
  }

  private case class SingleRegionFixture(
      coordinator: RegionExecutionCoordinator,
      rpcProbe: ControllerRpcProbe,
      workflowExecution: WorkflowExecution,
      region: Region,
      physicalOp: PhysicalOp,
      workerId: ActorVirtualIdentity,
      actorRefService: PekkoActorRefMappingService
  )

  private def createSingleRegionFixture(
      endWorkerResponse: WorkerRpcCall => Option[ControlReturn]
  ): SingleRegionFixture = {
    val physicalOp = createSourceOp("test-op")
    val workerId = createWorkerId(physicalOp)
    val region = createSingleWorkerRegion(1, physicalOp, workerId)

    val workflowExecution = WorkflowExecution()
    seedReusableWorkerExecution(workflowExecution, seedRegionId = 0, physicalOp, workerId)
    workflowExecution.initRegionExecution(region)

    val rpcProbe = new ControllerRpcProbe(endWorkerResponse)
    val controller = createControllerHarness()
    registerLiveWorker(controller.actorRefService, workerId)

    // Seed stale control channels to verify that successful termination removes them.
    rpcProbe.inputGateway.getChannel(ChannelIdentity(workerId, CONTROLLER, isControl = true))
    rpcProbe.outputGateway.getSequenceNumber(
      ChannelIdentity(CONTROLLER, workerId, isControl = true)
    )

    val coordinator = new RegionExecutionCoordinator(
      region,
      isRestart = false,
      workflowExecution,
      rpcProbe.asyncRPCClient,
      ControllerConfig(None, None, None, None),
      controller.actorService,
      controller.actorRefService
    )

    SingleRegionFixture(
      coordinator = coordinator,
      rpcProbe = rpcProbe,
      workflowExecution = workflowExecution,
      region = region,
      physicalOp = physicalOp,
      workerId = workerId,
      actorRefService = controller.actorRefService
    )
  }

  private def launchRegion(coordinator: RegionExecutionCoordinator): Unit = {
    await(coordinator.syncStatusAndTransitionRegionExecutionPhase())
  }

  private def requestRegionCompletion(
      coordinator: RegionExecutionCoordinator
  ): Future[Unit] = {
    coordinator.syncStatusAndTransitionRegionExecutionPhase()
  }

  private def workerState(fixture: SingleRegionFixture): WorkerState =
    fixture.workflowExecution
      .getRegionExecution(fixture.region.id)
      .getOperatorExecution(fixture.physicalOp.id)
      .getWorkerExecution(fixture.workerId)
      .getState

  private def assertControlChannelsAreRemoved(fixture: SingleRegionFixture): Unit = {
    assert(
      !fixture.rpcProbe.inputGateway.getAllControlChannels.exists(
        _.channelId == ChannelIdentity(fixture.workerId, CONTROLLER, isControl = true)
      )
    )
    assert(
      !fixture.rpcProbe.outputGateway.getActiveChannels.exists(
        _ == ChannelIdentity(CONTROLLER, fixture.workerId, isControl = true)
      )
    )
  }

  private def transientEndWorkerFailure: ControlError =
    ControlError(
      errorMessage = "transient EndWorker failure",
      errorDetails = "",
      stackTrace = "",
      language = ErrorLanguage.SCALA
    )
}
