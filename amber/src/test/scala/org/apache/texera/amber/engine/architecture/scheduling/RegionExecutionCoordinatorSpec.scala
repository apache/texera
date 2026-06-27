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

import com.twitter.util.{Duration => TwitterDuration, Future}
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

  it should "give up with a descriptive error once the EndWorker retry budget is exhausted" in {
    // EndWorker always fails: a worker that never finishes draining.
    val fixture = createSingleRegionFixture(
      endWorkerResponse = _ => Some(transientEndWorkerFailure),
      maxTerminationAttempts = 3,
      killRetryDelay = TwitterDuration.fromMilliseconds(5)
    )

    launchRegion(fixture.coordinator)
    val completion = requestRegionCompletion(fixture.coordinator)

    val failure = intercept[IllegalStateException] {
      await(completion)
    }
    assert(failure.getMessage.contains("could not be terminated after 3 attempts"))
    assert(!fixture.coordinator.isCompleted)
    assert(fixture.rpcProbe.endWorkerCalls.size == 3)
    assert(fixture.actorRefService.hasActorRef(fixture.workerId))
  }

  it should "give up after a single attempt when the budget is one" in {
    val fixture = createSingleRegionFixture(
      endWorkerResponse = _ => Some(transientEndWorkerFailure),
      maxTerminationAttempts = 1,
      killRetryDelay = TwitterDuration.fromMilliseconds(5)
    )

    launchRegion(fixture.coordinator)
    val completion = requestRegionCompletion(fixture.coordinator)

    val failure = intercept[IllegalStateException] {
      await(completion)
    }
    assert(failure.getMessage.contains("could not be terminated after 1 attempt"))
    assert(failure.getMessage.contains(fixture.workerId.toString))
    assert(fixture.rpcProbe.endWorkerCalls.size == 1)
  }

  it should "complete when EndWorker finally succeeds on the last permitted attempt" in {
    // Fail every attempt but the last permitted one. The give-up branch only fires when an attempt
    // both fails AND is the final one, so a success on attempt == budget must still complete the
    // region rather than report it as un-terminable. This pins the off-by-one boundary.
    val attempts = new atomic.AtomicInteger(0)
    val fixture = createSingleRegionFixture(
      endWorkerResponse = _ =>
        if (attempts.incrementAndGet() < 2) {
          Some(transientEndWorkerFailure)
        } else {
          Some(EmptyReturn())
        },
      maxTerminationAttempts = 2,
      killRetryDelay = TwitterDuration.fromMilliseconds(5)
    )

    launchRegion(fixture.coordinator)
    val completion = requestRegionCompletion(fixture.coordinator)

    await(completion)
    assert(fixture.coordinator.isCompleted)
    assert(fixture.rpcProbe.endWorkerCalls.size == 2)
    assert(!fixture.actorRefService.hasActorRef(fixture.workerId))
    assert(workerState(fixture) == WorkerState.TERMINATED)
  }

  it should "list every still-running worker and preserve the cause when giving up" in {
    // A region with several workers, all of which never finish draining. The give-up error must
    // name each stuck worker (so the user can act on it) and chain the underlying failure as cause.
    val fixture = createMultiWorkerGiveUpFixture(
      workerCount = 3,
      maxTerminationAttempts = 2,
      killRetryDelay = TwitterDuration.fromMilliseconds(5)
    )

    launchRegion(fixture.coordinator)
    val completion = requestRegionCompletion(fixture.coordinator)

    val failure = intercept[IllegalStateException] {
      await(completion)
    }
    assert(failure.getMessage.contains("could not be terminated after 2 attempts"))
    fixture.workerIds.foreach { workerId =>
      assert(failure.getMessage.contains(workerId.toString))
    }
    assert(failure.getCause != null)
    assert(!fixture.coordinator.isCompleted)
    // EndWorker is sent to every worker on every attempt.
    assert(fixture.rpcProbe.endWorkerCalls.size == fixture.workerIds.size * 2)
  }

  it should "default to a bounded ~30s termination budget" in {
    // 150 attempts * 200 ms ≈ 30 s. These defaults are the documented contract for how long a
    // stuck region blocks before failing loudly; pin them so changes are deliberate.
    assert(RegionExecutionCoordinator.DefaultMaxTerminationAttempts == 150)
    assert(
      RegionExecutionCoordinator.DefaultKillRetryDelay == TwitterDuration.fromMilliseconds(200)
    )
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
      endWorkerResponse: WorkerRpcCall => Option[ControlReturn],
      maxTerminationAttempts: Int = RegionExecutionCoordinator.DefaultMaxTerminationAttempts,
      killRetryDelay: TwitterDuration = RegionExecutionCoordinator.DefaultKillRetryDelay
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
      controller.actorRefService,
      maxTerminationAttempts,
      killRetryDelay
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

  private case class MultiWorkerFixture(
      coordinator: RegionExecutionCoordinator,
      rpcProbe: ControllerRpcProbe,
      workerIds: Seq[ActorVirtualIdentity]
  )

  // A region whose workers all fail EndWorker forever, used to exercise the give-up path's
  // aggregation over multiple workers.
  private def createMultiWorkerGiveUpFixture(
      workerCount: Int,
      maxTerminationAttempts: Int,
      killRetryDelay: TwitterDuration
  ): MultiWorkerFixture = {
    val physicalOp = createSourceOp("multi-op")
    val workerIds = createWorkerIds(physicalOp, workerCount)
    val region = createWorkerRegion(1, physicalOp, workerIds)

    val workflowExecution = WorkflowExecution()
    seedReusableWorkerExecutions(workflowExecution, seedRegionId = 0, physicalOp, workerIds)
    workflowExecution.initRegionExecution(region)

    val rpcProbe = new ControllerRpcProbe(_ => Some(transientEndWorkerFailure))
    val controller = createControllerHarness()
    workerIds.foreach(registerLiveWorker(controller.actorRefService, _))

    val coordinator = new RegionExecutionCoordinator(
      region,
      isRestart = false,
      workflowExecution,
      rpcProbe.asyncRPCClient,
      ControllerConfig(None, None, None, None),
      controller.actorService,
      controller.actorRefService,
      maxTerminationAttempts,
      killRetryDelay
    )

    MultiWorkerFixture(coordinator, rpcProbe, workerIds)
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
