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

import com.twitter.util.{Future, Promise}
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.ExecutionStateUpdate
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorConfig
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  EmptyRequest,
  TakeGlobalCheckpointRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  TakeGlobalCheckpointResponse
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState._
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceFs2Grpc
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.FaultToleranceConfig
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.web.WebsocketInput
import org.apache.texera.web.model.websocket.request.{
  WorkflowCheckpointRequest,
  WorkflowKillRequest,
  WorkflowPauseRequest,
  WorkflowResumeRequest
}
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.net.URI
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

class ExecutionRuntimeServiceSpec
    extends TestKit(ActorSystem("ExecutionRuntimeServiceSpec"))
    with AnyFlatSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  /** Real empty-plan client for constructor compatibility, with lifecycle collaborators overridden below. */
  private final class TestAmberClient(
      override val coordinatorInterface: CoordinatorServiceFs2Grpc[Future, Unit]
  ) extends AmberClient(
        system,
        new WorkflowContext(),
        PhysicalPlan(Set.empty, Set.empty),
        CoordinatorConfig(None, None, None, None),
        _ => ()
      ) {
    var shutdownCount = 0
    var stateCallback: ExecutionStateUpdate => Unit = _

    override def shutdown(): Unit = {
      shutdownCount += 1
      super.shutdown()
    }

    override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
      if (ct.runtimeClass == classOf[ExecutionStateUpdate]) {
        stateCallback = callback.asInstanceOf[ExecutionStateUpdate => Unit]
      }
      Disposable.empty()
    }

    def dispose(): Unit = super.shutdown()
  }

  /** Records resume wiring while skipping constructor-time client and workflow subscriptions. */
  private final class RecordingReconfigurationService
      extends ExecutionReconfigurationService(client = null, stateStore = null, workflow = null) {
    var resumeCallCount = 0

    override def performReconfigurationOnResume(): Unit = {
      resumeCallCount += 1
    }

    override protected def registerWorkerCompletionCallback(): Unit = {}

    override protected def registerCompletionDiffHandler(): Unit = {}
  }

  private final class Fixture(
      val client: TestAmberClient,
      val coordinator: CoordinatorServiceFs2Grpc[Future, Unit],
      val stateStore: ExecutionStateStore,
      val wsInput: WebsocketInput,
      val reconfigurationService: RecordingReconfigurationService,
      val runtime: ExecutionRuntimeService,
      val errors: ListBuffer[Throwable]
  ) {
    def close(): Unit = {
      runtime.unsubscribeAll()
      client.dispose()
    }
  }

  private def fixture(
      logConf: Option[FaultToleranceConfig] = Some(
        FaultToleranceConfig(new URI("file:///runtime-checkpoints/"))
      )
  ): Fixture = {
    val coordinator = mock[CoordinatorServiceFs2Grpc[Future, Unit]]
    val client = new TestAmberClient(coordinator)
    val stateStore = new ExecutionStateStore
    val errors = ListBuffer.empty[Throwable]
    val wsInput = new WebsocketInput(errors += _)
    val reconfigurationService = new RecordingReconfigurationService

    val runtime = new ExecutionRuntimeService(
      client,
      stateStore,
      wsInput,
      reconfigurationService,
      logConf,
      workflowId = 7L,
      emailNotificationEnabled = false,
      userEmailOpt = None,
      sessionUri = new URI("https://texera.example/session")
    )
    new Fixture(
      client,
      coordinator,
      stateStore,
      wsInput,
      reconfigurationService,
      runtime,
      errors
    )
  }

  "ExecutionRuntimeService" should "mark the runtime as pausing before sending a pause command" in {
    val f = fixture()
    try {
      (f.coordinator.pauseWorkflow _)
        .expects(EmptyRequest(), ())
        .onCall { (_: EmptyRequest, _: Unit) =>
          f.stateStore.metadataStore.getState.state shouldBe PAUSING
          Future.value(EmptyReturn())
        }

      f.wsInput.onNext(WorkflowPauseRequest(), None)

      f.stateStore.metadataStore.getState.state shouldBe PAUSING
      f.errors shouldBe empty
    } finally f.close()
  }

  it should "resume pending reconfiguration and transition to running after the RPC succeeds" in {
    val f = fixture()
    try {
      val resumeResult = Promise[EmptyReturn]()
      (f.coordinator.resumeWorkflow _)
        .expects(EmptyRequest(), ())
        .onCall { (_: EmptyRequest, _: Unit) =>
          f.reconfigurationService.resumeCallCount shouldBe 1
          resumeResult
        }

      f.wsInput.onNext(WorkflowResumeRequest(), None)
      f.stateStore.metadataStore.getState.state shouldBe RESUMING

      resumeResult.setValue(EmptyReturn())
      f.stateStore.metadataStore.getState.state shouldBe RUNNING
      f.errors shouldBe empty
    } finally f.close()
  }

  it should "kill the client and record a terminal timestamp when killed from the websocket" in {
    val f = fixture()
    try {
      f.wsInput.onNext(WorkflowKillRequest(), None)

      f.client.shutdownCount shouldBe 1
      f.stateStore.metadataStore.getState.state shouldBe KILLED
      f.stateStore.statsStore.getState.endTimeStamp should be > 0L
      f.errors shouldBe empty
    } finally f.close()
  }

  it should "dispatch a non-estimation checkpoint request below the configured log URI" in {
    val f = fixture()
    try {
      var captured: TakeGlobalCheckpointRequest = null
      (f.coordinator.takeGlobalCheckpoint _)
        .expects(*, ())
        .onCall { (request: TakeGlobalCheckpointRequest, _: Unit) =>
          captured = request
          Future.value(TakeGlobalCheckpointResponse(0L))
        }

      f.wsInput.onNext(WorkflowCheckpointRequest(), None)

      captured should not be null
      captured.estimationOnly shouldBe false
      captured.checkpointId.id should startWith("Checkpoint_")
      val destination = new URI(captured.destination)
      destination.getScheme shouldBe "file"
      destination shouldBe new URI("file:///runtime-checkpoints/").resolve(
        captured.checkpointId.toString
      )
      f.errors shouldBe empty
    } finally f.close()
  }

  it should "report a checkpoint request without fault-tolerance storage instead of dispatching it" in {
    val f = fixture(logConf = None)
    try {
      f.wsInput.onNext(WorkflowCheckpointRequest(), None)

      f.errors should have size 1
      f.errors.head.getMessage should include("Fault tolerance log folder is not established")
    } finally f.close()
  }

  it should "apply a captured completed callback, stop the client, and stamp the end time" in {
    val f = fixture()
    try {
      f.client.stateCallback should not be null
      f.client.stateCallback(ExecutionStateUpdate(COMPLETED))

      f.client.shutdownCount shouldBe 1
      f.stateStore.metadataStore.getState.state shouldBe COMPLETED
      f.stateStore.statsStore.getState.endTimeStamp should be > 0L
      f.errors shouldBe empty
    } finally f.close()
  }
}
