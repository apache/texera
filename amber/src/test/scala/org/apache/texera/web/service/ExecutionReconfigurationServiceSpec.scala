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

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.PhysicalOp
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.WorkflowReconfigureRequest
import org.apache.texera.web.storage.{ExecutionReconfigurationStore, ExecutionStateStore}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer

/**
  * Web-service-layer tests for ExecutionReconfigurationService.
  *
  * The end-to-end engine path (reconfigureWorkflow → Fries algorithm →
  * UpdateExecutor on workers) is covered by ReconfigurationSpec.
  * This spec focuses on the wiring inside performReconfigurationOnResume:
  * empty short-circuit, request construction, and store reset semantics.
  */
class ExecutionReconfigurationServiceSpec extends AnyFlatSpec with Matchers {

  private def mkPhysicalOp(name: String): PhysicalOp =
    PhysicalOp(
      id = PhysicalOpIdentity(OperatorIdentity(name), "main"),
      workflowId = WorkflowIdentity(0L),
      executionId = ExecutionIdentity(0L),
      opExecInitInfo = OpExecWithClassName(s"$name.Class", "")
    )

  /** Service variant that records dispatched requests and skips the AmberClient
    * registration / workflow-dependent diff handler so it can be constructed
    * without a live engine.
    */
  private class RecordingService(stateStore: ExecutionStateStore)
      extends ExecutionReconfigurationService(client = null, stateStore, workflow = null) {
    val captured: ArrayBuffer[WorkflowReconfigureRequest] = ArrayBuffer.empty
    override protected def dispatch(request: WorkflowReconfigureRequest): Unit =
      captured += request
    override protected def registerWorkerCompletionCallback(): Unit = ()
    override protected def registerCompletionDiffHandler(): Unit = ()
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
    stateStore.reconfigurationStore.updateState(_ =>
      ExecutionReconfigurationStore(unscheduledReconfigurations = List((op1, None), (op2, None)))
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
}
