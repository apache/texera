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

import com.twitter.util.{Await, Duration}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.NO_ALIGNMENT
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest,
  PropagateEmbeddedControlMessageRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  PropagateEmbeddedControlMessageResponse,
  RetrieveWorkflowStateResponse,
  ReturnInvocation,
  StringResponse
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RETRIEVE_STATE
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

/**
  * `retrieveWorkflowState` is the coordinator-side entry point that fans a "collect your state" request
  * out to every operator in the running workflow.
  *
  * It does so by wrapping the workflow's physical operators into a single `PropagateEmbeddedControlMessageRequest`
  * (targeting the `retrieveState` worker method) and dispatching it to itself via `coordinatorInterface`. The
  * returned `Future` only completes once every worker replies, which never happens in a plain-JVM spec with no
  * live workers -- so these tests do not await it. Instead they capture the one control message the handler
  * dispatches synchronously through the coordinator's output handler and assert on its shape.
  *
  * The harness mirrors the worker-side `EndHandlerSpec`: build a real `CoordinatorProcessor` plus a
  * `CoordinatorAsyncRPCHandlerInitializer`, then invoke the handler method directly (no ActorSystem).
  */
class RetrieveWorkflowStateHandlerSpec extends AnyFlatSpec with CoordinatorMessageFixture {

  private val rpcContext = AsyncRPCContext(COORDINATOR, COORDINATOR)

  /**
    * Assert that exactly one control message was dispatched and it carries a propagate-ECM invocation, then
    * return the unwrapped request for further inspection.
    */
  private def dispatchedRequest(
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ): PropagateEmbeddedControlMessageRequest = {
    assert(sent.size == 1)
    val invocations = controlInvocationsNamed(sent, "propagateEmbeddedControlMessage")
    assert(invocations.size == 1, s"expected propagateEmbeddedControlMessage, got: $sent")
    invocations.head.command match {
      case req: PropagateEmbeddedControlMessageRequest => req
      case other                                       => fail(s"unexpected control command: $other")
    }
  }

  behavior of "RetrieveWorkflowStateHandler"

  it should "dispatch a single propagate-ECM control message with no targets for an empty plan" in {
    val (init, sent) = createCoordinatorFixture(Some(PhysicalPlan(Set.empty, Set.empty)))

    init.retrieveWorkflowState(EmptyRequest(), rpcContext)

    val req = dispatchedRequest(sent)
    assert(req.targetOps.isEmpty)
    assert(req.scope.isEmpty)
    // No running region executions on a fresh WorkflowExecution, so nothing to start propagation from.
    assert(req.sourceOpToStartProp.isEmpty)
    assert(req.ecmType == NO_ALIGNMENT)
    assert(req.command == EmptyRequest())
    assert(req.methodName == METHOD_RETRIEVE_STATE.getBareMethodName)
    assert(req.id.id.startsWith("RetrieveWorkflowState_"))
  }

  it should "target every operator of a populated physical plan" in {
    val physicalPlan = buildWorkflow(
      List(TestOperators.headerlessSmallCsvScanOpDesc()),
      List(),
      new WorkflowContext()
    ).physicalPlan
    val (init, sent) = createCoordinatorFixture(Some(physicalPlan))

    init.retrieveWorkflowState(EmptyRequest(), rpcContext)

    val expectedOps = physicalPlan.operators.map(_.id)
    val req = dispatchedRequest(sent)
    assert(expectedOps.nonEmpty)
    // `operators` is a Set, so compare set-wise rather than assuming a stable Seq order.
    assert(req.targetOps.toSet == expectedOps)
    assert(req.scope.toSet == expectedOps)
  }

  it should "map StringResponse replies to their strings and fallback other returns to empty strings" in {
    val (init, sent) = createCoordinatorFixture(Some(PhysicalPlan(Set.empty, Set.empty)))

    val response = init.retrieveWorkflowState(EmptyRequest(), rpcContext)
    assert(sent.size == 1)
    val invocation = sent.head.payload.asInstanceOf[ControlInvocation]
    init.cp.asyncRPCClient.fulfillPromise(
      ReturnInvocation(
        invocation.commandId,
        PropagateEmbeddedControlMessageResponse(
          Map("worker-a" -> StringResponse("state-a"), "worker-b" -> EmptyReturn())
        )
      )
    )

    assert(
      Await.result(response, Duration.fromSeconds(5)) == RetrieveWorkflowStateResponse(
        Map("worker-a" -> "state-a", "worker-b" -> "")
      )
    )
  }

  it should "include only running region operators in sourceOpToStartProp" in {
    val runningOp = mkPhysicalOp("running")
    val completedOp = mkPhysicalOp("completed")
    val runningRegion = Region(
      RegionIdentity(1),
      physicalOps = Set(runningOp),
      physicalLinks = Set.empty,
      ports = Set(GlobalPortIdentity(runningOp.id, PortIdentity(), input = false))
    )
    val completedRegion = Region(
      RegionIdentity(2),
      physicalOps = Set(completedOp),
      physicalLinks = Set.empty,
      ports = Set(GlobalPortIdentity(completedOp.id, PortIdentity(), input = false))
    )
    val (init, sent) =
      createCoordinatorFixture(Some(PhysicalPlan(Set(runningOp, completedOp), Set.empty)))
    val runningExec = init.cp.workflowExecution.initRegionExecution(runningRegion)
    runningExec.initOperatorExecution(runningOp.id).initWorkerExecution(mkWorkerId(runningOp, 0))
    val completedExec = init.cp.workflowExecution.initRegionExecution(completedRegion)
    completedExec
      .initOperatorExecution(completedOp.id)
      .initWorkerExecution(mkWorkerId(completedOp, 0))
      .getOutputPortExecution(PortIdentity())
      .setCompleted()

    init.retrieveWorkflowState(EmptyRequest(), rpcContext)

    val req = dispatchedRequest(sent)
    assert(req.sourceOpToStartProp.toSet == Set(runningOp.id))
    assert(req.targetOps.toSet == Set(runningOp.id, completedOp.id))
  }
}
