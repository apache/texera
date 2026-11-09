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

import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.NO_ALIGNMENT
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest,
  PropagateEmbeddedControlMessageRequest
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RETRIEVE_STATE
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
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
class RetrieveWorkflowStateHandlerSpec extends AnyFlatSpec {

  private val rpcContext = AsyncRPCContext(COORDINATOR, COORDINATOR)

  /**
    * Build a coordinator handler initializer whose dispatched output messages are appended to the returned
    * buffer. `workflowScheduler.physicalPlan` defaults to null, so callers must assign it before invoking the
    * handler (which dereferences it).
    */
  private def newFixture()
      : (CoordinatorAsyncRPCHandlerInitializer, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Right(m) => sent += m
      case _        => ()
    }
    val cp = new CoordinatorProcessor(
      new WorkflowContext(),
      CoordinatorConfig(None, None, None, None),
      COORDINATOR,
      outputHandler
    )
    (new CoordinatorAsyncRPCHandlerInitializer(cp), sent)
  }

  /**
    * Assert that exactly one control message was dispatched and it carries a propagate-ECM invocation, then
    * return the unwrapped request for further inspection.
    */
  private def dispatchedRequest(
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ): PropagateEmbeddedControlMessageRequest = {
    assert(sent.size == 1)
    sent.head.payload match {
      case ci: ControlInvocation =>
        assert(ci.methodName == "propagateEmbeddedControlMessage")
        ci.command match {
          case req: PropagateEmbeddedControlMessageRequest => req
          case other                                       => fail(s"unexpected control command: $other")
        }
      case other => fail(s"unexpected dispatched payload: $other")
    }
  }

  behavior of "RetrieveWorkflowStateHandler"

  it should "dispatch a single propagate-ECM control message with no targets for an empty plan" in {
    val (init, sent) = newFixture()
    init.cp.workflowScheduler.physicalPlan = PhysicalPlan(Set.empty, Set.empty)

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
    val (init, sent) = newFixture()
    init.cp.workflowScheduler.physicalPlan = physicalPlan

    init.retrieveWorkflowState(EmptyRequest(), rpcContext)

    val expectedOps = physicalPlan.operators.map(_.id)
    val req = dispatchedRequest(sent)
    assert(expectedOps.nonEmpty)
    // `operators` is a Set, so compare set-wise rather than assuming a stable Seq order.
    assert(req.targetOps.toSet == expectedOps)
    assert(req.scope.toSet == expectedOps)
  }
}
