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
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EvaluatePythonExpressionRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

/**
  * `evaluatePythonExpression` is the coordinator-side fan-out for the debugger's
  * expression-evaluation panel: it resolves the logical operator the UI named to a single physical
  * operator, then asks every worker of that operator's *latest* execution to evaluate the
  * expression.
  *
  * Two things can silently break here. The resolution step is a precondition — a logical operator
  * that expanded into several physical operators (or into none) has no unambiguous worker set, and
  * the handler must refuse rather than pick one arbitrarily. The fan-out step must read the latest
  * operator execution, because an operator re-run in a later region execution has a different set
  * of live workers and the stale ones are gone.
  *
  * The handler dispatches through `workerInterface`, so these tests capture what the coordinator's
  * output handler emits instead of running workers (the harness mirrors
  * `RetrieveWorkflowStateHandlerSpec`).
  */
class EvaluatePythonExpressionHandlerSpec extends AnyFlatSpec {

  private val rpcContext = AsyncRPCContext(COORDINATOR, COORDINATOR)
  private val awaitTimeout = Duration.fromSeconds(1)

  private def mkPhysicalOp(logicalOpId: String, layerName: String): PhysicalOp =
    PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(logicalOpId), layerName),
      DEFAULT_WORKFLOW_ID,
      DEFAULT_EXECUTION_ID,
      OpExecInitInfo.Empty
    )

  private def mkWorkerId(physicalOp: PhysicalOp, index: Int): ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, physicalOp.id, index)

  /**
    * Build a coordinator handler initializer over a physical plan holding `physicalOps`. Dispatched
    * control messages are appended to the returned buffer.
    */
  private def newFixture(
      physicalOps: Set[PhysicalOp]
  ): (CoordinatorAsyncRPCHandlerInitializer, ArrayBuffer[WorkflowFIFOMessage]) = {
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
    cp.workflowScheduler.physicalPlan = PhysicalPlan(physicalOps, Set.empty)
    (new CoordinatorAsyncRPCHandlerInitializer(cp), sent)
  }

  /**
    * Register a region execution that owns `physicalOp` with the given workers. Region executions
    * are stored in creation order, and the handler resolves the operator through the *last* one
    * that knows it, so the call order of this helper is what decides which workers are "latest".
    */
  private def seedOperatorExecution(
      init: CoordinatorAsyncRPCHandlerInitializer,
      regionId: Long,
      physicalOp: PhysicalOp,
      workerIds: Seq[ActorVirtualIdentity]
  ): Unit = {
    val operatorExecution = init.cp.workflowExecution
      .initRegionExecution(Region(RegionIdentity(regionId), Set(physicalOp), Set.empty))
      .initOperatorExecution(physicalOp.id)
    workerIds.foreach(operatorExecution.initWorkerExecution)
  }

  /** The per-worker expression-evaluation invocations the handler dispatched. */
  private def dispatchedEvaluations(
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ): Seq[ControlInvocation] =
    sent.toSeq.collect {
      case WorkflowFIFOMessage(_, _, invocation: ControlInvocation)
          if invocation.methodName == "evaluatePythonExpression" =>
        invocation
    }

  behavior of "EvaluatePythonExpressionHandler"

  it should "refuse a logical operator that has no physical operator in the plan" in {
    val (init, sent) = newFixture(Set(mkPhysicalOp("present", "main")))

    val ex = intercept[RuntimeException] {
      init.evaluatePythonExpression(
        EvaluatePythonExpressionRequest("1 + 1", "absent"),
        rpcContext
      )
    }

    assert(ex.getMessage.contains("has 0 physical operators, expecting a single one"))
    assert(ex.getMessage.contains("absent"))
    assert(sent.isEmpty)
  }

  it should "refuse a logical operator that expanded into several physical operators" in {
    // A logical operator with more than one physical layer has no unambiguous worker set; the
    // handler must not silently evaluate against `physicalOps.head`.
    val (init, sent) = newFixture(
      Set(mkPhysicalOp("expanded", "first"), mkPhysicalOp("expanded", "second"))
    )

    val ex = intercept[RuntimeException] {
      init.evaluatePythonExpression(
        EvaluatePythonExpressionRequest("1 + 1", "expanded"),
        rpcContext
      )
    }

    assert(ex.getMessage.contains("has 2 physical operators, expecting a single one"))
    assert(sent.isEmpty)
  }

  it should "forward the request unchanged to every worker of the operator" in {
    val physicalOp = mkPhysicalOp("udf", "main")
    val (init, sent) = newFixture(Set(physicalOp))
    val workerIds = Seq(mkWorkerId(physicalOp, 0), mkWorkerId(physicalOp, 1))
    seedOperatorExecution(init, regionId = 1, physicalOp, workerIds)
    val request = EvaluatePythonExpressionRequest("tuple_['x']", "udf")

    init.evaluatePythonExpression(request, rpcContext)

    val invocations = dispatchedEvaluations(sent)
    assert(invocations.map(_.context.receiver).toSet == workerIds.toSet)
    assert(invocations.size == workerIds.size)
    // The worker-side handler needs the expression verbatim, so nothing may be rewritten on the
    // way out.
    assert(invocations.forall(_.command == request))
  }

  it should "target the workers of the operator's latest execution, not an earlier one" in {
    val physicalOp = mkPhysicalOp("udf", "main")
    val (init, sent) = newFixture(Set(physicalOp))
    val staleWorkerId = mkWorkerId(physicalOp, 0)
    val liveWorkerId = mkWorkerId(physicalOp, 1)
    // The operator was re-run: region 2's execution superseded region 1's, and region 1's worker
    // no longer exists.
    seedOperatorExecution(init, regionId = 1, physicalOp, Seq(staleWorkerId))
    seedOperatorExecution(init, regionId = 2, physicalOp, Seq(liveWorkerId))

    init.evaluatePythonExpression(EvaluatePythonExpressionRequest("1", "udf"), rpcContext)

    assert(dispatchedEvaluations(sent).map(_.context.receiver) == Seq(liveWorkerId))
  }

  it should "not answer before every targeted worker has replied" in {
    val physicalOp = mkPhysicalOp("udf", "main")
    val (init, sent) = newFixture(Set(physicalOp))
    seedOperatorExecution(
      init,
      regionId = 1,
      physicalOp,
      Seq(mkWorkerId(physicalOp, 0), mkWorkerId(physicalOp, 1))
    )

    val response = init.evaluatePythonExpression(
      EvaluatePythonExpressionRequest("1", "udf"),
      rpcContext
    )

    // Both worker invocations are outstanding, so the aggregate response must still be pending —
    // the UI panel shows one row per worker and a partial answer would be wrong.
    assert(dispatchedEvaluations(sent).size == 2)
    assert(!response.isDefined)
  }

  it should "answer immediately with no values when the operator has no workers" in {
    val physicalOp = mkPhysicalOp("udf", "main")
    val (init, sent) = newFixture(Set(physicalOp))
    seedOperatorExecution(init, regionId = 1, physicalOp, Seq.empty)

    val response = init.evaluatePythonExpression(
      EvaluatePythonExpressionRequest("1", "udf"),
      rpcContext
    )

    // Nothing to ask, so the handler must not stall on an empty collect.
    assert(dispatchedEvaluations(sent).isEmpty)
    assert(Await.result(response, awaitTimeout) == EvaluatePythonExpressionResponse(Seq.empty))
  }
}
