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

import com.twitter.util.Await
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.{PhysicalOp, PhysicalPlan}
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorAsyncRPCHandlerInitializer
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EvaluatePythonExpressionRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.flatspec.AnyFlatSpec

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
class EvaluatePythonExpressionHandlerSpec extends AnyFlatSpec with CoordinatorMessageFixture {

  private val rpcContext = AsyncRPCContext(COORDINATOR, COORDINATOR)

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

  behavior of "EvaluatePythonExpressionHandler"

  it should "refuse a logical operator that has no physical operator in the plan" in {
    val (init, sent) =
      createCoordinatorFixture(Some(PhysicalPlan(Set(mkPhysicalOp("present")), Set.empty)))

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
    val (init, sent) = createCoordinatorFixture(
      Some(
        PhysicalPlan(
          Set(mkPhysicalOp("expanded", "first"), mkPhysicalOp("expanded", "second")),
          Set.empty
        )
      )
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
    val physicalOp = mkPhysicalOp("udf")
    val (init, sent) = createCoordinatorFixture(Some(PhysicalPlan(Set(physicalOp), Set.empty)))
    val workerIds = Seq(mkWorkerId(physicalOp, 0), mkWorkerId(physicalOp, 1))
    seedOperatorExecution(init, regionId = 1, physicalOp, workerIds)
    val request = EvaluatePythonExpressionRequest("tuple_['x']", "udf")

    init.evaluatePythonExpression(request, rpcContext)

    val invocations = controlInvocationsNamed(sent, "evaluatePythonExpression")
    assert(invocations.map(_.context.receiver).toSet == workerIds.toSet)
    assert(invocations.size == workerIds.size)
    // The worker-side handler needs the expression verbatim, so nothing may be rewritten on the
    // way out.
    assert(invocations.forall(_.command == request))
  }

  it should "target the workers of the operator's latest execution, not an earlier one" in {
    val physicalOp = mkPhysicalOp("udf")
    val (init, sent) = createCoordinatorFixture(Some(PhysicalPlan(Set(physicalOp), Set.empty)))
    val staleWorkerId = mkWorkerId(physicalOp, 0)
    val liveWorkerId = mkWorkerId(physicalOp, 1)
    // The operator was re-run: region 2's execution superseded region 1's, and region 1's worker
    // no longer exists.
    seedOperatorExecution(init, regionId = 1, physicalOp, Seq(staleWorkerId))
    seedOperatorExecution(init, regionId = 2, physicalOp, Seq(liveWorkerId))

    init.evaluatePythonExpression(EvaluatePythonExpressionRequest("1", "udf"), rpcContext)

    assert(
      controlInvocationsNamed(sent, "evaluatePythonExpression")
        .map(_.context.receiver) == Seq(liveWorkerId)
    )
  }

  it should "not answer before every targeted worker has replied" in {
    val physicalOp = mkPhysicalOp("udf")
    val (init, sent) = createCoordinatorFixture(Some(PhysicalPlan(Set(physicalOp), Set.empty)))
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
    assert(controlInvocationsNamed(sent, "evaluatePythonExpression").size == 2)
    assert(!response.isDefined)
  }

  it should "answer immediately with no values when the operator has no workers" in {
    val physicalOp = mkPhysicalOp("udf")
    val (init, sent) = createCoordinatorFixture(Some(PhysicalPlan(Set(physicalOp), Set.empty)))
    seedOperatorExecution(init, regionId = 1, physicalOp, Seq.empty)

    val response = init.evaluatePythonExpression(
      EvaluatePythonExpressionRequest("1", "udf"),
      rpcContext
    )

    // Nothing to ask, so the handler must not stall on an empty collect.
    assert(controlInvocationsNamed(sent, "evaluatePythonExpression").isEmpty)
    assert(Await.result(response, awaitTimeout) == EvaluatePythonExpressionResponse(Seq.empty))
  }
}
