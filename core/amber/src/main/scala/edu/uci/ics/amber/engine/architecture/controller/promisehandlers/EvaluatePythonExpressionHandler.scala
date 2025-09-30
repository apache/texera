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

package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EvaluatePythonExpressionRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EvaluatePythonExpressionResponse
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity

trait EvaluatePythonExpressionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def evaluatePythonExpression(
      msg: EvaluatePythonExpressionRequest,
      ctx: AsyncRPCContext
  ): Future[EvaluatePythonExpressionResponse] = {
    val logicalOpId = new OperatorIdentity(msg.operatorId)
    val physicalOps = cp.workflowScheduler.physicalPlan.getPhysicalOpsOfLogicalOp(logicalOpId)
    if (physicalOps.size != 1) {
      val msg =
        s"logical operator $logicalOpId has ${physicalOps.size} physical operators, expecting a single one"
      throw new RuntimeException(msg)
    }

    val physicalOp = physicalOps.head
    val opExecution = cp.workflowExecution.getLatestOperatorExecution(physicalOp.id)

    Future
      .collect(
        opExecution.getWorkerIds
          .map(worker => workerInterface.evaluatePythonExpression(msg, mkContext(worker)))
          .toList
      )
      .map(evaluatedValues => {
        EvaluatePythonExpressionResponse(evaluatedValues)
      })
  }

}
