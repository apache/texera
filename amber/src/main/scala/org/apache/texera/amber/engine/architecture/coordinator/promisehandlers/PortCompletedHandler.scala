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

import com.twitter.util.Future
import org.apache.texera.amber.core.workflow.GlobalPortIdentity
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorAsyncRPCHandlerInitializer
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  PortCompletedRequest,
  QueryStatisticsRequest,
  StatisticsUpdateTarget
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.util.VirtualIdentityUtils

/** Notify the completion of a port:
  * - For input port, it means the worker has finished consuming and processing all the data
  * through this port, including all possible links to this port.
  * - For output port, it means the worker has finished sending all the data through this port.
  *
  * possible sender: worker
  */
trait PortCompletedHandler {
  this: CoordinatorAsyncRPCHandlerInitializer =>

  override def portCompleted(
      msg: PortCompletedRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    coordinatorInterface
      .coordinatorInitiateQueryStatistics(
        QueryStatisticsRequest(
          scala.Seq(ctx.sender),
          StatisticsUpdateTarget.BOTH_UI_AND_PERSISTENCE
        ),
        COORDINATOR
      )
      .map { _ =>
        val globalPortId = GlobalPortIdentity(
          VirtualIdentityUtils.getPhysicalOpId(ctx.sender),
          msg.portId,
          input = msg.input
        )
        cp.workflowExecutionManager.getRegionOfPortId(globalPortId) match {
          case Some(region) =>
            val regionExecution = cp.workflowExecution.getRegionExecution(region.id)
            val operatorExecution =
              regionExecution.getOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(ctx.sender))
            val workerExecution = operatorExecution.getWorkerExecution(ctx.sender)

            // set the port on this worker to be completed
            (if (msg.input) workerExecution.getInputPortExecution(msg.portId)
             else workerExecution.getOutputPortExecution(msg.portId)).setCompleted()

            // check if the port on this operator is completed
            val isPortCompleted =
              if (msg.input) operatorExecution.isInputPortCompleted(msg.portId)
              else operatorExecution.isOutputPortCompleted(msg.portId)

            if (isPortCompleted) {
              // Ask the coordinator to advance region executions in a later control round rather
              // than advancing here. Advancing inline would terminate the completed region and
              // send `EndWorker` to this very sender before this handler's own reply, and both
              // travel the same FIFO control channel — the worker would then process `EndWorker`
              // with the reply still queued behind it and reject the termination (see
              // `EndHandler`). See `WorkflowExecutionManager.requestAdvanceRegionExecutions()`.
              cp.workflowExecutionManager.requestAdvanceRegionExecutions()
            }
          case None => // currently "start" and "end" ports are not part of a region, thus no region can be found.
          // do nothing.
        }
        EmptyReturn()
      }
  }

}
