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
import edu.uci.ics.amber.core.WorkflowRuntimeException
import edu.uci.ics.amber.core.workflow.GlobalPortIdentity
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  FatalError
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  PortCompletedRequest,
  QueryStatisticsRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.util.VirtualIdentityUtils

/** Notify the completion of a port:
  * - For input port, it means the worker has finished consuming and processing all the data
  * through this port, including all possible links to this port.
  * - For output port, it means the worker has finished sending all the data through this port.
  *
  * possible sender: worker
  */
trait PortCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def portCompleted(
      msg: PortCompletedRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    controllerInterface
      .controllerInitiateQueryStatistics(QueryStatisticsRequest(scala.Seq(ctx.sender)), CONTROLLER)
      .map { _ =>
        val globalPortId = GlobalPortIdentity(
          VirtualIdentityUtils.getPhysicalOpId(ctx.sender),
          msg.portId,
          input = msg.input
        )
        cp.workflowExecutionCoordinator.getRegionOfPortId(globalPortId) match {
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
              cp.workflowExecutionCoordinator
                .coordinateRegionExecutors(cp.actorService)
                // Since this message is sent from a worker, any exception from the above code will be returned to that worker.
                // Additionally, a fatal error is sent to the client, indicating that the region cannot be scheduled.
                .onFailure {
                  case err: WorkflowRuntimeException =>
                    sendToClient(FatalError(err, err.relatedWorkerId))
                  case other =>
                    sendToClient(FatalError(other, None))
                }
            }
          case None => // currently "start" and "end" ports are not part of a region, thus no region can be found.
          // do nothing.
        }
        EmptyReturn()
      }
  }

}
