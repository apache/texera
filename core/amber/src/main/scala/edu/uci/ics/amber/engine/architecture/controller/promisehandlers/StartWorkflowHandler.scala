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
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.StartWorkflowResponse
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.RUNNING

/** start the workflow by starting the source workers
  * note that this SHOULD only be called once per workflow
  *
  * possible sender: client
  */
trait StartWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def startWorkflow(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[StartWorkflowResponse] = {
    if (cp.workflowExecution.getState.isUninitialized) {
      cp.workflowExecutionCoordinator
        .coordinateRegionExecutors(cp.actorService)
        .map(_ => {
          cp.controllerTimerService.enableStatusUpdate()
          StartWorkflowResponse(RUNNING)
        })
    } else {
      StartWorkflowResponse(cp.workflowExecution.getState)
    }
  }

}
