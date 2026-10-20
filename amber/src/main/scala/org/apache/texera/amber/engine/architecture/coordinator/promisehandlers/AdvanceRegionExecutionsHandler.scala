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
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  FatalError
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn

/** Advance the region executions of this workflow.
  *
  * A handler that needs region executions advanced sends this to itself rather than advancing
  * inline (see `PortCompletedHandler`), so that the advance — which terminates completed regions
  * and therefore sends `EndWorker` to their workers — runs in its own control round, after every
  * reply the requesting round owed.
  *
  * possible sender: coordinator (itself)
  */
trait AdvanceRegionExecutionsHandler {
  this: CoordinatorAsyncRPCHandlerInitializer =>

  override def coordinatorInitiateAdvanceRegionExecutions(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    cp.workflowExecutionManager
      .advanceRegionExecutions(cp.actorService)
      // The requester is the coordinator itself and discards this reply, so a failure has no
      // caller to propagate to. A fatal error is sent to the client, indicating that the region
      // cannot be scheduled.
      .onFailure {
        case err: WorkflowRuntimeException =>
          sendToClient(FatalError(err, err.relatedWorkerId))
        case other =>
          sendToClient(FatalError(other, None))
      }
    EmptyReturn()
  }

}
