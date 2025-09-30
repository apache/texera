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
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.NO_ALIGNMENT
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  PropagateEmbeddedControlMessageRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{
  RetrieveWorkflowStateResponse,
  StringResponse
}
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RETRIEVE_STATE
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.amber.core.virtualidentity.EmbeddedControlMessageIdentity

import java.time.Instant

trait RetrieveWorkflowStateHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def retrieveWorkflowState(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[RetrieveWorkflowStateResponse] = {
    val targetOps = cp.workflowScheduler.physicalPlan.operators.map(_.id).toSeq
    val msg = PropagateEmbeddedControlMessageRequest(
      cp.workflowExecution.getRunningRegionExecutions
        .flatMap(_.getAllOperatorExecutions.map(_._1))
        .toSeq,
      EmbeddedControlMessageIdentity("RetrieveWorkflowState_" + Instant.now().toString),
      NO_ALIGNMENT,
      targetOps,
      targetOps,
      EmptyRequest(),
      METHOD_RETRIEVE_STATE.getBareMethodName
    )
    controllerInterface
      .propagateEmbeddedControlMessage(
        msg,
        mkContext(SELF)
      )
      .map { ret =>
        RetrieveWorkflowStateResponse(ret.returns.map {
          case (actorId, value) =>
            val finalret = value match {
              case s: StringResponse => s.value
              case other =>
                ""
            }
            (actorId, finalret)
        })
      }
  }

}
