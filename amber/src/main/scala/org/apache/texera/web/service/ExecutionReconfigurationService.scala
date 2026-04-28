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

package org.apache.texera.web.service

import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.controller.{UpdateExecutorCompleted, Workflow}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  UpdateExecutorRequest,
  WorkflowReconfigureRequest
}
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.web.SubscriptionManager
import org.apache.texera.web.model.websocket.event.TexeraWebSocketEvent
import org.apache.texera.web.model.websocket.request.ModifyLogicRequest
import org.apache.texera.web.model.websocket.response.{
  ModifyLogicCompletedEvent,
  ModifyLogicResponse
}
import org.apache.texera.web.storage.{ExecutionReconfigurationStore, ExecutionStateStore}

import java.util.UUID
import scala.util.{Failure, Success}

class ExecutionReconfigurationService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    workflow: Workflow
) extends SubscriptionManager {

  // monitors notification from the engine that a reconfiguration on a worker is completed
  registerWorkerCompletionCallback()

  // monitors the reconfiguration state (completed workers) change,
  // notifies the frontend when all workers of an operator complete reconfiguration
  registerCompletionDiffHandler()

  // handles reconfigure workflow logic from frontend
  // validate the modify logic request and notifies the frontend
  // reconfigurations can only come when the workflow is paused,
  // they are not actually performed until the workflow is resumed
  def modifyOperatorLogic(modifyLogicRequest: ModifyLogicRequest): TexeraWebSocketEvent = {
    val newOp = modifyLogicRequest.operator
    val opId = newOp.operatorIdentifier
    val currentOp = workflow.logicalPlan.getOperator(opId)
    val reconfiguredPhysicalOp =
      currentOp.runtimeReconfiguration(
        workflow.context.workflowId,
        workflow.context.executionId,
        currentOp,
        newOp
      )
    reconfiguredPhysicalOp match {
      case Failure(exception) => ModifyLogicResponse(opId.id, isValid = false, exception.getMessage)
      case Success(op) => {
        stateStore.reconfigurationStore.updateState(old =>
          old.copy(unscheduledReconfigurations = old.unscheduledReconfigurations :+ op)
        )
        ModifyLogicResponse(opId.id, isValid = true, "")
      }
    }
  }

  // actually performs all reconfiguration requests the user made during pause
  // sends ModifyLogic messages to operators and workers,
  // see the Fries reconfiguration paper for the algorithm.
  // Note: StateTransferFunc is currently not threaded through to the engine —
  // the new UpdateExecutorRequest only carries (targetOpId, newOpExecInitInfo).
  def performReconfigurationOnResume(): Unit = {
    val reconfigurations = stateStore.reconfigurationStore.getState.unscheduledReconfigurations
    if (reconfigurations.isEmpty) {
      return
    }

    val reconfigurationId = UUID.randomUUID().toString
    val updateExecutorRequests = reconfigurations.map {
      case (op, _) => UpdateExecutorRequest(op.id, op.opExecInitInfo)
    }
    dispatch(
      WorkflowReconfigureRequest(
        reconfiguration = updateExecutorRequests,
        reconfigurationId = reconfigurationId
      )
    )

    // clear all un-scheduled reconfigurations, start a new reconfiguration ID
    stateStore.reconfigurationStore.updateState(_ =>
      ExecutionReconfigurationStore(currentReconfigId = Some(reconfigurationId))
    )
  }

  // Seam for unit testing the dispatch path without spinning up an AmberClient.
  protected def dispatch(request: WorkflowReconfigureRequest): Unit = {
    client.controllerInterface.reconfigureWorkflow(request, ())
  }

  // Seam for unit testing — production wires the engine's UpdateExecutorCompleted
  // events into the reconfiguration store so the diff handler above can fire
  // ModifyLogicCompletedEvent for the frontend.
  protected def registerWorkerCompletionCallback(): Unit = {
    client.registerCallback[UpdateExecutorCompleted]((evt: UpdateExecutorCompleted) => {
      onWorkerReconfigured(evt.id)
    })
  }

  // Exposed (instead of inlined in the callback) so tests can drive the
  // completion path directly.
  private[service] def onWorkerReconfigured(worker: ActorVirtualIdentity): Unit = {
    stateStore.reconfigurationStore.updateState(old =>
      old.copy(completedReconfigurations = old.completedReconfigurations + worker)
    )
  }

  // Seam for unit testing — the diff handler dereferences workflow.physicalPlan
  // to map worker → logical op, which makes constructing a service in tests
  // require a full Workflow. Tests override to no-op.
  protected def registerCompletionDiffHandler(): Unit = {
    addSubscription(
      stateStore.reconfigurationStore.registerDiffHandler((oldState, newState) => {
        if (
          oldState.completedReconfigurations != newState.completedReconfigurations
          && oldState.currentReconfigId == newState.currentReconfigId
        ) {
          val diff = newState.completedReconfigurations -- oldState.completedReconfigurations
          val newlyCompletedOps = diff
            .map(workerId => workflow.physicalPlan.getPhysicalOpByWorkerId(workerId).id)
            .map(opId => opId.logicalOpId.id)
          if (newlyCompletedOps.nonEmpty) {
            List(ModifyLogicCompletedEvent(newlyCompletedOps.toList))
          } else {
            List()
          }
        } else {
          List()
        }
      })
    )
  }
}
