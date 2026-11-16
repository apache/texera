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
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  EmbeddedControlMessageIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  UpdateExecutorCompleted
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.ALL_ALIGNMENT
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  WorkflowReconfigureRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_UPDATE_EXECUTOR
import org.apache.texera.amber.engine.common.FriesReconfigurationAlgorithm

import scala.collection.mutable

trait ReconfigurationHandler {
  this: CoordinatorAsyncRPCHandlerInitializer =>

  override def reconfigureWorkflow(
      msg: WorkflowReconfigureRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    if (
      msg.reconfiguration.exists(req =>
        cp.workflowScheduler.physicalPlan.getOperator(req.targetOpId).isSourceOperator
      )
    ) {
      throw new IllegalStateException(
        "Reconfiguration cannot be applied to source operators"
      )
    }
    val futures = mutable.ArrayBuffer[Future[_]]()
    lazy val physicalOpByWorker = mutable.Map.empty[ActorVirtualIdentity, PhysicalOpIdentity]
    val friesComponents =
      FriesReconfigurationAlgorithm.getReconfigurations(cp.workflowExecutionManager, msg)
    friesComponents.foreach { friesComponent =>
      if (friesComponent.scope.size == 1) {
        val updateExecutorRequest = friesComponent.reconfigurations.head
        val workerIds = cp.workflowExecution
          .getLatestOperatorExecution(updateExecutorRequest.targetOpId)
          .getWorkerIds
        workerIds.foreach { worker =>
          futures.append(
            notifyOnComplete(
              workerInterface.updateExecutor(updateExecutorRequest, mkContext(worker)),
              worker
            )
          )
        }
      } else {
        val workerCommands = friesComponent.reconfigurations.flatMap { updateReq =>
          val workers =
            cp.workflowExecution.getLatestOperatorExecution(updateReq.targetOpId).getWorkerIds
          workers.map { worker =>
            val (invocation, future) =
              createInvocation(METHOD_UPDATE_EXECUTOR.getBareMethodName, updateReq, worker)
            (worker, invocation, future)
          }
        }
        val cmdMapping: Map[String, ControlInvocation] = workerCommands.iterator.map {
          case (worker, invocation, _) => worker.name -> invocation
        }.toMap
        futures ++= workerCommands.iterator.map {
          case (worker, _, future) => notifyOnComplete(future, worker)
        }
        sendEmbeddedControlMessageToSources(
          EmbeddedControlMessageIdentity(msg.reconfigurationId),
          ALL_ALIGNMENT,
          friesComponent.scope,
          friesComponent.sources,
          cmdMapping,
          physicalOpByWorker
        )
      }
    }
    Future.join(futures.toList).map { _ =>
      EmptyReturn()
    }
  }

  // After a worker's updateExecutor completes, notify the client so the
  // ExecutionReconfigurationService can advance completedReconfigurations
  // and emit ModifyLogicCompletedEvent on the websocket.
  private def notifyOnComplete[T](future: Future[T], worker: ActorVirtualIdentity): Future[T] =
    future.onSuccess(_ => sendToClient(UpdateExecutorCompleted(worker)))

}
