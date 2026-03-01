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

package org.apache.texera.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import org.apache.texera.amber.core.virtualidentity.{
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.ALL_ALIGNMENT
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  WorkflowReconfigureRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.common.FriesReconfigurationAlgorithm
import org.apache.texera.amber.engine.common.virtualidentity.util.CONTROLLER
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_UPDATE_EXECUTOR

import scala.collection.mutable

trait ReconfigurationHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

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
    val friesComponents =
      FriesReconfigurationAlgorithm.getReconfigurations(cp.workflowExecutionCoordinator, msg)
    friesComponents.foreach { friesComponent =>
      if (friesComponent.scope.size == 1) {
        val updateExecutorRequest = friesComponent.reconfigurations.head
        val workerIds = cp.workflowExecution
          .getLatestOperatorExecution(updateExecutorRequest.targetOpId)
          .getWorkerIds
        workerIds.foreach { worker =>
          futures.append(workerInterface.updateExecutor(updateExecutorRequest, mkContext(worker)))
        }
      } else {
        val channelScope = cp.workflowExecution.getRunningRegionExecutions
          .flatMap(regionExecution =>
            regionExecution.getAllLinkExecutions
              .map(_._2)
              .flatMap(linkExecution => linkExecution.getAllChannelExecutions.map(_._1))
          )
          .filter(channelId => {
            friesComponent.scope
              .contains(VirtualIdentityUtils.getPhysicalOpId(channelId.fromWorkerId)) &&
              friesComponent.scope
                .contains(VirtualIdentityUtils.getPhysicalOpId(channelId.toWorkerId))
          })
        val controlChannels = friesComponent.sources.flatMap { source =>
          cp.workflowExecution.getLatestOperatorExecution(source).getWorkerIds.flatMap { worker =>
            Seq(
              ChannelIdentity(CONTROLLER, worker, isControl = true),
              ChannelIdentity(worker, CONTROLLER, isControl = true)
            )
          }
        }
        val finalScope = channelScope ++ controlChannels
        val cmdMapping =
          friesComponent.reconfigurations.flatMap { updateReq =>
            val workers =
              cp.workflowExecution.getLatestOperatorExecution(updateReq.targetOpId).getWorkerIds
            workers.map(worker =>
              worker.name -> createInvocation(
                METHOD_UPDATE_EXECUTOR.getBareMethodName,
                updateReq,
                worker
              )
            )
          }.toMap
        futures += cmdMapping.map {
          case (_, (_, singleWorkerUpdateFuture)) => singleWorkerUpdateFuture
        }
        friesComponent.sources.foreach { source =>
          cp.workflowExecution.getLatestOperatorExecution(source).getWorkerIds.foreach { worker =>
            sendECM(
              EmbeddedControlMessageIdentity(msg.reconfigurationId),
              ALL_ALIGNMENT,
              finalScope.toSet,
              cmdMapping.map(x => (x._1, x._2._1)),
              ChannelIdentity(actorId, worker, isControl = true)
            )
          }
        }
      }
    }
    Future.collect(futures.toList).map { _ =>
      EmptyReturn()
    }
  }

}
