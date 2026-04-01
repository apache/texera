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
import org.apache.texera.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate,
  RuntimeStatisticsPersist
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest,
  StatisticsUpdateTarget
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.apache.texera.amber.engine.common.virtualidentity.util.SELF

/** resume the entire workflow
  *
  * possible sender: controller, client
  */
trait ResumeHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def resumeWorkflow(msg: EmptyRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val resumedWorkerIds =
      cp.workflowExecution.getRunningRegionExecutions
        .flatMap(_.getAllOperatorExecutions.map(_._2))
        .flatMap(_.getWorkerIds)
        .toSeq

    // send all workers resume
    // resume message has no effect on non-paused workers
    Future
      .collect(
        resumedWorkerIds
          .map { workerId =>
            workerInterface.resumeWorker(EmptyRequest(), mkContext(workerId)).map { resp =>
              cp.workflowExecution
                .getLatestOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(workerId))
                .getWorkerExecution(workerId)
                .update(System.nanoTime(), resp.state)
            }
          }
      )
      .flatMap { _ =>
        val statsRefresh =
          if (resumedWorkerIds.nonEmpty) {
            controllerInterface.controllerInitiateQueryStatistics(
              QueryStatisticsRequest(
                resumedWorkerIds,
                StatisticsUpdateTarget.BOTH_UI_AND_PERSISTENCE
              ),
              mkContext(SELF)
            )
          } else {
            // Keep the old behavior for degenerate cases where no worker needs to be resumed.
            val stats = cp.workflowExecution.getAllRegionExecutionsStats
            val edgeStats = cp.workflowExecution.getAllRegionEdgeStatistics
            sendToClient(ExecutionStatsUpdate(stats, edgeStats))
            sendToClient(RuntimeStatisticsPersist(stats))
            Future.value(EmptyReturn())
          }

        statsRefresh.map { _ =>
          cp.controllerTimerService
            .enableStatusUpdate() //re-enabled it since it is disabled in pause
          cp.controllerTimerService
            .enableRuntimeStatisticsCollection() //re-enabled it since it is disabled in pause
          EmptyReturn()
        }
      }
  }

}
