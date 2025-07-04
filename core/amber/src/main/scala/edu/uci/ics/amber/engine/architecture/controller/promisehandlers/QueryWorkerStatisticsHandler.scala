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

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  QueryStatisticsRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.tableprofile.TableProfile
import edu.uci.ics.amber.util.VirtualIdentityUtils

trait QueryWorkerStatisticsHandler { this: ControllerAsyncRPCHandlerInitializer =>

  override def controllerInitiateQueryStatistics(
      msg: QueryStatisticsRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {

    // 1. decide whom to contact
    val workers: Iterable[ActorVirtualIdentity] =
      if (msg.filterByWorkers.nonEmpty) msg.filterByWorkers
      else
        cp.workflowExecution.getAllRegionExecutions
          .flatMap(_.getAllOperatorExecutions.map(_._2))
          .flatMap(_.getWorkerIds)

    // 2. fire queries in parallel
    val requests: Seq[Future[Unit]] = workers.map { wid =>
      val exec = cp.workflowExecution
        .getLatestOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(wid))
        .getWorkerExecution(wid)

      // query metrics first
      workerInterface.queryStatistics(EmptyRequest(), wid).flatMap { stat =>
        // update state & basic stats immediately
        exec.setState(stat.metrics.workerState)
        exec.setStats(stat.metrics.workerStatistics)

        if (stat.metrics.workerState == WorkerState.COMPLETED) {
          // worker finished – fetch its profile too
          workerInterface
            .queryTableProfile(EmptyRequest(), wid)
            .map { prof =>
              exec.setTableProfile(prof.tableProfiles)
            }
        } else {
          // not completed – record an empty profile
          exec.setTableProfile(new TableProfile(None, Seq.empty))
          Future.Unit
        }
      }
    }.toSeq

    // 3. when all workers replied, push aggregated view to UI
    Future.collect(requests).map { _ =>
      sendToClient(
        ExecutionStatsUpdate(
          cp.workflowExecution.getAllRegionExecutionsStats,
          cp.workflowExecution.getAllRegionExecutionTableProfiles
        )
      )
      .map { _ =>
        EmptyReturn()
      }
  }
}
