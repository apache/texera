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

package org.apache.texera.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PhysicalLink}
import org.apache.texera.amber.engine.architecture.common.{
  PekkoActorRefMappingService,
  PekkoActorService
}
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorConfig
import org.apache.texera.amber.engine.architecture.coordinator.ExecutionStateUpdate
import org.apache.texera.amber.engine.architecture.coordinator.execution.WorkflowExecution
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

class WorkflowExecutionManager(
    workflowExecution: WorkflowExecution,
    coordinatorConfig: CoordinatorConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {

  var schedule: Schedule = Schedule(Map.empty)

  private val executedRegions: mutable.ListBuffer[Set[Region]] = mutable.ListBuffer()

  private val regionExecutionManagers: mutable.HashMap[RegionIdentity, RegionExecutionManager] =
    mutable.HashMap()
  private val completionNotified: AtomicBoolean = new AtomicBoolean(false)

  @transient var actorRefService: PekkoActorRefMappingService = _

  def setupActorRefService(actorRefService: PekkoActorRefMappingService): Unit = {
    this.actorRefService = actorRefService
  }

  /**
    * Each invocation first syncs the internal statuses of each exisiting `RegionExecutionManager`, after which each
    * of the `RegionExecutionManager`s will launch the corresponding next phase of whenever needed until it is
    * in `Completed` status (phase).
    *
    * After the syncs, if there are no running region(s), it will start new regions (if available).
    */
  def advanceRegionExecutions(actorService: PekkoActorService): Future[Unit] = {
    val unfinishedRegionManagers =
      regionExecutionManagers.values.filter(!_.isCompleted).toSeq

    // Trigger sync for each unfinished region.
    unfinishedRegionManagers.foreach(_.syncStatusAndTransitionRegionExecutionPhase())

    // Wait only for region termination futures (kill path), then re-run the advance loop.
    val terminationFutures = unfinishedRegionManagers.flatMap(_.getTerminationFutureOpt)
    if (terminationFutures.nonEmpty) {
      return Future
        .collect(terminationFutures)
        .unit
        .flatMap(_ => advanceRegionExecutions(actorService))
    }

    if (regionExecutionManagers.values.exists(!_.isCompleted)) {
      // Some regions are still not completed yet. Cannot start the new regions.
      return Future.Unit
    }

    // All existing regions are completed. Start the next region (if any).
    val nextRegions = if (!schedule.hasNext) Set.empty[Region] else schedule.next()
    if (nextRegions.isEmpty) {
      if (workflowExecution.isCompleted && completionNotified.compareAndSet(false, true)) {
        asyncRPCClient.sendToClient(ExecutionStateUpdate(workflowExecution.getState))
      }
      return Future.Unit
    }

    executedRegions.append(nextRegions)
    Future
      .collect(
        nextRegions
          .map(region => {
            val isRestart = workflowExecution.hasRegionExecution(region.id)
            if (isRestart) {
              workflowExecution.restartRegionExecution(region)
            } else {
              workflowExecution.initRegionExecution(region)
            }
            regionExecutionManagers(region.id) = new RegionExecutionManager(
              region,
              isRestart,
              workflowExecution,
              asyncRPCClient,
              coordinatorConfig,
              actorService,
              actorRefService
            )
            regionExecutionManagers(region.id)
          })
          .map(_.syncStatusAndTransitionRegionExecutionPhase())
          .toSeq
      )
      .unit
  }

  def getRegionOfLink(link: PhysicalLink): Region = {
    getExecutingRegions.find(region => region.getLinks.contains(link)).get
  }

  def getRegionOfPortId(portId: GlobalPortIdentity): Option[Region] = {
    getExecutingRegions.find(region => region.getPorts.contains(portId))
  }

  def getExecutingRegions: Set[Region] = {
    executedRegions.flatten
      .filterNot(region => workflowExecution.getRegionExecution(region.id).isCompleted)
      .toSet
  }

  def hasUnfinishedRegionManagers: Boolean = {
    regionExecutionManagers.values.exists(!_.isCompleted)
  }

}
