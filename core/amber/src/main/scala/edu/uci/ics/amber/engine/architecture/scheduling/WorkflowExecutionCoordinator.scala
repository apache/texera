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

package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.{AkkaActorRefMappingService, AkkaActorService}
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.controller.execution.WorkflowExecution
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.core.workflow.{GlobalPortIdentity, PhysicalLink}

import scala.collection.mutable

class WorkflowExecutionCoordinator(
    getNextRegions: () => Set[Region],
    workflowExecution: WorkflowExecution,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {

  private val executedRegions: mutable.ListBuffer[Set[Region]] = mutable.ListBuffer()

  private val regionExecutionCoordinators
      : mutable.HashMap[RegionIdentity, RegionExecutionCoordinator] =
    mutable.HashMap()

  @transient var actorRefService: AkkaActorRefMappingService = _

  def setupActorRefService(actorRefService: AkkaActorRefMappingService): Unit = {
    this.actorRefService = actorRefService
  }

  /**
    * Each invocation will launch the next phase of region execution for the appropriate region(s):
    * If there are no running region(s), it will start the new regions (if available).
    * Otherwise, it will transition each running region to the next phase.
    * Note a region is only determined to be `Completed` (not running) when all the ports are completed in the
    * `ExecutingNonDependeePorts` phase.
    */
  def launchNextRegionPhase(actorService: AkkaActorService): Future[Unit] = {
    // Let all the regionExecutionCoordinators update their internal completed status.
    // This is needed because a regionExecutionCoordinator can only launch an execution phase asynchronously
    // and cannot know it is completed until the next invocation of this method (by the completion of each of
    // its ports.)
    regionExecutionCoordinators.values.filter(!_.isCompleted).foreach(_.syncCompletedStatus())

    if (regionExecutionCoordinators.values.exists(!_.isCompleted)) {
      // Some regions are not completed yet.
      Future
        .collect({
          regionExecutionCoordinators.values
            .filter(!_.isCompleted)
            .map(_.transitionRegionExecutionPhase())
            .toSeq
        })
        .unit
    } else {
      // All existing regions are completed. Start the next region (if any).
      Future
        .collect({
          val nextRegions = getNextRegions()
          executedRegions.append(nextRegions)
          nextRegions
            .map(region => {
              workflowExecution.initRegionExecution(region)
              regionExecutionCoordinators(region.id) = new RegionExecutionCoordinator(
                region,
                workflowExecution,
                asyncRPCClient,
                controllerConfig,
                actorService,
                actorRefService
              )
              regionExecutionCoordinators(region.id)
            })
            .map(_.transitionRegionExecutionPhase())
            .toSeq
        })
        .unit
    }
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

}
