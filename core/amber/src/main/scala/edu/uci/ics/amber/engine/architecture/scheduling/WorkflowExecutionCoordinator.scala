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
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
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

  /**
    * Each invocation will launch the next phase of a region execution, if there are any. This can either be launching
    * a new region's execution, or transitioning an exisitng region in an `ExecutingDependeePorts` phase to an
    * `ExecutingNonDependeePorts` phase.
    *
    * A region is only determined to be completed when it finishes the
    * `ExecutingNonDependeePorts` phase.
    */
  def launchNextRegionPhase(actorService: AkkaActorService): Future[Unit] = {
    // First check if any region is in an `ExecutingDependeePorts` phase. This will only be invoked by the completion of
    // a dependee ports.
    if (regionExecutionCoordinators.values.exists(!_.isCompleted)) {
      Future
        .collect({
          regionExecutionCoordinators.values
            .filter(!_.isCompleted)
            // Only when all the dependee ports of this region are completed will this invocation transition its phase.
            // If any dependee port is unfinished, it returns an empty future.
            .map(_.transitionRegionExecutionPhase())
            .toSeq
        })
        .unit
    } else if (workflowExecution.getRunningRegionExecutions.nonEmpty) {
      // If any region has not finished execution yet (i.e., in either of the two phases), skip.
      Future(())
    } else {
      // Current region is completed. Start the next region (if any).
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
                actorService
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
