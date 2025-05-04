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
    * Each invocation will execute the next batch of Regions that are ready to be executed, if there are any.
    */
  def executeNextRegions(actorService: AkkaActorService): Future[Unit] = {
    if (workflowExecution.getRunningRegionExecutions.nonEmpty) {
      return Future(())
    }
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
              controllerConfig
            )
            regionExecutionCoordinators(region.id)
          })
          .map(_.execute(actorService))
          .toSeq
      })
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

}
