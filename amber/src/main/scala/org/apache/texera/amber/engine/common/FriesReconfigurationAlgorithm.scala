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

package org.apache.texera.amber.engine.common

import org.apache.texera.amber.core.virtualidentity.PhysicalOpIdentity
import org.apache.texera.amber.core.workflow.PhysicalPlan
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  UpdateExecutorRequest,
  WorkflowReconfigureRequest
}
import org.apache.texera.amber.engine.architecture.scheduling.{Region, WorkflowExecutionManager}
import org.jgrapht.alg.connectivity.ConnectivityInspector

import scala.collection.mutable
import scala.jdk.CollectionConverters.{ListHasAsScala, SetHasAsScala}

object FriesReconfigurationAlgorithm {

  case class FriesComponent(
      sources: Set[PhysicalOpIdentity],
      scope: Set[PhysicalOpIdentity],
      reconfigurations: Set[UpdateExecutorRequest]
  )

  def getReconfigurations(
      workflowExecutionManager: WorkflowExecutionManager,
      reconfiguration: WorkflowReconfigureRequest
  ): Set[FriesComponent] = {
    // independently schedule reconfigurations for each region:
    workflowExecutionManager.getExecutingRegions
      .flatMap(region => computeMCS(region, reconfiguration.reconfiguration))
  }

  private def computeMCS(
      region: Region,
      reconfigurations: Seq[UpdateExecutorRequest]
  ): List[FriesComponent] = {

    // add all reconfiguration operators to M
    val reconfigOps = reconfigurations.iterator.map(_.targetOpId).toSet
    val M = mutable.Set.empty ++ reconfigOps

    // for each one-to-many operator, add it to M if its downstream has a reconfiguration operator
    region.getOperators.foreach { op =>
      if (op.isOneToManyOp) {
        val descendants = region.dag.getDescendants(op.id)
        if (reconfigOps.exists(descendants.contains)) {
          M += op.id
        }
      }
    }

    // compute MCS based on M
    var forwardVertices = Set.empty[PhysicalOpIdentity]
    var backwardVertices = Set.empty[PhysicalOpIdentity]

    val topologicalOps = region.topologicalIterator().toList

    topologicalOps.foreach { opId =>
      val op = region.getOperator(opId)
      val fromParent = op.inputPorts.valuesIterator
        .flatMap(_._2)
        .exists(link => forwardVertices.contains(link.fromOpId))
      if (M.contains(opId) || fromParent) {
        forwardVertices += opId
      }
    }

    topologicalOps.reverseIterator.foreach { opId =>
      val op = region.getOperator(opId)
      val fromChild = op.outputPorts.valuesIterator
        .flatMap(_._2)
        .exists(link => backwardVertices.contains(link.toOpId))
      if (M.contains(opId) || fromChild) {
        backwardVertices += opId
      }
    }

    val resultMCSOpIds = forwardVertices.intersect(backwardVertices)
    val newLinks =
      region.getLinks.filter(link =>
        resultMCSOpIds.contains(link.fromOpId) && resultMCSOpIds.contains(link.toOpId)
      )
    val mcsPlan = PhysicalPlan(resultMCSOpIds.map(region.getOperator), newLinks)

    val connectedSets = new ConnectivityInspector(mcsPlan.dag).connectedSets()
    val mcsSources = mcsPlan.getSourceOperatorIds
    connectedSets.asScala.toList.map { component =>
      val operators = component.asScala.toSet
      FriesComponent(
        operators.intersect(mcsSources),
        operators,
        reconfigurations.iterator.filter(req => operators.contains(req.targetOpId)).toSet
      )
    }
  }

}
