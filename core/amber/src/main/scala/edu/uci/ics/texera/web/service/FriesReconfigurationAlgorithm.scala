package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ChannelMarkerHandler.PropagateChannelMarker
import edu.uci.ics.amber.engine.architecture.scheduling.{Region, WorkflowExecutionCoordinator}
import edu.uci.ics.amber.engine.common.model.{PhysicalOp, PhysicalPlan}
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc
import org.jgrapht.alg.connectivity.ConnectivityInspector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.SetHasAsScala

object FriesReconfigurationAlgorithm {

  private def getOneToManyOperators(region: Region): Set[PhysicalOpIdentity] = {
    region.getOperators.filter(op => op.isOneToManyOp).map(op => op.id)
  }

  def scheduleReconfigurations(
      workflowExecutionCoordinator: WorkflowExecutionCoordinator,
      reconfigurations: List[(PhysicalOp, Option[StateTransferFunc])],
      epochMarkerId: String
  ): Set[PropagateChannelMarker] = {
    // independently schedule reconfigurations for each region:
    workflowExecutionCoordinator.getExecutingRegions
      .flatMap(region => computeMCS(region, reconfigurations, epochMarkerId))
  }

  private def computeMCS(
      region: Region,
      reconfigurations: List[(PhysicalOp, Option[StateTransferFunc])],
      epochMarkerId: String
  ): List[PropagateChannelMarker] = {

    // add all reconfiguration operators to M
    val reconfigOps = reconfigurations.map(reconfigOp => reconfigOp._1.id).toSet
    val M = mutable.Set.empty ++ reconfigOps

    // for each one-to-many operator, add it to M if its downstream has a reconfiguration operator
    val oneToManyOperators = getOneToManyOperators(region)
    oneToManyOperators.foreach(oneToManyOp => {
      val intersection = region.dag.getDescendants(oneToManyOp).asScala.intersect(reconfigOps)
      if (intersection.nonEmpty) {
        M += oneToManyOp
      }
    })

    // compute MCS based on M
    var forwardVertices: Set[PhysicalOpIdentity] = Set()
    var backwardVertices: Set[PhysicalOpIdentity] = Set()

    val topologicalOps = region.topologicalIterator().toList
    val reverseTopologicalOps = topologicalOps.reverse

    topologicalOps.foreach(opId => {
      val op = region.getOperator(opId)
      val parents = op.inputPorts.flatMap(_._2._2).map(_.fromOpId)
      val fromParent: Boolean = parents.exists(p => forwardVertices.contains(p))
      if (M.contains(opId) || fromParent) {
        forwardVertices += opId
      }
    })

    reverseTopologicalOps.foreach(opId => {
      val op = region.getOperator(opId)
      val children = op.outputPorts.flatMap(_._2._2).map(_.toOpId)
      val fromChildren: Boolean = children.exists(p => backwardVertices.contains(p))
      if (M.contains(opId) || fromChildren) {
        backwardVertices += opId
      }
    })

    val resultMCSOpIds = forwardVertices.intersect(backwardVertices)
    val newLinks =
      region.getLinks.filter(link =>
        resultMCSOpIds.contains(link.fromOpId) && resultMCSOpIds.contains(link.toOpId)
      )
    val mcsPlan = PhysicalPlan(resultMCSOpIds.map(opId => region.getOperator(opId)), newLinks)

    // find the MCS components,
    // for each component, send an epoch marker to each of its source operators
    val epochMarkers = new ArrayBuffer[PropagateChannelMarker]()

    val connectedSets = new ConnectivityInspector(mcsPlan.dag).connectedSets()
    connectedSets.forEach(component => {
      val componentSet = component.asScala.toSet
      val componentPlan = mcsPlan.getSubPlan(componentSet)

      // generate the reconfiguration command for this component
//      val reconfigCommand = UpdateMultipleExecutors(
//        reconfigurations
//          .filter(o => component.contains(o._1.id))
//          .map(o => UpdateExecutor(o._1, o._2))
//      )

      // find the source operators of the component
//      val sources = componentSet.intersect(mcsPlan.getSourceOperatorIds)
//      epochMarkers += PropagateChannelMarker(
//        sources,
//        ChannelMarkerIdentity(epochMarkerId),
//        RequireAlignment,
//        componentPlan,
//        reconfigurations.map(_._1.id).toSet,
//        reconfigCommand
//      )
    })

    epochMarkers.toList
  }

}
