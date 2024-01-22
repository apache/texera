package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ChannelMarkerHandler.PropagateChannelMarker
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.RegionPlan
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ModifyOperatorLogicHandler.{
  WorkerModifyLogic,
  WorkerModifyLogicMultiple
}
import edu.uci.ics.amber.engine.common.ambermessage.RequireAlignment
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelMarkerIdentity, PhysicalOpIdentity}
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import org.jgrapht.alg.connectivity.ConnectivityInspector

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.asScalaSet

object FriesReconfigurationAlgorithm {

  private def getOneToManyOperators(physicalPlan: PhysicalPlan): Set[PhysicalOpIdentity] = {
    physicalPlan.operators.filter(op => op.isOneToManyOp).map(op => op.id)
  }

  def scheduleReconfigurations(
      physicalPlan: PhysicalPlan,
      regionPlan: RegionPlan,
      reconfigurations: List[(PhysicalOp, Option[StateTransferFunc])],
      epochMarkerId: String
  ): Set[PropagateChannelMarker] = {
    // independently schedule reconfigurations for each region:
    regionPlan.regions
      .map(region => physicalPlan.getSubPlan(region.physicalOpIds))
      .flatMap(regionSubPlan => computeMCS(regionSubPlan, reconfigurations, epochMarkerId))
      .toSet
  }

  private def computeMCS(
      physicalPlan: PhysicalPlan,
      reconfigurations: List[(PhysicalOp, Option[StateTransferFunc])],
      epochMarkerId: String
  ): List[PropagateChannelMarker] = {

    // add all reconfiguration operators to M
    val reconfigOps = reconfigurations.map(reconfigOp => reconfigOp._1.id).toSet
    val M = mutable.Set.empty ++ reconfigOps

    // for each one-to-many operator, add it to M if its downstream has a reconfiguration operator
    val oneToManyOperators = getOneToManyOperators(physicalPlan)
    oneToManyOperators.foreach(oneToManyOp => {
      val intersection =
        physicalPlan.getDescendantPhysicalOpIds(oneToManyOp).toSet.intersect(reconfigOps)
      if (intersection.nonEmpty) {
        M += oneToManyOp
      }
    })

    // compute MCS based on M
    var forwardVertices: Set[PhysicalOpIdentity] = Set()
    var backwardVertices: Set[PhysicalOpIdentity] = Set()

    val topologicalOps = physicalPlan.topologicalIterator().toList
    val reverseTopologicalOps = topologicalOps.reverse

    topologicalOps.foreach(op => {
      val parents = physicalPlan.getUpstreamPhysicalOpIds(op)
      val fromParent: Boolean = parents.exists(p => forwardVertices.contains(p))
      if (M.contains(op) || fromParent) {
        forwardVertices += op
      }
    })

    reverseTopologicalOps.foreach(op => {
      val children = physicalPlan.getDownstreamPhysicalOpIds(op)
      val fromChildren: Boolean = children.exists(p => backwardVertices.contains(p))
      if (M.contains(op) || fromChildren) {
        backwardVertices += op
      }
    })

    val resultMCSOpIds = forwardVertices.intersect(backwardVertices)
    val mcsPlan = physicalPlan.getSubPlan(resultMCSOpIds)

    // find the MCS components,
    // for each component, send an epoch marker to each of its source operators
    val epochMarkers = new ArrayBuffer[PropagateChannelMarker]()

    val connectedSets = new ConnectivityInspector(mcsPlan.dag).connectedSets()
    connectedSets.forEach(component => {
      val componentSet = asScalaSet(component).toSet
      val componentPlan = mcsPlan.getSubPlan(componentSet)

      // generate the reconfiguration command for this component
      val reconfigCommand = WorkerModifyLogicMultiple(
        reconfigurations
          .filter(o => component.contains(o._1.id))
          .map(o => WorkerModifyLogic(o._1, o._2))
      )

      // find the source operators of the component
      val sources = componentSet.intersect(mcsPlan.getSourceOperatorIds)
      epochMarkers += PropagateChannelMarker(
        sources,
        ChannelMarkerIdentity(epochMarkerId),
        RequireAlignment,
        componentPlan,
        reconfigurations.map(_._1.id).toSet,
        reconfigCommand
      )
    })

    epochMarkers.toList
  }

}
