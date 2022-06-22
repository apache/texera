package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class PipelinedRegions(workflow: Workflow) {

  val allPipelinedRegions: ArrayBuffer[ArrayBuffer[OperatorIdentity]] =
    new ArrayBuffer[ArrayBuffer[OperatorIdentity]]()

  /**
    * Operators are reachable if they are connected directly by an upstream or downstream edge
    * and the edge (LinkIdentity) is not blocking.
    */
  private def getAdjacentReachableOperators(
      operatorId: OperatorIdentity
  ): ArrayBuffer[OperatorIdentity] = {
    val adjacentOperators = new ArrayBuffer[OperatorIdentity]()
    val upstreamOps = workflow.getDirectUpstreamOperators(operatorId)
    val downstreamOps = workflow.getDirectDownStreamOperators(operatorId)
    upstreamOps.foreach(upstreamOp => {
      val linkFromUpstreamOp = LinkIdentity(
        workflow.getOperator(upstreamOp).topology.layers.last.id,
        workflow.getOperator(operatorId).topology.layers.head.id
      )
      if (!workflow.getOperator(operatorId).isInputBlocking(linkFromUpstreamOp)) {
        // Non-blocking edge means that the upstream operator is in this region
        adjacentOperators.append(upstreamOp)
      }
    })
    downstreamOps.foreach(downstreamOp => {
      val linkToDownstreamOp = LinkIdentity(
        workflow.getOperator(operatorId).topology.layers.last.id,
        workflow.getOperator(downstreamOp).topology.layers.head.id
      )
      if (!workflow.getOperator(downstreamOp).isInputBlocking(linkToDownstreamOp)) {
        // Non-blocking edge means that the upstream operator is in this region
        adjacentOperators.append(downstreamOp)
      }
    })
    adjacentOperators
  }

  private def findOtherOperatorsInComponent(
      operatorId: OperatorIdentity,
      component: ArrayBuffer[OperatorIdentity],
      visited: mutable.HashSet[OperatorIdentity]
  ): Unit = {
    getAdjacentReachableOperators(operatorId).foreach(adjOp => {
      if (!visited.contains(adjOp)) {
        visited.add(adjOp)
        component.append(adjOp)
        findOtherOperatorsInComponent(adjOp, component, visited)
      }
    })
  }

  def findAllPipelinedRegions: Unit = {
    val allOperatorIds = workflow.getAllOperatorIds
    val visited = new mutable.HashSet[OperatorIdentity]()
    allOperatorIds.foreach(opId => {
      if (!visited.contains(opId)) {
        visited.add(opId)
        val weakConnComponent = new ArrayBuffer[OperatorIdentity]()
        weakConnComponent.append(opId)
        findOtherOperatorsInComponent(opId, weakConnComponent, visited)
        allPipelinedRegions.append(weakConnComponent)
      }
    })
  }
}
