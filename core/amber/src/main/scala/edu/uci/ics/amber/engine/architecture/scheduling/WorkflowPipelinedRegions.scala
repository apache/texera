package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowPipelinedRegions(workflow: Workflow) {

  val idToPipelinedRegions: mutable.HashMap[PipelinedRegionIdentity, PipelinedRegion] =
    new mutable.HashMap[PipelinedRegionIdentity, PipelinedRegion]()

  createPipelinedRegions()

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

  private def findAllPipelinedRegions(): Unit = {
    val allOperatorIds = workflow.getAllOperatorIds
    val visited = new mutable.HashSet[OperatorIdentity]()
    var regionCount = 0
    allOperatorIds.foreach(opId => {
      if (!visited.contains(opId)) {
        visited.add(opId)
        val weakConnComponent = new ArrayBuffer[OperatorIdentity]()
        weakConnComponent.append(opId)
        findOtherOperatorsInComponent(opId, weakConnComponent, visited)
        val regionId = PipelinedRegionIdentity(workflow.getWorkflowId(), regionCount.toString())
        idToPipelinedRegions(regionId) = new PipelinedRegion(regionId, weakConnComponent)
        regionCount += 1
      }
    })
  }

  private def getPipelinedRegionFromOperatorId(operatorId: OperatorIdentity): PipelinedRegion = {
    idToPipelinedRegions.values.find(p => p.getOperators().contains(operatorId)).get
  }

  private def orderRegions(): Unit = {
    val allOperatorIds = workflow.getAllOperatorIds
    allOperatorIds.foreach(opId => {
      // 1. Find dependencies between pipelined regions enforced by inputs of operators.
      // e.g 2 phase hash join requires build input to come first.
      val inputProcessingOrderForOp = workflow.getOperator(opId).getInputProcessingOrder()
      if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
        for (i <- 1 to inputProcessingOrderForOp.length - 1) {
          val prevInOrder = getPipelinedRegionFromOperatorId(inputProcessingOrderForOp(i - 1))
          val nextInOrder = getPipelinedRegionFromOperatorId(inputProcessingOrderForOp(i))

          if (
            prevInOrder.getId() != nextInOrder.getId() && !nextInOrder.dependsOn
              .contains(prevInOrder.getId())
          ) {
            nextInOrder.dependsOn.append(prevInOrder.getId())
          }
        }
      }

      // 2. Find dependencies due to blocking operators.
      // e.g. The region before and after a sort operator has dependencies
      val upstreamOps = workflow.getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upstreamOp => {
        val linkFromUpstreamOp = LinkIdentity(
          workflow.getOperator(upstreamOp).topology.layers.last.id,
          workflow.getOperator(opId).topology.layers.head.id
        )
        if (workflow.getOperator(opId).isInputBlocking(linkFromUpstreamOp)) {
          val prevInOrder = getPipelinedRegionFromOperatorId(upstreamOp)
          val nextInOrder = getPipelinedRegionFromOperatorId(opId)
          if (prevInOrder.getId() != nextInOrder.getId()) {
            if (!prevInOrder.blockingDowstreamOperatorsInOtherRegions.contains(opId)) {
              prevInOrder.blockingDowstreamOperatorsInOtherRegions.append(opId)
            }
            if (!nextInOrder.dependsOn.contains(prevInOrder.getId())) {
              nextInOrder.dependsOn.append(prevInOrder.getId())
            }
          }
        }
      })
    })
  }

  private def createPipelinedRegions(): Unit = {
    findAllPipelinedRegions()
    orderRegions()
  }

  def getScheduleableRegions(): ArrayBuffer[PipelinedRegion] = {
    val nextRegions = new ArrayBuffer[PipelinedRegion]()
    for (region <- idToPipelinedRegions.values) {
      if (!region.completed) {
        if (
          region.dependsOn.isEmpty || region.dependsOn
            .forall(p => idToPipelinedRegions(p).completed)
        ) {
          nextRegions.append(region)
        }
      }
    }
    nextRegions
  }

  def regionCompleted(region: PipelinedRegion): Unit = region.completed = true
}
