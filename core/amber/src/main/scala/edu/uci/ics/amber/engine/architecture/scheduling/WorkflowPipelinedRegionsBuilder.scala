package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.util.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowPipelinedRegionsBuilder(workflow: Workflow) {

  var idToPipelinedRegions: Map[PipelinedRegionIdentity, PipelinedRegion] = null

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
    val idToPipelinedRegionBuilder = new mutable.HashMap[PipelinedRegionIdentity, PipelinedRegion]()
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
        idToPipelinedRegionBuilder(regionId) =
          new PipelinedRegion(regionId, weakConnComponent.toArray)
        regionCount += 1
      }
    })
    idToPipelinedRegions = idToPipelinedRegionBuilder.toMap
  }

  private def getPipelinedRegionFromOperatorId(operatorId: OperatorIdentity): PipelinedRegion = {
    idToPipelinedRegions.values.find(p => p.getOperators().contains(operatorId)).get
  }

  private def orderRegions(): Unit = {
    val regionDependence =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[PipelinedRegionIdentity]]()
    val regionTerminalOperatorInOtherRegions =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[OperatorIdentity]]()
    val allOperatorIds = workflow.getAllOperatorIds
    allOperatorIds.foreach(opId => {
      // 1. Find dependencies between pipelined regions enforced by inputs of operators.
      // e.g 2 phase hash join requires build input to come first.
      val inputProcessingOrderForOp = workflow.getOperator(opId).getInputProcessingOrder()
      if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
        for (i <- 1 to inputProcessingOrderForOp.length - 1) {
          val prevInOrder = getPipelinedRegionFromOperatorId(
            toOperatorIdentity(inputProcessingOrderForOp(i - 1).from)
          )
          val nextInOrder =
            getPipelinedRegionFromOperatorId(toOperatorIdentity(inputProcessingOrderForOp(i).from))

          if (
            prevInOrder.getId() != nextInOrder.getId() && (!regionDependence.contains(
              nextInOrder
            ) || !regionDependence(nextInOrder).contains(prevInOrder.getId()))
          ) {
            val dependencies = regionDependence.getOrElseUpdate(
              nextInOrder,
              new ArrayBuffer[PipelinedRegionIdentity]()
            )
            dependencies.append(prevInOrder.getId())
            regionDependence(nextInOrder) = dependencies
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
            if (
              !regionDependence.contains(
                nextInOrder
              ) || !regionDependence(nextInOrder).contains(prevInOrder.getId())
            ) {
              val dependencies = regionDependence.getOrElseUpdate(
                nextInOrder,
                new ArrayBuffer[PipelinedRegionIdentity]()
              )
              dependencies.append(prevInOrder.getId())
              regionDependence(nextInOrder) = dependencies
            }

            if (
              !regionTerminalOperatorInOtherRegions.contains(
                prevInOrder
              ) || !regionTerminalOperatorInOtherRegions(prevInOrder).contains(opId)
            ) {

              val terminalOps = regionTerminalOperatorInOtherRegions.getOrElseUpdate(
                prevInOrder,
                new ArrayBuffer[OperatorIdentity]()
              )
              terminalOps.append(opId)
              regionTerminalOperatorInOtherRegions(prevInOrder) = terminalOps
            }

          }
        }
      })
    })

    idToPipelinedRegions.values.foreach(region => {
      region.dependsOn =
        regionDependence.getOrElse(region, new ArrayBuffer[PipelinedRegionIdentity]()).toArray
      region.blockingDowstreamOperatorsInOtherRegions = regionTerminalOperatorInOtherRegions
        .getOrElse(region, new ArrayBuffer[OperatorIdentity]())
        .toArray
    })
  }

  def buildPipelinedRegions(): Map[PipelinedRegionIdentity, PipelinedRegion] = {
    findAllPipelinedRegions()
    orderRegions()
    idToPipelinedRegions
  }

//  def getScheduleableRegions(): ArrayBuffer[PipelinedRegion] = {
//    val nextRegions = new ArrayBuffer[PipelinedRegion]()
//    for (region <- idToPipelinedRegions.values) {
//      if (!region.completed) {
//        if (
//          region.dependsOn.isEmpty || region.dependsOn
//            .forall(p => idToPipelinedRegions(p).completed)
//        ) {
//          nextRegions.append(region)
//        }
//      }
//    }
//    nextRegions
//  }
//
//  def regionCompleted(region: PipelinedRegion): Unit = region.completed = true
}
