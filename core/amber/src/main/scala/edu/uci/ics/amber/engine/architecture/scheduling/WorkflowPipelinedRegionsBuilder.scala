package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.util.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowPipelinedRegionsBuilder(workflow: Workflow) {

  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = null

  private def getBlockingEdgesRemovedDAG(): DirectedAcyclicGraph[OperatorIdentity, DefaultEdge] = {
    val dag = new DirectedAcyclicGraph[OperatorIdentity, DefaultEdge](classOf[DefaultEdge])
    workflow.getAllOperatorIds.foreach(opId => dag.addVertex(opId))
    workflow.getAllOperatorIds.foreach(opId => {
      val upstreamOps = workflow.getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upOpId => {
        val linkFromUpstreamOp = LinkIdentity(
          workflow.getOperator(upOpId).topology.layers.last.id,
          workflow.getOperator(opId).topology.layers.head.id
        )
        if (!workflow.getOperator(opId).isInputBlocking(linkFromUpstreamOp)) {
          dag.addEdge(upOpId, opId)
        }
      })
    })
    dag
  }

  private def findAllPipelinedRegions(): Unit = {
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )
    var regionCount = 0
    val biconnectivityInspector =
      new BiconnectivityInspector[OperatorIdentity, DefaultEdge](getBlockingEdgesRemovedDAG())
    biconnectivityInspector
      .getConnectedComponents()
      .forEach(component => {
        val regionId = PipelinedRegionIdentity(workflow.getWorkflowId(), regionCount.toString())
        val operatorArray = new ArrayBuffer[OperatorIdentity]()
        component.vertexSet().forEach(opId => operatorArray.append(opId))
        pipelinedRegionsDAG.addVertex(new PipelinedRegion(regionId, operatorArray.toArray))
        regionCount += 1
      })
  }

  private def getPipelinedRegionFromOperatorId(operatorId: OperatorIdentity): PipelinedRegion = {
    pipelinedRegionsDAG
      .vertexSet()
      .forEach(region =>
        if (region.getOperators().contains(operatorId)) {
          return region
        }
      )
    null
  }

  private def findDependenciesBetweenRegions(): Unit = {
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
            prevInOrder.getId() != nextInOrder
              .getId() && !pipelinedRegionsDAG.getDescendants(prevInOrder).contains(nextInOrder)
          ) {
            pipelinedRegionsDAG.addEdge(prevInOrder, nextInOrder)
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
            if (!pipelinedRegionsDAG.getDescendants(prevInOrder).contains(nextInOrder)) {
              pipelinedRegionsDAG.addEdge(prevInOrder, nextInOrder)
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

    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      region.blockingDowstreamOperatorsInOtherRegions = terminalOps.toArray
    }
  }

  def buildPipelinedRegions(): DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = {
    findAllPipelinedRegions()
    findDependenciesBetweenRegions()
    pipelinedRegionsDAG
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
