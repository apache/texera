package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.virtualidentity.util.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.workflow.OperatorLink
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.DirectedAcyclicGraph

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class PipelinedRegionDAGLink(
    originRegionId: PipelinedRegionIdentity,
    destinationRegion: PipelinedRegionIdentity
)

class WorkflowPipelinedRegions(workflow: Workflow) {
  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, PipelinedRegionDAGLink] = null

  createPipelinedRegions()

  private def getBlockingEdgesRemovedDAG(): DirectedAcyclicGraph[OperatorIdentity, LinkIdentity] = {
    val dag = new DirectedAcyclicGraph[OperatorIdentity, LinkIdentity](classOf[LinkIdentity])
    workflow.getAllOperatorIds.foreach(opId => dag.addVertex(opId))
    workflow.getAllOperatorIds.foreach(opId => {
      val upstreamOps = workflow.getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upOpId => {
        val linkFromUpstreamOp = LinkIdentity(
          workflow.getOperator(upOpId).topology.layers.last.id,
          workflow.getOperator(opId).topology.layers.head.id
        )
        if (!workflow.getOperator(opId).isInputBlocking(linkFromUpstreamOp)) {
          dag.addEdge(upOpId, opId, linkFromUpstreamOp)
        }
      })
    })
    dag
  }

  private def findAllPipelinedRegions(): Unit = {
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, PipelinedRegionDAGLink](
      classOf[PipelinedRegionDAGLink]
    )
    var regionCount = 0
    val biconnectivityInspector =
      new BiconnectivityInspector[OperatorIdentity, LinkIdentity](getBlockingEdgesRemovedDAG())
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

  private def orderRegions(): Unit = {
    val regionDependence =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[PipelinedRegion]]()
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
            ) || !regionDependence(nextInOrder).contains(prevInOrder))
          ) {
            val dependencies = regionDependence.getOrElseUpdate(
              nextInOrder,
              new ArrayBuffer[PipelinedRegion]()
            )
            dependencies.append(prevInOrder)
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
              ) || !regionDependence(nextInOrder).contains(prevInOrder)
            ) {
              val dependencies = regionDependence.getOrElseUpdate(
                nextInOrder,
                new ArrayBuffer[PipelinedRegion]()
              )
              dependencies.append(prevInOrder)
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

    for ((region, dependence) <- regionDependence) {
      dependence.foreach(upstreamRegion =>
        pipelinedRegionsDAG.addEdge(
          upstreamRegion,
          region,
          PipelinedRegionDAGLink(upstreamRegion.getId(), region.getId())
        )
      )
    }
    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      region.blockingDowstreamOperatorsInOtherRegions = terminalOps.toArray
    }
  }

  private def createPipelinedRegions(): Unit = {
    findAllPipelinedRegions()
    orderRegions()
  }
}
