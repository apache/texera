package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.scheduling.{ExecutionPlan, PipelinedRegion}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PartitioningPlan, PhysicalPlan}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Workflow(
    val workflowId: WorkflowIdentity,
    val originalLogicalPlan: LogicalPlan,
    val logicalPlan: LogicalPlan,
    val physicalPlan: PhysicalPlan,
    val executionPlan: ExecutionPlan,
    val partitioningPlan: PartitioningPlan
) extends java.io.Serializable {

  def getBlockingOutLinksOfRegion(region: PipelinedRegion): Set[LinkIdentity] = {
    val outLinks = new mutable.HashSet[LinkIdentity]()
    region.blockingDownstreamOperatorsInOtherRegions.foreach {
      case (opId, toPort) =>
        physicalPlan
          .getUpstream(opId)
          .foreach(upstream => {
            if (region.operators.contains(upstream)) {
              outLinks.add(LinkIdentity(upstream, 0, opId, toPort))
            }
          })
    }
    outLinks.toSet
  }

  /**
    * Returns the operators in a region whose all inputs are from operators that are not in this region.
    */
  def getSourcesOfRegion(region: PipelinedRegion): Array[LayerIdentity] = {
    val sources = new ArrayBuffer[LayerIdentity]()
    region.getOperators
      .foreach(opId => {
        val isSource = physicalPlan.getUpstream(opId).forall(up => !region.containsOperator(up))
        if (isSource) {
          sources.append(opId)
        }
      })
    sources.toArray
  }

  def getWorkflowId: WorkflowIdentity = workflowId

  /**
    * Returns the worker layer of the upstream operators that links to the `opId` operator's
    * worker layer.
    */
  def getUpStreamConnectedOpExecConfig(
      opID: LayerIdentity
  ): mutable.HashMap[LayerIdentity, OpExecConfig] = {
    val upstreamOperatorToLayers = new mutable.HashMap[LayerIdentity, OpExecConfig]()
    physicalPlan
      .getUpstream(opID)
      .foreach(uOpID => upstreamOperatorToLayers(uOpID) = physicalPlan.operatorMap(opID))
    upstreamOperatorToLayers
  }

  def getOpExecConfig(workerID: ActorVirtualIdentity): OpExecConfig =
    physicalPlan.operatorMap(VirtualIdentityUtils.getOperator(workerID))

  def getOpExecConfig(opID: LayerIdentity): OpExecConfig = physicalPlan.operatorMap(opID)

}
