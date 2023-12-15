package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.scheduling.{ExecutionPlan, PipelinedRegion}
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

class Workflow(
    val workflowId: WorkflowIdentity,
    val originalLogicalPlan: LogicalPlan,
    val logicalPlan: LogicalPlan,
    val physicalPlan: PhysicalPlan,
    val executionPlan: ExecutionPlan
) extends java.io.Serializable {

  def getBlockingOutPhysicalLinksOfRegion(region: PipelinedRegion): Set[PhysicalLinkIdentity] = {
    region.blockingDownstreamPhysicalOpIdsInOtherRegions.flatMap {
      case (physicalOpId, toPort) =>
        physicalPlan
          .getUpstreamPhysicalOpIds(physicalOpId)
          .filter(upstreamPhysicalOpId => region.operators.contains(upstreamPhysicalOpId))
          .map(upstreamPhysicalOpId =>
            PhysicalLinkIdentity(upstreamPhysicalOpId, 0, physicalOpId, toPort)
          )
    }.toSet
  }

  /**
    * Returns the operators in a region whose all inputs are from operators that are not in this region.
    */
  def getSourcePhysicalOpsOfRegion(region: PipelinedRegion): Array[PhysicalOpIdentity] = {
    region.getOperators
      .filter(physicalOpId =>
        physicalPlan
          .getUpstreamPhysicalOpIds(physicalOpId)
          .forall(up => !region.containsOperator(up))
      )
  }

}
