package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.scheduling.{ExecutionPlan, Region}
import edu.uci.ics.amber.engine.common.virtualidentity._
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}

class Workflow(
    val workflowId: WorkflowIdentity,
    val originalLogicalPlan: LogicalPlan,
    val logicalPlan: LogicalPlan,
    val physicalPlan: PhysicalPlan,
    val executionPlan: ExecutionPlan
) extends java.io.Serializable {

  /**
    * Returns the operators in a region whose all inputs are from operators that are not in this region.
    */
  def getSourcePhysicalOpsOfRegion(region: Region): List[PhysicalOpIdentity] = {
    region.physicalOpIds
      .filter(physicalOpId =>
        physicalPlan
          .getUpstreamPhysicalOpIds(physicalOpId)
          .forall(upstreamOpId => !region.physicalOpIds.contains(upstreamOpId))
      )
  }

}
