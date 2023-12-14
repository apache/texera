package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{PhysicalOpIdentity, WorkflowIdentity}

case class RegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

// A pipelined region can have a single source. A source is an operator with
// only blocking inputs or no inputs at all.
case class Region(
    id: RegionIdentity,
    operators: List[PhysicalOpIdentity],
    // These are the operators that receive blocking inputs from this region
    blockingDownstreamPhysicalOpIdsInOtherRegions: Array[(PhysicalOpIdentity, Int)] = Array.empty
) {

  def getId: RegionIdentity = id

  def getOperators: List[PhysicalOpIdentity] = operators

  def containsOperator(opId: PhysicalOpIdentity): Boolean = {
    this.operators.contains(opId)
  }
}
