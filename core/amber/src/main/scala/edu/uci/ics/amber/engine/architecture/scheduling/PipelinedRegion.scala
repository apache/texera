package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, WorkflowIdentity}

case class PipelinedRegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

// A pipelined region can have a single source. A source is an operator with
// only blocking inputs or no inputs at all.
case class PipelinedRegion(
    id: PipelinedRegionIdentity,
    operators: Array[LayerIdentity],
    // These are the operators that receive blocking inputs from this region
    blockingDownstreamOperatorsInOtherRegions: Array[LayerIdentity] = Array.empty
) {

  def getId(): PipelinedRegionIdentity = id

  def getOperators(): Array[LayerIdentity] = operators

  def containsOperator(opId: LayerIdentity): Boolean = {
    this.operators.contains(opId)
  }
}
