package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.util.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LayerIdentity,
  OperatorIdentity,
  WorkflowIdentity
}

case class PipelinedRegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

// A pipelined region can have a single source. A source is an operator with
// only blocking inputs or no inputs at all.
case class PipelinedRegion(
    id: PipelinedRegionIdentity,
    operators: Array[LayerIdentity],
    // These are the operators that receive blocking inputs from this region
    blockingDowstreamOperatorsInOtherRegions: Array[LayerIdentity] = Array.empty
) {
//  var blockingDowstreamOperatorsInOtherRegions: Array[OperatorIdentity] =
//    Array.empty // These are the operators that receive blocking inputs from this region

  def getId(): PipelinedRegionIdentity = id

  def getOperators(): Array[LayerIdentity] = operators

  def containsOperator(opId: LayerIdentity): Boolean = {
    this.operators.contains(opId)
  }
}
