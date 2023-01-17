package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, WorkflowIdentity}

case class PipelinedRegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

// A pipelined region can have a single source. A source is an operator with
// only blocking inputs or no inputs at all.
class PipelinedRegion(
    id: PipelinedRegionIdentity,
    operators: Array[OperatorIdentity]
) {
  var blockingDowstreamOperatorsInOtherRegions: Array[OperatorIdentity] =
    Array.empty // These are the operators that receive blocking inputs from this region

  def getId(): PipelinedRegionIdentity = id

  def getOperators(): Array[OperatorIdentity] = operators
}
