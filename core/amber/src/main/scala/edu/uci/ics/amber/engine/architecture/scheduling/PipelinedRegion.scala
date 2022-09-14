package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{LayerIdentity, OperatorIdentity, WorkflowIdentity}

case class PipelinedRegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

class PipelinedRegion(
    id: PipelinedRegionIdentity,
    operators: Array[LayerIdentity]
) {
  var blockingDowstreamOperatorsInOtherRegions: Array[LayerIdentity] =
    Array.empty // These are the operators that receive blocking inputs from this region

  def getId(): PipelinedRegionIdentity = id

  def getOperators(): Array[LayerIdentity] = operators
}
