package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, WorkflowIdentity}

case class PipelinedRegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

class PipelinedRegion(
    id: PipelinedRegionIdentity,
    operators: Array[OperatorIdentity]
) {
  var dependsOn: Array[PipelinedRegionIdentity] =
    null // The regions that must complete before this can start
  var blockingDowstreamOperatorsInOtherRegions: Array[OperatorIdentity] =
    null // These are the operators that receive blocking inputs from this region

  def getId(): PipelinedRegionIdentity = id

  def getOperators(): Array[OperatorIdentity] = operators
}
