package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, WorkflowIdentity}

import scala.collection.mutable.ArrayBuffer

case class PipelinedRegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

class PipelinedRegion(
    id: PipelinedRegionIdentity,
    operators: ArrayBuffer[OperatorIdentity]
) {
  var dependsOn: ArrayBuffer[PipelinedRegionIdentity] =
    new ArrayBuffer[
      PipelinedRegionIdentity
    ]() // The regions that must complete before this can start
  var completed = false
  var blockingDowstreamOperatorsInOtherRegions: ArrayBuffer[OperatorIdentity] =
    new ArrayBuffer[
      OperatorIdentity
    ] // These are the operators that receive blocking inputs from this region

  def getId(): PipelinedRegionIdentity = id

  def getOperators(): ArrayBuffer[OperatorIdentity] = operators
}
