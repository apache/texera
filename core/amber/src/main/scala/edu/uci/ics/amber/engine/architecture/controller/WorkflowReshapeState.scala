package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.Cancellable
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowReshapeState {
  var skewDetectionHandle: Option[Cancellable] = None
  var detectionCallCount = 0
  var previousSkewDetectionCallFinished = true
  var firstPhaseRequestsFinished = true
  var secondPhaseRequestsFinished = true
  var pauseMitigationRequestsFinished = true
  // Let `A -> B` be a workflow of two operators. Every worker of `A` records workload samples
  // for every worker of `B`. In `workloadSamples`, the key in the outer map is the worker of `A`.
  // The value is a map that has the workload samples for every worker of `B` as recorded in the
  // worker of `A`. Example: {A1 -> {B1->[100,200], B2->[300,200]}, A2 -> {B1->[500,300], B2->[700,800]}}
  var workloadSamples =
    new mutable.HashMap[ActorVirtualIdentity, mutable.HashMap[ActorVirtualIdentity, ArrayBuffer[
      Long
    ]]]()
  // contains skewed and helper mappings. A mapping does not mean that state
  // has been transferred. For that we need to check `skewedToStateTransferDone`.
  var skewedToHelperMappingHistory =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // contains skewed worker and whether state has been successfully transferred
  var skewedToStateTransferOrIntimationDone =
    new mutable.HashMap[ActorVirtualIdentity, Boolean]()
  // contains pairs which are in first phase of mitigation
  var skewedAndHelperInFirstPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // contains pairs which are in second phase of mitigation
  var skewedAndHelperInSecondPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
  // During mitigation it may happen that the helper receives too much data. If that happens,
  // we pause the mitigation and revert to the original partitioning logic.
  var skewedAndHelperInPauseMitigationPhase =
    new mutable.HashMap[ActorVirtualIdentity, ActorVirtualIdentity]()
}
