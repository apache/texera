package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.operator.StateTransferFunc
import edu.uci.ics.amber.virtualidentity.ActorVirtualIdentity

case class ExecutionReconfigurationStore(
    currentReconfigId: Option[String] = None,
    unscheduledReconfigurations: List[(PhysicalOp, Option[StateTransferFunc])] = List(),
    completedReconfigurations: Set[ActorVirtualIdentity] = Set()
)
