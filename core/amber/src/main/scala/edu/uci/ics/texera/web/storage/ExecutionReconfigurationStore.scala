package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.engine.common.model.PhysicalOp
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

case class ExecutionReconfigurationStore(
    currentReconfigId: Option[String] = None,
    unscheduledReconfigurations: List[(PhysicalOp, Option[StateTransferFunc])] = List(),
    completedReconfigurations: Set[ActorVirtualIdentity] = Set()
)
