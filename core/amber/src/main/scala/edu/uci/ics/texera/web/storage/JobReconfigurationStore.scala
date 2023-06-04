package edu.uci.ics.texera.web.storage

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

case class JobReconfigurationStore(
    currentReconfigId: Option[String] = None,
    unscheduledReconfigs: List[(OpExecConfig, Option[StateTransferFunc])] = List(),
    completedReconfigs: Set[ActorVirtualIdentity] = Set()
)
