package edu.uci.ics.amber.core.workflow

import edu.uci.ics.amber.core.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}

case class PhysicalOpOutputPortIdentity(
    physicalOpIdentity: PhysicalOpIdentity,
    outputPortId: PortIdentity
)

case class WorkflowSettings(
    dataTransferBatchSize: Int,
    outputPortsToViewResult: List[PhysicalOpOutputPortIdentity] = List.empty
)
