package edu.uci.ics.amber.core.workflow

case class WorkflowSettings(
    dataTransferBatchSize: Int,
    outputPortsNeedingStorage: Set[GlobalPortIdentity] = Set.empty
)
