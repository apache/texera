package edu.uci.ics.amber.core.workflow

case class WorkflowSettings(
    dataTransferBatchSize: Int,
    outputPortsToViewResult: Set[GlobalPortIdentity] = Set.empty
)
