package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.workflow.PartitionInfo

case class PortDescription(
    portID: String,
    displayName: String,
    allowMultiInputs: Boolean,
    isDynamicPort: Boolean,
    partitionRequirement: PartitionInfo,
    dependencies: List[Int] = List.empty
)
