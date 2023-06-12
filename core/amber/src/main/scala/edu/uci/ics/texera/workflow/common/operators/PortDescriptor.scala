package edu.uci.ics.texera.workflow.common.operators

import com.fasterxml.jackson.annotation.JsonProperty
import edu.uci.ics.texera.workflow.common.workflow.PartitionInfo

case class PortDescription(
    portID: String,
    displayName: String,
    allowMultiInputs: Boolean,
    isDynamicPort: Boolean,
    partitionRequirement: PartitionInfo,
    dependencies: List[Int]
)

trait PortDescriptor {
  @JsonProperty(required = false)
  var inputPorts: List[PortDescription] = null

  @JsonProperty(required = false)
  var outputPorts: List[PortDescription] = null
}
