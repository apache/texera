package edu.uci.ics.texera.workflow.common.operators

import com.fasterxml.jackson.annotation.JsonProperty
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.workflow.common.workflow.{PartitionInfo, SinglePartition}

object pdtest {
  def main(args: Array[String]): Unit = {
//    val t = "{\"portID\":\"input-0\",\"displayName\":\"testname\",\"allowMultiInputs\":true,\"isDynamicPort\":false,\"partitionRequirement\":{\"type\":\"single\"}}"
//    val v = Utils.objectMapper.readValue[PortDescription](t, classOf[PortDescription])
//    println(v)
    val v = PortDescription("12", "dis", true, true, SinglePartition())
    println(Utils.objectMapper.writeValueAsString(v))

    val t =
      "{\"portID\":\"12\",\"displayName\":\"dis\",\"allowMultiInputs\":true,\"isDynamicPort\":true,\"partitionRequirement\":{\"type\":\"single\"}}"
    val v2 = Utils.objectMapper.readValue[PortDescription](t, classOf[PortDescription])
    println(Utils.objectMapper.writeValueAsString(v2))
  }
}

case class PortDescription(
    portID: String,
    displayName: String,
    allowMultiInputs: Boolean,
    isDynamicPort: Boolean,
    partitionRequirement: PartitionInfo
)

trait CustomPortOperatorDescriptor extends OperatorDescriptor {

  @JsonProperty(required = false)
  var inputPorts: List[PortDescription] = null

  @JsonProperty(required = false)
  var outputPorts: List[PortDescription] = null
}
