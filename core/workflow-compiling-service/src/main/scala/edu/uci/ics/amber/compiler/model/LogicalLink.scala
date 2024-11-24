package edu.uci.ics.amber.compiler.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import edu.uci.ics.amber.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.workflow.PortIdentity

case class LogicalLink(
    @JsonProperty("fromOpId") fromOpId: OperatorIdentity,
    fromPortId: PortIdentity,
    @JsonProperty("toOpId") toOpId: OperatorIdentity,
    toPortId: PortIdentity
) {
  @JsonCreator
  def this(
      @JsonProperty("fromOpId") fromOpId: String,
      fromPortId: PortIdentity,
      @JsonProperty("toOpId") toOpId: String,
      toPortId: PortIdentity
  ) = {
    this(OperatorIdentity(fromOpId), fromPortId, OperatorIdentity(toOpId), toPortId)
  }
}
