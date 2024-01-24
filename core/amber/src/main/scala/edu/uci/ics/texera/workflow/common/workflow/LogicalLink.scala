package edu.uci.ics.texera.workflow.common.workflow

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

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
