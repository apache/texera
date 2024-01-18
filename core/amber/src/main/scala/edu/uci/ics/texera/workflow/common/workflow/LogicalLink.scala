package edu.uci.ics.texera.workflow.common.workflow

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}

case class LogicalLink(
    @JsonProperty("fromOpId") fromOpId: OperatorIdentity,
    fromPort: OutputPort,
    @JsonProperty("toOpId") toOpId: OperatorIdentity,
    toPort: InputPort
) {
  @JsonCreator
  def this(
      @JsonProperty("fromOpId") fromOpId: String,
      fromPort: OutputPort,
      @JsonProperty("toOpId") toOpId: String,
      toPort: InputPort
  ) = {
    this(OperatorIdentity(fromOpId), fromPort, OperatorIdentity(toOpId), toPort)
  }
}
