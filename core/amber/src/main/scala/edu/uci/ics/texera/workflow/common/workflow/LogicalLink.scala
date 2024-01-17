package edu.uci.ics.texera.workflow.common.workflow

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity

case class LogicalLink(@JsonProperty("fromOpId") fromOpId: OperatorIdentity, fromPort: NewOutputPort, @JsonProperty("toOpId") toOpId: OperatorIdentity, toPort: NewInputPort) {
  @JsonCreator
  def this(@JsonProperty("fromOpId") fromOpId: String, fromPort: NewOutputPort, @JsonProperty("toOpId") toOpId: String, toPort: NewInputPort) = {
    this(OperatorIdentity(fromOpId), fromPort, OperatorIdentity(toOpId), toPort)
  }
}
