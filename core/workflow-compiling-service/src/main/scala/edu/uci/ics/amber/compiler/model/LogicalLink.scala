package edu.uci.ics.amber.compiler.model

import com.fasterxml.jackson.annotation.{JsonCreator, JsonProperty}
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity
import scalax.collection.OneOrMore
import scalax.collection.generic.{AbstractDiEdge, MultiEdge}

case class LogicalLink(
    @JsonProperty("fromOpId") fromOpId: OperatorIdentity,
    fromPortId: PortIdentity,
    @JsonProperty("toOpId") toOpId: OperatorIdentity,
    toPortId: PortIdentity
) extends AbstractDiEdge[OperatorIdentity](fromOpId, toOpId)
    with MultiEdge {
  @JsonCreator
  def this(
      @JsonProperty("fromOpId") fromOpId: String,
      fromPortId: PortIdentity,
      @JsonProperty("toOpId") toOpId: String,
      toPortId: PortIdentity
  ) = {
    this(OperatorIdentity(fromOpId), fromPortId, OperatorIdentity(toOpId), toPortId)
  }

  override def extendKeyBy: OneOrMore[Any] = OneOrMore.one((fromPortId, toPortId))
}
