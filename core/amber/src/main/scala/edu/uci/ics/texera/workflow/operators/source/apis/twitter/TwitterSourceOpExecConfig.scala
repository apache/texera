package edu.uci.ics.texera.workflow.operators.source.apis.twitter
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig

class TwitterSourceOpExecConfig(
    operatorIdentifier: OperatorIdentity
) extends OpExecConfig(operatorIdentifier) {

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
