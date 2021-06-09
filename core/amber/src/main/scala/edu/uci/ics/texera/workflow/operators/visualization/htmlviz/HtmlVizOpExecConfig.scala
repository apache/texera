package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig

class HtmlVizOpExecConfig(id: OperatorIdentity, htmlContentAttrName: String)
    extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(id, "main"),
          _ => new HtmlVizOpExec(htmlContentAttrName),
          Constants.defaultNumWorkers,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }

}
