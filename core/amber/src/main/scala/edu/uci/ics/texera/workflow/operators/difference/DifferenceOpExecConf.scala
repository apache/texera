package edu.uci.ics.texera.workflow.operators.difference

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.operators.OpExecConfig

class DifferenceOpExecConf[K](
    id: OperatorIdentity
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          makeLayer(id, "main"),
          null,
          Constants.currentWorkerNum,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def checkStartDependencies(workflow: Workflow): Unit = {
    val rightLink = inputToOrdinalMapping.find(pair => pair._2 == 1).get._1
    topology.layers.head.initIOperatorExecutor = _ => new DifferenceOpExec(rightLink)
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
