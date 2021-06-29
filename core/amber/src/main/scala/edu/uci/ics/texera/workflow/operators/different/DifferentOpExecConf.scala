package edu.uci.ics.texera.workflow.operators.different

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig

//TODO: Mostly copied from HashJoinOpExecConf. Not quite understand some (all) of them.
class DifferentOpExecConf[K](
    id: OperatorIdentity
) extends OpExecConfig(id) {

  var rightTable: LinkIdentity = _

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(id, "main"),
          null,
          Constants.defaultNumWorkers,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def checkStartDependencies(workflow: Workflow): Unit = {
    val leftLink = inputToOrdinalMapping.find(pair => pair._2 == 0).get._1
    val rightLink = inputToOrdinalMapping.find(pair => pair._2 == 1).get._1
    rightTable = rightLink
    workflow.getSources(leftLink.from.toOperatorIdentity).foreach { source =>
      workflow.getOperator(source).topology.layers.head.startAfter(rightLink)
    }
    topology.layers.head.metadata = _ => new DifferentOpExec(rightLink)
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
