package edu.uci.ics.texera.workflow.operators.split

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{
  FollowPrevious,
  UseAll
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.util.{makeLayer, toOperatorIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class SplitOpExecConfig(
    id: OperatorIdentity,
    splitOpDesc: SplitOpDesc
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          makeLayer(id, "main"),
          i => new SplitOpExec(i, splitOpDesc, outputToOrdinalMapping),
          Constants.currentWorkerNum,
          FollowPrevious(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }
  var buildTable: LinkIdentity = _

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
