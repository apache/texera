package edu.uci.ics.texera.workflow.operators.intervalJoin

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.ManyToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class IntervalJoinOpExecConfig(
    id: OperatorIdentity,
    val operatorSchemaInfo: OperatorSchemaInfo,
    val leftAttributeName: String,
    val rightAttributeName: String,
    val constant: Long,
    val includeLeftBound: Boolean,
    val includeRightBound: Boolean,
    val timeIntervalType: Option[TimeIntervalType]
) extends OpExecConfig(id) {

  var leftInpueLink: LinkIdentity = _
  var rightInputLink: LinkIdentity = _

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(id, "main"),
          null,
          1,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }

}
