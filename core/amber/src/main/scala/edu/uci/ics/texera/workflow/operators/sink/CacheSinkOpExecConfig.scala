package edu.uci.ics.texera.workflow.operators.sink

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.ForceLocal
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RandomDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.SinkOpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.collection.mutable

class CacheSinkOpExecConfig(
    tag: OperatorIdentity,
    val operatorSchemaInfo: OperatorSchemaInfo,
    dest: mutable.MutableList[Tuple]
) extends SinkOpExecConfig(tag) {
  override lazy val topology = new Topology(
    Array(
      new WorkerLayer(
        makeLayer(tag, "main"),
        _ => new CacheSinkOpExec(dest),
        1,
        ForceLocal(),
        RandomDeployment()
      )
    ),
    Array()
  )

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
