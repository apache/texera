package edu.uci.ics.texera.workflow.operators.source.cache

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.ForceLocal
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RandomDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class CacheSourceOpExecConfig(tag: OperatorIdentity, src: mutable.MutableList[Tuple])
    extends OpExecConfig(tag) {
  assert(null != src)

  override lazy val topology = new Topology(
    Array(
      new WorkerLayer(
        makeLayer(tag, "main"),
        _ => new CacheSourceOpExec(src),
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
