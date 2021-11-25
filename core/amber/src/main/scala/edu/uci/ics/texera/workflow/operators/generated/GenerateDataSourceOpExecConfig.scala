package edu.uci.ics.texera.workflow.operators.generated

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable.ArrayBuffer

class GenerateDataSourceOpExecConfig(tag: OperatorIdentity,
                                     numWorkers: Int,
                                     schema:Schema,
                                     limit:Int,
                                     keyRange: (Int,Int),
                                     distributions: Array[(Int, Array[(Int,Int)])],
                                     delay: (Int, Int)) extends OpExecConfig(tag) {
  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(tag, "main"),
          i => {
            new GenerateDataSourceOpExec(limit, schema, keyRange, distributions.to[ArrayBuffer], delay)
          },
          numWorkers,
          UseAll(), // it's source operator
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = ???
}
