package edu.uci.ics.texera.workflow.operators.sink.managed

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.ForceLocal
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RandomDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.IncrementalOutputMode
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.operators.sink.storage.SinkStorageReader

class ProgressiveSinkOpExecConfig(
    tag: OperatorIdentity,
    val operatorSchemaInfo: OperatorSchemaInfo,
    outputMode: IncrementalOutputMode,
    storage: SinkStorageReader
) extends OpExecConfig(tag) {
  override lazy val topology = new Topology(
    Array(
      new WorkerLayer(
        makeLayer(tag, "main"),
        idx =>
          new ProgressiveSinkOpExec(operatorSchemaInfo, outputMode, storage.getStorageWriter()),
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
