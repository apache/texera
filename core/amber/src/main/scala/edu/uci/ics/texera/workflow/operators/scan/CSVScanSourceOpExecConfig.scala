package edu.uci.ics.texera.workflow.operators.scan

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import java.io.File

class CSVScanSourceOpExecConfig(
    tag: OperatorIdentity,
    numWorkers: Int,
    filePath: String,
    delimiter: Char,
    schema: Schema,
    hasHeader: Boolean
) extends OpExecConfig(tag) {
  override lazy val topology: Topology = {
    val totalBytes: Long = new File(filePath).length()
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(tag, "main"),
          i => {
            val endOffset =
              if (i != numWorkers - 1) totalBytes / numWorkers * (i + 1) else totalBytes
            new CSVScanSourceOpExec(
              filePath,
              totalBytes / numWorkers * i,
              endOffset,
              delimiter,
              schema,
              hasHeader
            )
          },
          numWorkers,
          UseAll(), // it's source operator
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }
  val totalBytes: Long = new File(filePath).length()

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
