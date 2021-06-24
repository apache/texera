package edu.uci.ics.texera.workflow.operators.source.scan.json

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig

import java.io.{BufferedReader, FileReader}
import scala.collection.convert.ImplicitConversions.`iterator asScala`

class JSONLScanSourceOpExecConfig(val tag: OperatorIdentity, val desc: JSONLScanSourceOpDesc)
    extends OpExecConfig(tag) {
  override lazy val topology: Topology = {
    val numWorkers = Constants.defaultNumWorkers
    val reader = new BufferedReader(new FileReader(desc.filePath.get))
    val offset = desc.offset.getOrElse(0).asInstanceOf[Int]
    var lines = reader.lines().iterator().drop(offset)
    desc.limit match {
      case Some(lim) => lines = lines.take(lim)
      case None =>
    }
    val count = lines.map(_ => 1).sum

    reader.close()

    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(tag, "main"),
          i => {
            val startOffset: Int = offset + count / numWorkers * i
            val endOffset = offset + (if (i != numWorkers - 1) count / numWorkers * (i + 1) else count)
            new JSONLScanSourceOpExec(desc, startOffset, endOffset)
          },
          numWorkers,
          UseAll(), // it's source operator
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
