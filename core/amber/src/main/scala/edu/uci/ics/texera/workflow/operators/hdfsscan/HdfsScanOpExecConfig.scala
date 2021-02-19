package edu.uci.ics.texera.workflow.operators.hdfsscan

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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.File
import java.net.URI
import java.util
import scala.collection.mutable.ArrayBuffer

class HdfsScanOpExecConfig(
    tag: OperatorIdentity,
    numWorkers: Int,
    host: String,
    filePath: String,
    delimiter: Char,
    indicesToKeep: util.ArrayList[Object],
    schema: Schema,
    header: Boolean
) extends OpExecConfig(tag) {
  val totalBytes: Long =
    FileSystem.get(new URI(host), new Configuration()).getFileStatus(new Path(filePath)).getLen

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(tag, "main"),
          i => {
            val endOffset =
              if (i != numWorkers - 1) totalBytes / numWorkers * (i + 1) else totalBytes
            new HdfsScanSourceOpExec(
              host,
              filePath,
              totalBytes / numWorkers * i,
              endOffset,
              delimiter,
              schema,
              indicesToKeep,
              header
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
  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }

}
