package texera.operators.localscan

import java.io.File

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.RoundRobinDeployment
import Engine.Architecture.DeploySemantics.DeploymentFilter.UseAll
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, GeneratorWorkerLayer}
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{LayerTag, OperatorTag}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import texera.common.operators.source.TexeraSourceOpExecConfig
import texera.common.tuple.schema.Schema

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class LocalCsvFileScanOpExecConfig(
    tag: OperatorTag,
    numWorkers: Int,
    filePath: String,
    delimiter: Char,
    schema: Schema,
    header: Boolean
) extends TexeraSourceOpExecConfig(tag) {
  val totalBytes: Long = new File(filePath).length()

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new GeneratorWorkerLayer(
          LayerTag(tag, "main"),
          i => {
            val endOffset =
              if (i != numWorkers - 1) totalBytes / numWorkers * (i + 1) else totalBytes
            new LocalCsvScanSourceOpExec(
              filePath,
              totalBytes / numWorkers * i,
              endOffset,
              delimiter,
              schema,
              header
            )
          },
          numWorkers,
          UseAll(), // it's source operator
          RoundRobinDeployment()
        )
      ),
      Array(),
      Map()
    )
  }

  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }
}
