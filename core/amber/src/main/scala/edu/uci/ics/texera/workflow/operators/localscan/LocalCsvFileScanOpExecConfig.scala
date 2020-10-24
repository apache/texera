package edu.uci.ics.texera.workflow.operators.localscan

import java.io.File

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{ActorLayer, GeneratorWorkerLayer}
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class LocalCsvFileScanOpExecConfig(
    tag: OperatorIdentifier,
    numWorkers: Int,
    filePath: String,
    delimiter: Char,
    schema: Schema,
    header: Boolean
) extends OpExecConfig(tag) {
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
