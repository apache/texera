package edu.uci.ics.texera.workflow.operators.pythonUDF

import java.util

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{ActorLayer, ProcessorWorkerLayer}
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class PythonUDFMetadata(
    tag: OperatorIdentifier,
    val numWorkers: Int,
    val pythonScriptFile: String,
    val inputColumns: mutable.Buffer[String],
    val outputColumns: mutable.Buffer[String],
    val outerFiles: mutable.Buffer[String],
    val batchSize: Int
) extends OpExecConfig(tag) {
  override lazy val topology: Topology = {
    new Topology(
      Array(
        new ProcessorWorkerLayer(
          LayerTag(tag, "main"),
          _ =>
            new PythonUDFOpExec(
              pythonScriptFile,
              new util.ArrayList[String](inputColumns.asJava),
              new util.ArrayList[String](outputColumns.asJava),
              new util.ArrayList[String](outerFiles.asJava),
              batchSize
            ),
          numWorkers,
          FollowPrevious(),
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

  override def getInputNum(from: OperatorIdentifier): Int = 0
}
