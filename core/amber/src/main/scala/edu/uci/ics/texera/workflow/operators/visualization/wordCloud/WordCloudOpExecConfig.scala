package edu.uci.ics.texera.workflow.operators.visualization.wordCloud

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.{
  FollowPrevious,
  UseAll
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.linksemantics.HashBasedShuffle
import edu.uci.ics.amber.engine.architecture.worker.WorkerState
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, OperatorIdentifier}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class WordCloudOpExecConfig(
    tag: OperatorIdentifier,
    val numWorkers: Int,
    val textColumn: String
) extends OpExecConfig(tag) {

  override lazy val topology: Topology = {
    val partialLayer = new WorkerLayer(
      LayerTag(tag, "localPieChartProcessor"),
      _ => new WordCloudOpPartialExec(textColumn),
      numWorkers,
      UseAll(),
      RoundRobinDeployment()
    )
    val finalLayer = new WorkerLayer(
      LayerTag(tag, "globalPieChartProcessor"),
      _ => new WordCloudOpFinalExec(),
      1,
      FollowPrevious(),
      RoundRobinDeployment()
    )
    new Topology(
      Array(
        partialLayer,
        finalLayer
      ),
      Array(
        new HashBasedShuffle(
          partialLayer,
          finalLayer,
          Constants.defaultBatchSize,
          x => x.asInstanceOf[Tuple].hashCode()
        )
      ),
      Map()
    )
  }

  override def assignBreakpoint(
      topology: Array[WorkerLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }

}
