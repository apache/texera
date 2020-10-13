package texera.common.operators.map

import engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import engine.architecture.deploysemantics.layer.{ActorLayer, ProcessorWorkerLayer}
import engine.architecture.worker.WorkerState
import engine.common.ambertag.{LayerTag, OperatorIdentifier}
import engine.common.Constants
import engine.operators.OpExecConfig
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import texera.common.tuple.TexeraTuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class TexeraMapOpExecConfig(
                             override val tag: OperatorIdentifier,
                             val mapOpExec: () => TexeraMapOpExec
) extends OpExecConfig(tag) {
  override lazy val topology: Topology = {
    new Topology(
      Array(
        new ProcessorWorkerLayer(
          LayerTag(tag, "main"),
          _ => mapOpExec.apply(),
          Constants.defaultNumWorkers,
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
}
