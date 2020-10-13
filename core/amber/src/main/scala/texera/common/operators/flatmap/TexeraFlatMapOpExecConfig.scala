package texera.common.operators.flatmap

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.RoundRobinDeployment
import Engine.Architecture.DeploySemantics.DeploymentFilter.FollowPrevious
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, ProcessorWorkerLayer}
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{LayerTag, OperatorIdentifier}
import Engine.Common.Constants
import Engine.Operators.OpExecConfig
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import texera.common.tuple.TexeraTuple

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class TexeraFlatMapOpExecConfig(
                                 override val tag: OperatorIdentifier,
                                 val mapOpExec: () => TexeraFlatMapOpExec
) extends OpExecConfig(tag) {
  override lazy val topology: Topology = {
    new Topology(
      Array(
        new ProcessorWorkerLayer(
          LayerTag(tag, "main"),
          _ =>mapOpExec.apply(),
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
