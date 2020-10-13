package texera.operators.sink

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.RandomDeployment
import Engine.Architecture.DeploySemantics.DeploymentFilter.ForceLocal
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, ProcessorWorkerLayer}
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{LayerTag, OperatorIdentifier}
import Engine.Operators.OpExecConfig
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class SimpleSinkOpExecConfig(tag: OperatorIdentifier) extends OpExecConfig(tag) {
  override lazy val topology = new Topology(
    Array(
      new ProcessorWorkerLayer(
        LayerTag(tag, "main"),
        _ => new SimpleSinkOpExec(),
        1,
        ForceLocal(),
        RandomDeployment()
      )
    ),
    Array(),
    Map()
  )

  override def assignBreakpoint(
      topology: Array[ActorLayer],
      states: mutable.AnyRefMap[ActorRef, WorkerState.Value],
      breakpoint: GlobalBreakpoint
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_) != WorkerState.Completed))
  }
}
