package Engine.Operators.Common.FlatMap

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.RoundRobinDeployment
import Engine.Architecture.DeploySemantics.DeploymentFilter.FollowPrevious
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, ProcessorWorkerLayer}
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{LayerTag, OperatorTag}
import Engine.Common.tuple.Tuple
import Engine.Operators.OperatorMetadata
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class FlatMapMetadata(
    override val tag: OperatorTag,
    val numWorkers: Int,
    val flatMapFunc: Tuple => TraversableOnce[Tuple]
) extends OperatorMetadata(tag) {
  override lazy val topology: Topology = {
    new Topology(
      Array(
        new ProcessorWorkerLayer(
          LayerTag(tag, "main"),
          _ =>
            new FlatMapTupleProcessor(
              flatMapFunc.asInstanceOf[(Tuple => TraversableOnce[Tuple]) with java.io.Serializable]
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
}
