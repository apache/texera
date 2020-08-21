package Engine.Operators.GroupBy

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.{RandomDeployment, RoundRobinDeployment}
import Engine.Architecture.DeploySemantics.DeploymentFilter.{FollowPrevious, ForceLocal, UseAll}
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, ProcessorWorkerLayer}
import Engine.Architecture.LinkSemantics.{AllToOne, HashBasedShuffle, LinkStrategy}
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{AmberTag, LayerTag, OperatorTag}
import Engine.Common.AmberTuple.Tuple
import Engine.Common.Constants
import Engine.Operators.OperatorMetadata
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext

class GroupByMetadata[T](
    tag: OperatorTag,
    val numWorkers: Int,
    val groupByField: Int,
    val aggregateField: Int,
    val aggregationType: AggregationType
) extends OperatorMetadata(tag) {
  override lazy val topology: Topology = {
    val partialLayer = new ProcessorWorkerLayer(
      LayerTag(tag, "localGroupBy"),
      _ => new GroupByLocalTupleProcessor[T](groupByField, aggregateField, aggregationType),
      numWorkers,
      UseAll(),
      RoundRobinDeployment()
    )
    val finalLayer = new ProcessorWorkerLayer(
      LayerTag(tag, "globalGroupBy"),
      _ => new GroupByGlobalTupleProcessor[T](aggregationType),
      numWorkers,
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
          x => x.get(0).hashCode()
        )
      ),
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
