package Engine.Operators.KeywordSearch

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.{RandomDeployment, RoundRobinDeployment}
import Engine.Architecture.DeploySemantics.DeploymentFilter.{FollowPrevious, UseAll}
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, ProcessorWorkerLayer}
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{AmberTag, LayerTag, OperatorTag}
import Engine.Operators.OperatorMetadata
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext


class KeywordSearchMetadata(tag:OperatorTag, val numWorkers:Int, val targetField:Int, val keyword:String) extends OperatorMetadata(tag){
  override lazy val topology: Topology = {
    new Topology(Array(
      new ProcessorWorkerLayer(LayerTag(tag,"main"),_ => new KeywordSearchTupleProcessor(targetField,keyword),
        numWorkers,
        FollowPrevious(),
        RoundRobinDeployment())
    ),Array(),Map())
  }
  override def assignBreakpoint(topology: Array[ActorLayer], states: mutable.AnyRefMap[ActorRef, WorkerState.Value], breakpoint: GlobalBreakpoint)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_)!= WorkerState.Completed))
  }
}
