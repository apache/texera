package Engine.Operators.SimpleCollection

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.DeploySemantics.DeployStrategy.RandomDeployment
import Engine.Architecture.DeploySemantics.DeploymentFilter.FollowPrevious
import Engine.Architecture.DeploySemantics.Layer.{ActorLayer, GeneratorWorkerLayer}
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{AmberTag, LayerTag, OperatorTag}
import Engine.Operators.OperatorMetadata
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext


class SimpleSourceOperatorMetadata(tag:OperatorTag, numWorkers:Int, limit:Int, delay:Int = 0) extends OperatorMetadata(tag) {
  override lazy val topology: Topology = {
    new Topology(Array(new GeneratorWorkerLayer(LayerTag(tag,"main"),
      i =>{
        if(i==0){
          new SimpleTupleProducer(limit/numWorkers+limit%numWorkers,delay)
        }
        else{
          new SimpleTupleProducer(limit/numWorkers,delay)
        }
      },numWorkers,FollowPrevious(),RandomDeployment())),
      Array(),Map())
  }
  override def assignBreakpoint(topology: Array[ActorLayer], states: mutable.AnyRefMap[ActorRef, WorkerState.Value], breakpoint: GlobalBreakpoint)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter): Unit = {
    breakpoint.partition(topology(0).layer.filter(states(_)!= WorkerState.Completed))
  }
}
