package Engine.Operators

import Engine.Architecture.Breakpoint.GlobalBreakpoint.GlobalBreakpoint
import Engine.Architecture.Controller.Workflow
import Engine.Architecture.DeploySemantics.Layer.ActorLayer
import Engine.Architecture.LinkSemantics.LinkStrategy
import Engine.Architecture.Worker.WorkerState
import Engine.Common.AmberTag.{AmberTag, LayerTag, OperatorTag}
import Engine.Common.AmberTuple.Tuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.concurrent.ExecutionContext

abstract class OperatorMetadata(val tag: OperatorTag) extends Serializable {

  class Topology(var layers:Array[ActorLayer],var links: Array[LinkStrategy],var dependencies:Map[LayerTag,Set[LayerTag]]) extends Serializable {
    assert(!dependencies.exists(x => x._2.contains(x._1)))
  }

  lazy val topology:Topology = null

  def runtimeCheck(workflow:Workflow): Option[mutable.HashMap[AmberTag, mutable.HashMap[AmberTag,mutable.HashSet[LayerTag]]]] = {
    //do nothing by default
    None
  }

  def requiredShuffle:Boolean = false

  def getShuffleHashFunction(layerTag: LayerTag):Tuple => Int = ???

  def assignBreakpoint(topology:Array[ActorLayer], states:mutable.AnyRefMap[ActorRef,WorkerState.Value], breakpoint:GlobalBreakpoint)(implicit timeout:Timeout, ec:ExecutionContext, log:LoggingAdapter)

}
