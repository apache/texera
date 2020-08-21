package Engine.Architecture.Breakpoint.GlobalBreakpoint

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Breakpoint.LocalBreakpoint.{ConditionalBreakpoint, ExceptionBreakpoint, LocalBreakpoint}
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.AssignBreakpoint
import Engine.Common.AmberTuple.Tuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class ExceptionGlobalBreakpoint(id:String) extends GlobalBreakpoint(id) {
  var exceptions: ArrayBuffer[(ActorRef,ExceptionBreakpoint)] = new ArrayBuffer[(ActorRef,ExceptionBreakpoint)]()

  override def acceptImpl(sender: ActorRef, localBreakpoint: LocalBreakpoint): Unit = {
    if (localBreakpoint.isTriggered) {
      exceptions.append((sender,localBreakpoint.asInstanceOf[ExceptionBreakpoint]))
    }
  }

  override def isTriggered: Boolean = exceptions.nonEmpty

  override def partitionImpl(layer: Array[ActorRef])(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter, id: String, version: Long): Iterable[ActorRef] = {
    for (x <- layer) {
    AdvancedMessageSending.blockingAskWithRetry(x, AssignBreakpoint(new ExceptionBreakpoint()), 10)
  }
  layer
}

  override def report(map:mutable.HashMap[(ActorRef,FaultedTuple),ArrayBuffer[String]]):Unit = {
    for(i <- exceptions){
      val k = (i._1,new FaultedTuple(i._2.triggeredTuple,i._2.triggeredTupleId,i._2.isInput))
      if(map.contains(k)){
        map(k).append(i._2.error.toString)
      }else{
        map(k) = ArrayBuffer[String](i._2.error.toString)
      }
    }
    exceptions.clear()
  }

  override def isCompleted: Boolean = false

  override def reset(): Unit = {
    super.reset()
    exceptions = new ArrayBuffer[(ActorRef,ExceptionBreakpoint)]()
  }
}
