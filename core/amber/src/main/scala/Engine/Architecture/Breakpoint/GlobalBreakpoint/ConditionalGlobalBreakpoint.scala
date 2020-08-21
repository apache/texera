package Engine.Architecture.Breakpoint.GlobalBreakpoint

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Breakpoint.LocalBreakpoint.{
  ConditionalBreakpoint,
  CountBreakpoint,
  LocalBreakpoint
}
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.{
  AssignBreakpoint,
  QueryBreakpoint,
  RemoveBreakpoint
}
import Engine.Common.AmberTuple.Tuple
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

class ConditionalGlobalBreakpoint(id: String, val predicate: Tuple => Boolean)
    extends GlobalBreakpoint(id) {

  var localbreakpoints: ArrayBuffer[(ActorRef, LocalBreakpoint)] =
    new ArrayBuffer[(ActorRef, LocalBreakpoint)]()

  override def acceptImpl(sender: ActorRef, localBreakpoint: LocalBreakpoint): Unit = {
    if (localBreakpoint.isTriggered) {
      localbreakpoints.append((sender, localBreakpoint))
    }
  }

  override def isTriggered: Boolean = localbreakpoints.nonEmpty

  override def partitionImpl(layer: Array[ActorRef])(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter,
      id: String,
      version: Long
  ): Iterable[ActorRef] = {
    for (x <- layer) {
      AdvancedMessageSending.blockingAskWithRetry(
        x,
        AssignBreakpoint(new ConditionalBreakpoint(predicate)),
        10
      )
    }
    layer
  }

  override def report(map: mutable.HashMap[(ActorRef, FaultedTuple), ArrayBuffer[String]]): Unit = {
    for (i <- localbreakpoints) {
      val k = (i._1, new FaultedTuple(i._2.triggeredTuple, i._2.triggeredTupleId, false))
      if (map.contains(k)) {
        map(k).append("condition unsatisfied")
      } else {
        map(k) = ArrayBuffer[String]("condition unsatisfied")
      }
    }
    localbreakpoints.clear()
  }

  override def isCompleted: Boolean = false

  override def reset(): Unit = {
    super.reset()
    localbreakpoints = new ArrayBuffer[(ActorRef, LocalBreakpoint)]()
  }

}
