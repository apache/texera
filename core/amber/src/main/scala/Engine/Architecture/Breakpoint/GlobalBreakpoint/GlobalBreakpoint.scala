package Engine.Architecture.Breakpoint.GlobalBreakpoint

import Engine.Architecture.Breakpoint.FaultedTuple
import Engine.Architecture.Breakpoint.LocalBreakpoint.LocalBreakpoint
import Engine.Common.AdvancedMessageSending
import Engine.Common.AmberMessage.WorkerMessage.{QueryBreakpoint, RemoveBreakpoint}
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

abstract class GlobalBreakpoint(val id: String) extends Serializable {

  var unReportedWorkers: mutable.HashSet[ActorRef] = new mutable.HashSet[ActorRef]()
  var allWorkers: mutable.HashSet[ActorRef] = new mutable.HashSet[ActorRef]()
  var version: Long = 0

  final def accept(sender: ActorRef, localBreakpoint: LocalBreakpoint): Boolean = {
    if (localBreakpoint.version == version && unReportedWorkers.contains(sender)) {
      unReportedWorkers.remove(sender)
      acceptImpl(sender, localBreakpoint)
      true
    } else {
      false
    }
  }

  def acceptImpl(sender: ActorRef, localBreakpoint: LocalBreakpoint): Unit

  def isTriggered: Boolean

  final def partition(
      layer: Array[ActorRef]
  )(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    version += 1
    val assigned = partitionImpl(layer)(timeout, ec, log, id, version)
    val assignedSet = assigned.toSet
    //remove the local breakpoints from unassigned nodes
    allWorkers
      .filter(!assignedSet.contains(_))
      .foreach(x => AdvancedMessageSending.blockingAskWithRetry(x, RemoveBreakpoint(id), 10))
    unReportedWorkers.clear()
    allWorkers.clear()
    assigned.foreach(allWorkers.add)
    assigned.foreach(unReportedWorkers.add)
  }

  def partitionImpl(layer: Array[ActorRef])(implicit
      timeout: Timeout,
      ec: ExecutionContext,
      log: LoggingAdapter,
      id: String,
      version: Long
  ): Iterable[ActorRef]

  def isRepartitionRequired: Boolean = unReportedWorkers.isEmpty

  def report(map: mutable.HashMap[(ActorRef, FaultedTuple), ArrayBuffer[String]]): Unit

  def isCompleted: Boolean

  def needCollecting: Boolean = unReportedWorkers.nonEmpty

  def collect(): Unit = {
    unReportedWorkers.foreach(x => x ! QueryBreakpoint(id))
  }

  def remove()(implicit timeout: Timeout, ec: ExecutionContext, log: LoggingAdapter): Unit = {
    allWorkers.foreach(x =>
      AdvancedMessageSending.blockingAskWithRetry(x, RemoveBreakpoint(id), 10)
    )
  }

  def reset(): Unit = {
    unReportedWorkers = new mutable.HashSet[ActorRef]()
    allWorkers = new mutable.HashSet[ActorRef]()
    version = 0
  }

}
