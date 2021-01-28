package edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint

import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.util.Timeout
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext

abstract class GlobalBreakpoint[T <: LocalBreakpoint](val id: String) extends Serializable {

  type localBreakpointType = T

  protected var version: Long = 0

  def hasSameVersion(ver:Long):Boolean = ver == version

  def increaseVersion():Unit = version += 1

  def partition(workers: Array[ActorVirtualIdentity]): Array[(ActorVirtualIdentity,LocalBreakpoint)]

  def collect(results:Iterable[localBreakpointType]): Unit

  def isResolved:Boolean

  def isTriggered:Boolean

}
