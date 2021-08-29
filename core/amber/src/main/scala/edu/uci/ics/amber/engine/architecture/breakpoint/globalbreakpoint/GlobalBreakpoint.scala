package edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.LocalBreakpoint
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

abstract class GlobalBreakpoint[T](val id: String) extends Serializable {
  type localBreakpointType = T

  protected var version: Long = 0

  def hasSameVersion(ver: Long): Boolean = ver == version

  def increaseVersion(): Unit = version += 1

  def partition(
      workers: Array[ActorVirtualIdentity]
  ): Array[(ActorVirtualIdentity, LocalBreakpoint)]

  def collect(results: Iterable[localBreakpointType]): Unit

  def isResolved: Boolean

  def isTriggered: Boolean

}
