package edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.{
  CountLocalBreakpoint,
  LocalBreakpoint
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

class CountGlobalBreakpoint(id: String, val target: Long)
    extends GlobalBreakpoint[CountLocalBreakpoint](id) {

  var current: Long = 0

  override def partition(
      workers: Array[ActorVirtualIdentity]
  ): Array[(ActorVirtualIdentity, LocalBreakpoint)] = {
    val remaining = target - current
    var currentSum = 0L
    val workerNum = workers.length
    var i = 0
    val assigned = ArrayBuffer[(ActorVirtualIdentity, LocalBreakpoint)]()
    breakable {
      while (i < workerNum) {
        if (remaining / workerNum > 0) {
          assigned.append(
            (workers(i), new CountLocalBreakpoint(id, version, remaining / workerNum))
          )
          currentSum += remaining / workerNum
        } else {
          assigned.append(
            (workers(i), new CountLocalBreakpoint(id, version, remaining - currentSum))
          )
          break()
        }
        i += 1
      }
    }
    assigned.toArray
  }

  override def collect(results: Iterable[CountLocalBreakpoint]): Unit = {
    results.foreach { bp =>
      current += bp.localCount
    }
  }

  override def isResolved: Boolean = isTriggered

  override def isTriggered: Boolean = current == target
}
