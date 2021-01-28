package edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.{CountLocalBreakpoint, LocalBreakpoint}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity

import scala.collection.mutable.ArrayBuffer

class CountGlobalBreakpoint(id:String, val target:Long) extends GlobalBreakpoint[CountLocalBreakpoint](id) {

  var current:Long = 0

  override def partition(workers: Array[ActorVirtualIdentity]): Array[(ActorVirtualIdentity, LocalBreakpoint)] = {
    val remaining = target - current
    var currentSum = 0L
    val length = workers.length
    var i = 0
    val ret = ArrayBuffer[(ActorVirtualIdentity, LocalBreakpoint)]()
    if (remaining / length > 0) {
      while (i < length - 1) {
        ret.append((workers(i), new CountLocalBreakpoint(id, version, remaining / length)))
        currentSum += remaining / length
        i += 1
      }
    }else{
      ret.append((workers(i), new CountLocalBreakpoint(id, version, remaining - currentSum)))
    }
    ret.toArray
  }

  override def collect(results: Iterable[CountLocalBreakpoint]): Unit = {
    results.foreach{
      bp => current += bp.localCount
    }
  }

  override def isResolved: Boolean = isTriggered

  override def isTriggered: Boolean = current == target
}
