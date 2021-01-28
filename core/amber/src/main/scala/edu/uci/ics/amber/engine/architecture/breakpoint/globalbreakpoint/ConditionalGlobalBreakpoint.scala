package edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.{ConditionalLocalBreakpoint, LocalBreakpoint}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.tuple.ITuple

class ConditionalGlobalBreakpoint(id:String, val predicate: ITuple => Boolean) extends GlobalBreakpoint[ConditionalLocalBreakpoint](id) {

  override def partition(workers: Array[ActorVirtualIdentity]): Array[(ActorVirtualIdentity, LocalBreakpoint)] = {
    workers.map(v => (v, new ConditionalLocalBreakpoint(id, version, predicate)))
  }

  override def collect(results: Iterable[ConditionalLocalBreakpoint]): Unit = {
  }

  override def isResolved: Boolean = false

  override def isTriggered: Boolean = true
}
