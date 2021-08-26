package edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint

import edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint.{
  ConditionalLocalBreakpoint,
  LocalBreakpoint
}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class ConditionalGlobalBreakpoint(id: String, val predicate: ITuple => Boolean)
    extends GlobalBreakpoint[ConditionalLocalBreakpoint](id) {

  var triggeredTuples: Array[ITuple] = Array.empty

  override def partition(
      workers: Array[ActorVirtualIdentity]
  ): Array[(ActorVirtualIdentity, LocalBreakpoint)] = {
    assignedWorkers = workers
    workers.map(v => (v, new ConditionalLocalBreakpoint(id, version, predicate)))
  }

  override def collect(results: Iterable[ConditionalLocalBreakpoint]): Unit = {
    triggeredTuples = results.filter(_.triggeredTuple != null).map(_.triggeredTuple).toArray
  }

  override def isResolved: Boolean = false

  override def isTriggered: Boolean = triggeredTuples.nonEmpty
}
