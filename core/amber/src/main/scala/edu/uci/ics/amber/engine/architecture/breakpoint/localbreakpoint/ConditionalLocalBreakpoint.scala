package edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint
import edu.uci.ics.amber.engine.common.tuple.ITuple

class ConditionalLocalBreakpoint(id:String, version:Long, val predicate: ITuple => Boolean) extends LocalBreakpoint(id, version) {
  override def checkCondition(tuple: ITuple): Boolean = predicate(tuple)
}
