package edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint
import edu.uci.ics.amber.engine.common.tuple.ITuple

class CountLocalBreakpoint(id:String, version:Long, target:Long) extends LocalBreakpoint(id, version) {

  var localCount = 0

  override def checkCondition(tuple: ITuple): Boolean = {
    localCount +=1
    localCount == target
  }
}
