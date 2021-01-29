package edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint

import edu.uci.ics.amber.engine.common.tuple.ITuple

abstract class LocalBreakpoint(val id: String, val version: Long) extends Serializable {

  def checkCondition(tuple: ITuple): Boolean

  var triggeredTuple: ITuple = _

}
