package edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint
import edu.uci.ics.amber.engine.common.tuple.ITuple

class ExceptionBreakpoint()(implicit id: String, version: Long)
    extends LocalBreakpoint(id, version) {
  var error: Exception = _
  override def accept(tuple: ITuple): Unit = {
    //empty
  }

  override def isTriggered: Boolean = error != null

  override def isDirty: Boolean = isTriggered

  override def reset(): Unit = {
    super.reset()
    error = null
  }
}
