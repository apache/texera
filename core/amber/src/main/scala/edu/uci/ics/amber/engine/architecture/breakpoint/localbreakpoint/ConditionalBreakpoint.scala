package edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint

import edu.uci.ics.amber.engine.common.tuple.Tuple

class ConditionalBreakpoint(val predicate: Tuple => Boolean)(implicit id: String, version: Long)
    extends LocalBreakpoint(id, version) {

  var _isTriggered = false

  override def accept(tuple: Tuple): Unit = {
    _isTriggered = predicate(tuple)
  }

  override def isTriggered: Boolean = _isTriggered

  override def isDirty: Boolean = isTriggered

  override def reset(): Unit = {
    super.reset()
    _isTriggered = false
  }
}
