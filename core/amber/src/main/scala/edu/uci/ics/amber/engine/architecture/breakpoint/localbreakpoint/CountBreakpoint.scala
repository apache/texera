package edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint

import edu.uci.ics.amber.engine.common.tuple.Tuple

class CountBreakpoint(val target: Long)(implicit id: String, version: Long)
    extends LocalBreakpoint(id, version) {

  var current: Long = 0

  override def accept(tuple: Tuple): Unit = {
    current += 1
  }

  override def isTriggered: Boolean = current == target

  override def isDirty: Boolean = isReported

  override def reset(): Unit = {
    super.reset()
    current = 0
  }
}
