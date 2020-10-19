package edu.uci.ics.amber.engine.architecture.breakpoint.localbreakpoint

import edu.uci.ics.amber.engine.common.tuple.ITuple

abstract class LocalBreakpoint(val id: String, val version: Long) extends Serializable {

  def accept(tuple: ITuple)

  def isTriggered: Boolean

  def needUserFix: Boolean = isTriggered

  var isReported = false

  var triggeredTuple: ITuple = _

  def isDirty: Boolean

  var isInput = false

  var triggeredTupleId: Long = -1

  def reset(): Unit = {
    isReported = false
    triggeredTuple = null
    isInput = false
    triggeredTupleId = -1
  }
}
