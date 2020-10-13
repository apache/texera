package engine.architecture.breakpoint.localbreakpoint
import engine.common.tuple.Tuple

class ExceptionBreakpoint()(implicit id: String, version: Long)
    extends LocalBreakpoint(id, version) {
  var error: Exception = _
  override def accept(tuple: Tuple): Unit = {
    //empty
  }

  override def isTriggered: Boolean = triggeredTuple != null

  override def isDirty: Boolean = isTriggered

  override def reset(): Unit = {
    super.reset()
    error = null
  }
}
