package edu.uci.ics.amber.engine.common.amberexception

class WorkflowRuntimeException(message: String)
    extends RuntimeException(message)
    with Serializable {

  def this(message: String, cause: Throwable) {
    this(message)
    initCause(cause)
  }

  def this(cause: Throwable) {
    this(Option(cause).map(_.toString).orNull, cause)
  }

  def this() {
    this(null: String)
  }

}
