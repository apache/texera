package edu.uci.ics.amber.core

import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

class WorkflowRuntimeException(
    val message: String,
    val relatedWorkerId: Option[ActorVirtualIdentity] = None
) extends RuntimeException(message)
    with Serializable {

  def this(message: String, cause: Throwable, relatedWorkerId: Option[ActorVirtualIdentity]) = {
    this(message, relatedWorkerId)
    initCause(cause)
  }

  def this(cause: Throwable, relatedWorkerId: Option[ActorVirtualIdentity]) = {
    this(Option(cause).map(_.toString).orNull, cause, relatedWorkerId)
  }

  def this(cause: Throwable) = {
    this(Option(cause).map(_.toString).orNull, cause, None)
  }

  def this() = {
    this(null: String)
  }

  override def toString: String = message

}
