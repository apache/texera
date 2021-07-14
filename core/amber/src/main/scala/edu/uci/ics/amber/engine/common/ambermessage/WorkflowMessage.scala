package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

sealed trait WorkflowMessage extends Serializable {
  val from: ActorVirtualIdentity
  val sequenceNumber: Long
}

case class WorkflowControlMessage(
    from: ActorVirtualIdentity,
    sequenceNumber: Long,
    payload: ControlPayload
) extends WorkflowMessage

case class WorkflowDataMessage(
    from: ActorVirtualIdentity,
    sequenceNumber: Long,
    payload: DataPayload
) extends WorkflowMessage
