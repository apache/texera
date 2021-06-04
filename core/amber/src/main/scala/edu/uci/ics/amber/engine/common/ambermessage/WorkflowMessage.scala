package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity

sealed trait WorkflowMessage extends Serializable {
  val from: VirtualIdentity
  val sequenceNumber: Long
}

case class WorkflowControlMessage(
    from: VirtualIdentity,
    sequenceNumber: Long,
    payload: ControlPayload
) extends WorkflowMessage

case class WorkflowDataMessage(
    from: VirtualIdentity,
    sequenceNumber: Long,
    payload: DataPayload
) extends WorkflowMessage
