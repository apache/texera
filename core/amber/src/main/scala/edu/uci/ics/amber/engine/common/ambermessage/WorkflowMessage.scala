package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object WorkflowMessageType extends Enumeration {
  type messageType = Value
  val CONTROL_MESSAGE: WorkflowMessageType.Value = Value("Control Message")
  val DATA_MESSAGE: WorkflowMessageType.Value = Value("Data Message")
}

sealed trait WorkflowMessage extends Serializable {
  val from: ActorVirtualIdentity
  val sequenceNumber: Long
  val msgType: WorkflowMessageType.Value
}

case class WorkflowControlMessage(
    from: ActorVirtualIdentity,
    sequenceNumber: Long,
    payload: ControlPayload,
    msgType: WorkflowMessageType.Value = WorkflowMessageType.CONTROL_MESSAGE
) extends WorkflowMessage

case class WorkflowDataMessage(
    from: ActorVirtualIdentity,
    sequenceNumber: Long,
    payload: DataPayload,
    msgType: WorkflowMessageType.Value = WorkflowMessageType.DATA_MESSAGE
) extends WorkflowMessage

// sent from network communicator to next worker to poll for credit information
case class CreditRequest(
    from: ActorVirtualIdentity,
    sequenceNumber: Long = -1,
    msgType: WorkflowMessageType.Value = WorkflowMessageType.CONTROL_MESSAGE
) extends WorkflowMessage
