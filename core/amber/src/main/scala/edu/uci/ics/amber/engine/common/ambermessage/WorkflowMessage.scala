package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

case class ChannelID(
    from: ActorVirtualIdentity,
    to: ActorVirtualIdentity,
    isControl: Boolean
) {
  override def toString: String = {
    s"Channel(${from.name},${to.name},${if (isControl) "control" else "data"})"
  }
}

case object WorkflowMessage {
  def getInMemSize(msg: WorkflowMessage): Long = {
    msg match {
      case dataMsg: WorkflowFIFOMessage =>
        dataMsg.payload match {
          case df: DataFrame => df.inMemSize
          case _             => 200L
        }
      case _ => 200L
    }
  }
}

sealed trait WorkflowMessage extends Serializable

case class WorkflowFIFOMessage(
    channel: ChannelID,
    sequenceNumber: Long,
    payload: WorkflowFIFOMessagePayload
) extends WorkflowMessage

case class WorkflowRecoveryMessage(
    from: ActorVirtualIdentity,
    payload: RecoveryPayload
)
