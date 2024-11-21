package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

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
    channelId: ChannelIdentity,
    sequenceNumber: Long,
    payload: WorkflowFIFOMessagePayload
) extends WorkflowMessage

case class WorkflowRecoveryMessage(
    from: ActorVirtualIdentity,
    payload: RecoveryPayload
)
