package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelID,
  ControlPayload,
  DataPayload,
  WorkflowFIFOMessage,
  WorkflowFIFOMessagePayload
}

import java.util.concurrent.atomic.AtomicLong
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import scala.collection.mutable

/**
  * NetworkOutput for generating sequence number when sending payloads
  * @param actorId ActorVirtualIdentity for the sender
  * @param handler actual sending logic
  */
class NetworkOutputGateway(
    val actorId: ActorVirtualIdentity,
    val handler: WorkflowFIFOMessage => Unit
) extends AmberLogging
    with Serializable {
  private val idToSequenceNums = new mutable.HashMap[ChannelID, AtomicLong]()

  def addOutputChannel(channel: ChannelID): Unit = {
    if (!idToSequenceNums.contains(channel)) {
      idToSequenceNums(channel) = new AtomicLong()
    }
  }

  private def sendToInternal(
      to: ActorVirtualIdentity,
      useControlChannel: Boolean,
      payload: WorkflowFIFOMessagePayload
  ): Unit = {
    var receiverId = to
    if (to == SELF) {
      // selfID and VirtualIdentity.SELF should be one key
      receiverId = actorId
    }
    val outChannelEndpointID = ChannelID(actorId, receiverId, useControlChannel)
    val seqNum = getSequenceNumber(outChannelEndpointID)
    handler(WorkflowFIFOMessage(outChannelEndpointID, seqNum, payload))
  }

  def sendTo(to: ActorVirtualIdentity, payload: ControlPayload): Unit = {
    sendToInternal(to, useControlChannel = true, payload)
  }

  def sendTo(to: ActorVirtualIdentity, payload: DataPayload): Unit = {
    sendToInternal(to, useControlChannel = false, payload)
  }

  def sendTo(channelId: ChannelID, payload: WorkflowFIFOMessagePayload): Unit = {
    val destChannelId = if (channelId.to == SELF) {
      // selfID and VirtualIdentity.SELF should be one key
      ChannelID(channelId.from, actorId, channelId.isControl)
    } else {
      channelId
    }
    val seqNum = getSequenceNumber(destChannelId)
    handler(WorkflowFIFOMessage(destChannelId, seqNum, payload))
  }

  def getFIFOState: Map[ChannelID, Long] = idToSequenceNums.map(x => (x._1, x._2.get())).toMap

  def getActiveChannels: Iterable[ChannelID] = idToSequenceNums.keys

  def getSequenceNumber(channel: ChannelID): Long = {
    idToSequenceNums.getOrElseUpdate(channel, new AtomicLong()).getAndIncrement()
  }

}
