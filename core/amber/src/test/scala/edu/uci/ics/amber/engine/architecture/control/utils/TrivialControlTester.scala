package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.{AmberProcessor, WorkflowActor}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelID,
  ControlPayload,
  DataPayload,
  WorkflowFIFOMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class TrivialControlTester(
    id: ActorVirtualIdentity
) extends WorkflowActor(logStorageType = "none", id) {
  val ap = new AmberProcessor(id, transferService.send)
  val initializer =
    new TesterAsyncRPCHandlerInitializer(ap.actorId, ap.asyncRPCClient, ap.asyncRPCServer)

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    val channel = ap.inputGateway.getChannel(workflowMsg.channel)
    channel.acceptMessage(workflowMsg)
    while (channel.isEnabled && channel.hasMessage) {
      val msg = channel.take
      msg.payload match {
        case payload: ControlPayload => ap.processControlPayload(msg.channel, payload)
        case _: DataPayload          => ???
        case _                       => ???
      }
    }
    sender ! NetworkAck(id, getInMemSize(workflowMsg), getQueuedCredit(workflowMsg.channel))
  }

  /** flow-control */
  override def getQueuedCredit(channelID: ChannelID): Long = 0L

  override def preStart(): Unit = {
    transferService.initialize()
  }

  override def handleBackpressure(isBackpressured: Boolean): Unit = {}

  override def initState(): Unit = {}
}
