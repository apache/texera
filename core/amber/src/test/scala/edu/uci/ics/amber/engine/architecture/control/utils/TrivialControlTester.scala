package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.{AmberProcessor, WorkflowActor}
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelID,
  ControlPayload,
  DataPayload,
  WorkflowFIFOMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class TrivialControlTester(
    id: ActorVirtualIdentity
) extends WorkflowActor(id) {
  val ap = new AmberProcessor(id, x => { transferService.send(x) })
  val initializer =
    new TesterAsyncRPCHandlerInitializer(ap.actorId, ap.asyncRPCClient, ap.asyncRPCServer)

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    val channel = ap.inputGateway.getChannel(workflowMsg.channel)
    channel.acceptMessage(workflowMsg)
    while (channel.isEnabled && channel.hasMessage) {
      val msg = channel.take
      msg.payload match {
        case payload: ControlPayload => ap.processControlPayload(msg.channel, payload)
        case payload: DataPayload    => ???
        case _                       => ???
      }
    }
    sender ! NetworkAck(id)
  }

  /** flow-control */
  override def getSenderCredits(channelID: ChannelID): Int = ???

  override def initState(): Unit = {}

  override def handleBackpressure(isBackpressured: Boolean): Unit = {}
}
