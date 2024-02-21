package edu.uci.ics.amber.engine.architecture.control.utils

import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.{AmberProcessor, WorkflowActor}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  WorkflowFIFOMessage
}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

class TrivialControlTester(
    id: ActorVirtualIdentity
) extends WorkflowActor(replayLogConfOpt = None, actorId = id) {
  val ap = new AmberProcessor(
    id,
    {
      case Left(value)  => ???
      case Right(value) => transferService.send(value)
    }
  )
  val initializer =
    new TesterAsyncRPCHandlerInitializer(ap.actorId, ap.asyncRPCClient, ap.asyncRPCServer)

  override def handleInputMessage(id: Long, workflowMsg: WorkflowFIFOMessage): Unit = {
    val channel = ap.inputGateway.getChannel(workflowMsg.channelId)
    channel.acceptMessage(workflowMsg)
    while (channel.isEnabled && channel.hasMessage) {
      val msg = channel.take
      msg.payload match {
        case payload: ControlPayload => ap.processControlPayload(msg.channelId, payload)
        case _: DataPayload          => ???
        case _                       => ???
      }
    }
    sender() ! NetworkAck(id, getInMemSize(workflowMsg), getQueuedCredit(workflowMsg.channelId))
  }

  /** flow-control */
  override def getQueuedCredit(channelId: ChannelIdentity): Long = 0L

  override def preStart(): Unit = {
    transferService.initialize()
  }

  override def handleBackpressure(isBackpressured: Boolean): Unit = {}

  override def initState(): Unit = {}
}
