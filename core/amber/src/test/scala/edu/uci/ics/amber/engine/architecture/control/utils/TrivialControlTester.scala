package edu.uci.ics.amber.engine.architecture.control.utils

import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage
}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer

class TrivialControlTester(id: ActorVirtualIdentity) extends WorkflowActor(id) {
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[TesterAsyncRPCHandlerInitializer]

  override def receive: Receive = {
    routeActorRefRelatedMessages orElse {
      case msg @ NetworkMessage(id, cmd: WorkflowControlMessage) =>
        logger.logInfo(s"received ${msg.internalMessage}")
        sender ! NetworkAck(id)
        // use promise manager to handle control messages
        controlInputPort.handleControlMessage(cmd)
      case other =>
        logger.logInfo(s"unhandled message: $other")
    }
  }
}
