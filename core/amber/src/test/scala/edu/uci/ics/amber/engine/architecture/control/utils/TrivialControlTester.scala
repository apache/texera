package edu.uci.ics.amber.engine.architecture.control.utils

import akka.actor.ActorRef
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class TrivialControlTester(id: ActorVirtualIdentity, parentNetworkCommunicationActorRef: ActorRef)
    extends WorkflowActor(id, parentNetworkCommunicationActorRef) {
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[TesterAsyncRPCHandlerInitializer]

  override def receive: Receive = {
    disallowActorRefRelatedMessages orElse {
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
