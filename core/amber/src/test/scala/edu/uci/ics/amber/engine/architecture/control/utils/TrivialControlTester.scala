package edu.uci.ics.amber.engine.architecture.control.utils

import akka.actor.ActorRef
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputPort
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, VirtualIdentity}
import edu.uci.ics.amber.error.ErrorUtils.safely
import edu.uci.ics.amber.error.WorkflowRuntimeError

class TrivialControlTester(id: ActorVirtualIdentity, parentNetworkCommunicationActorRef: ActorRef)
    extends WorkflowActor(id, parentNetworkCommunicationActorRef) {

  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayloadWithTryCatch)
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[TesterAsyncRPCHandlerInitializer]

  override def receive: Receive = {
    disallowActorRefRelatedMessages orElse {
      case NetworkMessage(
            id,
            internalMessage @ WorkflowControlMessage(from, sequenceNumber, payload)
          ) =>
        logger.logInfo(s"received ${internalMessage}")
        this.controlInputPort.handleMessage(
          this.sender(),
          id,
          from,
          sequenceNumber,
          payload
        )
      case other =>
        logger.logInfo(s"unhandled message: $other")
    }
  }

  def handleControlPayloadWithTryCatch(
      from: VirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    try {
      controlPayload match {
        // use control input port to pass control messages
        case invocation: ControlInvocation =>
          assert(from.isInstanceOf[ActorVirtualIdentity])
          asyncRPCServer.logControlInvocation(invocation, from)
          asyncRPCServer.receive(invocation, from.asInstanceOf[ActorVirtualIdentity])
        case ret: ReturnPayload =>
          asyncRPCClient.logControlReply(ret, from)
          asyncRPCClient.fulfillPromise(ret)
        case other =>
          logger.logError(
            WorkflowRuntimeError(
              s"unhandled control message: $other",
              "ControlInputPort",
              Map.empty
            )
          )
      }
    } catch safely {
      case e =>
        logger.logError(WorkflowRuntimeError(e, identifier.toString))
    }
  }
}
