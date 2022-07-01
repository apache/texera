package edu.uci.ics.amber.engine.architecture.control.utils

import akka.actor.ActorRef
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkInputPort
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.ambermessage.{ControlPayload, WorkflowControlMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.error.ErrorUtils.safely

class TrivialControlTester(id: ActorVirtualIdentity, parentNetworkCommunicationActorRef: ActorRef)
    extends WorkflowActor(id, parentNetworkCommunicationActorRef) {

  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](id, this.handleControlPayloadWithTryCatch)
  override val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[TesterAsyncRPCHandlerInitializer]

  override def receive: Receive = {
    disallowActorRefRelatedMessages orElse {
      case NetworkMessage(
            id,
            internalMessage @ WorkflowControlMessage(from, sequenceNumber, payload)
          ) =>
        logger.info(s"received $internalMessage")
        this.controlInputPort.handleMessage(
          this.sender(),
          Constants.unprocessedBatchesCreditLimitPerSender,
          id,
          from,
          sequenceNumber,
          payload
        )
      case other =>
        logger.info(s"unhandled message: $other")
    }
  }

  def handleControlPayloadWithTryCatch(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    try {
      controlPayload match {
        // use control input port to pass control messages
        case invocation: ControlInvocation =>
          assert(from.isInstanceOf[ActorVirtualIdentity])
          asyncRPCServer.logControlInvocation(invocation, from)
          asyncRPCServer.receive(invocation, from)
        case ret: ReturnInvocation =>
          asyncRPCClient.logControlReply(ret, from)
          asyncRPCClient.fulfillPromise(ret)
        case other =>
          logger.error(s"unhandled control message: $other")
      }
    } catch safely {
      case e =>
        logger.error("", e)
    }

  }
}
