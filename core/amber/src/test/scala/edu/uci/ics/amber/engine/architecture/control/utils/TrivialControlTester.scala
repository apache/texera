package edu.uci.ics.amber.engine.architecture.control.utils

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkAck
import edu.uci.ics.amber.engine.architecture.common.{AmberProcessor, WorkflowActor}
import edu.uci.ics.amber.engine.architecture.control.utils.TrivialControlTester.ControlTesterRPCClient
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.testerservice.RPCTesterFs2Grpc
import edu.uci.ics.amber.engine.common.CheckpointState
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  WorkflowFIFOMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

object TrivialControlTester {
  class ControlTesterRPCClient(outputGateway: NetworkOutputGateway, actorId: ActorVirtualIdentity)
      extends AsyncRPCClient(outputGateway, actorId) {
    val getProxy: RPCTesterFs2Grpc[Future, AsyncRPCContext] =
      AsyncRPCClient
        .createProxy[RPCTesterFs2Grpc[Future, AsyncRPCContext]](createPromise, outputGateway)
  }
}

class TrivialControlTester(
    id: ActorVirtualIdentity
) extends WorkflowActor(replayLogConfOpt = None, actorId = id) {
  val ap = new AmberProcessor(
    id,
    {
      case Left(value)  => ???
      case Right(value) => transferService.send(value)
    }
  ) {
    override val asyncRPCClient = new ControlTesterRPCClient(outputGateway, id)
  }
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

  override def loadFromCheckpoint(chkpt: CheckpointState): Unit = {}
}
