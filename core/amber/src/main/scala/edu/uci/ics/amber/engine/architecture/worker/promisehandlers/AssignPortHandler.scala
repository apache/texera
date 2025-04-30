package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AddInputChannelRequest,
  AssignPortRequest,
  AsyncRPCContext
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, READY, RUNNING}
import edu.uci.ics.amber.util.VirtualIdentityUtils.getFromActorIdForInputPortStorage

import java.net.URI

trait AssignPortHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def assignPort(msg: AssignPortRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val schema = Schema.fromRawSchema(msg.schema)
    if (msg.input) {
      val inputPortURIStrs = msg.storageUris.toList
      val inputPortURIs = inputPortURIStrs.map(uriStr => URI.create(uriStr))
      val partitionings = msg.partitionings.toList
      dp.inputManager.addPort(msg.portId, schema, inputPortURIs, partitionings)
      inputPortURIStrs.foreach { uriStr =>
        val toActorId = ctx.receiver
        val fromActorId = getFromActorIdForInputPortStorage(uriStr, toActorId)
        val channelId =
          ChannelIdentity(fromWorkerId = fromActorId, toWorkerId = toActorId, isControl = false)
        // Same as AddInputChannelHandler
        dp.inputGateway.getChannel(channelId).setPortId(msg.portId)
        dp.inputManager.getPort(msg.portId).channels(channelId) = false
        dp.stateManager.assertState(READY, RUNNING, PAUSED)
      }
    } else {
      val storageURIOption: Option[URI] = msg.storageUris.head match {
        case ""        => None
        case uriString => Some(URI.create(uriString))
      }
      dp.outputManager.addPort(msg.portId, schema, storageURIOption)
    }
    EmptyReturn()
  }

}
