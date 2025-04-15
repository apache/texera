package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AddInputChannelRequest, AssignPortRequest, AsyncRPCContext}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.util.VirtualIdentityUtils.getFromActorIdForInputPortStorage

import java.net.URI

trait AssignPortHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def assignPort(msg: AssignPortRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val schema = Schema.fromRawSchema(msg.schema)
    val futures = if (msg.input) {
      val inputPortURIStrs = msg.storageUris.toList
      val inputPortURIs = inputPortURIStrs.map(uriStr => URI.create(uriStr))
      dp.inputManager.addPort(msg.portId, schema, inputPortURIs)
      inputPortURIStrs.map{
        uriStr =>
          val fromActorId = getFromActorIdForInputPortStorage(uriStr)
          val toActorId = ctx.receiver
          val channelId = ChannelIdentity(fromWorkerId = fromActorId, toWorkerId = toActorId, isControl = false)
        workerInterface.addInputChannel(
          AddInputChannelRequest(channelId = channelId, portId = msg.portId),
          mkContext(ctx.receiver)
        )
      }
    } else {
      val storageURIOption: Option[URI] = msg.storageUris.head match {
        case ""        => None
        case uriString => Some(URI.create(uriString))
      }
      dp.outputManager.addPort(msg.portId, schema, storageURIOption)
      List.empty
    }
    Future.collect(futures).map { _ =>
      // returns when all has completed
      EmptyReturn()
    }
  }

}
