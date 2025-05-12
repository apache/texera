package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EndInputChannelRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def endWorker(request: EndInputChannelRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    logger.info("Ending the worker.")
    val channelId = if (dp.executor.isInstanceOf[SourceOperatorExecutor]) {
      ChannelIdentity(ActorVirtualIdentity("SOURCE_STARTER"), actorId, isControl = false)
    } else {

      println("refrwfwr", request.channelId)
      println("refrwfwrwefwefw", ctx)
      request.channelId
    }
    dp.endOfInputChannel(channelId)
    EmptyReturn()
  }
}
