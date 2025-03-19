package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EndInputChannelRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def endWorker(
      request: EndInputChannelRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    var channelId = request.channelId
    if (dp.executor.isInstanceOf[SourceOperatorExecutor]) {
      channelId = ChannelIdentity(ActorVirtualIdentity("SOURCE_STARTER"), actorId, isControl = false)
      // for source operator: add a virtual input channel just for kicking off the execution
      val dummyInputPortId = PortIdentity()
      dp.inputManager.addPort(dummyInputPortId, null)
      dp.inputGateway.getChannel(channelId).setPortId(dummyInputPortId)
    }
    dp.endOfInputChannel(channelId)
    EmptyReturn()
  }
}
