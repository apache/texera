package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AddInputChannelRequest,
  AsyncRPCContext
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, READY, RUNNING}

trait AddInputChannelHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def addInputChannel(
      msg: AddInputChannelRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    dp.inputGateway.getChannel(msg.channelId).setPortId(msg.portId)
    dp.inputManager.getPort(msg.portId).channels(msg.channelId) = false
    dp.stateManager.assertState(READY, RUNNING, PAUSED)
    EmptyReturn()
  }

}
