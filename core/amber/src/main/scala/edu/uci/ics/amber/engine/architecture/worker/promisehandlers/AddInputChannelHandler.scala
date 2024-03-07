package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddInputChannelHandler.AddInputChannel
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, READY, RUNNING}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

object AddInputChannelHandler {

  final case class AddInputChannel(
      channelId: ChannelIdentity,
      portId: PortIdentity
  ) extends ControlCommand[Unit]
}

trait AddInputChannelHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: AddInputChannel, sender) =>
    dp.stateManager.assertState(READY, RUNNING, PAUSED)
    dp.inputGateway.getChannel(msg.channelId).setPortId(msg.portId)
    dp.inputManager.getPort(msg.portId).channels(msg.channelId) = false
  }

}
