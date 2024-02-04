package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

object AssignPortHandler {

  final case class AssignPort(
      portId: PortIdentity,
      input: Boolean
  ) extends ControlCommand[Unit]
}

trait AssignPortHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: AssignPort, sender) =>
    if (msg.input) {
      dp.inputGateway.addPort(msg.portId)
    } else {
      dp.outputGateway.addPort(msg.portId)
    }
  }

}
