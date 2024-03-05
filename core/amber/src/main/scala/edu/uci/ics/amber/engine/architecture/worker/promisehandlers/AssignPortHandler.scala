package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

object AssignPortHandler {

  final case class AssignPort(
      portId: PortIdentity,
      input: Boolean,
      schema: Schema
  ) extends ControlCommand[Unit]
}

trait AssignPortHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: AssignPort, sender) =>
    if (msg.input) {
      dp.inputManager.addPort(msg.portId, msg.schema)
    } else {
      dp.outputManager.addPort(msg.portId, msg.schema)
    }
  }

}
