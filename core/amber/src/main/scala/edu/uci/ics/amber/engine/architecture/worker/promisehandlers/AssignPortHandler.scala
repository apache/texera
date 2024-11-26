package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AssignPortRequest,
  AsyncRPCContext
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

trait AssignPortHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def assignPort(msg: AssignPortRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val schema = Schema.fromRawSchema(msg.schema)
    if (msg.input) {
      dp.inputManager.addPort(msg.portId, schema)
    } else {
      dp.outputManager.addPort(msg.portId, schema)
    }
    EmptyReturn()
  }

}
