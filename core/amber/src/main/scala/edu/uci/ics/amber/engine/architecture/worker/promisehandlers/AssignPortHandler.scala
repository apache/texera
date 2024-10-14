package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AssignPortRequest, AsyncRPCContext}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

trait AssignPortHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def assignPort(msg: AssignPortRequest, ctx: AsyncRPCContext): Future[Empty] = {
    val schema = Schema.fromRawSchema(msg.schema)
    if (msg.input) {
      dp.inputManager.addPort(msg.portId, schema)
    } else {
      dp.outputManager.addPort(msg.portId, schema)
    }
    Empty()
  }

}
