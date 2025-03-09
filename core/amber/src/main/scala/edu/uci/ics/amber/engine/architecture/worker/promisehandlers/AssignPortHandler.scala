package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AssignPortRequest,
  AsyncRPCContext
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

import java.net.URI

trait AssignPortHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def assignPort(msg: AssignPortRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    val schema = Schema.fromRawSchema(msg.schema)
    if (msg.input) {
      dp.inputManager.addPort(msg.portId, schema)
    } else {
      val storageURIOption: Option[URI] = msg.storageUri match {
        case ""        => None
        case uriString => Some(URI.create(uriString))
      }
      dp.outputManager.addPort(msg.portId, schema, storageURIOption)
    }
    EmptyReturn()
  }

}
