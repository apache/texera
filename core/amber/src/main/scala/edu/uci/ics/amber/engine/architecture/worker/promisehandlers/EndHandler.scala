package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

trait EndHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def endWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    dp.processEndOfInputChannel()
    EmptyReturn()
  }
}
