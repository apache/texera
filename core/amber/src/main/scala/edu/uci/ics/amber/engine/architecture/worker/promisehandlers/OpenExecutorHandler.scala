package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

trait OpenExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def openExecutor(request: EmptyRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    dp.executor.open()
    EmptyReturn()
  }

}
