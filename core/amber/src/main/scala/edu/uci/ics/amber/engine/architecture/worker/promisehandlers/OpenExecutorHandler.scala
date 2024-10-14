package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenExecutorHandler.OpenExecutor
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

trait OpenExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def openExecutor(request: Empty, ctx: AsyncRPCContext): Future[Empty] = {
    dp.executor.open()
    Empty()
  }

}
