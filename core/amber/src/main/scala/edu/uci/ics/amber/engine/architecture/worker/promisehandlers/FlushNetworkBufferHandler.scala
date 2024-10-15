package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer

trait FlushNetworkBufferHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def flushNetworkBuffer(request: Empty, ctx: AsyncRPCContext): Future[Empty] = {
    dp.outputManager.flush()
    Empty()
  }

}
