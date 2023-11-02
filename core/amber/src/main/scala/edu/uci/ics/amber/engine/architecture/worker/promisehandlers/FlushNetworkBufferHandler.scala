package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer

trait FlushNetworkBufferHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (flush: FlushNetworkBuffer, sender) =>
    outputManager.flushAll()
  }
}
