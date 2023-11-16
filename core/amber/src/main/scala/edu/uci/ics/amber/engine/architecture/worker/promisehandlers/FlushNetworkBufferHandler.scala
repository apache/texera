package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer}

trait FlushNetworkBufferHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (flush: FlushNetworkBuffer, sender) =>
    dp.outputManager.flushAll()
  }
}
