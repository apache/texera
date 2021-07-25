package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

import java.util.concurrent.CompletableFuture

object ShutdownDPThreadHandler {
  final case class ShutdownDPThread() extends ControlCommand[Unit]
}

trait ShutdownDPThreadHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ShutdownDPThread, sender) =>
    {
      dataProcessor.shutdown()
      new CompletableFuture[Void]().get // wait here to be interrupted
      () // return unit. this will actually never be called
    }
  }

}
