package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import java.util.concurrent.CompletableFuture

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

object ShutdownDPThreadHandler {
  final case class ShutdownDPThread() extends ControlCommand[CommandCompleted]
}

trait ShutdownDPThreadHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ShutdownDPThread, sender) =>
    {
      dataProcessor.shutdown()
      new CompletableFuture[Void]().get // wait here to be interrupted
      CommandCompleted() // this will actually never be called
    }
  }

}
