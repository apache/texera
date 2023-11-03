package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

import java.util.concurrent.CompletableFuture

object ShutdownDPThreadHandler {
  final case class ShutdownDPThread() extends ControlCommand[Unit] {
    @transient
    val completed = new CompletableFuture[Unit]()
  }
}

trait ShutdownDPThreadHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ShutdownDPThread, sender) =>
    {
      dataProcessor.logManager.terminate()
      msg.completed.complete(())
      dataProcessor.shutdown()
      throw new InterruptedException() // actively interrupt itself
      ()
    }
  }

}
