package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object ShutdownDPThreadHandler {
  final case class ShutdownDPThread() extends ControlCommand[Unit]
}

trait ShutdownDPThreadHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ShutdownDPThread, sender) =>
    {
      dataProcessor.shutdown()
      throw new InterruptedException() // actively interrupt itself
      ()
    }
  }

}
