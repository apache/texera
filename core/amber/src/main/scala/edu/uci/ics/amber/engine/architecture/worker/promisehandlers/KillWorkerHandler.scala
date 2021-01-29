package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import akka.actor.PoisonPill
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.KillWorkerHandler.KillWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}


object KillWorkerHandler{
  final case class KillWorker() extends ControlCommand[CommandCompleted]
}


trait KillWorkerHandler {
  this:WorkerAsyncRPCHandlerInitializer =>

  registerHandler{
    (msg:KillWorker, sender) =>
      dataProcessor.shutdown()
      actorContext.self ! PoisonPill
      CommandCompleted()
  }

}
