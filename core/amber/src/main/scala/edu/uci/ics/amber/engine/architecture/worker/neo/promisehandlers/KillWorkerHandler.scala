package edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers

import akka.actor.PoisonPill
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.KillWorkerHandler.KillWorker
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
