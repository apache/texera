package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

import scala.collection.mutable

object StartWorkflowHandler {
  final case class StartWorkflow() extends ControlCommand[Unit]
}

/** start the workflow by starting the source workers
  * note that this SHOULD only be called once per workflow
  *
  * possible sender: client
  */
trait StartWorkflowHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StartWorkflow, sender) =>
    {
      val region = scheduler.getNextRegionToConstructAndPrepare()
      if (region != null) {
        scheduler.constructAndPrepare(region)
      } else {
        throw new WorkflowRuntimeException(s"No region to be scheduled.")
      }
    }
  }
}
