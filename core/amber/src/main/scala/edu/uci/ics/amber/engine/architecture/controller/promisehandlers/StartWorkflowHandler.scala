package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerAsyncRPCHandlerInitializer,
  ControllerState
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartWorkflowHandler.StartWorkflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.{CommandCompleted, ControlCommand}

import scala.collection.mutable

object StartWorkflowHandler {
  final case class StartWorkflow() extends ControlCommand[CommandCompleted]
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
      val startedLayers = mutable.HashSet[WorkerLayer]()
      Future
        .collect(
          workflow.getSourceLayers
            // get all startable layers
            .filter(layer => layer.canStart)
            .flatMap { layer =>
              startedLayers.add(layer)
              layer.workers.keys.map { worker =>
                send(StartWorker(), worker).map { ret =>
                  // update worker state
                  workflow.getWorkerInfo(worker).state = ret
                }
              }
            }
            .toSeq
        )
        .map { ret =>
          actorContext.parent ! ControllerState.Running // for testing
          enableStatusUpdate()
          CommandCompleted()
        }
    }
  }
}
