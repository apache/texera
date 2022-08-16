package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.StartPipelinedRegionHandler.StartPipelinedRegion
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

import scala.collection.mutable

object StartPipelinedRegionHandler {
  final case class StartPipelinedRegion(region: PipelinedRegion, firstRegion: Boolean)
      extends ControlCommand[Unit]
}

/** start the source workers of a region
  *
  * possible sender: controller
  */
trait StartPipelinedRegionHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: StartPipelinedRegion, sender) =>
    {
      if (!scheduler.runningRegions.contains(msg.region)) {
        Future
          .collect(
            workflow
              .getAllWorkersForOperators(scheduler.getSourcesOfRegion(msg.region))
              .map(worker =>
                send(StartWorker(), worker).map(ret =>
                  // update worker state
                  workflow.getWorkerInfo(worker).state = ret
                )
              )
          )
          .map(_ => {
            scheduler.runningRegions.add(msg.region)
            if (msg.firstRegion) {
              enableStatusUpdate()
              enableMonitoring()
              enableSkewHandling()
            }
          })
      } else {
        Future.Unit
      }
    }
  }
}
