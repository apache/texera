package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{ExecutionStateUpdate, ExecutionStatsUpdate, ReportCurrentProcessingTuple}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PauseHandler.PauseWorkflow
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ChannelMarkerHandler.PropagateChannelMarker
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryCurrentInputTupleHandler.QueryCurrentInputTuple
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.common.ambermessage.RequireAlignment
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelMarkerIdentity}
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import java.util.UUID
import scala.collection.mutable

object PauseHandler {

  final case class PauseWorkflow() extends ControlCommand[Unit]
}

/** pause the entire workflow
  *
  * possible sender: client, controller
  */
trait PauseHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[PauseWorkflow, Unit] { (msg, sender) =>
    {
      cp.controllerTimerService.disableStatusUpdate() // to be enabled in resume

      execute(
        PropagateChannelMarker(
          cp.workflowExecution.getAllRegionExecutions
            .flatMap(_.getAllOperatorExecutions.map(_._1))
            .toSet,
          ChannelMarkerIdentity(s"pause-${UUID.randomUUID()}"),
          RequireAlignment,
          cp.workflowScheduler.physicalPlan,
          cp.workflowScheduler.physicalPlan.operators.map(_.id),
          PauseWorker()
        ),
        sender
      ).flatMap { ret =>
        execute(ControllerInitiateQueryStatistics(), CONTROLLER)
      }.map{
        ret =>
          sendToClient(ExecutionStateUpdate(cp.workflowExecution.getState))
          logger.info(s"workflow paused")
      }
    }
  }
}
