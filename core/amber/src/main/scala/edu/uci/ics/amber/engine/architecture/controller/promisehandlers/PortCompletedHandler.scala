package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PortCompletedHandler.PortCompleted
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.scheduling.GlobalPortIdentity
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

import scala.collection.Seq

object PortCompletedHandler {
  final case class PortCompleted(portId: PortIdentity, input: Boolean) extends ControlCommand[Unit]
}

/** Notify the completion of a port:
  * - For input port, it means the worker has finished consuming and processing all the data
  *   through this port, including all possible links to this port.
  * - For output port, it means the worker has finished sending all the data through this port.
  *
  * possible sender: worker
  */
trait PortCompletedHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[PortCompleted, Unit] { (msg, sender) =>
    {
      val statsRequest =
        execute(ControllerInitiateQueryStatistics(Option(List(sender))), CONTROLLER)

      Future
        .collect(Seq(statsRequest))
        .flatMap { _ =>
          val globalPortId = GlobalPortIdentity(
            VirtualIdentityUtils.getPhysicalOpId(sender),
            msg.portId,
            input = msg.input
          )
          cp.workflowExecutionCoordinator.getRegionOfPortId(globalPortId) match {
            case Some(region) =>
              val regionExecution = cp.workflowExecution.getRegionExecution(region.id)
              val operatorExecution =
                regionExecution.getOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(sender))
              val workerExecution = operatorExecution.getWorkerExecution(sender)

              // set the port on this worker to be completed
              (if (msg.input) workerExecution.getInputPortExecution(msg.portId)
               else workerExecution.getOutputPortExecution(msg.portId)).setCompleted()

              // check if the port on this operator is completed
              val isPortCompleted =
                if (msg.input) operatorExecution.isInputPortCompleted(msg.portId)
                else operatorExecution.isOutputPortCompleted(msg.portId)

              if (isPortCompleted) {
                cp.workflowExecutionCoordinator.executeNextRegions(cp.actorService)
              } else {
                // if the port is not completed yet, do nothing
                Future(())
              }
            case None => // currently "start" and "end" ports are not part of a region, thus no region can be found.
            // do nothing.
          }
        }

    }
  }

}
