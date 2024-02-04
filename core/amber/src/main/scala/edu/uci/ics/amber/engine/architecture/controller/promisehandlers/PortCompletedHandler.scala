package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.PortCompletedHandler.PortCompleted
import edu.uci.ics.amber.engine.architecture.scheduling.GlobalPortIdentity
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

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
      val globalPortId = GlobalPortIdentity(
        VirtualIdentityUtils.getPhysicalOpId(sender),
        msg.portId,
        input = msg.input
      )

      val operatorExecution =
        cp.executionState.getOperatorExecution(VirtualIdentityUtils.getPhysicalOpId(sender))
      val workerExecution = operatorExecution.getWorkerExecution(sender)
      if (msg.input) {
        workerExecution.getInputPortExecution(msg.portId).setCompleted()
      } else {
        workerExecution.getOutputPortExecution(msg.portId).setCompleted()
      }

      if (
        operatorExecution.isInputPortCompleted(msg.portId) || operatorExecution
          .isOutputPortCompleted(msg.portId)
      ) {
        cp.workflowScheduler
          .onPortCompletion(cp.workflow, cp.actorService, globalPortId)
          .flatMap(_ => Future.Unit)
      } else {
        // if the link is not completed yet, do nothing
        Future(())
      }
    }
  }

}
