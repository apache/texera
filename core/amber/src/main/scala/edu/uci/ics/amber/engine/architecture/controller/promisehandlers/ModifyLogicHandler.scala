package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.UpdatePythonExecutorHandler.UpdatePythonExecutor
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateExecutorHandler.UpdateExecutor
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.error.ErrorUtils.mkConsoleMessage
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

object ModifyLogicHandler {

  final case class ModifyLogic(newOp: PhysicalOp, stateTransferFunc: Option[StateTransferFunc])
      extends ControlCommand[Unit]
}

/** retry the execution of the entire workflow
  *
  * possible sender: controller, client
  */
trait ModifyLogicHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler[ModifyLogic, Unit] { (msg, sender) =>
    {
      val operator = cp.workflowScheduler.physicalPlan.getOperator(msg.newOp.id)
      val opExecution = cp.workflowExecution.getRunningRegionExecutions
        .map(_.getOperatorExecution(msg.newOp.id))
        .head
      val workerCommand = if (operator.isPythonBased) {
        UpdatePythonExecutor(
          msg.newOp.getPythonCode,
          isSource = operator.isSourceOperator
        )
      } else {
        UpdateExecutor(msg.newOp, msg.stateTransferFunc)
      }
      Future
        .collect(opExecution.getWorkerIds.map { worker =>
          send(workerCommand, worker).onFailure((err: Throwable) => {
            logger.error("Failure when performing reconfiguration", err)
            // report error to frontend
            val errorEvt = ConsoleMessageTriggered(mkConsoleMessage(actorId, err))
            sendToClient(errorEvt)
          })
        }.toSeq)
        .unit
    }
  }
}
