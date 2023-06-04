package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.ModifyPythonOperatorLogicHandler.ModifyPythonOperatorLogic
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ModifyOperatorLogicHandler.WorkerModifyLogic
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.source.PythonUDFSourceOpExecV2

import scala.collection.mutable

object ModifyLogicHandler {

  final case class ModifyLogic(newOp: OpExecConfig, stateTransferFunc: Option[StateTransferFunc])
      extends ControlCommand[Unit]
}

/** retry the execution of the entire workflow
  *
  * possible sender: controller, client
  */
trait ModifyLogicHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ModifyLogic, sender) =>
    {
      val operator = workflow.physicalPlan.operatorMap(msg.newOp.id)

      val workerCommand = if (operator.isPythonOperator) {
        ModifyPythonOperatorLogic(
          msg.newOp.getPythonCode,
          isSource = operator.opExecClass.isAssignableFrom(classOf[PythonUDFSourceOpExecV2])
        )
      } else {
        WorkerModifyLogic(msg.newOp, msg.stateTransferFunc)
      }

      Future
        .collect(operator.getAllWorkers.map { worker =>
          send(workerCommand, worker).onFailure((err: Throwable) => {
            logger.error("Failure when performing reconfiguration", err)
            // report error to frontend
            val bpEvt = BreakpointTriggered(
              mutable.HashMap((worker, FaultedTuple(null, 0)) -> Array(err.toString)),
              operator.id.operator
            )
            sendToClient(bpEvt)
          })
        }.toSeq)
        .unit
    }
  }
}
