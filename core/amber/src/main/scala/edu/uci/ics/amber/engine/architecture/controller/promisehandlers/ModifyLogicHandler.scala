package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.BreakpointTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.ModifyOperatorLogicHandler.ModifyOperatorLogic
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpDescV2
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.source.PythonUDFSourceOpDescV2

import scala.collection.mutable

object ModifyLogicHandler {

  final case class ModifyLogic(operatorDescriptor: OperatorDescriptor) extends ControlCommand[Unit]
}

/** retry the execution of the entire workflow
  *
  * possible sender: controller, client
  */
trait ModifyLogicHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: ModifyLogic, sender) =>
    {
      val operatorUUID = msg.operatorDescriptor.operatorID
      val operatorId = new OperatorIdentity(msg.operatorDescriptor.context.jobId, operatorUUID)
      val operator = workflow.getOperator(operatorId)
      val modifyOperatorLogic: ModifyOperatorLogic = msg.operatorDescriptor match {
        case desc: PythonUDFOpDescV2 =>
          ModifyOperatorLogic(desc.code, isSource = false)
        case desc: PythonUDFSourceOpDescV2 =>
          ModifyOperatorLogic(desc.code, isSource = true)
        case desc =>
          logger.error(s"Unsupported operator for Modify Logic: $desc")
          null
      }
      Future
        .collect(operator.getAllWorkers.map { worker =>
          send(modifyOperatorLogic, worker).onFailure((err: Throwable) => {
            logger.error("Failure when sending Python UDF code", err)
            // report error to frontend
            sendToClient(
              BreakpointTriggered(
                mutable.HashMap(
                  (worker, FaultedTuple(null, 0)) -> Array(err.toString)
                ),
                operatorUUID
              )
            )
          })
        }.toSeq)
        .unit
    }
  }
}
