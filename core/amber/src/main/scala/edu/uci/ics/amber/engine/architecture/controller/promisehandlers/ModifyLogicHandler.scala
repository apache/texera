package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.ModifyPythonLogicHandler.ModifyPythonLogic
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpDescV2

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
      val code = msg.operatorDescriptor.asInstanceOf[PythonUDFOpDescV2].code
      Future
        .collect(workflow.getPythonWorkers.map { worker =>
          send(ModifyPythonLogic(code), worker)
        }.toSeq)
        .map { _ => }
    }
  }
}
