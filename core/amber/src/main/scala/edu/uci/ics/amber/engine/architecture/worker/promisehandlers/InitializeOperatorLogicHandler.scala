package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  OpExecInitInfo,
  OpExecInitInfoWithCode,
  OpExecInitInfoWithFunc
}
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.operators.udf.java.JavaRuntimeCompilation

object InitializeOperatorLogicHandler {
  final case class InitializeOperatorLogic(
      totalWorkerCount: Int,
      opExecInitInfo: OpExecInitInfo,
      isSource: Boolean
  ) extends ControlCommand[Unit]
}

trait InitializeOperatorLogicHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: InitializeOperatorLogic, sender) =>
    {
      dp.operator = msg.opExecInitInfo match {
        case OpExecInitInfoWithCode(codeGen) =>
          val (code, _) =
            codeGen(VirtualIdentityUtils.getWorkerIndex(actorId), msg.totalWorkerCount)
          val compiledClass = JavaRuntimeCompilation.compileCode(code)
          val constructor = compiledClass.getDeclaredConstructor()
          val instance = constructor.newInstance()
          instance.asInstanceOf[OperatorExecutor]

        case OpExecInitInfoWithFunc(opGen) =>
          opGen(VirtualIdentityUtils.getWorkerIndex(actorId), msg.totalWorkerCount)
      }
    }
  }

}
