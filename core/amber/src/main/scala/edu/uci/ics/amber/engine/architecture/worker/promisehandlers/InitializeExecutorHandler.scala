package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  OpExecInitInfo,
  OpExecInitInfoWithCode,
  OpExecInitInfoWithFunc
}
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeExecutorHandler.InitializeExecutor
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.operators.udf.java.JavaRuntimeCompilation

object InitializeExecutorHandler {
  final case class InitializeExecutor(
      totalWorkerCount: Int,
      opExecInitInfo: OpExecInitInfo,
      isSource: Boolean
  ) extends ControlCommand[Unit]
}

trait InitializeExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: InitializeExecutor, sender) =>
    {
      dp.executor = msg.opExecInitInfo match {
        case OpExecInitInfoWithCode(codeGen) =>
          val (code, _) =
            codeGen(VirtualIdentityUtils.getWorkerIndex(actorId), msg.totalWorkerCount)
          JavaRuntimeCompilation
            .compileCode(code)
            .getDeclaredConstructor()
            .newInstance()
            .asInstanceOf[OperatorExecutor]

        case OpExecInitInfoWithFunc(opGen) =>
          opGen(VirtualIdentityUtils.getWorkerIndex(actorId), msg.totalWorkerCount)
      }
    }
  }

}
