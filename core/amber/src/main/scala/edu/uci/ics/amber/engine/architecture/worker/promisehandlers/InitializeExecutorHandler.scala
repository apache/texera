package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo.generateJavaOpExec
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeExecutorHandler.InitializeExecutor
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

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
      dp.serializationManager.setOpInitialization(msg)
      dp.executor = generateJavaOpExec(
        msg.opExecInitInfo,
        VirtualIdentityUtils.getWorkerIndex(actorId),
        msg.totalWorkerCount
      )
    }

  }
}
