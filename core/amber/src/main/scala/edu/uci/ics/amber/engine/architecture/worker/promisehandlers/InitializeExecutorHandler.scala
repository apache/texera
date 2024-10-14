package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo.generateJavaOpExec
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, InitializeExecutorRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils


trait InitializeExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def initializeExecutor(msg: InitializeExecutorRequest, ctx: AsyncRPCContext): Future[Empty] = {
    dp.serializationManager.setOpInitialization(msg)
    dp.executor = generateJavaOpExec(
      msg.opExecInitInfo,
      VirtualIdentityUtils.getWorkerIndex(actorId),
      msg.totalWorkerCount
    )
  }

}
