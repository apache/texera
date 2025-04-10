package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.executor._
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  InitializeExecutorRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.operator.source.cache.CacheSourceOpExec
import edu.uci.ics.amber.util.VirtualIdentityUtils

import java.net.URI

trait InitializeExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def initializeExecutor(
      req: InitializeExecutorRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    dp.serializationManager.setOpInitialization(req)
    val workerIdx = VirtualIdentityUtils.getWorkerIndex(actorId)
    val workerCount = req.totalWorkerCount
    dp.executor = req.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        ExecFactory.newExecFromJavaClassName(className, descString, workerIdx, workerCount)
      case OpExecWithCode(code, _) => ExecFactory.newExecFromJavaCode(code)
      case OpExecSource(storageUri, _) =>
        new CacheSourceOpExec(URI.create(storageUri))
    }
    EmptyReturn()
  }

}
