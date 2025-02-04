package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.engine.architecture.logreplay.EmptyReplayLogManagerImpl
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, ContinueProcessingRequest, StopProcessingRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, DebuggerPause}

trait StepHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def continueProcessing(request: ContinueProcessingRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    logger.info(s"received $request")
    dp.pauseManager.debuggingMode = true
    dp.inputGateway.getAllChannels.foreach(c => c.enable(true))
    if(request.stepSize == -1){
      dp.pauseManager.resume(DebuggerPause)
    }else{
      if (dp.executor.isInstanceOf[SourceOperatorExecutor]){
        val logManager = new EmptyReplayLogManagerImpl(dp.outputHandler)
        (0 until request.stepSize.toInt).foreach(_ => dp.outputOneTuple(logManager))
      }else{
        dp.pauseManager.resume(DebuggerPause)
      }
      dp.outputManager.flush()
    }
    EmptyReturn()
  }

  override def stopProcessing(request: StopProcessingRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    logger.info(s"received $request")
    val logManager = new EmptyReplayLogManagerImpl(dp.outputHandler)
    if(!dp.executor.isInstanceOf[SourceOperatorExecutor]){
      while(dp.outputManager.hasUnfinishedOutput){
        dp.outputOneTuple(logManager)
      }
      dp.outputManager.flush()
    }
    dp.pauseManager.pause(DebuggerPause)
    dp.pauseManager.debuggingMode = false
    EmptyReturn()
  }


}
