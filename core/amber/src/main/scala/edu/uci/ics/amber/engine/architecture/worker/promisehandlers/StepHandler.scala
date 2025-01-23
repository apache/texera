package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.logreplay.EmptyReplayLogManagerImpl
import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, DebuggerPause}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StepHandler.{ContinueProcessing, StopProcessing}
import edu.uci.ics.amber.engine.common.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object StepHandler {
  final case class ContinueProcessing(stepSize:Option[Int] = None) extends ControlCommand[Unit]
  final case class StopProcessing() extends ControlCommand[Unit]
}

trait StepHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: ContinueProcessing, sender) =>
    logger.info(s"received $msg")
    dp.pauseManager.debuggingMode = true
    dp.inputGateway.getAllChannels.foreach(c => c.enable(true))
    if(msg.stepSize.isEmpty){
      dp.pauseManager.resume(DebuggerPause)
    }else{
      if (dp.executor.isInstanceOf[SourceOperatorExecutor]){
        val logManager = new EmptyReplayLogManagerImpl(dp.outputHandler)
        (0 until msg.stepSize.get).foreach(_ => dp.outputOneTuple(logManager))
        dp.outputManager.flush()
      }
    }
  }

  registerHandler { (msg: StopProcessing, sender) =>
    logger.info(s"received $msg")
    val logManager = new EmptyReplayLogManagerImpl(dp.outputHandler)
    if(!dp.executor.isInstanceOf[SourceOperatorExecutor]){
      while(dp.outputManager.hasUnfinishedOutput){
        dp.outputOneTuple(logManager)
      }
      dp.outputManager.flush()
    }
    dp.pauseManager.pause(DebuggerPause)
    dp.pauseManager.debuggingMode = false
  }

}
